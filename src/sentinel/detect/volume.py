"""Volume anomalies: a job's row count is far from what its own history says is normal.

Method: robust z-score against a baseline.
  baseline = this metric's values on the same weekday over the last `history_days`
             ("same_weekday"), or on the last `recent_days` dates ("recent"; per
             pipeline in config)
  expected = median(baseline)
  sigma    = max(1.4826 * MAD(baseline), mad_floor_fraction * expected)
  z        = (value - expected) / sigma
  anomaly  = |z| >= z_threshold AND |value / expected - 1| >= min_relative_change

Why median/MAD rather than mean/stddev: history contains past incidents (a 70% drop
day, a duplicate load). One such day drags a mean and inflates a stddev enough to hide
the *next* anomaly. The median and MAD barely move. 1.4826 * MAD estimates the standard
deviation for normal data, so the z threshold reads like a normal z.

Why same weekday: order volume has a weekly cycle (Saturday is about 1.3x Tuesday).
Pooled across all days, the cycle inflates the MAD and drags the median. Measured on the
52 Saturdays of 2026 from the generator, a *normal* Saturday pooled scores up to z = 3.8
(2 of 52 over 3.5: false positives; same-weekday max is 3.4), and a real 20% Saturday
drop lands near the all-days median (z ~ 0.3) and is missed. Comparing Saturdays with
Saturdays removes the cycle without fitting a model: the normal days pass and the 20%
drop is caught.

Why "recent" for some metrics: a cumulative snapshot (customers: every customer ever,
+25 a day) has no weekly cycle but a trend. A 56-day same-weekday median lags it by ~4
weeks of growth. While the table is small that is a big relative move: on the generator
(flat at 5,000 until 2026-01-01, then growing) it crossed both thresholds, +15%, on 5
clean days in February. The median of the last 7 days lags only ~4 days (< 2%).

Why the relative-change floor: same-weekday history is tight (5-8 points), so the
z-score alone is noisy. Over a year of clean generator data, 9 normal orders/payments
days scored |z| 3.5-4.3, all with |change| under 12%. Both conditions must hold, so
none of them is flagged; a 15% move is also roughly the smallest one worth paging on.

Cold start (fewer than `min_baseline_points` baseline points):
  - some history: flag only if |change vs the median of all history| >= 50%. That is
    wider than the weekly swing, so a normal day passes but a halved feed doesn't.
  - no history at all: flag only an empty output (0 rows), the one value that is wrong
    without context.

History = the latest *successful* run's value for each earlier ds (never the ds being
judged), so re-runs and backfills don't count twice.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from sentinel.detect.config import DetectionConfig, VolumeConfig, VolumeMetric
from sentinel.detect.findings import Finding, IncidentType
from sentinel.detect.observations import JobRun, Observations

MAD_TO_SIGMA = 1.4826


@dataclass(frozen=True)
class Assessment:
    anomalous: bool
    method: str  # robust_z_same_weekday | robust_z_recent | cold_start_ratio | no_history
    value: float
    expected: float | None
    change: float | None  # value / expected - 1
    z: float | None
    sigma: float | None
    points: int  # history points used

    @property
    def direction(self) -> str:
        return "drop" if (self.change or 0) < 0 or self.value == 0 else "spike"


def _baseline(
    target: date, history: list[tuple[date, float]], cfg: VolumeConfig, kind: str
) -> list[float]:
    if kind == "recent":
        lo = target - timedelta(days=cfg.recent_days)
        return [v for d, v in history if d >= lo]
    return [v for d, v in history if d.weekday() == target.weekday()]


def assess(
    value: float,
    target: date,
    history: list[tuple[date, float]],
    cfg: VolumeConfig,
    baseline: str = "same_weekday",
) -> Assessment:
    base = _baseline(target, history, cfg, baseline)
    if len(base) >= cfg.min_baseline_points:
        med = statistics.median(base)
        mad = statistics.median(abs(v - med) for v in base)
        sigma = max(MAD_TO_SIGMA * mad, cfg.mad_floor_fraction * med)
        change = _change(value, med)
        z = (value - med) / sigma if sigma > 0 else (0.0 if value == med else float("inf"))
        anomalous = abs(z) >= cfg.z_threshold and abs(change) >= cfg.min_relative_change
        method = f"robust_z_{baseline}"
        return Assessment(anomalous, method, value, med, change, z, sigma, len(base))
    values = [v for _, v in history]
    if values:
        med = statistics.median(values)
        change = _change(value, med)
        anomalous = abs(change) >= cfg.cold_start_min_change
        method = "cold_start_ratio"
        return Assessment(anomalous, method, value, med, change, None, None, len(values))
    return Assessment(value == 0, "no_history", value, None, None, None, None, 0)


def _change(value: float, expected: float) -> float:
    if expected == 0:
        return 0.0 if value == 0 else float("inf")
    return value / expected - 1


def _finite(x: float | None) -> float | None:
    return x if x is not None and math.isfinite(x) else None


def _value(run: JobRun, metric: VolumeMetric) -> int | None:
    return run.rows_in if metric.field == "rows_in" else run.rows_out


def detect(obs: Observations, cfg: DetectionConfig) -> list[Finding]:
    findings: list[Finding] = []
    for p in cfg.pipelines.values():
        for metric in p.volume_metrics:
            runs = [
                j
                for j in obs.jobs
                if j.pipeline == p.name
                and j.job_name == metric.job_name
                and j.status == "success"
                and _value(j, metric) is not None
            ]
            findings.extend(_detect_metric(p.name, metric, p.volume_baseline, runs, cfg))
    return findings


def _detect_metric(
    pipeline: str, metric: VolumeMetric, baseline: str, runs: list[JobRun], cfg: DetectionConfig
) -> list[Finding]:
    latest_per_ds: dict[str, float] = {}
    for r in runs:  # time-ordered, so the last write per ds wins
        latest_per_ds[r.ds] = float(_value(r, metric))

    by_ds: dict[str, list[tuple[JobRun, Assessment]]] = defaultdict(list)
    for r in runs:
        target = date.fromisoformat(r.ds)
        lo = target - timedelta(days=cfg.history_days)
        history = [
            (d, v)
            for ds, v in latest_per_ds.items()
            if lo <= (d := date.fromisoformat(ds)) < target
        ]
        a = assess(float(_value(r, metric)), target, history, cfg.volume, baseline)
        by_ds[r.ds].append((r, a))

    findings: list[Finding] = []
    for ds, seq in by_ds.items():
        bad = [i for i, (_, a) in enumerate(seq) if a.anomalous]
        if not bad:
            continue
        run, a = seq[bad[-1]]
        normal_after = [r for r, aa in seq[bad[-1] + 1 :] if not aa.anomalous]
        high = a.value == 0 or (
            a.change is not None and abs(a.change) >= cfg.volume.high_severity_change
        )
        change = _finite(a.change)
        pct = f"{change:+.0%}" if change is not None else "n/a"
        findings.append(
            Finding(
                type=IncidentType.VOLUME_ANOMALY,
                pipeline=pipeline,
                ds=ds,
                fingerprint=f"volume:{metric.label}:{a.direction}",
                severity="high" if high else "medium",
                title=(
                    f"{metric.label} {a.direction} for ds={ds}: {a.value:.0f} rows vs "
                    f"expected ~{a.expected:.0f} ({pct}, {a.method})"
                    if a.expected is not None
                    else f"{metric.label} is empty for ds={ds} (no history yet)"
                ),
                first_observed=seq[bad[0]][0].at,
                last_observed=run.at,
                cleared_at=normal_after[0].at if normal_after else None,
                task_id=run.task_id,
                evidence={
                    "metric": metric.label,
                    "value": a.value,
                    "expected": a.expected,
                    "change_pct": round(change * 100, 1) if change is not None else None,
                    "z": round(z, 2) if (z := _finite(a.z)) is not None else None,
                    "sigma": round(a.sigma, 1) if a.sigma is not None else None,
                    "method": a.method,
                    "history_points": a.points,
                    "weekday": date.fromisoformat(ds).strftime("%A"),
                    "thresholds": {
                        "z": cfg.volume.z_threshold,
                        "min_relative_change": cfg.volume.min_relative_change,
                        "cold_start_min_change": cfg.volume.cold_start_min_change,
                    },
                    "run_id": run.run_id,
                },
            )
        )
    return findings
