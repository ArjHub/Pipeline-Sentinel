"""Late data, two ways.

1. Freshness (always on for pipelines with a `freshness` rule). The GX suite records
   the observed max of the freshness column (e.g. customers.updated_at). The detector
   re-checks it against the per-pipeline SLA: lag = end of ds - observed max, and
   lag > max_staleness is late. It recomputes this instead of trusting GX's pass/fail,
   because it needs *how* late (for severity and for the diagnosis in Phase 4), and
   because the SLA lives in one config file.

2. Arrival (only if `arrival_sla_hours` is set). The pipeline's final task must succeed
   by end of ds + SLA. This catches data that never shows up, which produces no DQ
   result at all. It looks at the last `arrival_lookback_days` business dates relative
   to `now`. It is off by default locally (see config/detection.toml).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from sentinel.detect.config import DetectionConfig
from sentinel.detect.findings import Finding, IncidentType
from sentinel.detect.observations import DQRun, Observations

FRESHNESS_EXPECTATION = "expect_column_max_to_be_between"


def end_of_ds(ds: str) -> datetime:
    return datetime.combine(date.fromisoformat(ds), datetime.min.time(), UTC) + timedelta(days=1)


def _parse_ts(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        ts = pd.Timestamp(str(value))
    except (ValueError, TypeError):
        return None
    if ts is pd.NaT:
        return None
    return (ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")).to_pydatetime()


def _hours(td: timedelta) -> float:
    return round(td.total_seconds() / 3600, 2)


def detect_freshness(obs: Observations, cfg: DetectionConfig) -> list[Finding]:
    checks: dict[tuple[str, str], list[tuple[DQRun, datetime, timedelta, bool]]] = defaultdict(list)
    for run in obs.dq:
        p = cfg.pipelines.get(run.pipeline)
        if p is None or p.freshness is None:
            continue
        outcome = run.outcome(FRESHNESS_EXPECTATION, p.freshness.column)
        observed = _parse_ts(outcome.get("observed_value")) if outcome else None
        if observed is None:
            continue
        lag = end_of_ds(run.ds) - observed
        checks[(run.pipeline, run.ds)].append((run, observed, lag, lag > p.freshness.max_staleness))

    findings: list[Finding] = []
    for (pipeline, ds), seq in checks.items():
        stale = [i for i, c in enumerate(seq) if c[3]]
        if not stale:
            continue
        rule = cfg.pipelines[pipeline].freshness
        first, (run, observed, lag, _) = seq[stale[0]], seq[stale[-1]]
        fresh_after = [c[0] for c in seq[stale[-1] + 1 :] if not c[3]]
        high = lag >= rule.max_staleness * cfg.late_high_severity_multiple
        findings.append(
            Finding(
                type=IncidentType.LATE_DATA,
                pipeline=pipeline,
                ds=ds,
                fingerprint=f"freshness:{run.dataset}.{rule.column}",
                severity="high" if high else "medium",
                title=(
                    f"{run.dataset} is stale for ds={ds}: latest {rule.column} is "
                    f"{_hours(lag)} h before end of day (SLA {_hours(rule.max_staleness)} h)"
                ),
                first_observed=first[0].at,
                last_observed=run.at,
                cleared_at=fresh_after[0].at if fresh_after else None,
                task_id=run.task_id,
                evidence={
                    "check": "freshness",
                    "dataset": run.dataset,
                    "column": rule.column,
                    "observed_max": observed.isoformat(),
                    "expected_at_least": (end_of_ds(ds) - rule.max_staleness).isoformat(),
                    "lag_hours": _hours(lag),
                    "sla_hours": _hours(rule.max_staleness),
                    "gx_success": bool(run.success),
                    "run_id": run.run_id,
                },
            )
        )
    return findings


def detect_arrival(obs: Observations, cfg: DetectionConfig, now: datetime) -> list[Finding]:
    findings: list[Finding] = []
    for p in cfg.pipelines.values():
        if p.arrival_sla is None:
            continue
        for back in range(1, cfg.arrival_lookback_days + 1):
            ds = (now.date() - timedelta(days=back)).isoformat()
            deadline = end_of_ds(ds) + p.arrival_sla
            if now < deadline:
                continue
            done = [
                a.at
                for a in obs.attempts
                if (a.pipeline, a.ds, a.task_id, a.state) == (p.name, ds, p.final_task, "success")
            ]
            first_done = min(done) if done else None
            if first_done is not None and first_done <= deadline:
                continue
            late_by = (first_done or now) - deadline
            findings.append(
                Finding(
                    type=IncidentType.LATE_DATA,
                    pipeline=p.name,
                    ds=ds,
                    fingerprint=f"arrival:{p.final_task}",
                    severity="high" if first_done is None else "medium",
                    title=(
                        f"{p.name} ds={ds} not complete by its SLA "
                        f"(end of ds + {_hours(p.arrival_sla)} h)"
                        + ("" if first_done is None else f"; finished {_hours(late_by)} h late")
                    ),
                    first_observed=deadline,
                    last_observed=first_done or now,
                    cleared_at=first_done,
                    task_id=p.final_task,
                    evidence={
                        "check": "arrival",
                        "final_task": p.final_task,
                        "deadline": deadline.isoformat(),
                        "completed_at": first_done.isoformat() if first_done else None,
                        "late_by_hours": _hours(late_by),
                    },
                )
            )
    return findings


def detect(obs: Observations, cfg: DetectionConfig, now: datetime) -> list[Finding]:
    return detect_freshness(obs, cfg) + detect_arrival(obs, cfg, now)
