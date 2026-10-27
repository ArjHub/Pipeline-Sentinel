"""Detection metrics over the built scenario lakes (no API calls, nothing tuned).

The harness ran one detection pass per scenario, `detect_after_s` after the night's last
task, so the incidents table it left says *whether* a fault was caught, not *when*. To
measure when, this replays the live detector's schedule: one pass every
SENTINEL_DETECT_INTERVAL_S (60 s) from the night's start, each pass seeing only the bronze
events emitted up to that moment, with the unchanged detection config. The first pass that
raises a primary incident on the fault's pipeline for its business date is the detection
time. The lander's own delay (a 2 s consumer poll) is not modelled.

Raw findings vs. incidents: the per-signal findings the four detectors emit in the final
pass, and the primary incidents correlation turns them into.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from benchmark.harness import scenario
from benchmark.harness.catalog import Fault
from benchmark.harness.settings import Settings
from sentinel.detect import correlate, run_detectors
from sentinel.detect.config import load_config
from sentinel.detect.observations import Observations, load_bronze

POLL_S = 60


def _upto(obs: Observations, t: datetime) -> Observations:
    return Observations(
        attempts=[a for a in obs.attempts if a.at <= t],
        jobs=[j for j in obs.jobs if j.at <= t],
        dq=[d for d in obs.dq if d.at <= t],
    )


def _hits(incidents, fault: Fault) -> list:
    return [
        i
        for i in incidents
        if i.role == "primary"
        and i.pipeline == fault.recipe.expect_pipeline
        and fault.ds in i.evidence["affected_ds"]
    ]


def _on_ds(incidents, ds: str) -> list:
    return [i for i in incidents if ds in i.evidence["affected_ds"]]


def replay(fault: Fault, settings: Settings) -> dict[str, Any]:
    cfg = load_config()
    m = scenario.manifest(settings, fault.fault_id)
    if m is None:
        raise RuntimeError(f"{fault.fault_id} is not built")
    lake = scenario.lake_path(settings, fault.fault_id)
    start = datetime.fromisoformat(m["night_start"])
    end = datetime.fromisoformat(m["detect_now"])
    obs = load_bronze(lake / "bronze", (end - timedelta(days=cfg.history_days)).date())

    findings = run_detectors(obs, cfg, end)
    final = correlate(findings, cfg)
    night_findings = [f for f in findings if f.ds == fault.ds]
    primaries = [i for i in final if i.role == "primary"]
    row: dict[str, Any] = {
        "fault_id": fault.fault_id,
        "template": fault.template,
        "category": fault.label.category,
        "ds": fault.ds,
        "expect_pipeline": fault.recipe.expect_pipeline,
        "night_start": start.isoformat(),
        "raw_findings": len(night_findings),
        "raw_findings_all_ds": len(findings),
        "incidents": len(_on_ds(final, fault.ds)),
        "primary_incidents": len(_on_ds(primaries, fault.ds)),
        "primary_incidents_other_ds": len(primaries) - len(_on_ds(primaries, fault.ds)),
        "finding_types": sorted({f.type.value for f in night_findings}),
        "primary_types": sorted({i.type for i in _on_ds(primaries, fault.ds)}),
    }
    if fault.is_clean:
        row |= {"detected": None, "false_positive_incidents": row["primary_incidents"]}
        return row

    hit = _hits(final, fault)
    row["detected"] = bool(hit)
    row["detected_types"] = sorted({i.type for i in hit})
    if not hit:
        return row
    t = start
    while t <= end + timedelta(seconds=POLL_S):
        if _hits(correlate(run_detectors(_upto(obs, t), cfg, t), cfg), fault):
            break
        t += timedelta(seconds=POLL_S)
    first_seen = min(i.first_seen for i in hit)
    row |= {
        "first_evidence_at": first_seen.isoformat(),
        "detected_at_poll": t.isoformat(),
        # fault onset = the night's first DAG run starting (the faulty extract, the late or
        # missing upstream, the deploy's first run and the OOM all happen inside it)
        "latency_from_night_start_s": round((t - start).total_seconds(), 1),
        "latency_from_first_evidence_s": round((t - first_seen).total_seconds(), 1),
    }
    return row


def run(faults: list[Fault], settings: Settings, out=print) -> list[dict[str, Any]]:
    rows = []
    for n, f in enumerate(faults, 1):
        r = replay(f, settings)
        rows.append(r)
        out(
            f"[{n}/{len(faults)}] {f.fault_id} detected={r['detected']} "
            f"findings={r['raw_findings']} primaries={r['primary_incidents']} "
            f"latency={r.get('latency_from_night_start_s')}"
        )
    return rows


def write(rows: list[dict[str, Any]], path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, default=str) + "\n")
