"""Synthetic history: cloned nights carry the right numbers, versions and timing, and
6 weeks of it put the volume detector on its real (robust z) path."""

from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta

from benchmark.harness import history
from sentinel.detect import load_config, run_detectors
from sentinel.detect.observations import from_events
from sentinel.detect.volume import assess
from sentinel.events.models import DeployEvent, DQResult, JobMetric, TaskRun
from tests.unit.detect_fixtures import day_events, end_of

TDS = "2026-05-11"
CFG = load_config()


def _template() -> history.Template:
    events = day_events(TDS)
    return history.Template(TDS, end_of(TDS) + timedelta(minutes=30), events)


def _history(fault_ds: str, days: int = 42, seed: int = 1):
    rng = random.Random(seed)
    t = _template()
    d = date.fromisoformat(fault_ds)
    start = history.night_start(fault_ds, 300, 40, rng)
    initial, deploys = history.deploy_history(
        rng, d - timedelta(days=days), start, (4, 12), (9, 21)
    )
    events = history.deploy_events(initial, deploys, rng)
    for i in range(days, 0, -1):
        ds = (d - timedelta(days=i)).isoformat()
        s = history.night_start(ds, 300, 40, rng)
        v = history.versions_at(initial, deploys, s)
        events += history.clone_night(t, ds, s, rng.uniform(0.92, 1.12), v, "abc123def456", rng)
    return events, initial, deploys


def test_clone_recomputes_counts_and_freshness_per_day() -> None:
    ds = "2026-07-04"
    f = history.facts(ds)
    events, _, _ = _history("2026-07-05", days=1)
    night = [e for e in events if e.logical_date and e.logical_date.startswith(ds)]
    assert night and all(e.run_id == f"scheduled__{ds}T00:00:00+00:00" for e in night)
    jm = {e.payload.job_name: e.payload for e in night if isinstance(e.payload, JobMetric)}
    assert jm["orders_ingest"].rows_in == f.orders
    assert jm["payments_glue_job"].rows_in == f.payments
    assert jm["customers_dim"].rows_in == f.customers
    assert jm["customers_dim"].rows_out == f.inserted + f.updated
    assert jm["daily_revenue_agg"].rows_out == f.groups
    dq = {e.payload.dataset: e.payload for e in night if isinstance(e.payload, DQResult)}
    observed = {
        r.observed_value
        for r in dq["orders"].results
        if r.expectation_type == "expect_column_max_to_be_between"
    }
    assert observed == {f.max_ts["orders"]}
    assert dq["orders"].row_count == f.orders


def test_night_runs_after_midnight_with_stretched_but_ordered_timing() -> None:
    events, _, _ = _history("2026-07-05", days=1)
    night = [e for e in events if e.logical_date]
    first = min(e.emitted_at for e in night)
    assert datetime(2026, 7, 5, 0, 5, tzinfo=UTC) <= first < datetime(2026, 7, 5, 0, 10, tzinfo=UTC)
    tr = [e.payload for e in night if isinstance(e.payload, TaskRun)]
    assert all(p.finished_at >= p.started_at and p.hostname == "abc123def456" for p in tr)


def test_code_versions_follow_the_deploy_history() -> None:
    events, initial, deploys = _history("2026-08-20")
    deploy_ev = [e for e in events if isinstance(e.payload, DeployEvent)]
    assert deploy_ev and all(e.payload.git_sha and e.payload.changed_files for e in deploy_ev)
    for e in events:
        if isinstance(e.payload, JobMetric):
            v = history.versions_at(initial, deploys, e.emitted_at - timedelta(minutes=10))
            assert e.code_version == v[e.pipeline]


def test_clean_history_has_no_findings_and_volume_uses_robust_z() -> None:
    fault_ds = "2026-08-20"
    events, _, _ = _history(fault_ds)
    now = end_of(fault_ds) + timedelta(hours=3)
    assert run_detectors(from_events(events), CFG, now) == []
    obs = from_events(events)
    hist = [
        (date.fromisoformat(j.ds), float(j.rows_in))
        for j in obs.jobs
        if j.job_name == "orders_ingest"
    ]
    f = history.facts(fault_ds)
    a = assess(f.orders, date.fromisoformat(fault_ds), hist, CFG.volume, "same_weekday")
    assert a.method == "robust_z_same_weekday" and a.points == 6 and not a.anomalous
    c = [
        (date.fromisoformat(j.ds), float(j.rows_in))
        for j in obs.jobs
        if j.job_name == "customers_dim"
    ]
    a = assess(f.customers, date.fromisoformat(fault_ds), c, CFG.volume, "recent")
    assert a.method == "robust_z_recent"


def test_facts_match_the_generators_by_construction() -> None:
    f = history.facts(TDS)
    assert f.inserted == 25 and f.updated > 0 and 10 <= f.groups <= 18
    assert f.max_ts["orders"].startswith(TDS) and f.max_ts["orders"].endswith("+00:00")
