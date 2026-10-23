"""Whole-detector scenarios: clean replay (no false positives) and the Phase 1 faults
(each must produce exactly one primary incident)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from sentinel.detect import detect, from_events, load_config, run_detectors
from tests.unit.detect_fixtures import day_events, end_of, history

DS = "2026-10-20"  # a Tuesday
CFG = load_config()


def _now(ds: str = DS):
    return end_of(ds) + timedelta(hours=6)


@pytest.fixture(scope="module")
def five_weeks() -> list:
    return history(DS, 35)


def primaries(incidents):
    return [i for i in incidents if i.role == "primary"]


# --- No false positives ---------------------------------------------------------------


def test_replay_of_24h_clean_data_after_five_weeks_raises_nothing(five_weeks) -> None:
    obs = from_events(five_weeks + day_events(DS))
    assert run_detectors(obs, CFG, _now()) == []


def test_every_day_of_five_clean_weeks_raises_nothing(five_weeks) -> None:
    """Each day is judged against the history before it, including the cold-start days
    at the beginning: 36 days x 4 pipelines, every detector, zero findings."""
    obs = from_events(five_weeks + day_events(DS))
    assert {j.ds for j in obs.jobs} == {
        (date.fromisoformat(DS) - timedelta(days=i)).isoformat() for i in range(36)
    }
    assert detect(obs, CFG, _now()) == []


def test_7_day_clean_replay_detecting_once_a_day_raises_nothing(five_weeks) -> None:
    """Seven clean business dates replayed after five weeks of history, with detection
    run after each day lands (as `make detect` would be): zero findings, zero incidents
    at every step. Covers every weekday once."""
    events = list(five_weeks)
    start = date.fromisoformat(DS)
    for i in range(7):
        ds = (start + timedelta(days=i)).isoformat()
        events += day_events(ds)
        obs = from_events(events)
        assert run_detectors(obs, CFG, _now(ds)) == [], ds
        assert detect(obs, CFG, _now(ds)) == [], ds


def test_24h_of_clean_data_with_no_history_raises_nothing() -> None:
    """Cold start: the very first day the detector ever sees."""
    assert run_detectors(from_events(day_events(DS)), CFG, _now()) == []


def test_clean_replay_with_arrival_sla_enabled_raises_nothing(five_weeks) -> None:
    sla = {"pipelines": {p: {"arrival_sla_hours": 6} for p in CFG.pipelines}}
    cfg = load_config(overrides=sla)
    obs = from_events(five_weeks + day_events(DS))
    # Runs start 30 min after the end of ds and take minutes, well inside a 6 h SLA.
    assert run_detectors(obs, cfg, end_of(DS) + timedelta(hours=7)) == []


# --- The Phase 1 failure demos: exactly one primary each --------------------------------


@pytest.mark.parametrize(
    ("fault", "pipeline", "itype", "downstream_task"),
    [
        ("missing_amount", "orders_ingest", "schema_change", "wait_for_orders_ingest"),
        ("stale_customers", "customers_dim", "late_data", "wait_for_customers_dim"),
        ("volume_drop", "orders_ingest", "volume_anomaly", "aggregate_revenue"),
    ],
)
def test_each_fault_creates_exactly_one_primary(
    five_weeks, fault, pipeline, itype, downstream_task
) -> None:
    incidents = detect(from_events(five_weeks + day_events(DS, fault=fault)), CFG, _now())
    [primary] = primaries(incidents)
    assert (primary.pipeline, primary.type, primary.status) == (pipeline, itype, "open")
    assert primary.evidence["affected_ds"] == [DS]

    # The only other incident is daily_revenue_agg, linked to the primary as downstream.
    [down] = [i for i in incidents if i.role == "downstream"]
    assert down.pipeline == "daily_revenue_agg"
    assert down.root_incident_id == primary.incident_id
    signal_tasks = {s["task_id"] for s in down.evidence["occurrences"][0]["signals"]}
    assert downstream_task in signal_tasks


def test_missing_amount_is_one_incident_with_the_failure_as_a_symptom(five_weeks) -> None:
    incidents = detect(
        from_events(five_weeks + day_events(DS, fault="missing_amount")), CFG, _now()
    )
    [primary] = primaries(incidents)
    signals = primary.evidence["occurrences"][0]["signals"]
    assert [(s["type"], s["task_id"]) for s in signals] == [
        ("schema_change", "ingest_orders"),
        ("failed_job", "ingest_orders"),
    ]
    assert signals[0]["evidence"]["removed"] == ["amount"]
    assert primary.severity == "high"


def test_stale_customers_reports_how_late(five_weeks) -> None:
    incidents = detect(
        from_events(five_weeks + day_events(DS, fault="stale_customers")), CFG, _now()
    )
    [primary] = primaries(incidents)
    head = primary.evidence["occurrences"][0]["signals"][0]["evidence"]
    assert head["column"] == "updated_at"
    assert head["lag_hours"] == pytest.approx(48.08, abs=0.01)
    assert head["sla_hours"] == 24
    assert primary.severity == "high"  # >= 2 x SLA


def test_volume_drop_uses_same_weekday_history(five_weeks) -> None:
    incidents = detect(from_events(five_weeks + day_events(DS, fault="volume_drop")), CFG, _now())
    [primary] = primaries(incidents)
    ev = primary.evidence["occurrences"][0]["signals"][0]["evidence"]
    assert ev["method"] == "robust_z_same_weekday"
    assert ev["history_points"] == 5  # five earlier Tuesdays
    assert ev["change_pct"] == pytest.approx(-70, abs=1)


def test_volume_drop_is_still_caught_at_cold_start() -> None:
    """Only two days of history: the cold-start rule (>= 50% change) still fires."""
    events = history(DS, 2) + day_events(DS, fault="volume_drop")
    [primary] = primaries(detect(from_events(events), CFG, _now()))
    ev = primary.evidence["occurrences"][0]["signals"][0]["evidence"]
    assert (primary.type, ev["method"], ev["history_points"]) == (
        "volume_anomaly",
        "cold_start_ratio",
        2,
    )


# --- Lifecycle --------------------------------------------------------------------------


@pytest.mark.parametrize("fault", ["missing_amount", "stale_customers", "volume_drop"])
def test_fixing_and_rerunning_resolves_everything(five_weeks, fault) -> None:
    broken = day_events(DS, fault=fault)
    fixed = day_events(DS, start=end_of(DS) + timedelta(hours=2), try_number=2)
    incidents = detect(from_events(five_weeks + broken + fixed), CFG, _now())
    assert len(primaries(incidents)) == 1
    assert {i.status for i in incidents} == {"resolved"}


def test_same_fault_on_consecutive_days_is_one_incident(five_weeks) -> None:
    nxt = (date.fromisoformat(DS) + timedelta(days=1)).isoformat()
    events = (
        five_weeks
        + day_events(DS, fault="missing_amount")
        + day_events(nxt, fault="missing_amount")
    )
    incidents = detect(from_events(events), CFG, _now(nxt))
    [primary] = primaries(incidents)
    assert primary.evidence["affected_ds"] == [DS, nxt]
    assert primary.evidence["occurrence_count"] == 2
    [down] = [i for i in incidents if i.role == "downstream"]
    assert down.evidence["affected_ds"] == [DS, nxt]


def test_detection_is_deterministic(five_weeks) -> None:
    events = five_weeks + day_events(DS, fault="missing_amount")
    a = detect(from_events(events), CFG, _now())
    b = detect(from_events(list(reversed(events))), CFG, _now())
    assert a == b


# --- Recurrence: the benchmark injects the same fault more than once ---------------------


@pytest.mark.parametrize("fault", ["missing_amount", "stale_customers", "volume_drop"])
def test_fault_fixed_then_injected_again_is_a_second_numbered_incident(
    five_weeks, fault, tmp_path
) -> None:
    """Inject on DS, fix it, inject the same fault on DS+1: two primaries, the second
    `#2`, each with its own downstream, and both kept as separate rows in the store."""
    from sentinel.detect.store import read_incidents, upsert

    nxt = (date.fromisoformat(DS) + timedelta(days=1)).isoformat()
    fixed = day_events(DS, fault=None, start=end_of(DS) + timedelta(hours=2), try_number=2)
    path = tmp_path / "incidents"

    first = detect(from_events(five_weeks + day_events(DS, fault=fault)), CFG, _now())
    upsert(path, first, _now())
    events = five_weeks + day_events(DS, fault=fault) + fixed + day_events(nxt, fault=fault)
    second = detect(from_events(events), CFG, _now(nxt))
    upsert(path, second, _now(nxt))

    p1, p2 = primaries(second)
    assert p2.incident_id == p1.incident_id + "#2"
    assert (p1.status, p2.status) == ("resolved", "open")
    assert (p1.evidence["affected_ds"], p2.evidence["affected_ds"]) == ([DS], [nxt])
    downs = {i.root_incident_id: i for i in second if i.role == "downstream"}
    assert set(downs) == {p1.incident_id, p2.incident_id}

    rows = {r["incident_id"]: r["status"] for r in read_incidents(path)}
    assert rows[p1.incident_id] == "resolved" and rows[p2.incident_id] == "open"
    assert len(rows) == 4
