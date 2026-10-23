from __future__ import annotations

from datetime import timedelta

import pytest

from sentinel.detect import from_events, late_data, load_config
from tests.unit.detect_fixtures import Run, end_of

DS = "2026-10-20"
T0 = end_of(DS)
CFG = load_config()


def _dq(observed, minutes: int = 0, try_number: int = 1) -> list:
    r = Run("customers_dim", DS, T0 + timedelta(minutes=minutes), try_number)
    r.dq("validate_customers", "customers", 12000, ("updated_at", observed, True))
    return r.events


def fresh(obs, now=T0):
    return late_data.detect_freshness(from_events(obs), CFG)


@pytest.mark.parametrize(
    ("hours_behind", "severity"), [(23.9, None), (30, "medium"), (47.9, "medium"), (48, "high")]
)
def test_freshness_sla_and_severity(hours_behind, severity) -> None:
    observed = str(T0 - timedelta(hours=hours_behind))
    findings = fresh(_dq(observed))
    if severity is None:
        assert findings == []
    else:
        [f] = findings
        assert f.severity == severity
        assert f.evidence["lag_hours"] == pytest.approx(hours_behind)
        assert f.fingerprint == "freshness:customers.updated_at"


def test_detector_recomputes_lag_even_when_gx_passed() -> None:
    """GX and the detector share the SLA, but the detector re-derives it from the
    observed value; a GX pass with stale data (e.g. a looser suite) is still caught."""
    [f] = fresh(_dq(str(T0 - timedelta(days=3))))
    assert f.evidence["gx_success"] is True


def test_fresh_rerun_clears_staleness() -> None:
    events = _dq(str(T0 - timedelta(days=3))) + _dq(str(T0 - timedelta(minutes=5)), 60, 2)
    [f] = fresh(events)
    assert not f.active


@pytest.mark.parametrize("observed", [None, "not a timestamp", "NaT"])
def test_unparseable_observed_value_is_skipped(observed) -> None:
    assert fresh(_dq(observed)) == []


def test_timezone_naive_value_is_read_as_utc() -> None:
    [f] = fresh(_dq((T0 - timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")))
    assert f.evidence["lag_hours"] == 48


def test_pipelines_without_a_freshness_rule_or_outcome_are_skipped() -> None:
    r = Run("daily_revenue_agg", DS, T0)
    r.dq("validate_daily_revenue", "daily_revenue", 18)
    s = Run("customers_dim", DS, T0)
    s.dq("validate_customers", "customers", 12000)  # no freshness outcome recorded
    assert fresh(r.events + s.events) == []


# --- arrival SLA (disabled in the shipped config; enabled here) --------------------------

SLA = load_config(overrides={"pipelines": {"orders_ingest": {"arrival_sla_hours": 6}}})


def arrival(events, now):
    return late_data.detect_arrival(from_events(events), SLA, now)


def _finish(at_hours: float) -> list:
    r = Run("orders_ingest", DS, T0 + timedelta(hours=at_hours))
    r.task("validate_orders")
    return r.events


def test_no_run_by_the_deadline_is_active_and_high() -> None:
    findings = arrival([], T0 + timedelta(hours=7))
    [f] = [f for f in findings if f.ds == DS]
    assert (f.severity, f.active, f.fingerprint) == ("high", True, "arrival:validate_orders")


def test_finished_late_is_a_cleared_medium_finding() -> None:
    [f] = [f for f in arrival(_finish(8), T0 + timedelta(hours=9)) if f.ds == DS]
    assert (f.severity, f.active) == ("medium", False)
    assert f.evidence["late_by_hours"] == pytest.approx(2, abs=0.01)


def test_on_time_or_not_yet_due_raises_nothing() -> None:
    assert [f for f in arrival(_finish(1), T0 + timedelta(hours=9)) if f.ds == DS] == []
    assert [f for f in arrival([], T0 + timedelta(hours=5)) if f.ds == DS] == []


def test_arrival_only_looks_back_the_configured_days() -> None:
    now = T0 + timedelta(days=10, hours=12)  # both look-back days are past due
    assert {f.ds for f in arrival([], now)} == {
        (now.date() - timedelta(days=d)).isoformat() for d in (1, 2)
    }
