from __future__ import annotations

from datetime import timedelta

from sentinel.detect import from_events, load_config, schema_drift
from sentinel.detect.schema_drift import diff
from tests.unit.detect_fixtures import ORDERS_SCHEMA, Run, end_of

CFG = load_config()


def _job(ds: str, schema, status: str = "success", minutes: int = 0, try_number: int = 1) -> list:
    r = Run("orders_ingest", ds, end_of(ds) + timedelta(minutes=minutes), try_number)
    r.job("ingest_orders", "orders_ingest", status=status, schema=schema, rows_in=2000)
    return r.events


def detect(events):
    return schema_drift.detect(from_events(events), CFG)


NO_AMOUNT = [c for c in ORDERS_SCHEMA if c[0] != "amount"]
WITH_DISCOUNT = [*ORDERS_SCHEMA, ("discount_code", "string")]
RETYPED = [(n, "string" if n == "amount" else t) for n, t in ORDERS_SCHEMA]


def test_diff_reports_removed_added_and_retyped_columns() -> None:
    d = diff(tuple(ORDERS_SCHEMA), (*NO_AMOUNT[:2], ("customer_id", "long"), ("x", "int")))
    assert d.removed == ("amount", "currency", "order_ts", "status")
    assert d.added == ("x",)
    assert d.retyped == (("customer_id", "int", "long"),)
    assert d.key.startswith("-amount")
    assert not diff(tuple(ORDERS_SCHEMA), tuple(ORDERS_SCHEMA))


def test_first_schema_ever_is_only_a_baseline() -> None:
    assert detect(_job("2026-10-19", NO_AMOUNT)) == []


def test_removed_column_is_high_severity_with_baseline_evidence() -> None:
    [f] = detect(_job("2026-10-19", ORDERS_SCHEMA) + _job("2026-10-20", NO_AMOUNT, "failed"))
    assert (f.fingerprint, f.severity, f.active) == ("orders_ingest:-amount", "high", True)
    assert f.evidence["baseline_run"]["ds"] == "2026-10-19"
    assert f.evidence["removed"] == ["amount"]


def test_retyped_column_is_breaking() -> None:
    [f] = detect(_job("2026-10-19", ORDERS_SCHEMA) + _job("2026-10-20", RETYPED, "failed"))
    assert f.severity == "high"
    assert f.evidence["retyped"] == [{"column": "amount", "from": "double", "to": "string"}]


def test_failed_run_never_becomes_the_baseline() -> None:
    events = (
        _job("2026-10-19", ORDERS_SCHEMA)
        + _job("2026-10-20", NO_AMOUNT, "failed")
        + _job("2026-10-21", NO_AMOUNT, "failed")
    )
    findings = detect(events)
    assert [f.ds for f in findings] == ["2026-10-20", "2026-10-21"]
    assert {f.evidence["baseline_run"]["ds"] for f in findings} == {"2026-10-19"}
    assert all(f.active for f in findings)  # still broken: nothing clean came after


def test_added_column_on_success_is_medium_and_accepted_by_the_next_run() -> None:
    events = (
        _job("2026-10-19", ORDERS_SCHEMA)
        + _job("2026-10-20", WITH_DISCOUNT)
        + _job("2026-10-21", WITH_DISCOUNT)
    )
    [f] = detect(events)
    assert (f.ds, f.severity) == ("2026-10-20", "medium")
    assert not f.active  # 10-21 matched its new baseline, so the change was accepted


def test_fixed_rerun_clears_the_finding() -> None:
    events = (
        _job("2026-10-19", ORDERS_SCHEMA)
        + _job("2026-10-20", NO_AMOUNT, "failed")
        + _job("2026-10-20", ORDERS_SCHEMA, minutes=30, try_number=2)
    )
    [f] = detect(events)
    assert not f.active


def test_jobs_without_an_observed_schema_are_skipped() -> None:
    r = Run("orders_ingest", "2026-10-20", end_of("2026-10-20"))
    r.job("generate_orders", "generate_orders", rows_out=10)  # generators report no schema
    assert detect(r.events) == []
