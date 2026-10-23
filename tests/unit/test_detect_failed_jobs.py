from __future__ import annotations

from datetime import timedelta

from sentinel.detect import failed_jobs, from_events, load_config
from tests.unit.detect_fixtures import Run, end_of

DS = "2026-10-20"
T0 = end_of(DS)
CFG = load_config()


def _run(try_number: int, minutes: int = 0) -> Run:
    return Run("orders_ingest", DS, T0 + timedelta(minutes=minutes), try_number)


def detect(*runs: Run):
    return failed_jobs.detect(from_events([e for r in runs for e in r.events]), CFG)


def test_successful_attempts_raise_nothing() -> None:
    r = _run(1)
    r.task("ingest_orders")
    assert detect(r) == []


def test_failure_then_success_is_one_cleared_finding() -> None:
    a, b = _run(1), _run(2, minutes=10)
    a.job("ingest_orders", "orders_ingest", status="failed", error_type="SchemaMismatchError")
    a.task("ingest_orders", "failed")
    ok = b.task("ingest_orders")
    [f] = detect(a, b)
    assert f.fingerprint == "ingest_orders:SchemaMismatchError"
    assert f.severity == "high"
    assert f.cleared_at == ok.emitted_at


def test_repeated_identical_failures_merge_into_one_active_finding() -> None:
    runs = [_run(i, minutes=i) for i in (1, 2, 3)]
    for r in runs:
        r.task("ingest_orders", "failed", "Bash command failed. exit code 1.")
    [f] = detect(*runs)
    assert f.active
    assert f.evidence["attempts_failed"] == 3
    assert f.evidence["try_numbers"] == [1, 2, 3]
    assert f.first_observed < f.last_observed


def test_a_different_error_on_retry_supersedes_the_first() -> None:
    a, b = _run(1), _run(2, minutes=5)
    a.job("ingest_orders", "orders_ingest", status="failed", error_type="SchemaMismatchError")
    a.task("ingest_orders", "failed")
    b.job("ingest_orders", "orders_ingest", status="failed", error_type="AnalysisException")
    b.task("ingest_orders", "failed")
    first, second = sorted(detect(a, b), key=lambda f: f.first_observed)
    assert not first.active and second.active
    assert first.cleared_at == second.first_observed  # the retry no longer failed that way


def test_validate_failure_signature_is_the_failed_expectations() -> None:
    r = _run(1)
    r.dq(
        "validate_orders",
        "orders",
        2000,
        extra_failures=[("expect_column_values_to_not_be_null", "amount")],
    )
    r.task("validate_orders", "failed")
    [f] = detect(r)
    assert f.fingerprint == "validate_orders:dq:expect_column_values_to_not_be_null(amount)"
    assert f.evidence["failed_expectations"] == ["expect_column_values_to_not_be_null(amount)"]


def test_airflow_exception_text_has_digits_masked() -> None:
    a, b = _run(1), _run(2, minutes=5)
    a.task("ingest_orders", "failed", "Process killed: exit code 137 after 41s")
    b.task("ingest_orders", "failed", "Process killed: exit code 137 after 97s")
    [f] = detect(a, b)  # same problem despite different numbers
    assert f.fingerprint == "ingest_orders:Process killed: exit code # after #s"


def test_up_for_retry_is_not_a_failure() -> None:
    r = _run(1)
    r.task("ingest_orders", "up_for_retry")
    assert detect(r) == []
