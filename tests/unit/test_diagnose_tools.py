"""The six read-only tools over a real temp lake (bronze + incidents)."""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from sentinel.detect import load_config
from sentinel.diagnose.tools import TOOL_DEFS, Toolbox
from sentinel.events import DQResult, Event, ExpectationOutcome, UnexpectedValue
from tests.unit.detect_fixtures import Run, end_of
from tests.unit.diagnose_fixtures import DS, build_lake, log_event


@pytest.fixture
def box(tmp_path):
    leak = log_event(
        "orders_ingest",
        "ingest_orders",
        "retrying with password=hunter2hunter2 for ops@corp.example.com",
        end_of(DS) + timedelta(minutes=32),
        level="WARNING",
    )
    incident = build_lake(tmp_path, extra=[leak])
    return Toolbox(tmp_path, load_config(), incident)


def call(box, name, **args):
    text, err = box.call(name, args)
    return json.loads(text), err


def test_tool_defs_match_handlers(box) -> None:
    assert [t["name"] for t in TOOL_DEFS] == list(box.handlers)


def test_error_logs_default_to_the_incident_scope_and_are_redacted(box) -> None:
    out, err = call(box, "get_error_logs")
    assert not err and out["scope"] == {"pipeline": "orders_ingest", "ds": DS, "task_id": None}
    text = json.dumps(out)
    assert "missing required columns ['amount']" in text
    assert "hunter2" not in text and "ops@corp.example.com" not in text
    assert "[REDACTED:SECRET]" in text and "[REDACTED:EMAIL]" in text
    assert {i["level"] for i in out["untrusted_data"]} >= {"ERROR", "TASK_FAILED", "WARNING"}


def test_schema_diff_shows_the_removed_column_vs_last_good_run(box) -> None:
    out, _ = call(box, "get_schema_diff")
    [entry] = [e for e in out["untrusted_data"] if e["status"] == "failed"]
    assert entry["diff"]["removed"] == ["amount"] and entry["baseline"]["ds"] < DS


def test_deploys_and_code_versions(box) -> None:
    out, _ = call(box, "get_recent_deploys")
    assert out["untrusted_data"][0]["git_sha"] == "abc1234def"
    assert out["code_versions_by_ds"][0] == {"ds": DS, "code_version": "abc1234"}


def test_upstream_status_for_the_downstream_pipeline(box) -> None:
    out, _ = call(box, "get_upstream_status", pipeline="daily_revenue_agg")
    by = {u["pipeline"]: u for u in out["untrusted_data"]}
    assert by["orders_ingest"]["final_task_state"] == "no run seen"  # upstream_failed
    assert by["orders_ingest"]["open_incidents"][0]["type"] == "schema_change"
    assert by["payments_etl"]["final_task_state"] == "success"


def test_metric_history_and_dq(box) -> None:
    hist, _ = call(box, "get_metric_history", days=7)
    assert hist["untrusted_data"][0]["status"] == "failed"
    assert len({r["ds"] for r in hist["untrusted_data"]}) == 8
    dq, _ = call(box, "get_dq_failures", pipeline="customers_dim")
    assert dq["untrusted_data"][0]["failed_expectations"] == []


@pytest.mark.parametrize(
    ("name", "args", "msg"),
    [
        ("get_error_logs", {"pipeline": "hr_payroll"}, "unknown pipeline"),
        ("get_error_logs", {"ds": "2026-01-01"}, "outside this incident's window"),
        ("get_error_logs", {"ds": "yesterday"}, "YYYY-MM-DD"),
        ("trigger_dag_run", {}, "no such tool"),
    ],
)
def test_out_of_scope_or_unknown_calls_are_errors(box, name, args, msg) -> None:
    out, err = call(box, name, **args)
    assert err and msg in out["error"]


def test_output_is_capped_by_dropping_items_not_breaking_json(tmp_path) -> None:
    incident = build_lake(tmp_path)
    box = Toolbox(tmp_path, load_config(), incident, output_max_chars=900)
    text, _ = box.call("get_metric_history", {"days": 28})
    out = json.loads(text)  # still valid JSON
    assert len(text) <= 900 and out["truncated_items"] > 0


def test_terminal_jvm_error_survives_traceback_truncation(tmp_path) -> None:
    # Phase 5c: the fatal line sits mid-traceback, where truncation (head third + tail
    # two-thirds of 1500 chars) would cut it; it must still reach the model.
    teardown = "\n".join(f"py4j teardown line {i}" for i in range(200))
    tb = f"{teardown[:2000]}\n: java.lang.OutOfMemoryError: Java heap space\n{teardown}"
    oom = log_event(
        "orders_ingest",
        "ingest_orders",
        "task ingest_orders failed: exit code 1",
        end_of(DS) + timedelta(minutes=33),
        traceback=tb,
    )
    box = Toolbox(tmp_path, load_config(), build_lake(tmp_path, extra=[oom]))
    out, _ = call(box, "get_error_logs", task_id="ingest_orders")
    [item] = [i for i in out["untrusted_data"] if i["message"].startswith("task ingest_orders")]
    assert item["terminal_errors"] == [": java.lang.OutOfMemoryError: Java heap space"]
    assert "OutOfMemoryError" not in item["traceback_tail"]  # truncated away, as before
    others = [i for i in out["untrusted_data"] if i is not item]
    assert all("terminal_errors" not in i for i in others)  # absent when there are none


def _dq_event(results) -> Event:
    r = Run("payments_etl", DS, end_of(DS) + timedelta(minutes=50))
    return r._emit(
        "validate_payments",
        DQResult(
            dataset="payments",
            suite_name="payments.v1",
            success=False,
            evaluated=len(results),
            failed=len(results),
            row_count=2000,
            results=results,
        ),
    )


def test_dq_failures_report_the_real_unexpected_values(tmp_path) -> None:
    # Phase 5c: GX has no single observed value for column-map expectations; the tool
    # used to print "null", and the model read non-null bad values as nulls.
    negative = ExpectationOutcome(
        expectation_type="expect_column_values_to_be_between",
        column="amount",
        success=False,
        unexpected_count=64,
        unexpected_values=[
            UnexpectedValue(value=-120.5, count=1),
            UnexpectedValue(value=-3, count=1),
        ],
        missing_count=0,
    )
    method = ExpectationOutcome(
        expectation_type="expect_column_values_to_be_in_set",
        column="method",
        success=False,
        unexpected_count=812,
        unexpected_values=[UnexpectedValue(value="UPI-COLLECT", count=812)],
        missing_count=0,
    )
    old_style = ExpectationOutcome(  # a 1.1 event (built before the fix): no sample
        expectation_type="expect_column_values_to_not_be_null", column="paid_ts", success=False
    )
    box = Toolbox(
        tmp_path,
        load_config(),
        build_lake(tmp_path, extra=[_dq_event([negative, method, old_style])]),
    )
    out, _ = call(box, "get_dq_failures", pipeline="payments_etl")
    amount, method_out, old = out["untrusted_data"][0]["failed_expectations"]
    assert amount["unexpected_values_sample"] == [
        {"value": "-120.5", "count": 1},
        {"value": "-3", "count": 1},
    ]
    assert amount["null_count"] == 0 and "observed_value" not in amount
    assert method_out["unexpected_values_sample"] == [{"value": '"UPI-COLLECT"', "count": 812}]
    assert old == {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "paid_ts",
        "unexpected_count": None,
    }
    assert '"null"' not in json.dumps(out)
