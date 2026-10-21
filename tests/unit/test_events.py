from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from sentinel.events import (
    SCHEMA_VERSION,
    DQResult,
    Event,
    EventType,
    JobLog,
    JobMetric,
    JsonlFileSink,
    MemorySink,
    NullSink,
    RunContext,
    sink_from_env,
)
from sentinel.instrumentation import run_context_from_env

CTX = RunContext(pipeline="orders_ingest", run_id="r1", task_id="t1", code_version="abc123")


def _metric(**kw) -> JobMetric:
    now = datetime.now(UTC)
    base = dict(job_name="j", status="success", started_at=now, finished_at=now, duration_s=1.0)
    return JobMetric(**{**base, **kw})


def test_envelope_carries_context_and_routes_to_topic() -> None:
    ev = CTX.event(JobLog(level="INFO", logger="x", message="hi"))
    assert ev.schema_version == SCHEMA_VERSION
    assert ev.event_type is EventType.JOB_LOG
    assert ev.topic == "job_logs"
    assert (ev.pipeline, ev.run_id, ev.task_id) == ("orders_ingest", "r1", "t1")
    assert ev.code_version == "abc123"


@pytest.mark.parametrize(
    ("payload", "topic"),
    [
        (JobLog(level="ERROR", logger="x", message="boom"), "job_logs"),
        (None, "job_metrics"),
        (
            DQResult(
                dataset="orders",
                suite_name="s",
                success=True,
                evaluated=1,
                failed=0,
                row_count=5,
                results=[],
            ),
            "dq_results",
        ),
    ],
)
def test_json_roundtrip_preserves_payload_type(payload, topic) -> None:
    payload = payload or _metric(rows_out=3)
    ev = CTX.event(payload)
    restored = Event.model_validate_json(ev.model_dump_json())
    assert restored == ev
    assert type(restored.payload) is type(payload)
    assert restored.topic == topic


def test_unknown_payload_fields_are_rejected() -> None:
    raw = json.loads(CTX.event(JobLog(level="INFO", logger="x", message="m")).model_dump_json())
    raw["payload"]["surprise"] = 1
    with pytest.raises(ValidationError):
        Event.model_validate(raw)


def test_jsonl_sink_appends_one_line_per_event(tmp_path) -> None:
    sink = JsonlFileSink(tmp_path)
    for i in range(3):
        sink.emit(CTX.event(JobLog(level="INFO", logger="x", message=f"m{i}")))
    files = list((tmp_path / "job_logs").glob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text().splitlines()
    assert [Event.model_validate_json(line).payload.message for line in lines] == ["m0", "m1", "m2"]


def test_sink_from_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SENTINEL_EVENT_SINK", "none")
    assert isinstance(sink_from_env(), NullSink)
    monkeypatch.setenv("SENTINEL_EVENT_SINK", "file")
    monkeypatch.setenv("SENTINEL_EVENTS_DIR", str(tmp_path))
    assert isinstance(sink_from_env(), JsonlFileSink)
    monkeypatch.setenv("SENTINEL_EVENT_SINK", "carrier-pigeon")
    with pytest.raises(ValueError, match="carrier-pigeon"):
        sink_from_env()


def test_run_context_from_airflow_env(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "payments_etl")
    monkeypatch.setenv("AIRFLOW_CTX_DAG_RUN_ID", "manual__1")
    monkeypatch.setenv("AIRFLOW_CTX_TASK_ID", "run_payments_glue_job")
    monkeypatch.setenv("AIRFLOW_CTX_TRY_NUMBER", "2")
    monkeypatch.setenv("SENTINEL_CODE_VERSION", "deadbeef")
    ctx = run_context_from_env()
    assert ctx.pipeline == "payments_etl"
    assert ctx.try_number == 2
    assert ctx.code_version == "deadbeef"


def test_run_context_outside_airflow(monkeypatch) -> None:
    for var in ("AIRFLOW_CTX_DAG_ID", "AIRFLOW_CTX_DAG_RUN_ID", "AIRFLOW_CTX_TRY_NUMBER"):
        monkeypatch.delenv(var, raising=False)
    ctx = run_context_from_env(default_pipeline="adhoc_job")
    assert ctx.pipeline == "adhoc_job"
    assert ctx.run_id is None and ctx.try_number is None


def test_memory_sink_collects() -> None:
    sink = MemorySink()
    sink.emit(CTX.event(_metric()))
    assert len(sink.events) == 1
