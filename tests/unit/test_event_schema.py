"""The event contract: what parse_event accepts, what it rejects, and that the committed
JSON Schema matches the Pydantic models."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from sentinel.events import (
    SCHEMA_VERSION,
    DeployEvent,
    Event,
    EventType,
    InvalidEventError,
    JobLog,
    RunContext,
    TaskRun,
    parse_event,
)
from sentinel.events.schema import SCHEMA_FILE, render

CTX = RunContext(pipeline="orders_ingest", run_id="r1", task_id="ingest_orders")


def _good() -> dict:
    return json.loads(CTX.event(JobLog(level="INFO", logger="x", message="hi")).model_dump_json())


def test_every_event_has_the_required_envelope_fields() -> None:
    doc = _good()
    for key in ("schema_version", "event_id", "pipeline", "run_id", "emitted_at", "event_type"):
        assert doc[key], key
    assert doc["schema_version"] == SCHEMA_VERSION


def test_parse_good_event_roundtrips() -> None:
    raw = json.dumps(_good())
    assert parse_event(raw).model_dump_json() == Event.model_validate_json(raw).model_dump_json()
    assert parse_event(raw.encode()).payload.message == "hi"


def test_task_run_routes_to_job_metrics_topic() -> None:
    ev = CTX.event(TaskRun(task_id="t", state="failed", exception="boom"))
    assert ev.event_type is EventType.TASK_RUN
    assert ev.topic == "job_metrics"
    assert parse_event(ev.model_dump_json()).payload.exception == "boom"


def test_v1_0_events_still_parse() -> None:
    """Backward compatibility: a 1.0 deploy event (no git fields) parses under 1.1."""
    doc = json.loads(
        CTX.event(DeployEvent(component="orders_ingest", code_version="abc")).model_dump_json()
    )
    doc["schema_version"] = "1.0"
    del doc["payload"]["git_sha"], doc["payload"]["dirty"]
    assert parse_event(json.dumps(doc)).payload.git_sha is None


@pytest.mark.parametrize(
    ("mutate", "error_fragment"),
    [
        (lambda d: d.pop("event_id"), "event_id: Field required"),
        (lambda d: d.pop("pipeline"), "pipeline: Field required"),
        (lambda d: d.pop("emitted_at"), "emitted_at: Field required"),
        (lambda d: d.update(event_id=""), "event_id: String should have at least 1 character"),
        (lambda d: d.update(schema_version="2.0"), "unsupported schema_version '2.0'"),
        (lambda d: d.update(event_type="dq_result"), "does not match payload kind"),
        (lambda d: d["payload"].update(kind="mystery"), "payload: Input tag 'mystery'"),
        (lambda d: d["payload"].update(surprise=1), "Extra inputs are not permitted"),
        (lambda d: d.update(emitted_at="yesterday-ish"), "emitted_at"),
        (lambda d: d["payload"].pop("message"), "message: Field required"),
    ],
)
def test_bad_events_are_rejected_with_a_readable_reason(mutate, error_fragment) -> None:
    doc = _good()
    mutate(doc)
    with pytest.raises(InvalidEventError) as exc:
        parse_event(json.dumps(doc))
    assert error_fragment in str(exc.value)


@pytest.mark.parametrize("raw", [b"", b"not json", b"[]", b'{"truncated": '])
def test_non_json_or_wrong_shape_is_rejected(raw) -> None:
    with pytest.raises(InvalidEventError):
        parse_event(raw)


def test_emitted_at_is_timezone_aware_utc() -> None:
    ev = CTX.event(JobLog(level="INFO", logger="x", message="m"))
    assert ev.emitted_at.tzinfo is not None
    assert ev.emitted_at <= datetime.now(UTC)


def test_committed_json_schema_matches_models() -> None:
    assert SCHEMA_FILE.exists(), "run `make schemas`"
    assert SCHEMA_FILE.read_text() == render(), "schema drifted; run `make schemas` and commit"
