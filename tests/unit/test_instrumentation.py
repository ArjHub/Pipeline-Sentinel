from __future__ import annotations

import json
import logging

import pytest

from sentinel.events import JobLog, JobMetric, MemorySink, RunContext
from sentinel.instrumentation import configure_logging, instrumented_job
from sentinel.instrumentation.logging import JsonFormatter

CTX = RunContext(pipeline="p", run_id="r", task_id="t")


@pytest.fixture
def sink() -> MemorySink:
    s = MemorySink()
    configure_logging(CTX, s)
    yield s
    logging.getLogger().handlers.clear()


def _metrics(sink: MemorySink) -> list[JobMetric]:
    return [e.payload for e in sink.events if isinstance(e.payload, JobMetric)]


def _logs(sink: MemorySink) -> list[JobLog]:
    return [e.payload for e in sink.events if isinstance(e.payload, JobLog)]


def test_success_emits_metric_with_stats(sink) -> None:
    with instrumented_job("job_a", CTX, sink) as stats:
        stats.rows_in, stats.rows_out = 10, 8
        stats.extra["k"] = "v"
    [m] = _metrics(sink)
    assert m.status == "success"
    assert (m.rows_in, m.rows_out, m.extra) == (10, 8, {"k": "v"})
    assert m.duration_s >= 0
    assert m.error_type is None


def test_failure_emits_failed_metric_and_error_log_then_reraises(sink) -> None:
    with pytest.raises(KeyError), instrumented_job("job_b", CTX, sink) as stats:
        stats.rows_in = 5
        raise KeyError("missing column amount")
    [m] = _metrics(sink)
    assert m.status == "failed"
    assert m.error_type == "KeyError"
    assert "missing column amount" in m.error_message
    assert m.rows_in == 5
    errors = [log for log in _logs(sink) if log.level == "ERROR"]
    assert errors and errors[0].exc_type == "KeyError"
    assert "Traceback" in errors[0].traceback


def test_long_error_messages_are_truncated(sink) -> None:
    with pytest.raises(RuntimeError), instrumented_job("job_c", CTX, sink):
        raise RuntimeError("x" * 10_000)
    [m] = _metrics(sink)
    assert len(m.error_message) == 2000


def test_stdout_formatter_writes_json_with_context() -> None:
    record = logging.LogRecord("demo", logging.WARNING, __file__, 1, "hello %s", ("world",), None)
    doc = json.loads(JsonFormatter(CTX).format(record))
    assert doc["message"] == "hello world"
    assert doc["level"] == "WARNING"
    assert (doc["pipeline"], doc["run_id"], doc["task_id"]) == ("p", "r", "t")


def test_debug_logs_are_not_emitted_as_events(sink) -> None:
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger("demo").debug("noise")
    assert not [log for log in _logs(sink) if log.message == "noise"]


def test_broken_sink_does_not_break_job() -> None:
    class Exploding:
        def emit(self, event):
            raise OSError("disk full")

    with instrumented_job("job_d", CTX, Exploding()) as stats:
        stats.rows_out = 1  # the job completes despite telemetry failure
