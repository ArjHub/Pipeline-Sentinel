"""Publishing behaviour, especially when the broker is down: the job must not fail, must
not hang beyond the configured timeout, and every event must still be in the journal."""

from __future__ import annotations

import json
import logging
import socket
import time

from sentinel.events import Event, JobLog, JsonlFileSink, MemorySink, RunContext, sink_from_env
from sentinel.events.sinks import KafkaSink, TeeSink

CTX = RunContext(pipeline="orders_ingest", run_id="r1", task_id="t1")


def _events(n: int) -> list[Event]:
    return [CTX.event(JobLog(level="INFO", logger="x", message=f"m{i}")) for i in range(n)]


def _closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]  # nothing listens here once the socket closes


class FakeProducer:
    def __init__(self, fail_with: Exception | None = None) -> None:
        self.sent: list[dict] = []
        self.fail_with = fail_with
        self._pending: list = []

    def produce(self, topic, key, value, headers, on_delivery):
        if self.fail_with:
            raise self.fail_with
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": headers})
        self._pending.append(on_delivery)

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for cb in self._pending:
            cb(None, object())
        self._pending.clear()
        return 0


def test_kafka_sink_produces_to_topic_with_pipeline_key_and_headers() -> None:
    producer = FakeProducer()
    sink = KafkaSink("unused:9092", producer=producer)
    [ev] = _events(1)
    sink.emit(ev)
    assert sink.flush() == 0
    [msg] = producer.sent
    assert msg["topic"] == "job_logs"
    assert msg["key"] == b"orders_ingest"
    assert msg["headers"] == {"schema_version": ev.schema_version, "event_type": "job_log"}
    assert Event.model_validate_json(msg["value"]) == ev
    assert sink.delivered == 1


def test_produce_errors_are_counted_not_raised() -> None:
    sink = KafkaSink("unused:9092", producer=FakeProducer(fail_with=BufferError("queue full")))
    for ev in _events(3):
        sink.emit(ev)  # must not raise
    assert sink.flush() == 3


def test_broker_down_falls_back_to_journal_within_timeout(tmp_path, caplog) -> None:
    """Real librdkafka against a port where nothing listens."""
    journal = JsonlFileSink(tmp_path)
    sink = TeeSink(journal, KafkaSink(f"127.0.0.1:{_closed_port()}", timeout_ms=1500))
    logging.getLogger("sentinel.events.sink").propagate = True  # let caplog see it
    try:
        t0 = time.monotonic()
        for ev in _events(5):
            sink.emit(ev)
        with caplog.at_level(logging.WARNING, logger="sentinel.events.sink"):
            undelivered = sink.flush()
        elapsed = time.monotonic() - t0
    finally:
        logging.getLogger("sentinel.events.sink").propagate = False

    assert undelivered == 5
    assert elapsed < 10, f"flush took {elapsed:.1f}s; a dead broker must not stall jobs"
    lines = (tmp_path / "job_logs").glob("*.jsonl").__next__().read_text().splitlines()
    assert [json.loads(line)["payload"]["message"] for line in lines] == [f"m{i}" for i in range(5)]
    assert "not delivered to Kafka" in caplog.text
    assert "make replay-events" in caplog.text


def test_journal_failure_does_not_stop_publishing(tmp_path) -> None:
    class BrokenJournal:
        def emit(self, event):
            raise OSError("disk full")

        def flush(self, timeout_s=None):
            return 0

    publisher = MemorySink()
    sink = TeeSink(BrokenJournal(), publisher)
    sink.emit(_events(1)[0])
    assert len(publisher.events) == 1


def test_sink_from_env_kafka_is_journal_plus_publisher(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SENTINEL_EVENT_SINK", "kafka")
    monkeypatch.setenv("SENTINEL_EVENTS_DIR", str(tmp_path))
    monkeypatch.setenv("SENTINEL_KAFKA_BOOTSTRAP", f"127.0.0.1:{_closed_port()}")
    sink = sink_from_env()
    assert isinstance(sink, TeeSink)
    assert isinstance(sink.journal, JsonlFileSink)
    assert isinstance(sink.publisher, KafkaSink)


def test_sink_from_env_degrades_to_journal_if_kafka_client_missing(monkeypatch, tmp_path) -> None:
    import builtins

    real_import = builtins.__import__

    def no_kafka(name, *args, **kwargs):
        if name == "confluent_kafka":
            raise ImportError("no librdkafka here")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_kafka)
    monkeypatch.setenv("SENTINEL_EVENT_SINK", "kafka")
    monkeypatch.setenv("SENTINEL_EVENTS_DIR", str(tmp_path))
    assert isinstance(sink_from_env(), JsonlFileSink)
