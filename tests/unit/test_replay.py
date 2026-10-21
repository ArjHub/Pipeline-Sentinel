from __future__ import annotations

from sentinel.events import JobLog, JobMetric, JsonlFileSink, RunContext
from sentinel.events.replay import journal_files, replay
from sentinel.lander.bronze import BronzeWriter, RawMessage

CTX = RunContext(pipeline="orders_ingest")


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, bytes]] = []

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, t):
        return 0

    def flush(self, t):
        return 0


def _journal(tmp_path, n_logs: int = 3) -> JsonlFileSink:
    sink = JsonlFileSink(tmp_path)
    for i in range(n_logs):
        sink.emit(CTX.event(JobLog(level="INFO", logger="x", message=f"m{i}")))
    from datetime import UTC, datetime

    now = datetime.now(UTC)
    sink.emit(
        CTX.event(
            JobMetric(job_name="j", status="success", started_at=now, finished_at=now, duration_s=1)
        )
    )
    return sink


def test_replays_every_journal_line_to_its_topic(tmp_path) -> None:
    _journal(tmp_path)
    producer = RecordingProducer()
    sent, failed = replay(journal_files(tmp_path, None, None), producer)
    assert failed == 0
    assert dict(sent) == {"job_logs": 3, "job_metrics": 1}
    assert all(key == b"orders_ingest" for _, key, _ in producer.sent)


def test_topic_and_date_filters(tmp_path) -> None:
    _journal(tmp_path)
    assert [p.parent.name for p in journal_files(tmp_path, None, "job_metrics")] == ["job_metrics"]
    assert journal_files(tmp_path, "1999-01-01", None) == []


def test_corrupt_lines_are_forwarded_so_they_reach_dead_letter(tmp_path) -> None:
    _journal(tmp_path, n_logs=1)
    f = next((tmp_path / "job_logs").glob("*.jsonl"))
    f.write_text(f.read_text() + "{corrupt\n")
    producer = RecordingProducer()
    sent, _ = replay(journal_files(tmp_path, None, "job_logs"), producer)
    assert sent["job_logs"] == 2
    assert producer.sent[-1][1] is None  # no key could be derived from a corrupt line


def test_replay_into_bronze_twice_is_idempotent(tmp_path) -> None:
    """The full fallback story: journal -> replay -> lander, run twice."""
    _journal(tmp_path / "events")
    writer = BronzeWriter(tmp_path / "bronze")
    for attempt in range(2):
        producer = RecordingProducer()
        replay(journal_files(tmp_path / "events", None, None), producer)
        msgs = [RawMessage(t, 0, i, k, v) for i, (t, k, v) in enumerate(producer.sent)]
        result = writer.land(msgs)
        expected = 4 if attempt == 0 else 0
        assert sum(result.inserted.values()) == expected
