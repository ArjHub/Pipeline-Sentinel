from __future__ import annotations

import pytest

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


class FakeLagConsumer:
    """Two topics, two partitions each; committed offsets advance as the lander 'catches up'."""

    def __init__(self, committed: dict[tuple[str, int], int]) -> None:
        self.committed_offsets = committed
        self.high = {("a", 0): 10, ("a", 1): 5, ("b", 0): 3, ("b", 1): 0}

    def list_topics(self, timeout):
        class T:
            def __init__(self, n):
                self.partitions = dict.fromkeys(range(n))

        class M:
            topics = {"a": T(2), "b": T(2)}

        return M()

    def committed(self, tps, timeout):
        for tp in tps:
            tp.offset = self.committed_offsets.get((tp.topic, tp.partition), -1001)
        return tps

    def get_watermark_offsets(self, tp, timeout):
        return 0, self.high[(tp.topic, tp.partition)]


def test_consumer_lag_counts_uncommitted_messages_per_topic() -> None:
    pytest.importorskip("confluent_kafka")
    from sentinel.events.replay import consumer_lag

    # b/0 has never been committed (-1001): everything from the low watermark is lag.
    consumer = FakeLagConsumer({("a", 0): 7, ("a", 1): 5, ("b", 1): 0})
    assert consumer_lag(consumer, ["a", "b", "missing"]) == {"a": 3, "b": 3, "missing": 0}


def test_wait_caught_up_returns_when_lag_reaches_zero() -> None:
    from sentinel.events.replay import wait_caught_up

    lags = iter([{"t": 5}, {"t": 2}, {"t": 0}])
    clock = iter(range(100))
    ok, lag = wait_caught_up(
        lambda: next(lags), timeout_s=60, clock=lambda: next(clock), sleep=lambda s: None
    )
    assert ok and lag == {"t": 0}


def test_wait_caught_up_times_out_and_survives_broker_errors() -> None:
    from sentinel.events.replay import wait_caught_up

    def lag_fn():
        raise RuntimeError("broker down")

    t = iter(range(0, 1000, 10))
    ok, lag = wait_caught_up(lag_fn, timeout_s=30, clock=lambda: next(t), sleep=lambda s: None)
    assert not ok and lag == {"<broker unreachable>": -1}


def test_replay_main_warns_when_lander_does_not_catch_up(tmp_path, monkeypatch, capsys) -> None:
    from sentinel.events import replay as mod

    _journal(tmp_path)
    monkeypatch.setattr("confluent_kafka.Producer", lambda cfg: RecordingProducer(), raising=False)
    monkeypatch.setattr(mod, "_wait_for_lander", lambda b, g, t: print("WARNING: x") or 1)
    rc = mod.main(["--events-dir", str(tmp_path), "--bootstrap", "b:1", "--wait-timeout", "1"])
    assert rc == 1
    assert "WARNING" in capsys.readouterr().out
