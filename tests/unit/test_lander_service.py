"""The consume loop's delivery semantics, with a fake consumer: commit only after a
successful write; a failed write propagates and nothing is committed."""

from __future__ import annotations

import pytest

from sentinel.events import JobLog, RunContext
from sentinel.lander.bronze import BronzeWriter, LandResult
from sentinel.lander.service import consumer_config, run

CTX = RunContext(pipeline="p")


class Msg:
    def __init__(self, value: bytes, offset: int, err=None) -> None:
        self._value, self._offset, self._err = value, offset, err

    def error(self):
        return self._err

    def topic(self):
        return "job_logs"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def key(self):
        return b"p"

    def value(self):
        return self._value


class FakeConsumer:
    def __init__(self, batches: list[list[Msg]]) -> None:
        self.batches = batches
        self.commits = 0

    def consume(self, num_messages, timeout):
        return self.batches.pop(0) if self.batches else []

    def commit(self, asynchronous):
        assert asynchronous is False
        self.commits += 1


def _msg(i: int) -> Msg:
    ev = CTX.event(JobLog(level="INFO", logger="x", message=f"m{i}"))
    return Msg(ev.model_dump_json().encode(), i)


def test_lands_batches_commits_each_and_exits_when_idle(tmp_path) -> None:
    consumer = FakeConsumer([[_msg(0), _msg(1)], [], [_msg(2)]])
    total = run(consumer, BronzeWriter(tmp_path), poll_timeout_s=0, idle_exit_s=0.05)
    assert total == 3
    assert consumer.commits == 2  # one per non-empty batch, none for the empty poll


def test_consumer_errors_are_skipped_not_landed(tmp_path) -> None:
    consumer = FakeConsumer([[Msg(b"", 0, err="broker transport failure"), _msg(1)]])
    assert run(consumer, BronzeWriter(tmp_path), poll_timeout_s=0, idle_exit_s=0.01) == 1


def test_failed_write_is_not_committed(tmp_path) -> None:
    class ExplodingWriter(BronzeWriter):
        def land(self, messages) -> LandResult:
            raise OSError("disk full")

    consumer = FakeConsumer([[_msg(0)]])
    with pytest.raises(OSError):
        run(consumer, ExplodingWriter(tmp_path), poll_timeout_s=0, idle_exit_s=0.01)
    assert consumer.commits == 0  # the batch will be re-delivered after restart


def test_consumer_never_auto_commits() -> None:
    cfg = consumer_config("b:9092", "g")
    assert cfg["enable.auto.commit"] is False
    assert cfg["auto.offset.reset"] == "earliest"
