"""Task callbacks with a fake TaskInstance (no Airflow needed)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from sentinel.events import JobLog, MemorySink, TaskRun
from sentinel.instrumentation import airflow_callbacks as cb

START = datetime(2026, 10, 21, 8, 0, tzinfo=UTC)


@dataclass
class FakeTI:
    dag_id: str = "orders_ingest"
    task_id: str = "ingest_orders"
    run_id: str = "run_all__2026-10-20"
    try_number: int = 2
    start_date: datetime = START
    end_date: datetime | None = START + timedelta(seconds=12.5)
    duration: float | None = None
    hostname: str = "airflow-scheduler"
    operator: str = "BashOperator"


@pytest.fixture
def sink(monkeypatch) -> MemorySink:
    s = MemorySink()
    monkeypatch.setattr(cb, "sink_from_env", lambda: s)
    monkeypatch.setenv("SENTINEL_CODE_VERSION", "abc1234")
    return s


def _ctx(ti: FakeTI, exc: Exception | None = None) -> dict:
    return {"ti": ti, "exception": exc, "logical_date": datetime(2026, 10, 20, tzinfo=UTC)}


def test_success_emits_one_task_run(sink) -> None:
    cb.on_success(_ctx(FakeTI()))
    [ev] = sink.events
    assert isinstance(ev.payload, TaskRun)
    assert ev.topic == "job_metrics"
    assert (ev.pipeline, ev.task_id, ev.run_id, ev.try_number) == (
        "orders_ingest",
        "ingest_orders",
        "run_all__2026-10-20",
        2,
    )
    assert ev.logical_date.startswith("2026-10-20")
    assert ev.code_version == "abc1234"
    assert ev.payload.state == "success"
    assert ev.payload.duration_s == 12.5  # derived from start/end when duration is unset


def test_failure_emits_task_run_and_error_log_with_log_tail(sink, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", str(tmp_path))
    ti = FakeTI()
    log_dir = tmp_path / f"dag_id={ti.dag_id}" / f"run_id={ti.run_id}" / f"task_id={ti.task_id}"
    log_dir.mkdir(parents=True)
    lines = [f"line {i}" for i in range(200)] + ["SchemaMismatchError: missing ['amount']"]
    (log_dir / "attempt=2.log").write_text("\n".join(lines))
    (log_dir / "attempt=1.log").write_text("an older attempt")

    cb.on_failure(_ctx(ti, RuntimeError("Bash command failed. exit code 1")))
    task_run, error_log = sink.events
    assert task_run.payload.state == "failed"
    assert "exit code 1" in task_run.payload.exception
    assert isinstance(error_log.payload, JobLog)
    assert error_log.topic == "job_logs"
    assert error_log.payload.level == "ERROR"
    assert error_log.payload.exc_type == "RuntimeError"
    tail = error_log.payload.traceback
    assert tail.endswith("SchemaMismatchError: missing ['amount']")
    assert "line 0\n" not in tail and len(tail.splitlines()) == cb.LOG_TAIL_LINES


def test_retry_state(sink) -> None:
    cb.on_retry(_ctx(FakeTI(), TimeoutError("slow")))
    assert sink.events[0].payload.state == "up_for_retry"


def test_missing_log_file_gives_no_tail_but_still_emits(sink, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", str(tmp_path))
    cb.on_failure(_ctx(FakeTI(), ValueError("x")))
    assert sink.events[1].payload.traceback is None


def test_callback_never_raises(monkeypatch) -> None:
    class Boom:
        def emit(self, event):
            raise OSError("broker and disk both gone")

        def flush(self, timeout_s=None):
            return 0

    monkeypatch.setattr(cb, "sink_from_env", lambda: Boom())
    cb.on_failure(_ctx(FakeTI(), ValueError("x")))  # must not raise
    cb.on_success({"not": "a real context"})  # must not raise either


def _write_log(tmp_path, monkeypatch, lines: list[str]) -> FakeTI:
    monkeypatch.setenv("AIRFLOW__LOGGING__BASE_LOG_FOLDER", str(tmp_path))
    ti = FakeTI()
    log_dir = tmp_path / f"dag_id={ti.dag_id}" / f"run_id={ti.run_id}" / f"task_id={ti.task_id}"
    log_dir.mkdir(parents=True)
    (log_dir / "attempt=2.log").write_text("\n".join(lines))
    return ti


OOM = (
    "py4j.protocol.Py4JJavaError: An error occurred while calling o57.save.\n"
    ": java.lang.OutOfMemoryError: Cannot reserve 8192 bytes of direct buffer memory"
)


def test_terminal_jvm_error_outside_the_tail_is_kept(sink, tmp_path, monkeypatch) -> None:
    # Phase 5c: PySpark's Py4J teardown pushed the OutOfMemoryError out of the last 60
    # lines, so every OOM fault in the v1 benchmark was diagnosed `unknown`.
    teardown = [f"py4j.clientserver - Closing down clientserver connection {i}" for i in range(150)]
    ti = _write_log(
        tmp_path, monkeypatch, ["starting job", *OOM.splitlines(), *teardown, "exit code 1"]
    )
    cb.on_failure(_ctx(ti, RuntimeError("Bash command failed. exit code 1")))
    tail = sink.events[1].payload.traceback
    head, rest = tail.split(cb.TAIL_HEADER + "\n")
    assert head.startswith(cb.EARLIER_HEADER)
    assert "java.lang.OutOfMemoryError: Cannot reserve 8192 bytes" in head
    assert "o57.save" not in head  # only the terminal-error line, not its neighbours
    assert rest.endswith("exit code 1") and len(rest.splitlines()) == cb.LOG_TAIL_LINES
    assert len(tail) <= cb.LOG_TAIL_MAX_CHARS


def test_terminal_error_already_in_the_tail_is_not_repeated(sink, tmp_path, monkeypatch) -> None:
    ti = _write_log(tmp_path, monkeypatch, [f"line {i}" for i in range(100)] + OOM.splitlines())
    cb.on_failure(_ctx(ti, RuntimeError("exit code 1")))
    tail = sink.events[1].payload.traceback
    assert cb.EARLIER_HEADER not in tail and tail.count("OutOfMemoryError") == 1
