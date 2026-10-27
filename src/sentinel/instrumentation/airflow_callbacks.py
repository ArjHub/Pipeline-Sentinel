"""Airflow task callbacks that publish `task_run` events (and, on failure, a `job_log`
with the tail of the task log).

They complement the in-job instrumentation: if the job process is OOM-killed, a sensor
times out, or the bash command itself is wrong, the job never gets to emit its own
`job_metric`, but Airflow still runs these callbacks.

Deliberately no `airflow` imports: the callbacks only read attributes off the context,
which keeps them unit-testable without an Airflow install. They must never raise:
telemetry can't be allowed to change a task's outcome.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sentinel.events.models import JobLog, RunContext, TaskRun
from sentinel.events.sinks import sink_from_env
from sentinel.instrumentation.context import code_version_for
from sentinel.instrumentation.terminal_errors import terminal_errors

log = logging.getLogger("sentinel.callbacks")

LOG_TAIL_LINES = 60
LOG_TAIL_MAX_CHARS = 6000
EXCEPTION_MAX_CHARS = 2000
EARLIER_HEADER = "[sentinel] terminal error lines from earlier in this log:"
TAIL_HEADER = f"[sentinel] last {LOG_TAIL_LINES} lines:"


def _log_dir() -> Path:
    base = os.environ.get("AIRFLOW__LOGGING__BASE_LOG_FOLDER")
    if not base:
        base = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "logs")
    return Path(base)


def log_tail(dag_id: str, run_id: str, task_id: str, try_number: int | None) -> str | None:
    """Last lines of this attempt's local task log (the default Airflow 2 file layout).

    Terminal JVM error lines (sentinel.instrumentation.terminal_errors) that fall before
    the tail are kept too, under EARLIER_HEADER ahead of the tail: PySpark's teardown
    otherwise pushes e.g. an OutOfMemoryError out of the last LOG_TAIL_LINES lines."""
    task_dir = _log_dir() / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
    candidates = [task_dir / f"attempt={try_number}.log"] if try_number else []
    candidates += sorted(task_dir.glob("attempt=*.log"), key=lambda p: p.stat().st_mtime)[::-1]
    for path in candidates:
        if path.is_file():
            with path.open("rb") as fh:
                fh.seek(0, os.SEEK_END)
                fh.seek(max(0, fh.tell() - 64 * 1024))
                text = fh.read().decode("utf-8", errors="replace")
            tail = "\n".join(text.splitlines()[-LOG_TAIL_LINES:])
            with path.open(encoding="utf-8", errors="replace") as fh:
                # one line per distinct error, annotated with how often it occurred
                earlier = [e.annotated() for e in terminal_errors(fh) if e.line not in tail]
            if not earlier:
                return tail[-LOG_TAIL_MAX_CHARS:]
            head = "\n".join([EARLIER_HEADER, *earlier, TAIL_HEADER]) + "\n"
            return head + tail[-(LOG_TAIL_MAX_CHARS - len(head)) :]
    return None


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else (str(value) if value else None)


def _emit(context: dict[str, Any], state: str) -> None:
    try:
        ti = context["ti"]
        exc = context.get("exception")
        ctx = RunContext(
            pipeline=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            logical_date=_iso(context.get("logical_date")),
            try_number=ti.try_number,
            code_version=code_version_for(ti.dag_id),
        )
        finished = ti.end_date or datetime.now(UTC)
        duration = ti.duration
        if duration is None and ti.start_date is not None:
            duration = (finished - ti.start_date).total_seconds()

        sink = sink_from_env()
        sink.emit(
            ctx.event(
                TaskRun(
                    task_id=ti.task_id,
                    operator=getattr(ti, "operator", None),
                    state=state,
                    try_number=ti.try_number,
                    started_at=ti.start_date,
                    finished_at=finished,
                    duration_s=round(duration, 3) if duration is not None else None,
                    hostname=getattr(ti, "hostname", None),
                    exception=str(exc)[:EXCEPTION_MAX_CHARS] if exc else None,
                )
            )
        )
        if state != "success":
            sink.emit(
                ctx.event(
                    JobLog(
                        level="ERROR",
                        logger="airflow.task",
                        message=f"task {ti.task_id} {state}: {exc}"[:EXCEPTION_MAX_CHARS],
                        exc_type=type(exc).__name__ if exc else None,
                        traceback=log_tail(ti.dag_id, ti.run_id, ti.task_id, ti.try_number),
                    )
                )
            )
        # Callbacks may run in a process that exits via os._exit, so flush explicitly.
        sink.flush()
    except Exception:
        log.warning("PipelineSentinel callback failed (task outcome unaffected)", exc_info=True)


def on_success(context: dict[str, Any]) -> None:
    _emit(context, "success")


def on_failure(context: dict[str, Any]) -> None:
    _emit(context, "failed")


def on_retry(context: dict[str, Any]) -> None:
    _emit(context, "up_for_retry")
