"""Structured JSON logging for jobs.

Every record goes to stdout as one JSON line (so it shows up readable in the Airflow
task log) and, at INFO and above, is also emitted as a `job_log` event.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from datetime import UTC, datetime

from sentinel.events.models import JobLog, RunContext
from sentinel.events.sinks import EventSink


class JsonFormatter(logging.Formatter):
    def __init__(self, ctx: RunContext) -> None:
        super().__init__()
        self.ctx = ctx

    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline": self.ctx.pipeline,
            "task_id": self.ctx.task_id,
            "run_id": self.ctx.run_id,
        }
        if record.exc_info and record.exc_info[0] is not None:
            doc["exc_type"] = record.exc_info[0].__name__
        return json.dumps(doc, default=str)


class EventLogHandler(logging.Handler):
    def __init__(self, ctx: RunContext, sink: EventSink, level: int = logging.INFO) -> None:
        super().__init__(level)
        self.ctx = ctx
        self.sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        exc_type = tb = None
        if record.exc_info and record.exc_info[0] is not None:
            exc_type = record.exc_info[0].__name__
            tb = "".join(traceback.format_exception(*record.exc_info))
        payload = JobLog(
            level=record.levelname,
            logger=record.name,
            message=record.getMessage(),
            exc_type=exc_type,
            traceback=tb,
        )
        try:
            self.sink.emit(self.ctx.event(payload))
        except Exception:  # never let telemetry break the job
            self.handleError(record)


def configure_logging(ctx: RunContext, sink: EventSink, level: int = logging.INFO) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(JsonFormatter(ctx))
    root.addHandler(stdout)
    root.addHandler(EventLogHandler(ctx, sink))
    # py4j is chatty at INFO and its lines are not useful job logs.
    logging.getLogger("py4j").setLevel(logging.WARNING)
