"""`instrumented_job` wraps a job body and emits one `job_metric` event on exit.

We use a Python wrapper rather than a JVM SparkListener: it captures what diagnosis
needs (duration, row counts, observed input schema, exception) and is testable
without Spark.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from sentinel.events.models import ColumnSpec, JobMetric, RunContext
from sentinel.events.sinks import EventSink

log = logging.getLogger("sentinel.job")

_MAX_ERROR_CHARS = 2000


@dataclass
class JobStats:
    rows_in: int | None = None
    rows_out: int | None = None
    input_schema: list[ColumnSpec] | None = None
    output_path: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@contextmanager
def instrumented_job(job_name: str, ctx: RunContext, sink: EventSink) -> Iterator[JobStats]:
    stats = JobStats()
    started = datetime.now(UTC)
    t0 = time.monotonic()
    status = "success"
    error_type = error_message = None
    log.info("job started: %s", job_name)
    try:
        yield stats
    except BaseException as exc:
        status = "failed"
        error_type = type(exc).__name__
        error_message = str(exc)[:_MAX_ERROR_CHARS]
        log.exception("job failed: %s: %s", job_name, error_type)
        raise
    finally:
        metric = JobMetric(
            job_name=job_name,
            status=status,
            started_at=started,
            finished_at=datetime.now(UTC),
            duration_s=round(time.monotonic() - t0, 3),
            rows_in=stats.rows_in,
            rows_out=stats.rows_out,
            input_schema=stats.input_schema,
            output_path=stats.output_path,
            error_type=error_type,
            error_message=error_message,
            extra=stats.extra,
        )
        try:
            sink.emit(ctx.event(metric))
        except Exception:
            log.warning("failed to emit job_metric for %s", job_name, exc_info=True)
        if status == "success":
            log.info(
                "job finished: %s rows_in=%s rows_out=%s duration_s=%s",
                job_name,
                stats.rows_in,
                stats.rows_out,
                metric.duration_s,
            )
