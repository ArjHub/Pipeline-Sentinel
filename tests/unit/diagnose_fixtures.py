"""A temp lake for diagnosis tests: bronze from detect_fixtures plus the job_log events
the real jobs emit on failure, and incidents written by the real detection pass."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from pathlib import Path

from sentinel.detect import load_config as load_detection_config
from sentinel.detect.runner import run_once
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.events import DeployEvent, Event, EventType, JobLog
from sentinel.lander.bronze import BronzeWriter, RawMessage
from tests.unit.detect_fixtures import CODE_VERSION, NS, day_events, end_of, history

DS = "2026-10-20"
NOW = end_of(DS) + timedelta(hours=6)
TRACEBACK = (
    "Traceback (most recent call last):\n"
    '  File "/opt/sentinel/pipelines/jobs/orders_ingest.py", line 42, in run\n'
    '    require_columns(raw, REQUIRED, "orders")\n'
    "pipelines.jobs.spark.SchemaMismatchError: orders: missing required columns ['amount'];"
    " observed columns ['order_id', 'customer_id', 'order_ts', 'currency', 'status']"
)


def log_event(
    pipeline: str,
    task_id: str,
    message: str,
    at: datetime,
    *,
    level="ERROR",
    traceback: str | None = None,
    ds: str = DS,
) -> Event:
    return Event(
        event_id=str(uuid.uuid5(NS, f"log|{pipeline}|{task_id}|{at.isoformat()}|{message}")),
        event_type=EventType.JOB_LOG,
        emitted_at=at,
        pipeline=pipeline,
        run_id=f"run_all__{ds}",
        task_id=task_id,
        logical_date=f"{ds}T00:00:00+00:00",
        try_number=1,
        code_version=CODE_VERSION,
        payload=JobLog(
            level=level,
            logger="sentinel.job",
            message=message,
            exc_type="SchemaMismatchError" if traceback else None,
            traceback=traceback,
        ),
    )


def deploy_event(pipeline: str, at: datetime, version: str = CODE_VERSION) -> Event:
    return Event(
        event_id=str(uuid.uuid5(NS, f"deploy|{pipeline}|{at.isoformat()}")),
        event_type=EventType.DEPLOY,
        emitted_at=at,
        pipeline=pipeline,
        code_version=version,
        payload=DeployEvent(
            component=pipeline,
            code_version=version,
            previous_version="0ld0000",
            changed_files=[f"pipelines/jobs/{pipeline}.py"],
            author="dev",
            message="tune ingest",
            git_sha="abc1234def",
            dirty=False,
        ),
    )


def land(lake: Path, events: list[Event]) -> None:
    BronzeWriter(lake / "bronze").land(
        [
            RawMessage(e.topic, 0, i, None, e.model_dump_json().encode())
            for i, e in enumerate(events)
        ]
    )


def build_lake(lake: Path, fault: str = "missing_amount", extra: list[Event] = ()) -> dict:
    """Land 3 weeks of history + one faulty day, run detection, return the primary."""
    events = history(DS, 21) + day_events(DS, fault=fault)
    at = end_of(DS) + timedelta(minutes=31)
    if fault == "missing_amount":
        events.append(
            log_event(
                "orders_ingest",
                "ingest_orders",
                "job failed: orders_ingest: SchemaMismatchError",
                at,
                traceback=TRACEBACK,
            )
        )
    events.append(deploy_event("orders_ingest", end_of(DS) - timedelta(days=3)))
    land(lake, events + list(extra))
    run_once(lake, load_detection_config(), NOW)
    [primary] = [r for r in read_incidents(incidents_path(lake)) if r["role"] == "primary"]
    return primary
