"""Bronze events -> typed records the detectors work on.

The detector reads only the bronze Delta tables (never Airflow or the pipelines), so
everything it knows comes from `job_metrics` (job_metric + task_run) and `dq_results`.

`ds` is the business date: the first 10 characters of the event's `logical_date`.
Events without one (deploys, ad-hoc runs) are ignored here.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sentinel.events.models import Event


@dataclass(frozen=True)
class TaskAttempt:
    """One finished attempt of one Airflow task, from its task_run callback (or, if the
    callback is missing, from the job's own job_metric)."""

    pipeline: str
    ds: str
    task_id: str
    try_number: int | None
    state: str  # success | failed | up_for_retry
    at: datetime
    run_id: str | None
    event_id: str
    exception: str | None = None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class JobRun:
    pipeline: str
    ds: str
    task_id: str | None
    job_name: str
    status: str  # success | failed
    at: datetime
    rows_in: int | None
    rows_out: int | None
    input_schema: tuple[tuple[str, str], ...] | None  # (name, type); None/empty = not observed
    error_type: str | None
    error_message: str | None
    try_number: int | None
    run_id: str | None
    code_version: str
    event_id: str


@dataclass(frozen=True)
class DQRun:
    pipeline: str
    ds: str
    task_id: str | None
    dataset: str
    at: datetime
    success: bool
    results: tuple[dict[str, Any], ...]
    try_number: int | None
    run_id: str | None
    event_id: str

    def outcome(self, expectation_type: str, column: str | None) -> dict[str, Any] | None:
        for r in self.results:
            if r.get("expectation_type") == expectation_type and r.get("column") == column:
                return r
        return None

    @property
    def failed_expectations(self) -> list[str]:
        return sorted(
            f"{r['expectation_type']}({r.get('column') or ''})"
            for r in self.results
            if not r.get("success")
        )


@dataclass
class Observations:
    attempts: list[TaskAttempt] = field(default_factory=list)
    jobs: list[JobRun] = field(default_factory=list)
    dq: list[DQRun] = field(default_factory=list)
    events_read: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.attempts.sort(key=lambda a: (a.at, a.event_id))
        self.jobs.sort(key=lambda j: (j.at, j.event_id))
        self.dq.sort(key=lambda d: (d.at, d.event_id))


def _ds(logical_date: str | None) -> str | None:
    if not logical_date or len(logical_date) < 10:
        return None
    try:
        return date.fromisoformat(logical_date[:10]).isoformat()
    except ValueError:
        return None


def from_rows(rows: Iterable[dict[str, Any]]) -> Observations:
    """Rows shaped like bronze: envelope columns + `payload` (dict or JSON string)."""
    attempts: list[TaskAttempt] = []
    jobs: list[JobRun] = []
    dq: list[DQRun] = []
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["event_type"]] = counts.get(row["event_type"], 0) + 1
        ds = _ds(row.get("logical_date"))
        if ds is None:
            continue
        p = row["payload"]
        p = json.loads(p) if isinstance(p, str) else p
        common = {
            "pipeline": row["pipeline"],
            "ds": ds,
            "at": row["emitted_at"],
            "run_id": row.get("run_id"),
            "event_id": row["event_id"],
            "try_number": row.get("try_number"),
        }
        kind = row["event_type"]
        if kind == "task_run":
            attempts.append(
                TaskAttempt(
                    task_id=p.get("task_id") or row.get("task_id") or "?",
                    state=p["state"],
                    exception=p.get("exception"),
                    **{**common, "try_number": p.get("try_number") or common["try_number"]},
                )
            )
        elif kind == "job_metric":
            schema = p.get("input_schema") or None
            jobs.append(
                JobRun(
                    task_id=row.get("task_id"),
                    job_name=p["job_name"],
                    status=p["status"],
                    rows_in=p.get("rows_in"),
                    rows_out=p.get("rows_out"),
                    input_schema=tuple((c["name"], c["type"]) for c in schema) if schema else None,
                    error_type=p.get("error_type"),
                    error_message=p.get("error_message"),
                    code_version=row.get("code_version") or "unknown",
                    **common,
                )
            )
        elif kind == "dq_result":
            dq.append(
                DQRun(
                    task_id=row.get("task_id"),
                    dataset=p["dataset"],
                    success=bool(p["success"]),
                    results=tuple(p.get("results") or ()),
                    **common,
                )
            )
    _merge_job_failures_into_attempts(attempts, jobs)
    return Observations(attempts, jobs, dq, counts)


def _merge_job_failures_into_attempts(attempts: list[TaskAttempt], jobs: list[JobRun]) -> None:
    """Attach the job's own error to its task attempt; if the callback never arrived,
    the job_metric stands in for the attempt."""
    index = {(a.pipeline, a.ds, a.task_id, a.try_number): i for i, a in enumerate(attempts)}
    for j in jobs:
        if j.task_id is None:
            continue
        key = (j.pipeline, j.ds, j.task_id, j.try_number)
        if key in index:
            a = attempts[index[key]]
            if j.status == "failed":
                attempts[index[key]] = TaskAttempt(
                    **{**a.__dict__, "error_type": j.error_type, "error_message": j.error_message}
                )
        else:
            attempts.append(
                TaskAttempt(
                    pipeline=j.pipeline,
                    ds=j.ds,
                    task_id=j.task_id,
                    try_number=j.try_number,
                    state=j.status,
                    at=j.at,
                    run_id=j.run_id,
                    event_id=j.event_id,
                    error_type=j.error_type,
                    error_message=j.error_message,
                )
            )
            index[key] = len(attempts) - 1


def from_events(events: Iterable[Event]) -> Observations:
    """Typed events -> Observations (tests build fixtures from real Event objects)."""
    return from_rows(
        {
            **e.model_dump(mode="python", exclude={"payload"}),
            "event_type": e.event_type.value,
            "payload": e.payload.model_dump(mode="json"),
        }
        for e in events
    )


BRONZE_COLUMNS = [
    "event_id",
    "event_type",
    "pipeline",
    "run_id",
    "task_id",
    "logical_date",
    "try_number",
    "code_version",
    "emitted_at",
    "payload",
]
DETECTOR_TOPICS = ("job_metrics", "dq_results")


def load_bronze(bronze_root: Path, since: date) -> Observations:
    """Read the detector's topics from bronze, pruning partitions older than `since`."""
    import pyarrow.dataset as pads
    from deltalake import DeltaTable

    rows: list[dict[str, Any]] = []
    for topic in DETECTOR_TOPICS:
        path = bronze_root / topic
        if not DeltaTable.is_deltatable(str(path)):
            continue
        dataset = DeltaTable(str(path)).to_pyarrow_dataset()
        table = dataset.to_table(
            columns=BRONZE_COLUMNS, filter=pads.field("event_date") >= since.isoformat()
        )
        rows.extend(table.to_pylist())
    return from_rows(rows)
