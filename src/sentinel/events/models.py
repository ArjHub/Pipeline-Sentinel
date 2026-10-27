"""Versioned event envelope shared by every producer and consumer.

Every event has the same envelope (who/when/where) and a typed payload selected by
`payload.kind`.

Versioning policy (SCHEMA_VERSION = "MAJOR.MINOR"):
- MINOR bump for additive changes (a new payload kind, a new optional field). Consumers
  accept any MINOR of a MAJOR they support, so old events keep parsing.
- MAJOR bump for anything breaking (removing/renaming/retyping a field). Consumers reject
  unsupported MAJORs into the dead-letter table rather than guessing.

History: 1.0 initial; 1.1 adds `task_run` (Airflow callbacks) and git fields on deploys;
1.2 adds `unexpected_values` and `missing_count` to DQ expectation outcomes.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

SCHEMA_VERSION = "1.2"
SUPPORTED_MAJORS = {"1"}


class EventType(StrEnum):
    JOB_LOG = "job_log"
    JOB_METRIC = "job_metric"
    DQ_RESULT = "dq_result"
    DEPLOY = "deploy_event"
    TASK_RUN = "task_run"


# Kafka topic for each event type (topics are created by redpanda-init). Task-run facts
# from Airflow callbacks share `job_metrics` with job metrics: both describe how an
# execution went, and the detector reads them together.
TOPIC_FOR: dict[EventType, str] = {
    EventType.JOB_LOG: "job_logs",
    EventType.JOB_METRIC: "job_metrics",
    EventType.TASK_RUN: "job_metrics",
    EventType.DQ_RESULT: "dq_results",
    EventType.DEPLOY: "deploy_events",
}


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ColumnSpec(_Payload):
    name: str
    type: str
    nullable: bool = True


class JobLog(_Payload):
    kind: Literal["job_log"] = "job_log"
    level: str
    logger: str
    message: str
    exc_type: str | None = None
    traceback: str | None = None


class JobMetric(_Payload):
    kind: Literal["job_metric"] = "job_metric"
    job_name: str
    status: Literal["success", "failed"]
    started_at: datetime
    finished_at: datetime
    duration_s: float
    rows_in: int | None = None
    rows_out: int | None = None
    input_schema: list[ColumnSpec] | None = None
    output_path: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class UnexpectedValue(_Payload):
    value: Any = None
    count: int


class ExpectationOutcome(_Payload):
    expectation_type: str
    column: str | None = None
    success: bool
    observed_value: Any = None
    unexpected_count: int | None = None
    unexpected_percent: float | None = None
    # Column-map expectations have no single observed value; these say what was actually
    # in the column: a sample of the unexpected values with their counts (GX's
    # partial_unexpected_counts), and how many values were null.
    unexpected_values: list[UnexpectedValue] | None = None
    missing_count: int | None = None


class DQResult(_Payload):
    kind: Literal["dq_result"] = "dq_result"
    dataset: str
    suite_name: str
    success: bool
    evaluated: int
    failed: int
    row_count: int
    results: list[ExpectationOutcome]


class DeployEvent(_Payload):
    kind: Literal["deploy_event"] = "deploy_event"
    component: str
    code_version: str
    previous_version: str | None = None
    changed_files: list[str] = Field(default_factory=list)
    author: str | None = None
    message: str | None = None
    git_sha: str | None = None  # 1.1
    dirty: bool | None = None  # 1.1: uncommitted changes were deployed


class TaskRun(_Payload):
    """One Airflow task attempt finishing, from on_success/on_failure/on_retry callbacks.

    This catches failures that the job's own instrumentation can't see, such as a
    process killed by the OOM killer, a sensor timeout, or a bad bash command."""

    kind: Literal["task_run"] = "task_run"
    task_id: str
    operator: str | None = None
    state: Literal["success", "failed", "up_for_retry"]
    try_number: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_s: float | None = None
    hostname: str | None = None
    exception: str | None = None


AnyPayload = JobLog | JobMetric | DQResult | DeployEvent | TaskRun
Payload = Annotated[AnyPayload, Field(discriminator="kind")]

_KIND_TO_TYPE = {
    "job_log": EventType.JOB_LOG,
    "job_metric": EventType.JOB_METRIC,
    "dq_result": EventType.DQ_RESULT,
    "deploy_event": EventType.DEPLOY,
    "task_run": EventType.TASK_RUN,
}


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = SCHEMA_VERSION
    # Required on the wire, no defaults: if a consumer filled these in, a replayed event
    # would get a new id (or a new emitted date, hence partition) and dedupe would break.
    # Producers get them from RunContext.event().
    event_id: str = Field(min_length=1)
    event_type: EventType
    emitted_at: datetime
    pipeline: str
    run_id: str | None = None
    task_id: str | None = None
    logical_date: str | None = None
    try_number: int | None = None
    code_version: str = "unknown"
    payload: Payload

    @property
    def topic(self) -> str:
        return TOPIC_FOR[self.event_type]

    @model_validator(mode="after")
    def _check_consistency(self) -> Event:
        major = self.schema_version.split(".", 1)[0]
        if major not in SUPPORTED_MAJORS:
            raise ValueError(
                f"unsupported schema_version {self.schema_version!r} "
                f"(supported majors: {sorted(SUPPORTED_MAJORS)})"
            )
        if _KIND_TO_TYPE[self.payload.kind] != self.event_type:
            raise ValueError(
                f"event_type {self.event_type.value!r} does not match payload kind "
                f"{self.payload.kind!r}"
            )
        return self


class InvalidEventError(ValueError):
    """Raised by `parse_event`; the message is what lands in the dead-letter table."""


def parse_event(raw: bytes | str) -> Event:
    """Strictly parse one serialized event (the single gate every consumer uses)."""
    try:
        return Event.model_validate_json(raw)
    except ValidationError as exc:
        errors = "; ".join(
            f"{'.'.join(map(str, e['loc'])) or '<root>'}: {e['msg']}" for e in exc.errors()
        )
        raise InvalidEventError(errors) from exc


class RunContext(BaseModel):
    """Where an event came from. Built from Airflow's AIRFLOW_CTX_* env vars in tasks."""

    pipeline: str
    run_id: str | None = None
    task_id: str | None = None
    logical_date: str | None = None
    try_number: int | None = None
    code_version: str = "unknown"

    def event(self, payload: AnyPayload) -> Event:
        return Event(
            event_id=str(uuid.uuid4()),
            emitted_at=datetime.now(UTC),
            event_type=_KIND_TO_TYPE[payload.kind],
            payload=payload,
            **self.model_dump(),
        )
