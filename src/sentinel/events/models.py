"""Versioned event envelope shared by every producer and consumer.

Every event has the same envelope (who/when/where) and a typed payload selected by
`payload.kind`. Bump SCHEMA_VERSION on any breaking change to a payload.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = "1.0"


class EventType(StrEnum):
    JOB_LOG = "job_log"
    JOB_METRIC = "job_metric"
    DQ_RESULT = "dq_result"
    DEPLOY = "deploy_event"


# Kafka topic for each event type (topics are created by redpanda-init).
TOPIC_FOR: dict[EventType, str] = {
    EventType.JOB_LOG: "job_logs",
    EventType.JOB_METRIC: "job_metrics",
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


class ExpectationOutcome(_Payload):
    expectation_type: str
    column: str | None = None
    success: bool
    observed_value: Any = None
    unexpected_count: int | None = None
    unexpected_percent: float | None = None


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


Payload = Annotated[JobLog | JobMetric | DQResult | DeployEvent, Field(discriminator="kind")]

_KIND_TO_TYPE = {
    "job_log": EventType.JOB_LOG,
    "job_metric": EventType.JOB_METRIC,
    "dq_result": EventType.DQ_RESULT,
    "deploy_event": EventType.DEPLOY,
}


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = SCHEMA_VERSION
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    emitted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
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


class RunContext(BaseModel):
    """Where an event came from. Built from Airflow's AIRFLOW_CTX_* env vars in tasks."""

    pipeline: str
    run_id: str | None = None
    task_id: str | None = None
    logical_date: str | None = None
    try_number: int | None = None
    code_version: str = "unknown"

    def event(self, payload: JobLog | JobMetric | DQResult | DeployEvent) -> Event:
        return Event(
            event_type=_KIND_TO_TYPE[payload.kind],
            payload=payload,
            **self.model_dump(),
        )
