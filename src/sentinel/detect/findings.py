"""What a detector returns: one Finding per (problem, pipeline, ds).

A Finding is a *signal*, not yet an incident. `correlate.py` decides which findings are
one root problem and which pipeline is the primary incident.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class IncidentType(StrEnum):
    FAILED_JOB = "failed_job"
    SCHEMA_CHANGE = "schema_change"
    LATE_DATA = "late_data"
    VOLUME_ANOMALY = "volume_anomaly"


# When several findings describe one pipeline run, the most *specific cause* names the
# incident. A failed task is a symptom; a schema change, stale data or a volume shift is
# a reason. Among causes, a schema diff is the most concrete evidence, then lateness
# (stale data usually also shrinks volume, not the other way round).
CAUSE_PRIORITY = {
    IncidentType.SCHEMA_CHANGE: 3,
    IncidentType.LATE_DATA: 2,
    IncidentType.VOLUME_ANOMALY: 1,
    IncidentType.FAILED_JOB: 0,
}
SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}


@dataclass(frozen=True)
class Finding:
    type: IncidentType
    pipeline: str
    ds: str
    # What the problem is, independent of when: the same fingerprint on another day is
    # the same root problem recurring (e.g. "orders_ingest:-amount").
    fingerprint: str
    severity: str
    title: str
    first_observed: datetime
    last_observed: datetime
    # When a later, clean observation of the same check superseded this one
    # (e.g. the task's next attempt succeeded). None = still active.
    cleared_at: datetime | None
    task_id: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def active(self) -> bool:
        return self.cleared_at is None

    @property
    def is_cause(self) -> bool:
        return self.type is not IncidentType.FAILED_JOB

    def summary(self) -> dict[str, Any]:
        return {
            "type": self.type.value,
            "task_id": self.task_id,
            "fingerprint": self.fingerprint,
            "severity": self.severity,
            "active": self.active,
            "title": self.title,
            "first_observed": self.first_observed.isoformat(),
            "last_observed": self.last_observed.isoformat(),
            "cleared_at": self.cleared_at.isoformat() if self.cleared_at else None,
            "evidence": self.evidence,
        }


def max_severity(values: list[str]) -> str:
    return max(values, key=SEVERITY_RANK.__getitem__)
