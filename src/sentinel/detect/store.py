"""The Delta `incidents` table (lake/silver/incidents), upserted on dedup_key.

Detection recomputes every incident from bronze on each run, then writes only what
changed. Re-running over unchanged bronze writes nothing (no new Delta version), and
incident_id/first_seen/detected_at of an existing incident never change.

An incident that a later run no longer derives is marked `superseded`:
  - an open one always (for example its failed task was re-attributed as a symptom once
    the cause's event landed, or its events aged out of `history_days`), so nothing
    stays open forever;
  - a resolved one only if its data is still inside the bronze window being read
    (`window_start`), i.e. detection re-derived that period and it is no longer there
    (for example a downstream incident whose root became a recurrence #2). A resolved
    incident that merely aged out of the window is history and is left alone.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from sentinel.detect.correlate import Incident

INCIDENT_SCHEMA = pa.schema(
    [
        ("incident_id", pa.string()),
        ("pipeline", pa.string()),
        ("type", pa.string()),
        ("severity", pa.string()),
        ("evidence", pa.string()),  # JSON
        ("first_seen", pa.timestamp("us", tz="UTC")),
        ("last_seen", pa.timestamp("us", tz="UTC")),
        ("status", pa.string()),
        ("dedup_key", pa.string()),
        ("role", pa.string()),
        ("root_incident_id", pa.string()),
        ("title", pa.string()),
        ("detected_at", pa.timestamp("us", tz="UTC")),
        ("updated_at", pa.timestamp("us", tz="UTC")),
    ]
)
# Fields whose change counts as an update (detected_at/updated_at are bookkeeping).
_CONTENT = [
    "incident_id",
    "pipeline",
    "type",
    "severity",
    "evidence",
    "first_seen",
    "last_seen",
    "status",
    "role",
    "root_incident_id",
    "title",
]


@dataclass
class UpsertResult:
    new: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    superseded: list[str] = field(default_factory=list)


def incidents_path(lake_root: Path) -> Path:
    return lake_root / "silver" / "incidents"


def to_row(inc: Incident) -> dict[str, Any]:
    return {
        "incident_id": inc.incident_id,
        "pipeline": inc.pipeline,
        "type": inc.type,
        "severity": inc.severity,
        "evidence": json.dumps(inc.evidence, sort_keys=True, default=str),
        "first_seen": inc.first_seen,
        "last_seen": inc.last_seen,
        "status": inc.status,
        "dedup_key": inc.dedup_key,
        "role": inc.role,
        "root_incident_id": inc.root_incident_id,
        "title": inc.title,
    }


def read_incidents(path: Path) -> list[dict[str, Any]]:
    if not DeltaTable.is_deltatable(str(path)):
        return []
    return DeltaTable(str(path)).to_pyarrow_table().to_pylist()


def upsert(
    path: Path, incidents: list[Incident], now: datetime, window_start: datetime | None = None
) -> UpsertResult:
    existing = {r["dedup_key"]: r for r in read_incidents(path)}
    result = UpsertResult()
    changed: list[dict[str, Any]] = []

    for inc in incidents:
        row = to_row(inc)
        old = existing.get(inc.dedup_key)
        if old is None:
            changed.append({**row, "detected_at": now, "updated_at": now})
            result.new.append(inc.incident_id)
            continue
        row["first_seen"] = min(old["first_seen"], row["first_seen"])
        if all(old[k] == row[k] for k in _CONTENT):
            result.unchanged.append(inc.incident_id)
            continue
        changed.append({**row, "detected_at": old["detected_at"], "updated_at": now})
        result.updated.append(inc.incident_id)

    derived = {i.dedup_key for i in incidents}
    for key, old in existing.items():
        if key in derived:
            continue
        rederived = window_start is not None and old["last_seen"] >= window_start
        if old["status"] == "open" or (old["status"] == "resolved" and rederived):
            changed.append({**old, "status": "superseded", "updated_at": now})
            result.superseded.append(old["incident_id"])

    if changed:
        table = pa.Table.from_pylist(changed, schema=INCIDENT_SCHEMA)
        if not existing and not DeltaTable.is_deltatable(str(path)):
            path.mkdir(parents=True, exist_ok=True)
            write_deltalake(str(path), table)
        else:
            (
                DeltaTable(str(path))
                .merge(table, "t.dedup_key = s.dedup_key", source_alias="s", target_alias="t")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
    return result
