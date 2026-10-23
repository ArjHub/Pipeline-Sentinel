"""Audit log of every human action request (lake/silver/audit), accepted or refused.

Append-only Delta. It is also the source of truth for human incident states: the
detector overlays the latest *accepted* action per incident (see `human_states`) when
it recomputes incidents, so `dismissed` / `retrying` survive every detection pass
without the detector owning them.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

AUDIT_SCHEMA = pa.schema(
    [
        ("audit_id", pa.string()),
        ("at", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),  # slack | mock-slack | cli
        ("actor", pa.string()),  # Slack user id/name as sent by Slack
        ("action", pa.string()),  # retry | retry_confirmed | dismiss
        ("incident_id", pa.string()),
        ("decision", pa.string()),  # accepted | refused | error
        ("reason", pa.string()),
        ("new_status", pa.string()),  # retrying | dismissed | null
        ("targets", pa.string()),  # JSON [{dag_id, run_id, cleared}]
        ("diagnosis_id", pa.string()),
        ("retry_safe", pa.bool_()),
        ("request_ts", pa.string()),  # Slack X-Slack-Request-Timestamp
    ]
)
HUMAN_STATUS = {"retry": "retrying", "retry_confirmed": "retrying", "dismiss": "dismissed"}


def audit_path(lake: Path) -> Path:
    return lake / "silver" / "audit"


def record(lake: Path, **fields: Any) -> dict[str, Any]:
    at = fields.pop("at", None) or datetime.now(UTC)
    row = {k: None for k in AUDIT_SCHEMA.names}
    row.update(fields)
    row["at"] = at
    row["targets"] = json.dumps(fields.get("targets") or [])
    row["audit_id"] = (
        "AUD-"
        + hashlib.sha256(
            f"{at.isoformat()}|{row['incident_id']}|{row['action']}|{row['actor']}".encode()
        ).hexdigest()[:12]
    )
    path = audit_path(lake)
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), pa.Table.from_pylist([row], schema=AUDIT_SCHEMA), mode="append")
    return row


def read_all(lake: Path) -> list[dict[str, Any]]:
    path = audit_path(lake)
    if not DeltaTable.is_deltatable(str(path)):
        return []
    return sorted(DeltaTable(str(path)).to_pyarrow_table().to_pylist(), key=lambda r: r["at"])


def human_states(lake: Path) -> dict[str, tuple[str, datetime]]:
    """incident_id -> (status, when) of the latest accepted action."""
    out: dict[str, tuple[str, datetime]] = {}
    for r in read_all(lake):
        if r["decision"] == "accepted" and r["new_status"]:
            out[r["incident_id"]] = (r["new_status"], r["at"])
    return out
