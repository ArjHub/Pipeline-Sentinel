"""The Delta `diagnoses` table (lake/silver/diagnoses): one row per diagnosis attempt,
append-only. A re-diagnosis adds a row; `latest()` is what Slack and the retry gate use.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from sentinel.diagnose.agent import DiagnosisRun

DIAGNOSIS_SCHEMA = pa.schema(
    [
        ("diagnosis_id", pa.string()),
        ("incident_id", pa.string()),
        ("model", pa.string()),
        ("prompt_version", pa.string()),
        ("prompt_sha256", pa.string()),
        ("status", pa.string()),
        ("result", pa.string()),  # Diagnosis JSON (null for budget_refused / dry_run)
        ("top_category", pa.string()),
        ("top_confidence", pa.float64()),
        ("retry_safe", pa.bool_()),
        ("errors", pa.string()),  # JSON list
        ("llm_calls", pa.int64()),
        ("tool_calls", pa.int64()),
        ("tools_used", pa.string()),  # JSON list
        ("input_tokens", pa.int64()),
        ("output_tokens", pa.int64()),
        ("cache_read_input_tokens", pa.int64()),
        ("cache_creation_input_tokens", pa.int64()),
        ("cost_usd", pa.float64()),
        ("latency_s", pa.float64()),
        ("created_at", pa.timestamp("us", tz="UTC")),
    ]
)


def diagnoses_path(lake: Path) -> Path:
    return lake / "silver" / "diagnoses"


def to_row(run: DiagnosisRun, prompt_text: str, now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    d = run.diagnosis
    return {
        "diagnosis_id": "DX-"
        + hashlib.sha256(f"{run.incident_id}|{now.isoformat()}".encode()).hexdigest()[:12],
        "incident_id": run.incident_id,
        "model": run.model,
        "prompt_version": run.prompt_version,
        "prompt_sha256": hashlib.sha256(prompt_text.encode()).hexdigest(),
        "status": run.status,
        "result": d.model_dump_json() if d else None,
        "top_category": d.top.category if d else None,
        "top_confidence": d.top.confidence if d else None,
        "retry_safe": d.retry_safe if d else None,
        "errors": json.dumps(run.errors),
        "llm_calls": run.llm_calls,
        "tool_calls": run.tool_calls,
        "tools_used": json.dumps(run.tools_used),
        "input_tokens": run.usage.input_tokens,
        "output_tokens": run.usage.output_tokens,
        "cache_read_input_tokens": run.usage.cache_read_input_tokens,
        "cache_creation_input_tokens": run.usage.cache_creation_input_tokens,
        "cost_usd": round(run.cost_usd, 8),
        "latency_s": run.latency_s,
        "created_at": now,
    }


def append(lake: Path, row: dict[str, Any]) -> None:
    path = diagnoses_path(lake)
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), pa.Table.from_pylist([row], schema=DIAGNOSIS_SCHEMA), mode="append")


def read_all(lake: Path) -> list[dict[str, Any]]:
    path = diagnoses_path(lake)
    if not DeltaTable.is_deltatable(str(path)):
        return []
    rows = DeltaTable(str(path)).to_pyarrow_table().to_pylist()
    return sorted(rows, key=lambda r: r["created_at"])


def latest(lake: Path, incident_id: str) -> dict[str, Any] | None:
    rows = [r for r in read_all(lake) if r["incident_id"] == incident_id]
    return rows[-1] if rows else None
