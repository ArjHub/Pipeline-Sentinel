"""Land raw Kafka messages into Delta bronze tables, one table per topic.

Idempotency: a row's identity is its `event_id`. New rows are written with a Delta
MERGE ... WHEN NOT MATCHED INSERT, so re-delivering or replaying the same events never
creates duplicates. Combined with "commit Kafka offsets only after the Delta write
succeeds" (service.py), that gives at-least-once delivery with exactly-once rows.

Bronze keeps the full payload as JSON text. Typing and interpreting payloads is the
detector's job (Phase 3); bronze is the faithful, replayable record.

Invalid messages (bad JSON, schema violations, unsupported schema_version, an event on
the wrong topic) go to the `_dead_letter` table along with the validation error.
"""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from sentinel.events.models import TOPIC_FOR, Event, InvalidEventError, parse_event

TOPICS = sorted(set(TOPIC_FOR.values()))
DEAD_LETTER = "_dead_letter"
MAX_RAW_CHARS = 100_000

BRONZE_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("schema_version", pa.string()),
        ("event_type", pa.string()),
        ("pipeline", pa.string()),
        ("run_id", pa.string()),
        ("task_id", pa.string()),
        ("logical_date", pa.string()),
        ("try_number", pa.int64()),
        ("code_version", pa.string()),
        ("emitted_at", pa.timestamp("us", tz="UTC")),
        ("payload", pa.string()),
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("landed_at", pa.timestamp("us", tz="UTC")),
        ("event_date", pa.string()),  # partition column: UTC date of emitted_at
    ]
)

DEAD_LETTER_SCHEMA = pa.schema(
    [
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("key", pa.string()),
        ("raw_value", pa.string()),
        ("error", pa.string()),
        ("received_at", pa.timestamp("us", tz="UTC")),
        ("received_date", pa.string()),  # partition column
    ]
)


@dataclass(frozen=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


@dataclass
class LandResult:
    inserted: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    duplicates: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    dead_letters: dict[str, int] = field(default_factory=lambda: defaultdict(int))


def bronze_root() -> Path:
    return Path(os.environ.get("SENTINEL_LAKE_ROOT", "data/lake")) / "bronze"


def _decode(b: bytes | None) -> str | None:
    return None if b is None else b.decode("utf-8", errors="replace")


def _row(event: Event, msg: RawMessage, now: datetime) -> dict:
    return {
        "event_id": event.event_id,
        "schema_version": event.schema_version,
        "event_type": event.event_type.value,
        "pipeline": event.pipeline,
        "run_id": event.run_id,
        "task_id": event.task_id,
        "logical_date": event.logical_date,
        "try_number": event.try_number,
        "code_version": event.code_version,
        "emitted_at": event.emitted_at.astimezone(UTC),
        "payload": event.payload.model_dump_json(),
        "kafka_topic": msg.topic,
        "kafka_partition": msg.partition,
        "kafka_offset": msg.offset,
        "landed_at": now,
        "event_date": event.emitted_at.astimezone(UTC).strftime("%Y-%m-%d"),
    }


def _dead_row(msg: RawMessage, error: str, now: datetime) -> dict:
    raw = _decode(msg.value)
    return {
        "kafka_topic": msg.topic,
        "kafka_partition": msg.partition,
        "kafka_offset": msg.offset,
        "key": _decode(msg.key),
        "raw_value": raw[:MAX_RAW_CHARS] if raw is not None else None,
        "error": error[:5000],
        "received_at": now,
        "received_date": now.strftime("%Y-%m-%d"),
    }


def _merge_insert_new(
    path: Path, rows: list[dict], schema: pa.Schema, keys: list[str], partition: str
) -> int:
    """Insert rows whose key isn't in the table yet; return how many were inserted."""
    table = pa.Table.from_pylist(rows, schema=schema)
    if not DeltaTable.is_deltatable(str(path)):
        path.mkdir(parents=True, exist_ok=True)
        write_deltalake(str(path), table, partition_by=[partition])
        return table.num_rows
    # Including the partition column in the predicate lets Delta prune files: a replayed
    # event always has the same event_date, so only that partition is scanned.
    predicate = " AND ".join(f"t.{k} = s.{k}" for k in [*keys, partition])
    metrics = (
        DeltaTable(str(path))
        .merge(source=table, predicate=predicate, source_alias="s", target_alias="t")
        .when_not_matched_insert_all()
        .execute()
    )
    return int(metrics.get("num_target_rows_inserted", 0))


class BronzeWriter:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or bronze_root()

    def table_path(self, topic: str) -> Path:
        return self.root / topic

    def land(self, messages: list[RawMessage], now: datetime | None = None) -> LandResult:
        """`now` is the landed_at stamp (default: the current time)."""
        now = now or datetime.now(UTC)
        result = LandResult()
        rows: dict[str, dict[str, dict]] = defaultdict(dict)  # topic -> event_id -> row
        dead: list[dict] = []

        for msg in messages:
            try:
                if msg.value is None:
                    raise InvalidEventError("empty message value")
                event = parse_event(msg.value)
                if event.topic != msg.topic:
                    raise InvalidEventError(
                        f"event_type {event.event_type.value!r} belongs on topic "
                        f"{event.topic!r}, received on {msg.topic!r}"
                    )
            except InvalidEventError as exc:
                dead.append(_dead_row(msg, str(exc), now))
                result.dead_letters[msg.topic] += 1
                continue
            if event.event_id in rows[msg.topic]:
                result.duplicates[msg.topic] += 1  # duplicate within this batch
                continue
            rows[msg.topic][event.event_id] = _row(event, msg, now)

        for topic, by_id in rows.items():
            batch = list(by_id.values())
            inserted = _merge_insert_new(
                self.table_path(topic), batch, BRONZE_SCHEMA, ["event_id"], "event_date"
            )
            result.inserted[topic] += inserted
            result.duplicates[topic] += len(batch) - inserted
        if dead:
            _merge_insert_new(
                self.table_path(DEAD_LETTER),
                dead,
                DEAD_LETTER_SCHEMA,
                ["kafka_topic", "kafka_partition", "kafka_offset"],
                "received_date",
            )
        return result
