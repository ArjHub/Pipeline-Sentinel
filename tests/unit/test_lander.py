"""Bronze landing on real Delta tables (delta-rs): idempotency, dead-lettering, layout."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from deltalake import DeltaTable

from sentinel.events import DQResult, Event, JobLog, JobMetric, RunContext, TaskRun
from sentinel.lander.bronze import DEAD_LETTER, BronzeWriter, RawMessage

CTX = RunContext(
    pipeline="orders_ingest",
    run_id="run_1",
    task_id="ingest_orders",
    logical_date="2026-10-20T00:00:00+00:00",
    code_version="abc1234",
)
NOW = datetime(2026, 10, 21, 9, 30, tzinfo=UTC)


def _events() -> list[Event]:
    return [
        CTX.event(JobLog(level="INFO", logger="x", message="started")),
        CTX.event(JobLog(level="ERROR", logger="x", message="boom", exc_type="KeyError")),
        CTX.event(
            JobMetric(
                job_name="orders_ingest",
                status="success",
                started_at=NOW,
                finished_at=NOW,
                duration_s=1.5,
                rows_in=10,
                rows_out=10,
            )
        ),
        CTX.event(TaskRun(task_id="ingest_orders", state="success", try_number=1)),
        CTX.event(
            DQResult(
                dataset="orders",
                suite_name="orders.v1",
                success=True,
                evaluated=13,
                failed=0,
                row_count=10,
                results=[],
            )
        ),
    ]


def _messages(events: list[Event], start_offset: int = 0) -> list[RawMessage]:
    return [
        RawMessage(e.topic, 0, start_offset + i, e.pipeline.encode(), e.model_dump_json().encode())
        for i, e in enumerate(events)
    ]


def _count(writer: BronzeWriter, table: str) -> int:
    path = writer.table_path(table)
    return len(DeltaTable(str(path)).to_pandas()) if DeltaTable.is_deltatable(str(path)) else 0


@pytest.fixture
def writer(tmp_path) -> BronzeWriter:
    return BronzeWriter(tmp_path / "bronze")


def test_one_table_per_topic_with_expected_counts(writer) -> None:
    result = writer.land(_messages(_events()))
    assert dict(result.inserted) == {"job_logs": 2, "job_metrics": 2, "dq_results": 1}
    assert _count(writer, "job_logs") == 2
    assert _count(writer, "job_metrics") == 2  # job_metric + task_run share the topic
    assert _count(writer, "dq_results") == 1
    assert _count(writer, DEAD_LETTER) == 0


def test_replaying_the_same_events_creates_no_duplicates(writer) -> None:
    events = _events()
    writer.land(_messages(events))
    # Same events, re-delivered at new offsets (what a journal replay looks like).
    again = writer.land(_messages(events, start_offset=100))
    assert sum(again.inserted.values()) == 0
    assert sum(again.duplicates.values()) == len(events)
    assert _count(writer, "job_logs") == 2 and _count(writer, "job_metrics") == 2


def test_duplicates_inside_one_batch_are_dropped(writer) -> None:
    [ev] = _events()[:1]
    result = writer.land(_messages([ev, ev, ev]))
    assert result.inserted["job_logs"] == 1
    assert result.duplicates["job_logs"] == 2


def test_new_events_still_land_after_a_replay(writer) -> None:
    events = _events()
    writer.land(_messages(events))
    fresh = CTX.event(JobLog(level="INFO", logger="x", message="new"))
    result = writer.land(_messages([events[0], fresh]))
    assert result.inserted["job_logs"] == 1
    assert result.duplicates["job_logs"] == 1


def test_rows_keep_envelope_columns_and_full_payload(writer) -> None:
    writer.land(_messages(_events()))
    df = DeltaTable(str(writer.table_path("job_logs"))).to_pandas()
    row = df[df["payload"].str.contains("boom")].iloc[0]
    assert (row.pipeline, row.run_id, row.task_id, row.code_version) == (
        "orders_ingest",
        "run_1",
        "ingest_orders",
        "abc1234",
    )
    assert row.logical_date.startswith("2026-10-20")
    assert json.loads(row.payload)["exc_type"] == "KeyError"
    assert (row.kafka_topic, row.kafka_partition) == ("job_logs", 0)


def test_partitioned_by_emitted_date(writer) -> None:
    ev = Event.model_validate(
        {**json.loads(_events()[0].model_dump_json()), "emitted_at": "2026-10-19T23:59:59Z"}
    )
    writer.land(_messages([ev]))
    table = DeltaTable(str(writer.table_path("job_logs")))
    assert table.metadata().partition_columns == ["event_date"]
    assert table.to_pandas()["event_date"].tolist() == ["2026-10-19"]


@pytest.mark.parametrize(
    ("value", "error_fragment"),
    [
        (b"not json at all", "Invalid JSON"),
        (None, "empty message value"),
        (json.dumps({"schema_version": "9.0"}).encode(), "Field required"),
    ],
)
def test_invalid_messages_go_to_dead_letter_with_error(writer, value, error_fragment) -> None:
    good = _events()[0]
    msgs = [RawMessage("job_logs", 2, 7, b"k", value), *_messages([good], start_offset=8)]
    result = writer.land(msgs)
    assert result.dead_letters["job_logs"] == 1
    assert result.inserted["job_logs"] == 1  # one bad message doesn't block the batch
    dl = DeltaTable(str(writer.table_path(DEAD_LETTER))).to_pandas()
    assert len(dl) == 1
    assert error_fragment in dl.iloc[0]["error"]
    assert (dl.iloc[0]["kafka_partition"], dl.iloc[0]["kafka_offset"]) == (2, 7)


def test_unsupported_schema_major_is_dead_lettered(writer) -> None:
    doc = json.loads(_events()[0].model_dump_json())
    doc["schema_version"] = "2.0"
    writer.land([RawMessage("job_logs", 0, 0, None, json.dumps(doc).encode())])
    dl = DeltaTable(str(writer.table_path(DEAD_LETTER))).to_pandas()
    assert "unsupported schema_version '2.0'" in dl.iloc[0]["error"]
    assert _count(writer, "job_logs") == 0


def test_event_on_wrong_topic_is_dead_lettered(writer) -> None:
    ev = _events()[0]  # a job_log
    writer.land([RawMessage("dq_results", 0, 0, None, ev.model_dump_json().encode())])
    dl = DeltaTable(str(writer.table_path(DEAD_LETTER))).to_pandas()
    assert "belongs on topic 'job_logs', received on 'dq_results'" in dl.iloc[0]["error"]


def test_dead_letter_is_idempotent_per_offset(writer) -> None:
    bad = RawMessage("job_logs", 0, 42, None, b"{{{")
    writer.land([bad])
    writer.land([bad])
    assert _count(writer, DEAD_LETTER) == 1
