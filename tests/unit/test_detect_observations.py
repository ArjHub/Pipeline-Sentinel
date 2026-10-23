from __future__ import annotations

from datetime import date, timedelta

from sentinel.detect.observations import from_events, from_rows, load_bronze
from sentinel.lander.bronze import BronzeWriter, RawMessage
from tests.unit.detect_fixtures import Run, day_events, end_of

DS = "2026-10-20"
T0 = end_of(DS)


def test_job_error_is_attached_to_its_task_attempt() -> None:
    r = Run("orders_ingest", DS, T0)
    r.job("ingest_orders", "orders_ingest", status="failed", error_type="SchemaMismatchError")
    r.task("ingest_orders", "failed")
    [attempt] = from_events(r.events).attempts
    assert (attempt.state, attempt.error_type) == ("failed", "SchemaMismatchError")


def test_job_metric_stands_in_when_the_callback_never_arrived() -> None:
    r = Run("orders_ingest", DS, T0)
    r.job("ingest_orders", "orders_ingest", status="failed", error_type="OOMError")
    [attempt] = from_events(r.events).attempts
    assert (attempt.task_id, attempt.state, attempt.error_type) == (
        "ingest_orders",
        "failed",
        "OOMError",
    )


def test_events_without_a_usable_logical_date_are_ignored_but_counted() -> None:
    r = Run("orders_ingest", DS, T0)
    ev = r.task("ingest_orders")
    rows = [
        {
            **ev.model_dump(mode="python", exclude={"payload"}),
            "event_type": "task_run",
            "payload": ev.payload.model_dump(mode="json"),
            "logical_date": ld,
        }
        for ld in (None, "bad", "2026-13-45T00:00:00")
    ]
    obs = from_rows(rows)
    assert obs.attempts == [] and obs.events_read == {"task_run": 3}


def test_load_bronze_reads_landed_events_and_prunes_old_partitions(tmp_path) -> None:
    events = day_events(DS)
    msgs = [
        RawMessage(e.topic, 0, i, e.pipeline.encode(), e.model_dump_json().encode())
        for i, e in enumerate(events)
    ]
    BronzeWriter(tmp_path).land(msgs)
    obs = load_bronze(tmp_path, date.fromisoformat(DS))
    expected = from_events(events)
    assert len(obs.attempts) == len(expected.attempts)
    assert len(obs.jobs) == len(expected.jobs) and len(obs.dq) == len(expected.dq)
    assert obs.jobs[0].input_schema == expected.jobs[0].input_schema
    # Events were emitted on DS+1, so a cutoff after that reads nothing.
    later = load_bronze(tmp_path, date.fromisoformat(DS) + timedelta(days=2))
    assert later.jobs == [] and later.attempts == []
    assert load_bronze(tmp_path / "missing", date.fromisoformat(DS)).jobs == []
