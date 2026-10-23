from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

from deltalake import DeltaTable

from sentinel.detect import detect, from_events, load_config
from sentinel.detect.store import read_incidents, upsert
from tests.unit.detect_fixtures import day_events

DS = "2026-10-20"
NOW = datetime(2026, 10, 21, 6, tzinfo=UTC)
CFG = load_config()


def _incidents(fault="missing_amount"):
    return detect(from_events(day_events(DS, fault=fault)), CFG, NOW)


def test_first_write_then_rerun_is_a_no_op(tmp_path) -> None:
    incidents = _incidents()
    res = upsert(tmp_path, incidents, NOW)
    assert len(res.new) == 2 and not res.updated
    version = DeltaTable(str(tmp_path)).version()
    again = upsert(tmp_path, _incidents(), NOW + timedelta(hours=1))
    assert len(again.unchanged) == 2 and not (again.new or again.updated or again.superseded)
    assert DeltaTable(str(tmp_path)).version() == version  # nothing written


def test_update_keeps_identity_and_detected_at(tmp_path) -> None:
    incidents = _incidents()
    upsert(tmp_path, incidents, NOW)
    later = NOW + timedelta(hours=2)
    resolved = [dataclasses.replace(i, status="resolved") for i in incidents]
    res = upsert(tmp_path, resolved, later)
    assert len(res.updated) == 2
    rows = {r["incident_id"]: r for r in read_incidents(tmp_path)}
    assert len(rows) == 2
    for inc in incidents:
        row = rows[inc.incident_id]
        assert row["status"] == "resolved"
        assert row["detected_at"] == NOW and row["updated_at"] == later


def test_first_seen_never_moves_later(tmp_path) -> None:
    [inc, *_] = _incidents()
    upsert(tmp_path, [inc], NOW)
    upsert(tmp_path, [dataclasses.replace(inc, first_seen=inc.first_seen + timedelta(days=1))], NOW)
    [row] = [r for r in read_incidents(tmp_path) if r["incident_id"] == inc.incident_id]
    assert row["first_seen"] == inc.first_seen


def test_open_incident_no_longer_derived_is_superseded(tmp_path) -> None:
    upsert(tmp_path, _incidents("missing_amount"), NOW)
    res = upsert(tmp_path, [], NOW + timedelta(hours=1))
    assert len(res.superseded) == 2
    assert {r["status"] for r in read_incidents(tmp_path)} == {"superseded"}
    # Superseded rows are left alone afterwards.
    assert not upsert(tmp_path, [], NOW + timedelta(hours=2)).superseded


def test_reading_a_missing_table_is_empty(tmp_path) -> None:
    assert read_incidents(tmp_path / "nope") == []


def test_evidence_is_json_and_round_trips(tmp_path) -> None:
    import json

    upsert(tmp_path, _incidents("stale_customers"), NOW)
    ev = [json.loads(r["evidence"]) for r in read_incidents(tmp_path)]
    assert all(e["affected_ds"] == [DS] for e in ev)
