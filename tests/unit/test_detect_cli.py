"""`python -m sentinel.detect` end to end on a temp lake: bronze written by the real
lander code, incidents written to Delta, idempotent on re-run."""

from __future__ import annotations

from sentinel.detect.__main__ import main
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.lander.bronze import BronzeWriter, RawMessage
from tests.unit.detect_fixtures import day_events, history

DS = "2026-10-20"
NOW = "2026-10-21T06:00:00+00:00"


def _land(lake, events) -> None:
    BronzeWriter(lake / "bronze").land(
        [
            RawMessage(e.topic, 0, i, None, e.model_dump_json().encode())
            for i, e in enumerate(events)
        ]
    )


def test_cli_detects_writes_and_is_idempotent(tmp_path, capsys) -> None:
    _land(tmp_path, history(DS, 21) + day_events(DS, fault="missing_amount"))
    assert main(["--lake", str(tmp_path), "--now", NOW]) == 0
    out = capsys.readouterr().out
    assert "incidents: 2 (primary=1, downstream=1)" in out
    assert "new=2 updated=0 unchanged=0 superseded=0" in out
    assert "schema_change   orders_ingest" in out and "└─" in out

    assert main(["--lake", str(tmp_path), "--now", NOW]) == 0
    assert "new=0 updated=0 unchanged=2 superseded=0" in capsys.readouterr().out
    assert len(read_incidents(incidents_path(tmp_path))) == 2


def test_dry_run_writes_nothing_and_clean_data_reports_none(tmp_path, capsys) -> None:
    _land(tmp_path, history(DS, 7) + day_events(DS))
    assert main(["--lake", str(tmp_path), "--now", "2026-10-21T06:00:00", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "findings: 0" in out and "(none)" in out and "written to" not in out
    assert not incidents_path(tmp_path).exists()
