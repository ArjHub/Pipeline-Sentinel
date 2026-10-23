"""incident -> agent -> diagnoses table -> Slack, the CLI, the detector hook and the mock."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from sentinel.detect.store import incidents_path, read_incidents
from sentinel.diagnose import store
from sentinel.diagnose.__main__ import main
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import RuleBasedLLM, ScriptedLLM, response, tool_use
from sentinel.diagnose.run import DiagnosisWorker, NotDiagnosable, diagnose_incident
from sentinel.notify.slack import MockSlack, incident_message
from tests.unit.diagnose_fixtures import build_lake
from tests.unit.test_diagnose_agent import answer, final


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setenv("SENTINEL_LEDGER_PATH", str(tmp_path / "ledger.json"))
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.setenv("SENTINEL_MOCK_SLACK_PATH", str(tmp_path / "slack.jsonl"))
    inc = build_lake(tmp_path)
    return tmp_path, inc


def test_diagnosis_is_stored_with_tokens_cost_and_prompt_version_then_posted(lake) -> None:
    path, inc = lake
    iid = inc["incident_id"]
    llm = ScriptedLLM([response([tool_use("get_error_logs")], "tool_use"), final(answer(iid))])
    mock = MockSlack(path / "slack.jsonl")
    out = diagnose_incident(path, iid, llm=llm, notifier=mock, ledger=Ledger(path / "ledger.json"))
    row = store.latest(path, iid)
    assert row["status"] == "ok" and row["model"] == "claude-haiku-5-5"
    assert row["prompt_version"] == "diagnosis_v1" and len(row["prompt_sha256"]) == 64
    assert (row["input_tokens"], row["output_tokens"]) == (2000, 400)
    assert row["cost_usd"] == pytest.approx(2 * (1000 * 0.10 + 200 * 0.50) / 1e6)
    assert row["top_category"] == "schema_drift" and row["retry_safe"] is False
    assert json.loads(row["result"])["incident_id"] == iid and row["latency_s"] >= 0
    [msg] = mock.records()
    assert msg["id"] == out.message_id and msg["incident_id"] == iid


def test_block_kit_message_has_summary_cause_confidence_evidence_fix_and_buttons(lake) -> None:
    path, inc = lake
    iid = inc["incident_id"]
    diagnose_incident(
        path,
        iid,
        llm=ScriptedLLM([response([tool_use("get_error_logs")], "tool_use"), final(answer(iid))]),
        notifier=MockSlack(path / "x.jsonl"),
        ledger=Ledger(path / "l.json"),
    )
    msg = incident_message(inc, store.latest(path, iid))
    text = json.dumps(msg["blocks"])
    for needle in (
        "schema_drift",
        "90%",
        "missing required columns",
        "Suggested fix",
        "restore amount",
        "diagnosis_v1",
        "Retry safe",
    ):
        assert needle in text, needle
    actions = msg["blocks"][-1]
    assert [e["action_id"] for e in actions["elements"]] == ["retry", "dismiss"]
    assert "style" not in actions["elements"][0]  # not highlighted: retry_safe is false
    assert json.loads(actions["elements"][0]["value"]) == {
        "incident_id": iid,
        "diagnosis_id": store.latest(path, iid)["diagnosis_id"],
    }


def test_without_an_llm_the_incident_is_posted_without_a_diagnosis(lake) -> None:
    path, inc = lake
    mock = MockSlack(path / "slack.jsonl")
    out = diagnose_incident(path, inc["incident_id"], notifier=mock)
    assert out.run is None and store.read_all(path) == []
    assert "No diagnosis available" in json.dumps(mock.records()[0]["blocks"])


def test_rule_based_mock_produces_a_valid_labelled_diagnosis_for_free(lake) -> None:
    path, inc = lake
    out = diagnose_incident(
        path,
        inc["incident_id"],
        llm=RuleBasedLLM(),
        notifier=MockSlack(path / "s.jsonl"),
        ledger=Ledger(path / "l.json"),
    )
    assert out.run.status == "ok" and out.row["model"] == "mock-rule-based"
    assert out.row["top_category"] == "schema_drift" and out.row["cost_usd"] == 0
    assert out.run.diagnosis.summary.startswith("[MOCK")
    assert not (path / "l.json").exists()  # mock calls are not billable, nothing recorded


def test_downstream_and_unknown_incidents_are_not_diagnosable(lake) -> None:
    path, _ = lake
    down = next(r for r in read_incidents(incidents_path(path)) if r["role"] == "downstream")
    with pytest.raises(NotDiagnosable, match="downstream"):
        diagnose_incident(path, down["incident_id"])
    with pytest.raises(NotDiagnosable, match="no incident"):
        diagnose_incident(path, "INC-0000000000")


def test_cli_dry_run_mock_and_no_key(lake, capsys) -> None:
    path, inc = lake
    iid = inc["incident_id"]
    assert main(["--incident", iid, "--lake", str(path), "--dry-run", "--no-notify"]) == 0
    out = capsys.readouterr().out
    assert "status=dry_run" in out and "worst case $" in out and "of $10.00" in out
    assert main(["--incident", iid, "--lake", str(path)]) == 2
    assert "ANTHROPIC_API_KEY is not set" in capsys.readouterr().out
    assert main(["--incident", iid, "--lake", str(path), "--mock"]) == 0
    assert '"category": "schema_drift"' in capsys.readouterr().out


def test_worker_diagnoses_in_the_background_and_survives_failures(lake, monkeypatch) -> None:
    path, inc = lake
    monkeypatch.setenv("SENTINEL_LLM", "mock")
    w = DiagnosisWorker(path)
    w.submit(SimpleNamespace(incident_id=inc["incident_id"])).result(timeout=60)
    w.submit(SimpleNamespace(incident_id="INC-missing")).result(timeout=60)  # logged only
    w.shutdown()
    assert store.latest(path, inc["incident_id"])["model"] == "mock-rule-based"
    assert len(MockSlack(path / "slack.jsonl").records()) == 1
