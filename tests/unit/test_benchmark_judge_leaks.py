"""The judge (fake LLM only), the leak scan, the frozen prompts and ledger purposes."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta

import pytest

from benchmark.harness import judge, leaks
from benchmark.harness.catalog import Fault, Label
from benchmark.harness.settings import load
from sentinel.diagnose.ledger import PURPOSES, Ledger
from sentinel.diagnose.llm import ScriptedLLM, response, text
from tests.unit.detect_fixtures import end_of
from tests.unit.diagnose_fixtures import DS, build_lake, land, log_event

ROOT = load().catalog_path.parents[2]
# The Phase 4 prompt, frozen for the benchmark (README: sha256 3507e5f0...b31d).
DIAGNOSIS_V1_SHA256 = "3507e5f05b152391fc4dce6539ff71302e204976fd83c687d5a3ba425038b31d"
JUDGE_V1_SHA256 = "e9fdaad0fb3b9c2c9c126f29959e310b83ab1e6cf990c9e6236518e3d2343fe3"


def _sha(path: str) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def test_prompts_are_frozen() -> None:
    assert _sha("prompts/diagnosis_v1.md") == DIAGNOSIS_V1_SHA256
    assert _sha("prompts/judge_v1.md") == JUDGE_V1_SHA256


def test_judge_verdict_is_ledgered_as_judge(tmp_path) -> None:
    ledger = Ledger(tmp_path / "l.json")
    llm = ScriptedLLM([response([text(json.dumps({"verdict": "partial", "reason": "r"}))])])
    v = judge.judge(
        llm,
        ledger,
        prompt="p",
        root_cause="truth",
        ref="F-X",
        diagnosis={
            "root_causes": [{"category": "unknown", "explanation": "e", "suggested_fix": "f"}]
        },
    )
    assert v.verdict == "partial" and v.cost_usd > 0
    calls = json.loads(ledger.path.read_text())["calls"]
    assert [c["purpose"] for c in calls] == ["judge"]
    req = llm.requests[0]
    assert "<ground_truth>\ntruth\n</ground_truth>" in req["messages"][0]["content"]
    assert req["output_config"]["format"]["schema"]["properties"]["verdict"]["enum"] == list(
        judge.VERDICTS
    )


def test_judge_bad_output_is_an_error_verdict_not_a_crash(tmp_path) -> None:
    llm = ScriptedLLM([response([text("not json")])])
    v = judge.judge(
        llm, Ledger(tmp_path / "l.json"), prompt="p", root_cause="t", diagnosis=None, ref="x"
    )
    assert v.verdict == "error"


def test_judge_is_budget_gated(tmp_path) -> None:
    from sentinel.diagnose.ledger import BudgetExceededError

    llm = ScriptedLLM([])
    with pytest.raises(BudgetExceededError):
        judge.judge(
            llm,
            Ledger(tmp_path / "l.json", budget_usd=0.0),
            prompt="p",
            root_cause="t",
            diagnosis=None,
            ref="x",
        )
    assert llm.requests == []


def test_ledger_rejects_unknown_purposes(tmp_path) -> None:
    from sentinel.diagnose.pricing import Usage

    ledger = Ledger(tmp_path / "l.json")
    assert PURPOSES == ("diagnosis", "benchmark", "judge", "test")
    with pytest.raises(ValueError, match="purpose"):
        ledger.record(model="claude-haiku-5-5", usage=Usage(1, 1), purpose="misc", ref="x")
    ledger.record(model="claude-haiku-5-5", usage=Usage(1000, 100), purpose="judge", ref="x")
    ledger.record(model="claude-haiku-5-5", usage=Usage(1000, 100), purpose="benchmark", ref="y")
    by = ledger.breakdown()
    assert set(by) == {("judge", "claude-haiku-5-5"), ("benchmark", "claude-haiku-5-5")}
    assert by[("judge", "claude-haiku-5-5")][0] == 1


FAULT = Fault(
    "F-ABCDE2",
    "benchmark",
    DS,
    "schema.drop",
    {},
    Label("schema_drift", (), "The orders source extract stopped including the `amount` column."),
)


def test_leak_scan_is_clean_on_a_normal_lake(tmp_path) -> None:
    build_lake(tmp_path)
    assert leaks.scan(tmp_path, leaks.tokens([FAULT])) == []


@pytest.mark.parametrize(
    "message",
    [
        "fault F-ABCDE2 injected here",
        "schema.drop",
        "ground truth: schema_drift",
        "The orders source extract stopped including the `amount` column.",
        "Benchmark run",
    ],
)
def test_leak_scan_finds_planted_text(tmp_path, message) -> None:
    build_lake(tmp_path)
    land(
        tmp_path,
        [log_event("orders_ingest", "ingest_orders", message, end_of(DS) + timedelta(minutes=40))],
    )
    assert leaks.scan(tmp_path, leaks.tokens([FAULT]))
