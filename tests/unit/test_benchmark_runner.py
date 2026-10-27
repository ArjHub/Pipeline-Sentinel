"""The benchmark runner: resume, the ledger gate, dry run, prompt selection, purpose."""

from __future__ import annotations

import dataclasses
import json

import pytest

from benchmark.harness import runner
from benchmark.harness.catalog import Fault, Label
from benchmark.harness.settings import load
from sentinel.diagnose.config import load_config, prompt_path
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import RuleBasedLLM, ScriptedLLM, response, text
from sentinel.diagnose.schema import unknown
from tests.unit.diagnose_fixtures import build_lake

FAULTS = [
    Fault(f"F-TEST{c}2", "benchmark", "2026-10-20", "clean.none", {}, Label("none", (), "x"))
    for c in "ABC"
]


@pytest.fixture
def setup(tmp_path, monkeypatch):
    settings = dataclasses.replace(
        load(), work_dir=tmp_path / "work", results_dir=tmp_path / "results"
    )
    incident = None
    for f in FAULTS:
        d = settings.work_dir / "scenarios" / f.fault_id
        incident = build_lake(d / "lake")
        (d / "manifest.json").write_text(
            json.dumps({"targets": [incident["incident_id"]], "build_hash": "h"})
        )
    monkeypatch.setenv("SENTINEL_AUTO_DIAGNOSE", "0")
    ledger = Ledger(tmp_path / "ledger.json", budget_usd=10.0)
    return settings, ledger, incident


def _run(settings, ledger, out=None, **kw):
    kw.setdefault("llm_factory", lambda m: RuleBasedLLM())
    lines = out if out is not None else []
    rc = runner.run(
        FAULTS,
        settings,
        split="benchmark",
        model="claude-haiku-5-5",
        repeats=kw.pop("repeats", 2),
        prompt="diagnosis_v1",
        ledger=ledger,
        out=lines.append,
        **kw,
    )
    return rc, lines


def _results(settings):
    return runner.load_results(
        runner.results_path(settings, "benchmark", "claude-haiku-5-5", "diagnosis_v1")
    )


def test_plan_is_repeat_major(setup) -> None:
    settings, _, _ = setup
    items, assumed = runner.plan(FAULTS, settings, 2)
    assert assumed == 0
    assert [(i.fault_id[-2], i.repeat) for i in items] == [
        ("A", 0),
        ("B", 0),
        ("C", 0),
        ("A", 1),
        ("B", 1),
        ("C", 1),
    ]


def test_dry_run_estimates_and_calls_nothing(setup) -> None:
    settings, ledger, _ = setup
    rc, out = _run(settings, ledger, dry_run=True, llm_factory=lambda m: pytest.fail("LLM made"))
    assert rc == 0
    text_out = "\n".join(out)
    assert "6 to run" in text_out and "dry run: no API call made" in text_out
    assert "$  0.0175" in text_out  # 6 x Haiku mean $0.00291
    assert _results(settings) == []


def test_results_are_saved_per_item_and_resume_skips_them(setup) -> None:
    settings, ledger, _ = setup

    class Interrupting(RuleBasedLLM):
        n = 0

        def create(self, **params):
            Interrupting.n += 1
            if Interrupting.n > 4:  # each mock diagnosis is 2 requests: die in the 3rd
                raise KeyboardInterrupt
            return super().create(**params)

    with pytest.raises(KeyboardInterrupt):
        _run(settings, ledger, llm_factory=lambda m: Interrupting())
    assert len(_results(settings)) == 2  # both finished items were on disk already

    rc, _ = _run(settings, ledger)  # no RESUME: refuses to append to an existing run
    assert rc == 2
    rc, out = _run(settings, ledger, resume=True)
    assert rc == 0 and any("2 already done, 4 to run" in line for line in out)
    rows = _results(settings)
    assert len(rows) == 6 and len({r["key"] for r in rows}) == 6
    assert all(r["prompt_version"] == "diagnosis_v1" and r["status"] == "ok" for r in rows)


def test_error_items_are_kept_unless_retry_errors(setup) -> None:
    settings, ledger, incident = setup
    path = runner.results_path(settings, "benchmark", "claude-haiku-5-5", "diagnosis_v1")
    key = f"{FAULTS[0].fault_id}|{incident['incident_id']}|0"
    runner.append_result(path, {"key": key, "status": "error", "cost_usd": 0.001})
    _, out = _run(settings, ledger, resume=True, dry_run=True)
    assert any("1 already done, 5 to run" in line for line in out)
    _, out = _run(settings, ledger, resume=True, dry_run=True, retry_errors=True)
    assert any("0 already done, 6 to run" in line for line in out)


def test_gate_refuses_when_spent_plus_estimate_exceeds_budget(setup, tmp_path) -> None:
    settings, _, _ = setup
    ledger = Ledger(tmp_path / "tight.json", budget_usd=0.03)
    rc, out = _run(settings, ledger, llm_factory=lambda m: pytest.fail("LLM made"))
    assert rc == 3 and any(line.startswith("REFUSED") for line in out)


def test_refuses_with_auto_diagnosis_on(setup, monkeypatch) -> None:
    settings, ledger, _ = setup
    monkeypatch.setenv("SENTINEL_AUTO_DIAGNOSE", "1")
    rc, out = _run(settings, ledger, llm_factory=lambda m: pytest.fail("LLM made"))
    assert rc == 2 and "SENTINEL_AUTO_DIAGNOSE=0" in out[-1]


def test_real_calls_are_ledgered_as_benchmark(setup) -> None:
    settings, ledger, incident = setup
    iid = incident["incident_id"]

    def scripted(model):
        answer = unknown(iid, "not enough evidence").model_dump_json()
        return ScriptedLLM([response([text(answer)]) for _ in range(6)], model=model)

    rc, _ = _run(settings, ledger, llm_factory=scripted)
    assert rc == 0
    calls = json.loads(ledger.path.read_text())["calls"]
    assert len(calls) == 6 and {c["purpose"] for c in calls} == {"benchmark"}


def test_prompt_version_is_selectable_and_must_exist() -> None:
    assert load_config(prompt="diagnosis_v1").prompt_version == "diagnosis_v1"
    assert prompt_path("v1").name == "diagnosis_v1.md"
    with pytest.raises(FileNotFoundError):
        load_config(prompt="diagnosis_v2")


def test_unbuilt_scenarios_are_assumed_in_the_estimate_and_block_a_real_run(setup) -> None:
    settings, ledger, _ = setup
    extra = Fault("F-TESTD2", "benchmark", "2026-10-20", "clean.none", {}, Label("none", (), "x"))
    out: list[str] = []
    rc = runner.run(
        [*FAULTS, extra],
        settings,
        split="benchmark",
        model="claude-haiku-5-5",
        repeats=1,
        prompt="diagnosis_v1",
        ledger=ledger,
        out=out.append,
        llm_factory=lambda m: pytest.fail("LLM made"),
    )
    assert rc == 2 and any("not built" in line for line in out)


def test_a_labelled_run_has_its_own_folder_and_never_resumes_the_unlabelled_one(setup) -> None:
    # Phase 5c: the post-hoc re-run must not touch (or resume into) the v1 results.
    settings, ledger, _ = setup
    assert _run(settings, ledger, repeats=1)[0] == 0
    v1 = runner.results_path(settings, "benchmark", "claude-haiku-5-5", "diagnosis_v1")
    before = v1.read_bytes()
    rc, out = _run(settings, ledger, repeats=1, label="posthoc-tool-fixes")
    post = runner.results_path(
        settings, "benchmark", "claude-haiku-5-5", "diagnosis_v1", "posthoc-tool-fixes"
    )
    assert rc == 0 and any("0 already done, 3 to run" in line for line in out)
    assert post.parent.name == "benchmark__claude-haiku-5-5__diagnosis_v1__posthoc-tool-fixes"
    assert v1.read_bytes() == before
    rows = runner.load_results(post)
    assert len(rows) == 3 and {r["label"] for r in rows} == {"posthoc-tool-fixes"}
    assert all(r["guardrail_applied"] is False for r in rows)
    with pytest.raises(ValueError, match="run label"):
        runner.results_path(settings, "benchmark", "m", "p", "../v1")
