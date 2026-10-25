"""The manual trial: stratified seeded selection, the label shown only after the
diagnosis is on disk, resume, and the human's view of the evidence."""

from __future__ import annotations

import dataclasses
import json
from collections import Counter

import pytest

from benchmark.harness import manual
from benchmark.harness.catalog import Fault, Label
from benchmark.harness.runner import load_results
from benchmark.harness.settings import load
from tests.unit.diagnose_fixtures import build_lake

FAMS = {
    "schema": "schema.drop",
    "late": "late.truncated",
    "oom": "oom.direct_memory",
    "deploy": "deploy.code_regression",
    "upstream": "upstream.dag_missing",
    "dq": "dq.null_spike",
    "amb": "amb.business_dip",
}


def _faults() -> list[Fault]:
    out = []
    for fam, tpl in FAMS.items():
        for i in range(3):
            out.append(
                Fault(
                    f"F-{fam[:3].upper()}{i}{'Z' * (3 - len(fam[:3]))}2",
                    "benchmark",
                    "2026-10-20",
                    tpl,
                    {},
                    Label("schema_drift", (), f"truth {fam} {i}"),
                )
            )
    out.append(
        Fault("F-CLEAN2", "benchmark", "2026-10-20", "clean.none", {}, Label("none", (), "none"))
    )
    return out


@pytest.fixture
def setup(tmp_path):
    settings = dataclasses.replace(
        load(), work_dir=tmp_path / "work", manual_dir=tmp_path / "manual"
    )
    faults = _faults()
    lake = None
    for f in faults:
        d = settings.work_dir / "scenarios" / f.fault_id
        d.mkdir(parents=True)
        if lake is None:
            incident = build_lake(d / "lake")
            lake = d / "lake"
        else:
            import shutil

            shutil.copytree(lake, d / "lake")
        targets = [] if f.is_clean else [incident["incident_id"]]
        (d / "manifest.json").write_text(json.dumps({"targets": targets}))
    return settings, faults


def test_selection_is_stratified_seeded_and_skips_clean_runs(setup) -> None:
    settings, faults = setup
    a = manual.select(faults, settings)
    b = manual.select(faults, settings)
    assert [f.fault_id for f in a] == [f.fault_id for f in b]
    fams = Counter(manual.family(f) for f in a)
    assert len(a) == 10 and set(fams) == set(FAMS) and max(fams.values()) == 2
    assert all(not f.is_clean for f in a)


class Script:
    """Feeds keypresses/answers and records output, with a fake clock."""

    def __init__(self, answers: list[str]) -> None:
        self.answers = answers
        self.lines: list[str] = []
        self.t = 0.0
        self.events: list[str] = []

    def out(self, s: str) -> None:
        self.lines.append(s)

    def ask(self, prompt: str) -> str:
        self.t += 30
        return self.answers.pop(0)

    def key(self) -> str:
        self.events.append(f"key@{len(self.lines)}")
        return " "

    def clock(self) -> float:
        return self.t

    def io(self) -> manual.IO:
        return manual.IO(self.out, self.ask, self.key, self.clock)


def test_label_appears_only_after_the_diagnosis_is_recorded(setup) -> None:
    settings, faults = setup
    s = Script(["", "1", "orders lost amount", "n"] + ["", "2", "x", "?"] * 9)
    results = settings.manual_dir / "trials.jsonl"
    seen_before_record = []

    real_append = manual.append_result

    def spy(path, record):
        if record["kind"] == "diagnosis":
            f = next(x for x in faults if x.fault_id == record["fault_id"])
            seen_before_record.append(any(f.label.root_cause in line for line in s.lines))
        real_append(path, record)

    manual.append_result = spy
    try:
        assert manual.run(faults, settings, reveal="each", io=s.io()) == 0
    finally:
        manual.append_result = real_append
    assert seen_before_record == [False] * 10
    rows = load_results(results)
    diag = [r for r in rows if r["kind"] == "diagnosis"]
    assert len(diag) == 10 and diag[0]["category"] == "schema_drift"
    assert diag[0]["seconds"] == 120.0 and diag[0]["retry_safe"] is False
    assert [r["kind"] for r in rows[:2]] == ["diagnosis", "label"]


def test_reveal_end_and_resume(setup) -> None:
    settings, faults = setup
    s = Script(["", "1", "x", "y"] * 3)
    with pytest.raises(IndexError):  # the person stops after 3 trials (out of answers)
        manual.run(faults, settings, reveal="end", io=s.io())
    rows = load_results(settings.manual_dir / "trials.jsonl")
    assert [r["kind"] for r in rows] == ["diagnosis"] * 3  # no label shown yet
    s2 = Script(["", "1", "x", "y"] * 7)
    manual.run(faults, settings, reveal="end", io=s2.io())
    rows = load_results(settings.manual_dir / "trials.jsonl")
    kinds = Counter(r["kind"] for r in rows)
    assert kinds == {"diagnosis": 10, "label": 10}
    assert len({r["fault_id"] for r in rows if r["kind"] == "diagnosis"}) == 10


def test_inspect_uses_the_agents_tools_on_the_trial_lake(setup) -> None:
    settings, faults = setup
    f = next(x for x in faults if not x.is_clean)
    manual.prepare(settings, f, 1)
    out = json.loads(manual.inspect(settings, "get_error_logs", {}, None))
    assert out["tool"] == "get_error_logs" and out["count"] >= 1
    assert "<incident_record" in manual.incident_record(settings, None)
