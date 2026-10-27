"""Phase 5b scoring: correctness rules, metrics, agreement statistics, judge resumability."""

from __future__ import annotations

import xml.dom.minidom

import pytest

from benchmark.analysis import charts, human, judging, posthoc, score
from benchmark.harness.catalog import Fault, Label


def _fault(fid: str, cat: str, acceptable: tuple[str, ...] = (), template: str = "t.x") -> Fault:
    return Fault(fid, "benchmark", "2026-08-01", template, {}, Label(cat, acceptable, "truth"))


def _result(
    fid: str,
    cat: str | None,
    repeat: int = 0,
    status: str = "ok",
    conf: float = 0.8,
    retry: bool = False,
    tools: list[str] | None = None,
    evidence_kind: str = "log_line",
    extra: list[str] = (),
) -> dict:
    causes = [
        {
            "category": c,
            "confidence": conf,
            "explanation": "e",
            "suggested_fix": "f",
            "retry_safe": retry,
            "retry_safe_reason": "r",
            "evidence": [{"kind": evidence_kind, "source_tool": "x", "quote": "q" * 12}],
        }
        for c in ([cat] if cat else []) + list(extra)
    ]
    return {
        "key": f"{fid}|INC-{fid}|{repeat}",
        "fault_id": fid,
        "incident_id": f"INC-{fid}",
        "repeat": repeat,
        "model": "claude-haiku-5-5",
        "status": status,
        "diagnosis": {"root_causes": causes} if cat else None,
        "latency_s": 10.0,
        "cost_usd": 0.003,
        "llm_calls": 2,
        "tool_calls": len(tools or []),
        "tools_used": tools or [],
        "input_tokens": 10,
        "cache_read_input_tokens": 5,
        "cache_creation_input_tokens": 0,
        "output_tokens": 100,
        "errors": [],
    }


def _row(r: dict, f: Fault, retry=None, judged=None) -> dict:
    return score.item_row(r, f, judged or {}, retry or {}, target=True)


def test_category_correct_uses_label_and_acceptable() -> None:
    f = _fault("F-1", "data_quality", ("unknown",))
    assert _row(_result("F-1", "data_quality"), f)["correct"]
    assert _row(_result("F-1", "unknown"), f)["correct"]
    assert not _row(_result("F-1", "schema_drift"), f)["correct"]


def test_unfinished_diagnosis_is_wrong_and_a_root_mismatch() -> None:
    f = _fault("F-1", "oom")
    row = _row(_result("F-1", "oom", status="capped"), f)
    assert not row["correct"] and row["predicted"] == score.NO_DIAGNOSIS
    assert row["root_verdict"] == "mismatch"


def test_top3_counts_a_lower_ranked_right_answer() -> None:
    f = _fault("F-1", "oom")
    row = _row(_result("F-1", "bad_deploy", extra=["upstream_outage", "oom"]), f)
    assert not row["correct"] and row["top3"]


def test_retry_truth_either_is_not_scored() -> None:
    f = _fault("F-1", "schema_drift", template="schema.add")
    row = _row(_result("F-1", "schema_drift"), f, {"schema.add": "either"})
    assert row["retry_correct"] is None
    g = _fault("F-2", "oom", template="oom.direct_memory")
    r = _row(_result("F-2", "oom", retry=True), g, {"oom.direct_memory": False})
    assert r["retry_correct"] is False


def test_irrelevant_deploy_only_when_truth_is_not_a_deploy() -> None:
    assert _row(_result("F-1", "oom", evidence_kind="deploy"), _fault("F-1", "oom"))[
        "deploy_cited_irrelevant"
    ]
    assert not _row(
        _result("F-1", "bad_deploy", evidence_kind="deploy"), _fault("F-1", "bad_deploy")
    )["deploy_cited_irrelevant"]


def test_repeated_tool_calls_counts_extra_calls() -> None:
    assert score.repeated_tool_calls(["a", "b", "a", "a"]) == 2
    assert score.repeated_tool_calls(["a", "b"]) == 0


def test_model_metrics_detected_vs_end_to_end_and_variation() -> None:
    faults = [
        _fault("F-1", "oom"),
        _fault("F-2", "unknown"),
        _fault("F-3", "late_data"),
        _fault("F-C", "none"),
    ]
    det = {
        "F-1": {"detected": True},
        "F-2": {"detected": True},
        "F-3": {"detected": False},
        "F-C": {"detected": None},
    }
    items = [
        _row(_result("F-1", "oom", 0), faults[0]),
        _row(_result("F-1", "bad_deploy", 1), faults[0]),
        _row(_result("F-2", "unknown", 0), faults[1]),
        _row(_result("F-2", "unknown", 1), faults[1]),
    ]
    m = score.model_metrics(items, faults, det)
    assert m["accuracy_detected"]["top1"] == {"k": 3, "n": 4, "rate": 0.75}
    assert m["accuracy_end_to_end"]["top1"] == {"k": 3, "n": 6, "rate": 0.5}
    assert m["unknown"]["correct_unknown"] == 2 and m["unknown"]["wrong_unknown"] == 0
    assert m["variation"]["flipping_faults"] == ["F-1"]
    assert m["variation"]["correct_in_k_of_n_repeats"] == {"2/2": 1, "1/2": 1, "0/2": 0}
    assert m["confusion"]["oom"]["bad_deploy"] == 1


def test_detection_metrics_counts_misses_and_clean_false_positives() -> None:
    det = [
        {
            "fault_id": "A",
            "detected": True,
            "raw_findings": 3,
            "primary_incidents": 1,
            "incidents": 2,
            "primary_incidents_other_ds": 0,
            "latency_from_night_start_s": 60,
            "latency_from_first_evidence_s": 40,
            "detected_types": ["failed_job"],
        },
        {
            "fault_id": "B",
            "detected": False,
            "raw_findings": 0,
            "primary_incidents": 0,
            "incidents": 0,
            "primary_incidents_other_ds": 0,
        },
        {
            "fault_id": "C",
            "detected": None,
            "raw_findings": 1,
            "primary_incidents": 1,
            "incidents": 1,
            "primary_incidents_other_ds": 0,
            "false_positive_incidents": 1,
        },
    ]
    m = score.detection_metrics(det)
    assert m["detected"]["rate"] == 0.5 and m["missed"] == ["B"]
    assert m["false_positive_incidents_on_clean_runs"] == 1
    assert m["raw_findings"] == 4 and m["primary_incidents"] == 2


def test_cohen_kappa_known_values() -> None:
    assert human.cohen_kappa([0, 1, 2, 3], [0, 1, 2, 3], [0, 1, 2, 3], "quadratic") == 1.0
    # unweighted: 2x2 table [[20,5],[10,15]] -> po=.7, pe=.5, kappa=.4
    a = ["y"] * 25 + ["n"] * 25
    b = ["y"] * 20 + ["n"] * 5 + ["y"] * 10 + ["n"] * 15
    assert human.cohen_kappa(a, b, ["y", "n"]) == pytest.approx(0.4)


def test_judge_todo_skips_done_and_retries_errors() -> None:
    rows = [_result("F-1", "oom"), _result("F-2", None, status="capped")]
    done = [
        {"key": rows[0]["key"], "judge": "root", "error": None},
        {"key": rows[0]["key"], "judge": "fix", "error": "unparseable"},
    ]
    assert [(r["key"], k) for r, k in judging.todo(rows, done)] == [(rows[0]["key"], "fix")]


def test_charts_are_well_formed_svg() -> None:
    conf = {
        "oom": {"oom": 2, "bad_deploy": 1, "unknown": 0},
        "late_data": {"oom": 0, "bad_deploy": 0, "unknown": 3},
    }
    xml.dom.minidom.parseString(charts.confusion("claude-haiku-5-5", conf))
    xml.dom.minidom.parseString(
        charts.strip({"claude-haiku-5-5": [1.0, 2.0, 3.0]}, "t", "s", lambda v: f"{v:.0f}")
    )
    manual = [
        {
            "trial": 1,
            "truth": "oom",
            "human_correct": True,
            "human_seconds": 200.0,
            "haiku_detect_plus_diagnose_s": 60.0,
        }
    ]
    xml.dom.minidom.parseString(charts.manual_vs_agent(manual, ["claude-haiku-5-5"]))


def test_posthoc_guardrail_counterfactual_only_changes_unknown_safe_rows() -> None:
    oom = _fault("F-1", "oom", template="oom.direct_memory")
    late = _fault("F-2", "upstream_outage", template="upstream.dag_late")
    retry = {"oom.direct_memory": False, "upstream.dag_late": True}
    rows = [
        _row(_result("F-1", "unknown", retry=True), oom, retry),  # the v1 OOM pattern
        _row(_result("F-2", "late_data", retry=True), late, retry),  # known cause: untouched
    ]
    guarded = posthoc.with_guardrail(rows)
    assert [r["retry_pred"] for r in guarded] == [False, True]
    assert [r["retry_correct"] for r in guarded] == [True, True]
    assert rows[0]["retry_pred"] is True  # the v1 rows themselves are not modified


def test_posthoc_before_after_rows_per_category_and_total() -> None:
    oom, dq = _fault("F-1", "oom"), _fault("F-2", "data_quality")
    before = [_row(_result("F-1", "unknown", repeat=r), oom) for r in range(3)] + [
        _row(_result("F-2", "data_quality"), dq)
    ]
    after = [_row(_result("F-1", "oom", repeat=r), oom) for r in range(3)] + [
        _row(_result("F-2", "data_quality"), dq)
    ]
    rows = {r["category"]: r for r in posthoc.before_after_rows(before, after)}
    assert (rows["oom"]["v1_top1"], rows["oom"]["posthoc_top1"]) == ("0/3", "3/3")
    assert (rows["oom"]["v1_unknown"], rows["oom"]["posthoc_unknown"]) == (3, 0)
    assert rows["all"]["v1_top1"] == "1/4" and rows["all"]["posthoc_top1"] == "4/4"
    [f1] = [r for r in posthoc.fault_rows(before, after) if r["fault_id"] == "F-1"]
    assert f1["v1_predicted"] == "unknown unknown unknown" and f1["changed"]


def test_before_after_chart_is_well_formed_svg() -> None:
    per = {"oom": {"top1": {"k": 0, "n": 9, "rate": 0.0}, "top3": {}, "faults": 3}}
    after = {"oom": {"top1": {"k": 9, "n": 9, "rate": 1.0}, "top3": {}, "faults": 3}}
    svg = charts.before_after(per, after)
    xml.dom.minidom.parseString(svg)
    assert "v1 (Phase 5b)" in svg and "Post-hoc tool fixes" in svg and "not held-out" in svg


def test_posthoc_guardrail_counterfactual_can_cover_oom_too() -> None:
    # Phase 5d: the extended guardrail, applied offline to earlier rounds.
    oom = _fault("F-1", "oom", template="oom.direct_memory")
    retry = {"oom.direct_memory": False}
    rows = [_row(_result("F-1", "oom", retry=True), oom, retry)]
    assert posthoc.with_guardrail(rows)[0]["retry_pred"] is True  # 5c rule: unknown only
    both = posthoc.with_guardrail(rows, ("unknown", "oom"))
    assert both[0]["retry_pred"] is False and both[0]["retry_correct"] is True


def test_posthoc_rounds_rows_compare_every_run_per_category() -> None:
    oom = _fault("F-1", "oom", template="oom.direct_memory")
    retry = {"oom.direct_memory": False}
    judged = {
        (f"F-1|INC-F-1|{r}", "root"): {"verdict": v}
        for r, v in enumerate(["match", "partial", "partial"])
    }

    def run(cat, safe, cost=0.003):
        rows = [
            _row(
                {
                    **_result("F-1", cat, repeat=r, retry=safe),
                    "cost_usd": cost,
                    "latency_s": 10.0 + r,
                },
                oom,
                retry,
                judged,
            )
            for r in range(3)
        ]
        return rows

    runs = {
        "v1": run("unknown", True),
        "round1": run("oom", True),
        "round2": run("oom", False, 0.002),
    }
    rows = {r["category"]: r for r in posthoc.rounds_rows(runs)}
    o = rows["oom"]
    assert (o["v1_top1"], o["round1_top1"], o["round2_top1"]) == ("0/3", "3/3", "3/3")
    assert o["round2_root_m_p_x"] == "1/2/0"
    assert (o["round1_retry_correct"], o["round2_retry_correct"]) == ("0/3", "3/3")
    assert (o["round1_unsafe_called_safe"], o["round2_unsafe_called_safe"]) == (3, 0)
    assert o["round2_mean_cost_usd"] == 0.002 and o["round2_median_latency_s"] == 11.0
    assert rows["all"]["faults"] == 1
    [f] = posthoc.runs_fault_rows(runs)
    assert (
        f["v1_predicted"] == "unknown unknown unknown"
        and f["round2_retry_safe"] == "False False False"
    )
    assert f["changed"]  # v1 vs. the last round


def test_rounds_chart_is_well_formed_svg() -> None:
    def per(k):
        return {"oom": {"top1": {"k": k, "n": 9, "rate": k / 9}, "top3": {}, "faults": 3}}

    svg = charts.rounds({"v1": per(0), "round1": per(9), "round2": per(9)})
    xml.dom.minidom.parseString(svg)
    assert "Post-hoc round 1" in svg and "Post-hoc round 2" in svg and "not held-out" in svg


def test_agreement_reads_a_spreadsheet_saved_scores_csv(tmp_path, monkeypatch) -> None:
    # The scored sheet came back with CRLF line ends and a leading blank line.
    (tmp_path / "key.json").write_text('{"S01": {"run": "r", "key": "k"}}')
    (tmp_path / "scores.csv").write_bytes(
        b"\xef\xbb\xbf\r\nsample_id,fix_score,root_verdict\r\nS01,3,match\r\n"
    )
    monkeypatch.setattr(
        human,
        "load_results",
        lambda p: [
            {"key": "k", "judge": "fix", "score": 3, "error": None},
            {"key": "k", "judge": "root", "verdict": "match", "error": None},
        ],
    )
    settings = type("S", (), {"results_dir": tmp_path})()
    res = human.agreement(settings, tmp_path)
    assert res["fix"]["n"] == 1 and res["fix"]["exact"] == 1.0 and res["root"]["exact"] == 1.0
