"""Score the benchmark runs against the sealed labels (no API calls).

Inputs: the benchmark split of the catalog, benchmark/sealed/retry_safe.toml, the detection
replay (detection.json), each run's results.jsonl + judge.jsonl, the manual trials.
Outputs (benchmark/results/report/): metrics.json, diagnoses.csv (one row per diagnosis),
faults.csv (one row per fault x model x repeat), confusion_<model>.csv, manual.csv.

Definitions (also in the README):
- The *fault-level* diagnosis is the one for the primary incident on the fault's expected
  pipeline. A fault whose night raised a second primary incident (the business dip also
  dips payments) has that extra diagnosis in the per-diagnosis cost/latency figures and the
  failure analysis, but not in accuracy.
- Category correct: status `ok` and the top cause's category is the label or one of its
  acceptable alternatives. A diagnosis that didn't finish `ok` counts as wrong.
- Top-3: any of the first three causes has an accepted category.
- Root cause correct: the root-cause judge says `match` (`partial` reported separately).
- Accuracy on detected faults divides by detected faults x repeats; end-to-end divides by
  all injected faults x repeats (a detector miss counts as wrong).
"""

from __future__ import annotations

import csv
import json
import statistics
import tomllib
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from benchmark.harness import scenario
from benchmark.harness.catalog import Fault
from benchmark.harness.runner import load_results
from benchmark.harness.settings import Settings
from sentinel.diagnose.schema import CATEGORIES

BUCKETS = [(0.0, 0.5), (0.5, 0.7), (0.7, 0.85), (0.85, 1.0001)]
NO_DIAGNOSIS = "(no diagnosis)"


def accepted(f: Fault) -> set[str]:
    return {f.label.category, *f.label.acceptable}


def unknown_ok(f: Fault) -> bool:
    return "unknown" in accepted(f)


def retry_truth(path: Path) -> dict[str, bool | str]:
    with path.open("rb") as fh:
        return tomllib.load(fh)["truth"]


def _q(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    xs = sorted(xs)
    i = (len(xs) - 1) * q
    lo, hi = int(i), min(int(i) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (i - lo), 6)


def dist(xs: list[float]) -> dict[str, Any]:
    return {
        "n": len(xs),
        "mean": round(statistics.fmean(xs), 6) if xs else None,
        "median": _q(xs, 0.5),
        "p90": _q(xs, 0.9),
        "min": min(xs) if xs else None,
        "max": max(xs) if xs else None,
        "total": round(sum(xs), 6),
    }


def _rate(k: int, n: int) -> dict[str, Any]:
    return {"k": k, "n": n, "rate": round(k / n, 4) if n else None}


def repeated_tool_calls(tools: list[str]) -> int:
    """Calls to a tool already called in the same diagnosis. Arguments aren't recorded,
    so this counts same-tool repeats (an upper bound on identical repeats)."""
    return sum(c - 1 for c in Counter(tools).values() if c > 1)


def item_row(r: dict, f: Fault, judged: dict, retry: dict, target: bool) -> dict[str, Any]:
    d = r.get("diagnosis") or {}
    causes = d.get("root_causes") or []
    ok = r["status"] == "ok" and bool(causes)
    top = causes[0] if ok else {}
    acc = accepted(f)
    rt = retry.get(f.template, "either")
    deploy_cited = any(e.get("kind") == "deploy" for e in top.get("evidence", []))
    jr, jf = judged.get((r["key"], "root"), {}), judged.get((r["key"], "fix"), {})
    return {
        "model": r["model"],
        "fault_id": r["fault_id"],
        "incident_id": r["incident_id"],
        "fault_level": target,
        "repeat": r["repeat"],
        "template": f.template,
        "truth": f.label.category,
        "acceptable": "|".join(f.label.acceptable),
        "status": r["status"],
        "predicted": top.get("category", NO_DIAGNOSIS),
        "confidence": top.get("confidence"),
        "correct": ok and top.get("category") in acc,
        "top3": ok and any(c["category"] in acc for c in causes[:3]),
        "predicted_unknown": top.get("category") == "unknown",
        "truth_allows_unknown": unknown_ok(f),
        "root_verdict": jr.get("verdict") if ok else "mismatch",
        "fix_score": jf.get("score") if ok else None,
        "retry_pred": top.get("retry_safe"),
        "retry_truth": rt,
        "retry_correct": (top.get("retry_safe") == rt) if ok and isinstance(rt, bool) else None,
        "deploy_cited": deploy_cited,
        "deploy_cited_irrelevant": deploy_cited and f.label.category != "bad_deploy",
        "bad_deploy_ranked_irrelevant": ok
        and f.label.category != "bad_deploy"
        and any(c["category"] == "bad_deploy" for c in causes),
        "latency_s": r["latency_s"],
        "cost_usd": r["cost_usd"],
        "llm_calls": r["llm_calls"],
        "tool_calls": r["tool_calls"],
        "repeated_tool_calls": repeated_tool_calls(r["tools_used"]),
        "tools_used": " ".join(r["tools_used"]),
        "input_tokens": r["input_tokens"]
        + r["cache_read_input_tokens"]
        + r["cache_creation_input_tokens"],
        "uncached_input_tokens": r["input_tokens"],
        "output_tokens": r["output_tokens"],
        "explanation": top.get("explanation", ""),
        "suggested_fix": top.get("suggested_fix", ""),
        "errors": " | ".join(r.get("errors") or []),
    }


def load_run(run_dir: Path, faults: dict[str, Fault], targets: dict[str, str], retry):
    rows = load_results(run_dir / "results.jsonl")
    judged = {
        (j["key"], j["judge"]): j
        for j in load_results(run_dir / "judge.jsonl")
        if j.get("error") is None
    }
    return [
        item_row(
            r, faults[r["fault_id"]], judged, retry, targets.get(r["fault_id"]) == r["incident_id"]
        )
        for r in rows
    ]


def fault_targets(settings: Settings, faults: list[Fault]) -> dict[str, str]:
    """fault id -> the primary incident on its expected pipeline (the fault-level one)."""
    out = {}
    for f in faults:
        m = scenario.manifest(settings, f.fault_id) or {}
        pipe = {i["incident_id"]: i["pipeline"] for i in m.get("incidents", [])}
        hits = [t for t in m.get("targets", []) if pipe.get(t) == f.recipe.expect_pipeline]
        if hits:
            out[f.fault_id] = hits[0]
    return out


def model_metrics(items: list[dict], faults: list[Fault], detection: dict[str, dict]):
    injected = [f for f in faults if not f.is_clean]
    detected = {f.fault_id for f in injected if detection[f.fault_id]["detected"]}
    fl = [i for i in items if i["fault_level"]]
    repeats = sorted({i["repeat"] for i in fl})
    by_repeat = defaultdict(list)
    for i in fl:
        by_repeat[i["repeat"]].append(i)

    def acc(rows, key, denom):
        return _rate(sum(bool(r[key]) for r in rows), denom)

    n_det, n_all = len(detected) * len(repeats), len(injected) * len(repeats)
    det_rows = [i for i in fl if i["fault_id"] in detected]
    oks = [i for i in fl if i["status"] == "ok"]

    per_cat: dict[str, Any] = {}
    for cat in sorted({f.label.category for f in injected}):
        rows = [i for i in fl if i["truth"] == cat]
        per_cat[cat] = {
            "top1": acc(rows, "correct", len(rows)),
            "top3": acc(rows, "top3", len(rows)),
            "faults": len({r["fault_id"] for r in rows}),
        }
    labels = [c for c in CATEGORIES] + [NO_DIAGNOSIS]
    truths = sorted({f.label.category for f in injected})
    confusion = {t: {p: 0 for p in labels} for t in truths}
    for i in fl:
        confusion[i["truth"]][i["predicted"]] += 1

    calib = []
    for lo, hi in BUCKETS:
        rows = [i for i in oks if i["confidence"] is not None and lo <= i["confidence"] < hi]
        calib.append(
            {
                "bucket": f"{lo:.2f}-{min(hi, 1.0):.2f}",
                "n": len(rows),
                "mean_confidence": round(statistics.fmean(r["confidence"] for r in rows), 4)
                if rows
                else None,
                "accuracy": round(sum(r["correct"] for r in rows) / len(rows), 4) if rows else None,
            }
        )
    brier = (
        round(statistics.fmean((r["confidence"] - r["correct"]) ** 2 for r in oks), 4)
        if oks
        else None
    )

    unk_truth = [i for i in fl if i["truth_allows_unknown"]]
    pred_unk = [i for i in fl if i["predicted_unknown"]]
    retry_rows = [i for i in fl if i["retry_correct"] is not None]
    root = Counter(i["root_verdict"] for i in fl)
    fixes = [i["fix_score"] for i in fl if i["fix_score"] is not None]

    variation = None
    if len(repeats) > 1:
        per_fault = defaultdict(list)
        for i in sorted(fl, key=lambda i: i["repeat"]):
            per_fault[i["fault_id"]].append(i)
        same_cat = sum(len({i["predicted"] for i in v}) == 1 for v in per_fault.values())
        pattern = Counter(sum(i["correct"] for i in v) for v in per_fault.values())
        variation = {
            "faults": len(per_fault),
            "same_top_category_all_repeats": _rate(same_cat, len(per_fault)),
            "correct_in_k_of_n_repeats": {
                f"{k}/{len(repeats)}": pattern.get(k, 0) for k in range(len(repeats), -1, -1)
            },
            "flipping_faults": sorted(
                fid for fid, v in per_fault.items() if len({i["predicted"] for i in v}) > 1
            ),
            "same_retry_safe_all_repeats": _rate(
                sum(len({i["retry_pred"] for i in v}) == 1 for v in per_fault.values()),
                len(per_fault),
            ),
        }

    all_items = items
    return {
        "diagnoses": len(all_items),
        "fault_level_diagnoses": len(fl),
        "repeats": len(repeats),
        "status": dict(Counter(i["status"] for i in all_items)),
        "accuracy_detected": {
            "top1": acc(det_rows, "correct", n_det),
            "top3": acc(det_rows, "top3", n_det),
            "top1_and_root_cause_match": _rate(
                sum(r["correct"] and r["root_verdict"] == "match" for r in det_rows), n_det
            ),
            "top1_per_repeat": [
                acc(
                    [i for i in by_repeat[r] if i["fault_id"] in detected], "correct", len(detected)
                )
                for r in repeats
            ],
        },
        "accuracy_end_to_end": {
            "top1": acc(det_rows, "correct", n_all),
            "top3": acc(det_rows, "top3", n_all),
            "top1_and_root_cause_match": _rate(
                sum(r["correct"] and r["root_verdict"] == "match" for r in det_rows), n_all
            ),
        },
        "per_category": per_cat,
        "confusion": confusion,
        "root_cause_judge": {k: root.get(k, 0) for k in ("match", "partial", "mismatch")},
        "fix_quality_judge": {
            "scores": {str(s): fixes.count(s) for s in (3, 2, 1, 0)},
            "mean": round(statistics.fmean(fixes), 3) if fixes else None,
            "unjudged": len(fl) - len(fixes),
        },
        "unknown": {
            "truth_allows_unknown": len(unk_truth),
            "correct_unknown": sum(i["predicted_unknown"] for i in unk_truth),
            "specific_answer_on_unknown_truth": sum(
                not i["predicted_unknown"] and not i["correct"] for i in unk_truth
            ),
            "specific_and_accepted_on_unknown_truth": sum(
                not i["predicted_unknown"] and i["correct"] for i in unk_truth
            ),
            "wrong_unknown": sum(not i["truth_allows_unknown"] for i in pred_unk),
            "predicted_unknown": len(pred_unk),
            "unknown_rate": _rate(len(pred_unk), len(fl)),
            "early_unknown_le_2_tool_calls": sum(
                i["tool_calls"] <= 2 and not i["truth_allows_unknown"] for i in pred_unk
            ),
        },
        "calibration": calib,
        "brier": brier,
        "retry_safe": {
            "scored": len(retry_rows),
            "correct": _rate(sum(i["retry_correct"] for i in retry_rows), len(retry_rows)),
            "said_safe_when_unsafe": sum(
                i["retry_pred"] is True and i["retry_truth"] is False for i in retry_rows
            ),
            "said_unsafe_when_safe": sum(
                i["retry_pred"] is False and i["retry_truth"] is True for i in retry_rows
            ),
            "not_scored_either": sum(i["retry_truth"] == "either" for i in fl),
        },
        "per_diagnosis": {
            "latency_s": dist([i["latency_s"] for i in all_items]),
            "cost_usd": dist([i["cost_usd"] for i in all_items]),
            "llm_calls": dist([i["llm_calls"] for i in all_items]),
            "tool_calls": dist([i["tool_calls"] for i in all_items]),
            "input_tokens_incl_cache": dist([i["input_tokens"] for i in all_items]),
            "output_tokens": dist([i["output_tokens"] for i in all_items]),
        },
        "known_issues": {
            "diagnoses_with_repeated_tool_calls": _rate(
                sum(i["repeated_tool_calls"] > 0 for i in all_items), len(all_items)
            ),
            "repeated_tool_calls_total": sum(i["repeated_tool_calls"] for i in all_items),
            "irrelevant_deploy_cited": _rate(
                sum(i["deploy_cited_irrelevant"] for i in fl),
                sum(i["truth"] != "bad_deploy" for i in fl),
            ),
            "bad_deploy_ranked_anywhere_when_not": _rate(
                sum(i["bad_deploy_ranked_irrelevant"] for i in fl),
                sum(i["truth"] != "bad_deploy" for i in fl),
            ),
            "bad_deploy_predicted_when_not": sum(
                i["predicted"] == "bad_deploy" and i["truth"] != "bad_deploy" for i in fl
            ),
        },
        "variation": variation,
    }


def detection_metrics(det: list[dict]) -> dict[str, Any]:
    injected = [d for d in det if d["detected"] is not None]
    clean = [d for d in det if d["detected"] is None]
    hit = [d for d in injected if d["detected"]]
    return {
        "injected_faults": len(injected),
        "detected": _rate(len(hit), len(injected)),
        "missed": [d["fault_id"] for d in injected if not d["detected"]],
        "clean_runs": len(clean),
        "false_positive_incidents_on_clean_runs": sum(d["false_positive_incidents"] for d in clean),
        "clean_runs_with_any_finding": sum(d["raw_findings"] > 0 for d in clean),
        "raw_findings": sum(d["raw_findings"] for d in det),
        "primary_incidents": sum(d["primary_incidents"] for d in det),
        "all_incidents": sum(d["incidents"] for d in det),
        "primary_incidents_other_ds": sum(d["primary_incidents_other_ds"] for d in det),
        "latency_from_night_start_s": dist([d["latency_from_night_start_s"] for d in hit]),
        "latency_from_first_evidence_s": dist([d["latency_from_first_evidence_s"] for d in hit]),
        "detected_as": dict(Counter(t for d in hit for t in d["detected_types"])),
    }


def manual_rows(settings: Settings, faults: dict[str, Fault], items_by_model, det) -> list[dict]:
    rows = load_results(settings.manual_dir / "trials.jsonl")
    trials = sorted((r for r in rows if r["kind"] == "diagnosis"), key=lambda r: r["trial"])
    out = []
    for t in trials:
        f = faults[t["fault_id"]]
        d = det[f.fault_id]
        row = {
            "trial": t["trial"],
            "fault_id": f.fault_id,
            "truth": f.label.category,
            "human_category": t["category"],
            "human_correct": t["category"] in accepted(f),
            "human_seconds": t["seconds"],
            "human_retry_safe": t.get("retry_safe"),
            "detection_s_from_first_evidence": d.get("latency_from_first_evidence_s"),
        }
        for model, items in items_by_model.items():
            mine = [i for i in items if i["fault_id"] == f.fault_id and i["fault_level"]]
            if not mine:
                continue
            lat = statistics.median(i["latency_s"] for i in mine)
            short = model.split("-")[1]
            row[f"{short}_diagnosis_s_median"] = round(lat, 1)
            row[f"{short}_detect_plus_diagnose_s"] = (
                round(lat + d["latency_from_first_evidence_s"], 1) if d.get("detected") else None
            )
            row[f"{short}_correct"] = f"{sum(i['correct'] for i in mine)}/{len(mine)}"
        out.append(row)
    return out


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def run(
    faults: list[Fault],
    settings: Settings,
    models: list[str],
    prompt: str,
    report_dir: Path,
    out=print,
) -> tuple[dict[str, Any], list[dict], list[dict]]:
    det_rows = json.loads((report_dir / "detection.json").read_text())
    det = {d["fault_id"]: d for d in det_rows}
    by_id = {f.fault_id: f for f in faults}
    targets = fault_targets(settings, faults)
    retry = retry_truth(settings.catalog_path.parent / "retry_safe.toml")
    items_by_model = {}
    for model in models:
        run_dir = settings.results_dir / f"benchmark__{model}__{prompt}"
        if (run_dir / "results.jsonl").exists():
            items_by_model[model] = load_run(run_dir, by_id, targets, retry)
    metrics = {
        "prompt": prompt,
        "detection": detection_metrics(det_rows),
        "models": {m: model_metrics(items, faults, det) for m, items in items_by_model.items()},
    }
    manual = manual_rows(settings, by_id, items_by_model, det)
    hm = [r["human_seconds"] for r in manual]
    metrics["manual"] = {
        "trials": len(manual),
        "human_correct": _rate(sum(r["human_correct"] for r in manual), len(manual)),
        "human_seconds": dist(hm),
        "agent": {},
    }
    for model in items_by_model:
        short = model.split("-")[1]
        dd = [
            r[f"{short}_detect_plus_diagnose_s"]
            for r in manual
            if r.get(f"{short}_detect_plus_diagnose_s") is not None
        ]
        dg = [
            r[f"{short}_diagnosis_s_median"]
            for r in manual
            if r.get(f"{short}_diagnosis_s_median") is not None
        ]
        metrics["manual"]["agent"][model] = {
            "diagnosis_s": dist(dg),
            "detect_plus_diagnose_s": dist(dd),
        }
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    write_csv(report_dir / "diagnoses.csv", [i for v in items_by_model.values() for i in v])
    write_csv(report_dir / "detection.csv", det_rows)
    write_csv(report_dir / "manual.csv", manual)
    for model, mm in metrics["models"].items():
        conf = mm["confusion"]
        cols = [c for c in [*CATEGORIES, NO_DIAGNOSIS] if any(conf[t][c] for t in conf)]
        write_csv(
            report_dir / f"confusion_{model}.csv",
            [{"truth": t, **{c: conf[t][c] for c in cols}} for t in conf],
        )
    out(f"wrote {report_dir}/metrics.json and CSVs for {', '.join(items_by_model)}")
    return metrics, [i for v in items_by_model.values() for i in v], manual
