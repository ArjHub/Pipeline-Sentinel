"""Phase 5c: score a labelled, post-hoc run against v1, without touching v1's outputs.

The post-hoc run used its own scenario lakes (SENTINEL_BENCHMARK_WORK), rebuilt with the
tool fixes, so its fault-level incident ids come from those manifests and detection is
replayed over those lakes. v1's diagnoses are re-scored from the v1 lakes' manifests, and
the result is checked against the committed v1 metrics.json (which is only read).

Also scores "v1 + guardrail only": v1's diagnoses with retry_safe forced to false where the
top cause was unknown (offline, no API), to separate the guardrail from the tool fixes.

Everything here is post-hoc: the fixes were chosen after reading v1's failures on these same
25 faults, so the "after" numbers are not a held-out estimate.

Outputs (benchmark/results/report/posthoc/): metrics.json, before_after.csv (per true
category), faults.csv (per fault, v1 vs. post-hoc answers), diagnoses.csv, detection.json,
confusion_<model>.csv and charts/before_after.svg.
"""

from __future__ import annotations

import dataclasses
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from benchmark.analysis import charts, detection
from benchmark.analysis.score import (
    CATEGORIES,
    NO_DIAGNOSIS,
    _rate,
    detection_metrics,
    fault_targets,
    load_run,
    model_metrics,
    retry_truth,
    write_csv,
)
from benchmark.harness.catalog import Fault
from benchmark.harness.runner import load_results, results_path
from benchmark.harness.settings import Settings

NOTE = (
    "Post-hoc, not held-out: the tool fixes and the guardrail were chosen after reading the "
    "v1 failures on these same 25 benchmark faults."
)


def with_guardrail(items: list[dict]) -> list[dict]:
    """v1 rows as they would score had the guardrail existed (retry_safe forced to false
    when the top cause is unknown). Nothing else changes."""
    out = []
    for i in items:
        i = dict(i)
        if i["predicted"] == "unknown" and i["retry_pred"] is True:
            i["retry_pred"] = False
            if isinstance(i["retry_truth"], bool):
                i["retry_correct"] = i["retry_truth"] is False
        out.append(i)
    return out


def _cat_row(cat: str, rows: list[dict]) -> dict[str, Any]:
    retry = [r for r in rows if r["retry_correct"] is not None]
    return {
        "top1": _rate(sum(r["correct"] for r in rows), len(rows)),
        "root_match": _rate(sum(r["root_verdict"] == "match" for r in rows), len(rows)),
        "unknown": sum(r["predicted_unknown"] for r in rows),
        "retry_correct": _rate(sum(r["retry_correct"] for r in retry), len(retry)),
        "said_safe_when_unsafe": sum(
            r["retry_pred"] is True and r["retry_truth"] is False for r in retry
        ),
    }


def before_after_rows(before: list[dict], after: list[dict]) -> list[dict[str, Any]]:
    b_fl = [i for i in before if i["fault_level"]]
    a_fl = [i for i in after if i["fault_level"]]
    rows = []
    for cat in [*sorted({i["truth"] for i in b_fl}), "all"]:
        b = _cat_row(cat, [i for i in b_fl if cat in ("all", i["truth"])])
        a = _cat_row(cat, [i for i in a_fl if cat in ("all", i["truth"])])
        row: dict[str, Any] = {
            "category": cat,
            "faults": len({i["fault_id"] for i in b_fl if cat in ("all", i["truth"])}),
        }
        for k in ("top1", "root_match", "retry_correct"):
            row[f"v1_{k}"] = f"{b[k]['k']}/{b[k]['n']}"
            row[f"posthoc_{k}"] = f"{a[k]['k']}/{a[k]['n']}"
        for k in ("unknown", "said_safe_when_unsafe"):
            row[f"v1_{k}"] = b[k]
            row[f"posthoc_{k}"] = a[k]
        rows.append(row)
    return rows


def fault_rows(before: list[dict], after: list[dict]) -> list[dict[str, Any]]:
    by: dict[str, dict[str, list[dict]]] = defaultdict(lambda: {"v1": [], "posthoc": []})
    for tag, items in (("v1", before), ("posthoc", after)):
        for i in sorted(items, key=lambda i: i["repeat"]):
            if i["fault_level"]:
                by[i["fault_id"]][tag].append(i)
    rows = []
    for fid, v in sorted(by.items()):
        f = (v["v1"] or v["posthoc"])[0]
        row = {"fault_id": fid, "template": f["template"], "truth": f["truth"]}
        for tag in ("v1", "posthoc"):
            rs = v[tag]
            row[f"{tag}_predicted"] = " ".join(r["predicted"] for r in rs)
            row[f"{tag}_correct"] = f"{sum(r['correct'] for r in rs)}/{len(rs)}"
            row[f"{tag}_root"] = " ".join(str(r["root_verdict"]) for r in rs)
            row[f"{tag}_retry_safe"] = " ".join(str(r["retry_pred"]) for r in rs)
        row["changed"] = row["v1_predicted"] != row["posthoc_predicted"]
        rows.append(row)
    return rows


def run(
    faults: list[Fault],
    settings: Settings,
    *,
    model: str,
    prompt: str,
    label: str,
    v1_work: Path,
    report_dir: Path,
    out=print,
) -> dict[str, Any]:
    if settings.work_dir.resolve() == v1_work.resolve():
        raise SystemExit("posthoc scoring needs the labelled run's own lakes (LABEL=...)")
    out_dir = report_dir / "posthoc"
    by_id = {f.fault_id: f for f in faults}
    retry = retry_truth(settings.catalog_path.parent / "retry_safe.toml")

    det_rows = detection.run(faults, settings, out=out)
    detection.write(det_rows, out_dir / "detection.json")
    det = {d["fault_id"]: d for d in det_rows}
    run_dir = results_path(settings, "benchmark", model, prompt, label).parent
    after_items = load_run(run_dir, by_id, fault_targets(settings, faults), retry)

    v1_settings = dataclasses.replace(settings, work_dir=v1_work)
    v1_dir = results_path(settings, "benchmark", model, prompt).parent
    before_items = load_run(v1_dir, by_id, fault_targets(v1_settings, faults), retry)
    v1_det = {d["fault_id"]: d for d in json.loads((report_dir / "detection.json").read_text())}
    before = model_metrics(before_items, faults, v1_det)
    committed = json.loads((report_dir / "metrics.json").read_text())["models"][model]
    after = model_metrics(after_items, faults, det)
    guard = model_metrics(with_guardrail(before_items), faults, v1_det)
    raw = load_results(run_dir / "results.jsonl")

    metrics = {
        "label": label,
        "model": model,
        "prompt": prompt,
        "post_hoc": True,
        "held_out": False,
        "note": NOTE,
        "v1_rescored_equals_committed": json.dumps(before, sort_keys=True)
        == json.dumps(committed, sort_keys=True),
        "detection": detection_metrics(det_rows),
        "guardrail_applied": sum(bool(r.get("guardrail_applied")) for r in raw),
        "scenario_build_hashes": sorted({r.get("scenario_build_hash", "") for r in raw}),
        "before": before,
        "after": after,
        "v1_plus_guardrail_only": {"retry_safe": guard["retry_safe"]},
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n")
    write_csv(out_dir / "before_after.csv", before_after_rows(before_items, after_items))
    write_csv(out_dir / "faults.csv", fault_rows(before_items, after_items))
    write_csv(out_dir / "diagnoses.csv", after_items)
    conf = after["confusion"]
    cols = [c for c in [*CATEGORIES, NO_DIAGNOSIS] if any(conf[t][c] for t in conf)]
    write_csv(
        out_dir / f"confusion_{model}.csv",
        [{"truth": t, **{c: conf[t][c] for c in cols}} for t in conf],
    )
    chart = out_dir / "charts" / "before_after.svg"
    chart.parent.mkdir(parents=True, exist_ok=True)
    chart.write_text(charts.before_after(before["per_category"], after["per_category"]))
    out(f"wrote {out_dir}/metrics.json, CSVs and {chart}")
    return metrics
