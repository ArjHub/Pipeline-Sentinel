"""Blind human scoring of fix quality, and judge-vs-human agreement.

`sample` draws 15 fault-level diagnoses that finished `ok` (seeded; spread over the true
categories and both models) and writes, for the human:
  benchmark/results/human/sheet.md   ground truth + top cause + fix, the rubric text
  benchmark/results/human/scores.csv sample_id, fix_score (0-3), root_verdict (optional)
and, for scoring only, benchmark/results/human/key.json (sample_id -> run + result key).
The sheet hides the model, the confidence and both judges' scores.

`agreement` joins the filled-in scores.csv with the judges: exact and within-one
agreement and quadratic-weighted Cohen's kappa for the 0-3 fix score; exact agreement and
Cohen's kappa for the root-cause verdict, if the human filled it in.
"""

from __future__ import annotations

import csv
import json
import random
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from benchmark.harness.catalog import Fault
from benchmark.harness.runner import load_results
from benchmark.harness.settings import Settings

SIZE = 15
SEED = 5151


def rubric(settings: Settings) -> str:
    text = settings.judge_fix_prompt.read_text()
    return text[text.index("- `3`") : text.index("Reply with only")].strip()


def sample(
    faults: list[Fault],
    settings: Settings,
    models: list[str],
    prompt: str,
    targets: dict[str, str],
    out_dir: Path,
    out=print,
) -> int:
    if (out_dir / "key.json").exists():
        out(f"{out_dir}/key.json exists; not re-drawing (delete the folder to redraw)")
        return 2
    by_id = {f.fault_id: f for f in faults}
    pool: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    for model in models:
        run_dir = settings.results_dir / f"benchmark__{model}__{prompt}"
        for r in load_results(run_dir / "results.jsonl"):
            if r["status"] == "ok" and targets.get(r["fault_id"]) == r["incident_id"]:
                pool[by_id[r["fault_id"]].label.category].append((run_dir.name, r))
    rng = random.Random(SEED)
    cats = sorted(pool)
    picked: list[tuple[str, dict]] = []
    while len(picked) < SIZE and any(pool.values()):
        for c in cats:  # round-robin over true categories, then models within one
            if pool[c] and len(picked) < SIZE:
                taken = Counter(run for run, _ in picked)
                rng.shuffle(pool[c])
                pool[c].sort(key=lambda x: taken[x[0]])
                picked.append(pool[c].pop(0))
    rng.shuffle(picked)
    out_dir.mkdir(parents=True, exist_ok=True)
    key, lines = (
        {},
        [
            "# Fix-quality scoring sheet (blind)",
            "",
            "Score each suggested fix 0-3 with the rubric below, in `scores.csv` (`fix_score`).",
            "Optionally, also say whether the explanation names the true root cause",
            "(`root_verdict`: match / partial / mismatch). The model, its confidence and the",
            "judge's scores are hidden. Don't open `key.json` until you're done.",
            "",
            "## Rubric (the same text the LLM judge gets)",
            "",
            rubric(settings),
            "",
        ],
    )
    for n, (run, r) in enumerate(picked, 1):
        sid = f"S{n:02d}"
        key[sid] = {"run": run, "key": r["key"]}
        top = r["diagnosis"]["root_causes"][0]
        lines += [
            f"## {sid}",
            "",
            f"**Ground truth:** {by_id[r['fault_id']].label.root_cause}",
            "",
            f"**Category given:** {top['category']}",
            "",
            f"**Explanation:** {top['explanation']}",
            "",
            f"**Suggested fix:** {top['suggested_fix']}",
            "",
            f"**Retry safe:** {top['retry_safe']} ({top['retry_safe_reason']})",
            "",
        ]
    (out_dir / "sheet.md").write_text("\n".join(lines))
    (out_dir / "key.json").write_text(json.dumps(key, indent=2) + "\n")
    with (out_dir / "scores.csv").open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["sample_id", "fix_score", "root_verdict"])
        for sid in key:
            w.writerow([sid, "", ""])
    out(f"wrote {out_dir}/sheet.md and scores.csv ({len(picked)} diagnoses)")
    return 0


def cohen_kappa(a: list, b: list, labels: list, weights: str | None = None) -> float | None:
    n = len(a)
    if n == 0:
        return None
    idx = {lab: i for i, lab in enumerate(labels)}
    k = len(labels)

    def w(i: int, j: int) -> float:
        if weights == "quadratic":
            return ((i - j) / (k - 1)) ** 2
        return 0.0 if i == j else 1.0

    pa, pb = Counter(a), Counter(b)
    obs = sum(w(idx[x], idx[y]) for x, y in zip(a, b, strict=True)) / n
    exp = sum(w(idx[x], idx[y]) * pa[x] * pb[y] for x in labels for y in labels) / n**2
    return round(1 - obs / exp, 4) if exp else None


def agreement(settings: Settings, out_dir: Path) -> dict[str, Any]:
    key = json.loads((out_dir / "key.json").read_text())
    with (out_dir / "scores.csv").open() as fh:
        human = {r["sample_id"]: r for r in csv.DictReader(fh)}
    judged: dict[tuple[str, str, str], dict] = {}
    for run in {v["run"] for v in key.values()}:
        for j in load_results(settings.results_dir / run / "judge.jsonl"):
            if j.get("error") is None:
                judged[(run, j["key"], j["judge"])] = j
    fix_pairs, root_pairs, rows = [], [], []
    for sid, k in key.items():
        h = human.get(sid, {})
        hf = h.get("fix_score", "").strip()
        hr = h.get("root_verdict", "").strip().lower()
        jf = judged.get((k["run"], k["key"], "fix"), {}).get("score")
        jr = judged.get((k["run"], k["key"], "root"), {}).get("verdict")
        if re.fullmatch(r"[0-3]", hf) and jf is not None:
            fix_pairs.append((int(hf), jf))
        if hr in ("match", "partial", "mismatch") and jr:
            root_pairs.append((hr, jr))
        rows.append(
            {
                "sample_id": sid,
                **k,
                "human_fix": hf,
                "judge_fix": jf,
                "human_root": hr,
                "judge_root": jr,
            }
        )
    res: dict[str, Any] = {"samples": len(key), "rows": rows}
    if fix_pairs:
        h, j = zip(*fix_pairs, strict=True)
        res["fix"] = {
            "n": len(fix_pairs),
            "exact": round(sum(x == y for x, y in fix_pairs) / len(fix_pairs), 4),
            "within_one": round(sum(abs(x - y) <= 1 for x, y in fix_pairs) / len(fix_pairs), 4),
            "kappa_quadratic": cohen_kappa(list(h), list(j), [0, 1, 2, 3], "quadratic"),
            "human_mean": round(sum(h) / len(h), 3),
            "judge_mean": round(sum(j) / len(j), 3),
        }
    if root_pairs:
        h, j = zip(*root_pairs, strict=True)
        res["root"] = {
            "n": len(root_pairs),
            "exact": round(sum(x == y for x, y in root_pairs) / len(root_pairs), 4),
            "kappa": cohen_kappa(list(h), list(j), ["match", "partial", "mismatch"]),
        }
    (out_dir / "agreement.json").write_text(json.dumps(res, indent=2) + "\n")
    return res
