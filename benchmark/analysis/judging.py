"""Run both LLM judges over a finished benchmark run: resumable, ledger-gated.

For every diagnosis with status `ok` in <run>/results.jsonl, two judge calls (Haiku by
default, config/benchmark.toml [judge]):
  root  prompts/judge_v1.md      match | partial | mismatch
  fix   prompts/judge_fix_v1.md  0-3 rubric
Each verdict is appended to <run>/judge.jsonl and fsync'ed as it arrives, keyed by
"<result key>|root" or "|fix", so a re-run skips what is already judged (never pays
twice). Diagnoses that didn't finish `ok` are not judged; scoring counts them as wrong.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from benchmark.harness import judge
from benchmark.harness.catalog import Fault
from benchmark.harness.runner import append_result, load_results
from benchmark.harness.settings import Settings
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import AnthropicLLM, LLMClient

KINDS = ("root", "fix")


def judge_path(run_dir: Path) -> Path:
    return run_dir / "judge.jsonl"


def todo(rows: list[dict], done: list[dict]) -> list[tuple[dict, str]]:
    have = {(d["key"], d["judge"]) for d in done if d.get("error") is None}
    return [
        (r, k)
        for r in rows
        if r["status"] == "ok" and r["diagnosis"]
        for k in KINDS
        if (r["key"], k) not in have
    ]


def run(
    faults: list[Fault],
    settings: Settings,
    run_dir: Path,
    *,
    dry_run: bool = False,
    ledger: Ledger | None = None,
    llm_factory: Callable[[str], LLMClient] = AnthropicLLM,
    out: Callable[[str], None] = print,
) -> int:
    rows = load_results(run_dir / "results.jsonl")
    path = judge_path(run_dir)
    pending = todo(rows, load_results(path))
    ledger = ledger or Ledger()
    est = len(pending) * settings.judge_estimate_usd
    out(
        f"judge {run_dir.name}: {len(rows)} results, {len(pending)} judge calls to run "
        f"({settings.judge_model}); estimate ${est:.4f}; ledger spent ${ledger.spent():.4f} "
        f"of ${ledger.budget_usd:.2f}"
    )
    if ledger.spent() + est > ledger.budget_usd:
        out("REFUSED: over budget")
        return 3
    if dry_run or not pending:
        return 0
    truth = {f.fault_id: f.label.root_cause for f in faults}
    prompts = {
        "root": settings.judge_prompt.read_text(),
        "fix": settings.judge_fix_prompt.read_text(),
    }
    llm = llm_factory(settings.judge_model)
    for n, (r, kind) in enumerate(pending, 1):
        common = dict(
            prompt=prompts[kind],
            root_cause=truth[r["fault_id"]],
            diagnosis=r["diagnosis"],
            ref=f"{run_dir.name}|{r['key']}|{kind}",
            max_tokens=settings.judge_max_tokens,
        )
        rec = {"key": r["key"], "judge": kind, "model": settings.judge_model, "error": None}
        if kind == "root":
            v = judge.judge(llm, ledger, **common)
            rec |= {"verdict": v.verdict, "reason": v.reason, "cost_usd": v.cost_usd}
            if v.verdict == "error":
                rec["error"] = v.reason
        else:
            s = judge.judge_fix(llm, ledger, **common)
            rec |= {"score": s.score, "reason": s.reason, "cost_usd": s.cost_usd}
            if s.score is None:
                rec["error"] = s.reason
        rec["finished_at"] = datetime.now(UTC).isoformat()
        append_result(path, rec)
        out(f"[{n}/{len(pending)}] {r['key']} {kind} -> {rec.get('verdict', rec.get('score'))}")
    return 0
