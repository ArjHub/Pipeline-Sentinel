"""Run the diagnosis agent over the scenario lakes: resumable, ledger-gated.

Work items: every primary incident a fault's night produced (usually one; a clean run
usually none), times `repeats`, in repeat-major order (all faults once, then all again),
so no repeat runs right after itself and reuses its own prompt cache.

Every item's result is appended to results.jsonl and fsync'ed as soon as it finishes.
A re-run with --resume skips items already in the file, so a timeout or a sleeping
laptop costs at most the item that was in flight. Items that ended in `error` (an API
failure) are kept and skipped too unless --retry-errors; `budget_refused` items cost
nothing and are always retried.

Before anything runs, the cost of the pending items is estimated and printed. The run
is refused if ledger spent + the "high" estimate > the $10 budget. Each diagnosis then
also reserves its own worst case under the token caps (sentinel.diagnose.agent), which
is what makes the cap hard. --dry-run stops after the estimate.

Results hold the agent's output only. Labels stay in the sealed catalog; scoring joins
them later (Phase 5b).
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmark.harness import scenario
from benchmark.harness.catalog import Fault
from benchmark.harness.catalog import sha256 as catalog_sha256
from benchmark.harness.settings import Settings
from sentinel.detect.config import load_config as load_detection_config
from sentinel.diagnose.agent import ESTIMATE_SAFETY, Agent
from sentinel.diagnose.config import DiagnosisConfig, load_config
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import AnthropicLLM, LLMClient
from sentinel.diagnose.pricing import worst_case_usd
from sentinel.diagnose.run import find_incident
from sentinel.diagnose.tools import Toolbox

DONE_STATUSES = ("ok", "invalid", "refused", "capped", "error")


@dataclass(frozen=True)
class Item:
    fault_id: str
    incident_id: str
    repeat: int

    @property
    def key(self) -> str:
        return f"{self.fault_id}|{self.incident_id}|{self.repeat}"


@dataclass(frozen=True)
class Estimate:
    items: int
    expected_usd: float
    high_usd: float
    worst_usd: float
    judge_usd: float
    assumed: int  # faults whose scenario isn't built (1 incident assumed)

    @property
    def gate_usd(self) -> float:
        return self.high_usd + self.judge_usd


def plan(
    faults: list[Fault], settings: Settings, repeats: int
) -> tuple[list[Item], int]:  # (items, faults assumed to have one incident)
    targets: dict[str, list[str]] = {}
    assumed = 0
    for f in faults:
        m = scenario.manifest(settings, f.fault_id)
        if m is None:
            targets[f.fault_id] = ["(not built)"]
            assumed += 1
        else:
            targets[f.fault_id] = m["targets"]
    items = [
        Item(f.fault_id, iid, r)
        for r in range(repeats)
        for f in faults
        for iid in targets[f.fault_id]
    ]
    return items, assumed


def estimate(
    items: list[Item],
    model: str,
    settings: Settings,
    cfg: DiagnosisConfig,
    assumed: int,
    judge: bool,
) -> Estimate:
    n = len(items)
    worst = ESTIMATE_SAFETY * worst_case_usd(
        model, cfg.agent.max_input_tokens, cfg.agent.max_output_tokens
    )
    if model not in settings.mean_usd:
        raise ValueError(f"no measured cost for {model} in config/benchmark.toml [estimate]")
    return Estimate(
        items=n,
        expected_usd=n * settings.mean_usd[model],
        high_usd=n * settings.max_usd[model] * settings.high_multiple,
        worst_usd=n * worst,
        judge_usd=n * settings.judge_estimate_usd if judge else 0.0,
        assumed=assumed,
    )


def results_path(settings: Settings, split: str, model: str, prompt_version: str) -> Path:
    return settings.results_dir / f"{split}__{model}__{prompt_version}" / "results.jsonl"


def load_results(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def append_result(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, default=str) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def pending(items: list[Item], done: list[dict[str, Any]], retry_errors: bool) -> list[Item]:
    finished = {
        r["key"]
        for r in done
        if r["status"] in DONE_STATUSES and not (retry_errors and r["status"] == "error")
    }
    return [i for i in items if i.key not in finished]


def _record(item: Item, run, cfg: DiagnosisConfig, scenario_hash: str, cat_sha: str) -> dict:
    d = run.diagnosis
    return {
        "key": item.key,
        "fault_id": item.fault_id,
        "incident_id": item.incident_id,
        "repeat": item.repeat,
        "model": run.model,
        "prompt_version": run.prompt_version,
        "prompt_sha256": hashlib.sha256(cfg.prompt_text.encode()).hexdigest(),
        "status": run.status,
        "diagnosis": d.model_dump() if d else None,
        "top_category": d.top.category if d else None,
        "top_confidence": d.top.confidence if d else None,
        "retry_safe": d.retry_safe if d else None,
        "llm_calls": run.llm_calls,
        "tool_calls": run.tool_calls,
        "tools_used": run.tools_used,
        "input_tokens": run.usage.input_tokens,
        "output_tokens": run.usage.output_tokens,
        "cache_read_input_tokens": run.usage.cache_read_input_tokens,
        "cache_creation_input_tokens": run.usage.cache_creation_input_tokens,
        "cost_usd": round(run.cost_usd, 8),
        "latency_s": run.latency_s,
        "errors": run.errors,
        "finished_at": datetime.now(UTC).isoformat(),
        "scenario_build_hash": scenario_hash,
        "catalog_sha256": cat_sha,
    }


def run(
    faults: list[Fault],
    settings: Settings,
    *,
    split: str,
    model: str,
    repeats: int,
    prompt: str,
    dry_run: bool = False,
    resume: bool = False,
    retry_errors: bool = False,
    judge: bool = False,
    ledger: Ledger | None = None,
    llm_factory: Callable[[str], LLMClient] = AnthropicLLM,
    out: Callable[[str], None] = print,
) -> int:
    cfg = load_config(model=model, prompt=prompt)
    ledger = ledger or Ledger(budget_usd=cfg.budget_usd)
    path = results_path(settings, split, model, cfg.prompt_version)
    done = load_results(path)
    if done and not resume and not dry_run:
        out(
            f"{path} already has {len(done)} results. Use RESUME=1 to continue that run "
            "(nothing is repeated), or move the file away to start over."
        )
        return 2
    items, assumed = plan(faults, settings, repeats)
    todo = pending(items, done, retry_errors)
    est = estimate(todo, model, settings, cfg, assumed, judge)
    spent, budget = ledger.spent(), ledger.budget_usd
    out(
        f"benchmark split={split} faults={len(faults)} model={model} prompt={cfg.prompt_version} "
        f"repeats={repeats}"
    )
    out(f"results: {path}")
    out(
        f"work items: {len(items)} total, {len(items) - len(todo)} already done, {len(todo)} to run"
    )
    if assumed:
        out(
            f"  ({assumed} scenario(s) not built yet: 1 incident each assumed; "
            "`make benchmark-build` for exact counts)"
        )
    out("cost estimate for the items to run:")
    out(f"  expected  ${est.expected_usd:8.4f}   (Phase 4 mean per diagnosis x {est.items})")
    out(f"  high      ${est.high_usd:8.4f}   (2 x the dearest Phase 4 diagnosis; gate uses this)")
    out(
        f"  worst     ${est.worst_usd:8.4f}   (every diagnosis at its token caps; not reachable "
        "in practice, each one reserves this before it starts)"
    )
    if judge:
        out(f"  judge     ${est.judge_usd:8.4f}")
    out(
        f"ledger: spent ${spent:.4f} of ${budget:.2f}; spent + gate estimate = "
        f"${spent + est.gate_usd:.4f}"
    )
    if spent + est.gate_usd > budget:
        out(f"REFUSED: ${spent:.4f} + ${est.gate_usd:.4f} > ${budget:.2f}")
        return 3
    if dry_run:
        out("dry run: no API call made")
        return 0
    if os.environ.get("SENTINEL_AUTO_DIAGNOSE") != "0":
        out(
            "REFUSED: set SENTINEL_AUTO_DIAGNOSE=0 for benchmark runs (the harness triggers "
            "every diagnosis itself, so cost and latency are measured cleanly)"
        )
        return 2
    missing = sorted({i.fault_id for i in todo if scenario.manifest(settings, i.fault_id) is None})
    if missing:
        out(f"REFUSED: {len(missing)} scenario(s) not built; run `make benchmark-build` first")
        return 2

    llm = llm_factory(model)
    det_cfg = load_detection_config()
    t = cfg.tools
    cat_sha = catalog_sha256(settings.catalog_path)
    for n, item in enumerate(todo, 1):
        lake = scenario.lake_path(settings, item.fault_id)
        incident = find_incident(lake, item.incident_id)
        toolbox = Toolbox(
            lake,
            det_cfg,
            incident,
            ds_window_days=t.ds_window_days,
            max_items=t.max_items,
            item_max_chars=t.item_max_chars,
            traceback_max_chars=t.traceback_max_chars,
            output_max_chars=t.output_max_chars,
        )
        r = Agent(cfg, llm, ledger, purpose="benchmark").diagnose(incident, toolbox)
        m = scenario.manifest(settings, item.fault_id) or {}
        append_result(path, _record(item, r, cfg, m.get("build_hash", ""), cat_sha))
        out(
            f"[{n}/{len(todo)}] {item.fault_id} r{item.repeat} {r.status} "
            f"${r.cost_usd:.5f} {r.latency_s:.1f}s"
        )
        if r.status == "budget_refused":
            out("stopping: the ledger refused the next diagnosis")
            return 3
    return 0
