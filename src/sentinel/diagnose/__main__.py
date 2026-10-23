"""python -m sentinel.diagnose --incident INC-...   (make diagnose INCIDENT=...)

Diagnoses one primary incident, stores it in the `diagnoses` table and posts it to
Slack (the mock unless SLACK_BOT_TOKEN/SLACK_CHANNEL are set).

  --dry-run   no API call: shows the budget check and worst-case cost only
  --mock      use the rule-based MOCK instead of Claude (no key needed, $0)
  --model     claude-haiku-5-5 (default) | claude-sonnet-5-5 (or env SENTINEL_MODEL)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from sentinel.cli import exit_now
from sentinel.diagnose.config import load_config
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import RuleBasedLLM, llm_from_env
from sentinel.diagnose.run import NotDiagnosable, diagnose_incident


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sentinel.diagnose")
    parser.add_argument("--incident", required=True)
    parser.add_argument("--lake", default=os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))
    parser.add_argument("--model", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--mock", action="store_true", help="rule-based MOCK, no API")
    parser.add_argument("--no-notify", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    cfg = load_config(model=args.model)
    ledger = Ledger(budget_usd=cfg.budget_usd)
    llm = RuleBasedLLM() if args.mock else llm_from_env(cfg.agent.model)
    if llm is None and not args.dry_run:
        print("ANTHROPIC_API_KEY is not set. Use --mock for the rule-based mock, or --dry-run.")
        return 2
    if llm is None:  # dry run without a key: still show the gate for the real model
        from sentinel.diagnose.llm import ScriptedLLM

        llm = ScriptedLLM([], model=cfg.agent.model)
    print(f"model={llm.model}  prompt={cfg.prompt_version}  ledger={ledger.path}")
    print(f"spent so far ${ledger.spent():.4f} of ${cfg.budget_usd:.2f}")
    try:
        out = diagnose_incident(
            Path(args.lake),
            args.incident,
            cfg=cfg,
            llm=llm,
            ledger=ledger,
            notify=not args.no_notify,
            dry_run=args.dry_run,
        )
    except NotDiagnosable as exc:
        print(exc)
        return 2
    run = out.run
    print(
        f"status={run.status} calls={run.llm_calls} tool_calls={run.tool_calls} "
        f"tools={run.tools_used}"
    )
    u = run.usage
    print(
        f"tokens in={u.input_tokens} out={u.output_tokens} cache_read={u.cache_read_input_tokens}"
        f" cache_write={u.cache_creation_input_tokens}  cost=${run.cost_usd:.5f}  "
        f"latency={run.latency_s}s"
    )
    for e in run.errors:
        print(f"  note: {e}")
    if run.diagnosis:
        print(json.dumps(run.diagnosis.model_dump(), indent=2))
    if out.message_id:
        print(f"posted to Slack: {out.message_id}")
    return 0 if run.status in ("ok", "dry_run") else 1


if __name__ == "__main__":
    exit_now(main())
