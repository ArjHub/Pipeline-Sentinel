"""Diagnoses table + LLM ledger spend.   make query-diagnoses [ID=INC-...]"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from sentinel.cli import exit_now
from sentinel.diagnose import store
from sentinel.diagnose.config import load_config
from sentinel.diagnose.ledger import Ledger


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", default=os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))
    parser.add_argument("--id", help="incident id: print its latest diagnosis in full")
    args = parser.parse_args()
    lake = Path(args.lake)
    cfg = load_config()
    ledger = Ledger(budget_usd=cfg.budget_usd)
    rows = store.read_all(lake)
    print(f"diagnoses: {store.diagnoses_path(lake)}  total={len(rows)}")
    print(f"ledger: {ledger.path}  spent=${ledger.spent():.5f} of ${cfg.budget_usd:.2f}\n")
    if args.id:
        row = store.latest(lake, args.id)
        if not row:
            print(f"no diagnosis for {args.id}")
            return 1
        out = {k: (str(v) if k == "created_at" else v) for k, v in row.items()}
        out["result"] = json.loads(row["result"]) if row["result"] else None
        out["errors"] = json.loads(row["errors"])
        print(json.dumps(out, indent=2))
        return 0
    print(
        f"{'created':<17} {'incident':<18} {'model':<17} {'status':<9} {'top cause':<16} "
        f"{'conf':>5} {'retry':<5} {'tools':>5} {'tokens in/out':>14} {'cost':>9}"
    )
    for r in rows:
        print(
            f"{r['created_at']:%Y-%m-%d %H:%M} {r['incident_id']:<18} {r['model']:<17} "
            f"{r['status']:<9} {(r['top_category'] or '-'):<16} "
            f"{(r['top_confidence'] or 0):>5.2f} {r['retry_safe']!s:<5} {r['tool_calls']:>5} "
            f"{str(r['input_tokens']) + '/' + str(r['output_tokens']):>14} ${r['cost_usd']:>8.5f}"
        )
    return 0


if __name__ == "__main__":
    exit_now(main())
