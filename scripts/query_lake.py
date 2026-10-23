"""Print what's in the lake for one ds: curated table row counts and emitted events.

make query DS=2026-10-20   (runs in the jobs venv inside the scheduler container)
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path

from deltalake import DeltaTable

from pipelines.common.config import DATASETS, curated_path, lake_root
from sentinel.cli import exit_now


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    args = parser.parse_args()

    print(f"lake: {lake_root()}  ds={args.ds}\n")
    print(f"{'table':<16}{'version':>8}{'rows(ds)':>10}")
    empty = []
    for name in DATASETS:
        path = curated_path(name)
        if not (path / "_delta_log").exists():
            print(f"{name:<16}{'-':>8}{'no table':>10}")
            empty.append(name)
            continue
        table = DeltaTable(str(path))
        if "ds" in table.metadata().partition_columns:
            rows = table.to_pandas(partitions=[("ds", "=", args.ds)]).shape[0]
        else:
            rows = table.to_pandas().shape[0]
        print(f"{name:<16}{table.version():>8}{rows:>10}")
        if rows == 0:
            empty.append(name)
    if empty:
        print(
            f"\nNo data for ds={args.ds} in: {', '.join(empty)}."
            f"\nRun `make run-all DATE={args.ds}` first."
        )

    events_dir = Path(os.environ.get("SENTINEL_EVENTS_DIR", "data/events"))
    counts: Counter[tuple[str, str]] = Counter()
    for f in sorted(events_dir.glob("*/*.jsonl")):
        for line in f.read_text().splitlines():
            ev = json.loads(line)
            if ev.get("logical_date", "") and not ev["logical_date"].startswith(args.ds):
                continue
            counts[(ev["event_type"], ev["pipeline"])] += 1
    print(f"\nevents for ds={args.ds} (from {events_dir}):")
    for (etype, pipeline), n in sorted(counts.items()):
        print(f"  {etype:<12} {pipeline:<20} {n}")


if __name__ == "__main__":
    main()
    exit_now(0)
