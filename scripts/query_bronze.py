"""Row counts and sample rows for every bronze table.

    make query-bronze DATE=2026-10-20

DATE is the *business* date (Airflow ds / logical_date). Bronze is partitioned by the
date an event was *emitted*, which is usually the day after ds for a daily batch, so
rows are filtered by logical_date here. Deploy events have no logical date, so all of
them are shown.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from deltalake import DeltaTable

from sentinel.cli import exit_now
from sentinel.lander.bronze import DEAD_LETTER, TOPICS, bronze_root

SAMPLES = 3


def _short(payload: str, n: int = 90) -> str:
    try:
        doc = json.loads(payload)
    except json.JSONDecodeError:
        return payload[:n]
    keys = (
        "kind",
        "state",
        "status",
        "job_name",
        "level",
        "message",
        "dataset",
        "success",
        "component",
        "code_version",
        "changed_files",
    )
    picked = {k: doc[k] for k in keys if k in doc}
    if "changed_files" in picked:
        picked["changed_files"] = f"[{len(picked['changed_files'])} files]"
    text = json.dumps(picked, default=str)
    return text if len(text) <= n else text[: n - 3] + "..."


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True)
    args = parser.parse_args()
    root = bronze_root()
    print(f"bronze: {root}  ds={args.ds}\n")
    missing = []

    for topic in TOPICS:
        path = root / topic
        if not DeltaTable.is_deltatable(str(path)):
            print(f"== {topic}: no table yet")
            missing.append(topic)
            continue
        df = DeltaTable(str(path)).to_pandas()
        if topic != "deploy_events":
            df = df[df["logical_date"].fillna("").str.startswith(args.ds)]
        dates = DeltaTable(str(path)).to_pandas(columns=["event_date"])["event_date"]
        parts = sorted(dates.unique())
        by_type = df.groupby("event_type").size().to_dict() if len(df) else {}
        label = "all" if topic == "deploy_events" else f"ds={args.ds}"
        print(f"== {topic}: {len(df)} rows ({label})  by type={by_type}  partitions={parts}")
        cols = ["event_type", "pipeline", "task_id", "code_version", "payload"]
        for _, row in df.sort_values("emitted_at").head(SAMPLES)[cols].iterrows():
            print(
                f"   {row.event_type:<12} {row.pipeline:<18} {row.task_id!s:<22} "
                f"{row.code_version:<10} {_short(row.payload)}"
            )
        if len(df) == 0:
            missing.append(topic)
        print()

    dl = root / DEAD_LETTER
    n_dead = len(DeltaTable(str(dl)).to_pandas()) if DeltaTable.is_deltatable(str(dl)) else 0
    print(f"== {DEAD_LETTER}: {n_dead} rows")
    if missing:
        print(
            f"\nNo bronze rows for ds={args.ds} in: {', '.join(missing)}."
            f"\nRun `make run-all DATE={args.ds}` first"
            " (and check `docker compose logs sentinel-lander`)."
        )
    return 0


if __name__ == "__main__":
    os.environ.setdefault("SENTINEL_LAKE_ROOT", "data/lake")
    exit_now(main())
