"""python -m sentinel.detect [--now ISO] [--dry-run]   (make detect)

One batch pass: read bronze (last `history_days`), run the four detectors, correlate,
upsert the incidents table, print a summary. Safe to run as often as you like.
"""

from __future__ import annotations

import argparse
import os
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sentinel.cli import exit_now
from sentinel.detect import correlate, load_bronze, load_config, run_detectors
from sentinel.detect.config import config_path
from sentinel.detect.store import incidents_path, upsert


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sentinel.detect")
    parser.add_argument("--lake", default=os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--now", help="evaluation time (ISO); default: current UTC time")
    parser.add_argument("--dry-run", action="store_true", help="print, don't write")
    args = parser.parse_args(argv)

    now = datetime.fromisoformat(args.now) if args.now else datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    cfg = load_config(args.config)
    lake = Path(args.lake)
    since = (now - timedelta(days=cfg.history_days)).date()

    obs = load_bronze(lake / "bronze", since)
    print(f"config: {args.config or config_path()}")
    print(
        f"bronze since {since}: "
        + ", ".join(f"{k}={v}" for k, v in sorted(obs.events_read.items()))
        + f"  -> {len(obs.attempts)} task attempts, {len(obs.jobs)} job runs, "
        f"{len(obs.dq)} DQ results"
    )
    findings = run_detectors(obs, cfg, now)
    by_type = Counter(f.type.value for f in findings)
    print(
        f"findings: {len(findings)} "
        + (f"({', '.join(f'{k}={v}' for k, v in sorted(by_type.items()))})" if findings else "")
    )
    incidents = correlate(findings, cfg)
    roles = Counter(i.role for i in incidents)
    print(
        f"incidents: {len(incidents)} (primary={roles['primary']}, "
        f"downstream={roles['downstream']})"
    )

    if not args.dry_run:
        path = incidents_path(lake)
        res = upsert(path, incidents, now)
        print(
            f"written to {path}: new={len(res.new)} updated={len(res.updated)} "
            f"unchanged={len(res.unchanged)} superseded={len(res.superseded)}"
        )

    open_primary = [i for i in incidents if i.status == "open" and i.role == "primary"]
    print(f"\nopen primary incidents: {len(open_primary)}")
    for i in open_primary:
        downstream = [d for d in incidents if d.root_incident_id == i.incident_id]
        print(f"  {i.incident_id}  {i.severity:<6} {i.type:<15} {i.pipeline:<18} {i.title}")
        for d in downstream:
            print(f"      └─ {d.incident_id} downstream {d.pipeline} ({d.status})")
    if not open_primary:
        print("  (none)")
    print("\nmake query-incidents  # all incidents, with evidence for one: ID=INC-...")
    return 0


if __name__ == "__main__":
    exit_now(main())
