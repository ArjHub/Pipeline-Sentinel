"""List incidents (primary first, downstream nested under their root), or show one.

make query-incidents                 # all incidents
make query-incidents STATUS=open     # only open ones
make query-incidents ID=INC-1a2b3c4d5e   # one incident with its full evidence JSON
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sentinel.cli import exit_now
from sentinel.detect.store import incidents_path, read_incidents


def _ts(v) -> str:
    return v.strftime("%Y-%m-%d %H:%M") if v else "-"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", default="", help="open | resolved | superseded (default all)")
    parser.add_argument("--id", default="")
    args = parser.parse_args()

    path = incidents_path(Path(os.environ.get("SENTINEL_LAKE_ROOT", "data/lake")))
    rows = read_incidents(path)
    if not rows:
        print(f"no incidents table at {path} yet: run `make detect` first")
        return 0

    if args.id:
        match = [r for r in rows if r["incident_id"] == args.id]
        if not match:
            print(f"{args.id} not found")
            return 1
        r = dict(match[0])
        r["evidence"] = json.loads(r["evidence"])
        print(json.dumps(r, indent=2, default=str))
        return 0

    shown = [r for r in rows if not args.status or r["status"] == args.status]
    counts = {s: sum(r["status"] == s for r in rows) for s in ("open", "resolved", "superseded")}
    summary = " ".join(f"{k}={v}" for k, v in counts.items())
    print(f"incidents: {path}  total={len(rows)} {summary}")
    hdr = f"{'incident_id':<15} {'status':<10} {'sev':<6} {'type':<15} {'pipeline':<18} "
    print(f"\n{hdr}{'first_seen':<17} {'last_seen':<17} ds / title")
    children: dict[str, list[dict]] = {}
    for r in shown:
        if r["root_incident_id"]:
            children.setdefault(r["root_incident_id"], []).append(r)

    def line(r: dict, indent: str = "") -> str:
        ds = ",".join(json.loads(r["evidence"]).get("affected_ds", []))
        return (
            f"{indent}{r['incident_id']:<{15 - len(indent)}} {r['status']:<10} "
            f"{r['severity']:<6} {r['type']:<15} {r['pipeline']:<18} {_ts(r['first_seen']):<17} "
            f"{_ts(r['last_seen']):<17} [{ds}] {r['title']}"
        )

    shown_ids = {r["incident_id"] for r in shown}
    for r in sorted(shown, key=lambda r: r["first_seen"]):
        if r["root_incident_id"] and r["root_incident_id"] in shown_ids:
            continue  # printed under its root
        print(line(r))
        for c in sorted(children.get(r["incident_id"], []), key=lambda c: c["first_seen"]):
            print(line(c, "  └ "))
    return 0


if __name__ == "__main__":
    exit_now(main())
