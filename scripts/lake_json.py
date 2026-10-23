"""Print incidents, latest diagnoses and audit rows as JSON (for demo_e2e.py)."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from sentinel.actions import audit
from sentinel.cli import exit_now
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.diagnose import store


def main() -> int:
    lake = Path(os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))
    out = {
        "incidents": read_incidents(incidents_path(lake)),
        "diagnoses": store.read_all(lake),
        "audit": audit.read_all(lake),
    }
    sys.stdout.write(json.dumps(out, default=str))
    return 0


if __name__ == "__main__":
    exit_now(main())
