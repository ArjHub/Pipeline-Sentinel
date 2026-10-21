"""Run all four DAGs for one logical date and wait for them to finish.

    python3 scripts/run_all.py [--date YYYY-MM-DD] [--timeout 1800]

If a run for that date already exists it is cleared and re-run in place, so this is
safe to repeat. Exit code is 0 only if every DAG run succeeds.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sentinel.airflow_client import TERMINAL_STATES, AirflowApiError, AirflowClient

DAGS = ["orders_ingest", "customers_dim", "payments_etl", "daily_revenue_agg"]


def wait_for_dags(client: AirflowClient, timeout_s: int = 180) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            errors = client.import_errors()
            if errors:
                for e in errors:
                    print(f"IMPORT ERROR {e['filename']}: {e['stack_trace'][:400]}")
                raise SystemExit(2)
            missing = set(DAGS) - set(client.list_dag_ids())
            if not missing:
                return
        except (AirflowApiError, OSError) as exc:
            missing = {f"(api not ready: {exc})"}
        if time.monotonic() > deadline:
            raise SystemExit(f"DAGs not loaded after {timeout_s}s: {sorted(missing)}")
        time.sleep(5)


def start_run(client: AirflowClient, dag_id: str, ds: str) -> str:
    logical = f"{ds}T00:00:00+00:00"
    client.unpause(dag_id)
    existing = client.find_run(dag_id, logical)
    if existing:
        client.clear_run(dag_id, existing["dag_run_id"], only_failed=False)
        print(f"{dag_id}: cleared existing run {existing['dag_run_id']}")
        return existing["dag_run_id"]
    run_id = f"run_all__{ds}"
    client.trigger(dag_id, run_id, logical)
    print(f"{dag_id}: triggered {run_id}")
    return run_id


def main() -> int:
    parser = argparse.ArgumentParser()
    yesterday = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    parser.add_argument("--date", default=yesterday)
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    date.fromisoformat(args.date)  # validate

    client = AirflowClient.from_env()
    wait_for_dags(client)
    run_ids = {dag: start_run(client, dag, args.date) for dag in DAGS}

    t0 = time.monotonic()
    states: dict[str, str] = {}
    while True:
        states = {dag: client.get_run(dag, rid)["state"] for dag, rid in run_ids.items()}
        elapsed = int(time.monotonic() - t0)
        print(f"[{elapsed:>4}s] " + "  ".join(f"{d}={s}" for d, s in states.items()), flush=True)
        if all(s in TERMINAL_STATES for s in states.values()):
            break
        if elapsed > args.timeout:
            print("timed out waiting for DAG runs")
            return 1
        time.sleep(10)

    failed = [d for d, s in states.items() if s != "success"]
    for dag in failed:
        for ti in client.task_instances(dag, run_ids[dag]):
            if ti["state"] not in ("success", None):
                print(f"  {dag}.{ti['task_id']}: {ti['state']}")
    print(f"\nds={args.date}: {len(DAGS) - len(failed)}/{len(DAGS)} DAG runs succeeded")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
