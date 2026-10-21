"""End-to-end on the running stack: trigger orders_ingest, then assert that its events
went Airflow/jobs -> Redpanda -> lander -> Delta bronze, tagged with the run and the
deployed code version.

Run with `make test-e2e` (needs `make up`).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pandas as pd
import pytest
from deltalake import DeltaTable

from sentinel.airflow_client import AirflowClient

pytestmark = pytest.mark.integration

LAKE = Path(os.environ.get("SENTINEL_E2E_LAKE_ROOT", "/opt/sentinel/data/lake"))
DEPLOY_STATE = LAKE.parent / "deploy" / "state.json"
DS = "2026-08-01"


def _wait(predicate, timeout_s: float, what: str, interval: float = 3.0):
    """Poll until predicate() returns something other than None."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = predicate()
        if value is not None:
            return value
        time.sleep(interval)
    pytest.fail(f"timed out after {timeout_s}s waiting for {what}")


def _bronze(topic: str, run_id: str) -> pd.DataFrame:
    path = LAKE / "bronze" / topic
    if not DeltaTable.is_deltatable(str(path)):
        return pd.DataFrame()
    df = DeltaTable(str(path)).to_pandas()
    return df[df["run_id"] == run_id]


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    if not os.environ.get("AIRFLOW_ADMIN_PASSWORD"):
        pytest.skip("stack credentials not available (run via `make test-e2e`)")
    c = AirflowClient.from_env()
    try:
        c.health()
    except OSError:
        pytest.skip("Airflow is not reachable; run `make up` first")
    return c


def test_dag_run_events_land_in_bronze(client) -> None:
    run_id = f"e2e__{uuid.uuid4().hex[:10]}"
    # A unique time-of-day keeps the logical date unique per test run; ds stays DS.
    secs = uuid.uuid4().int % 86_000
    logical = f"{DS}T{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}+00:00"
    client.unpause("orders_ingest")
    client.trigger("orders_ingest", run_id, logical)

    def finished():
        state = client.get_run("orders_ingest", run_id)["state"]
        return state if state in ("success", "failed") else None

    state = _wait(finished, 300, "the DAG run to finish")
    assert state == "success"

    def all_landed():
        metrics = _bronze("job_metrics", run_id)
        task_runs = metrics[metrics["event_type"] == "task_run"]
        landed = len(task_runs) >= 3 and len(_bronze("dq_results", run_id)) >= 1
        return metrics if landed else None

    metrics = _wait(all_landed, 90, "events to land in bronze")

    task_runs = metrics[metrics["event_type"] == "task_run"]
    payloads = [json.loads(p) for p in task_runs["payload"]]
    tasks = {"generate_orders", "ingest_orders", "validate_orders"}
    assert {p["task_id"] for p in payloads} == tasks
    assert {p["state"] for p in payloads} == {"success"}

    job_metrics = [json.loads(p) for p in metrics[metrics["event_type"] == "job_metric"]["payload"]]
    by_job = {m["job_name"]: m for m in job_metrics}
    assert by_job["orders_ingest"]["status"] == "success"
    assert by_job["orders_ingest"]["rows_out"] == by_job["generate_orders"]["rows_out"] > 0
    assert by_job["orders_ingest"]["extra"]["spark_version"] == "3.5.4"

    [dq] = [json.loads(p) for p in _bronze("dq_results", run_id)["payload"]]
    assert dq["dataset"] == "orders" and dq["success"] is True

    logs = _bronze("job_logs", run_id)
    assert len(logs) > 0
    assert set(logs["task_id"]) <= {"generate_orders", "ingest_orders", "validate_orders"}

    # Every event carries the version recorded by `make deploy`, and a deploy event for
    # that version exists in bronze.
    version = json.loads(DEPLOY_STATE.read_text())["pipelines"]["orders_ingest"]["version"]
    assert set(metrics["code_version"]) == {version}
    deploys = DeltaTable(str(LAKE / "bronze" / "deploy_events")).to_pandas()
    mine = deploys[(deploys["pipeline"] == "orders_ingest") & (deploys["code_version"] == version)]
    assert len(mine) >= 1

    # Nothing from this run was dead-lettered.
    dl_path = LAKE / "bronze" / "_dead_letter"
    if DeltaTable.is_deltatable(str(dl_path)):
        dl = DeltaTable(str(dl_path)).to_pandas()
        assert not dl["raw_value"].fillna("").str.contains(run_id).any()
