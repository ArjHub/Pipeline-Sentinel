"""Runs with Airflow's interpreter (not the jobs venv): DAGs must import cleanly and
have the expected shape."""

from __future__ import annotations

import itertools
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.sensors.external_task import ExternalTaskSensor

DAGS_DIR = Path(__file__).resolve().parents[2] / "pipelines" / "dags"

EXPECTED = {
    "orders_ingest": ["generate_orders", "ingest_orders", "validate_orders"],
    "customers_dim": ["generate_customers", "build_customers_dim", "validate_customers"],
    "payments_etl": ["generate_payments", "run_payments_glue_job", "validate_payments"],
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag) -> None:
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {*EXPECTED, "daily_revenue_agg"}


@pytest.mark.parametrize(("dag_id", "chain"), EXPECTED.items())
def test_linear_chains(dagbag, dag_id, chain) -> None:
    dag = dagbag.dags[dag_id]
    assert [t.task_id for t in dag.topological_sort()] == chain
    for upstream, downstream in itertools.pairwise(chain):
        assert downstream in dag.get_task(upstream).downstream_task_ids


def test_daily_revenue_waits_on_every_upstream_validation(dagbag) -> None:
    dag = dagbag.dags["daily_revenue_agg"]
    sensors = [t for t in dag.tasks if isinstance(t, ExternalTaskSensor)]
    assert {(s.external_dag_id, s.external_task_id) for s in sensors} == {
        (dag_id, chain[-1]) for dag_id, chain in EXPECTED.items()
    }
    for s in sensors:
        assert s.mode == "reschedule"
        assert "failed" in s.failed_states  # fail fast on upstream failure
        assert "aggregate_revenue" in s.downstream_task_ids


def test_spark_tasks_use_the_bounded_pool(dagbag) -> None:
    spark_tasks = {
        "ingest_orders",
        "build_customers_dim",
        "run_payments_glue_job",
        "aggregate_revenue",
    }
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if task.task_id in spark_tasks:
                assert task.pool == "spark", task.task_id


def test_jobs_run_in_the_isolated_venv_with_templated_ds(dagbag) -> None:
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if hasattr(task, "bash_command"):
                assert task.bash_command.startswith("/opt/venv/jobs/bin/python -m pipelines.")
                assert "--ds {{ ds }}" in task.bash_command


def test_defaults(dagbag) -> None:
    for dag in dagbag.dags.values():
        assert dag.schedule_interval is None
        assert dag.catchup is False
        assert "pipeline-sentinel" in dag.tags
        assert all(t.retries == 0 for t in dag.tasks)


def test_every_task_publishes_task_run_events(dagbag) -> None:
    from sentinel.instrumentation import airflow_callbacks as cb

    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert cb.on_success in _as_list(task.on_success_callback), task.task_id
            assert cb.on_failure in _as_list(task.on_failure_callback), task.task_id
            assert cb.on_retry in _as_list(task.on_retry_callback), task.task_id


def _as_list(callbacks) -> list:
    if callbacks is None:
        return []
    return list(callbacks) if isinstance(callbacks, list | tuple) else [callbacks]
