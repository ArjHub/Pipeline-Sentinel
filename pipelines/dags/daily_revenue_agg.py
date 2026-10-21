"""Waits for all three upstream DAGs (same logical date) to pass validation, then
aggregates. Sensors fail fast if an upstream run fails, instead of waiting out the
timeout."""

import os

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from pipelines.common.dag_helpers import (
    DEFAULT_ARGS,
    SCHEDULE,
    START_DATE,
    TAGS,
    spark_task,
    validate_task,
)

UPSTREAM = {
    "orders_ingest": "validate_orders",
    "customers_dim": "validate_customers",
    "payments_etl": "validate_payments",
}

with DAG(
    dag_id="daily_revenue_agg",
    description="Join orders, payments and customers into daily revenue (PySpark)",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "spark", "downstream"],
):
    sensors = [
        ExternalTaskSensor(
            task_id=f"wait_for_{dag_id}",
            external_dag_id=dag_id,
            external_task_id=task_id,
            allowed_states=["success"],
            failed_states=["failed", "upstream_failed", "skipped"],
            mode="reschedule",
            poke_interval=10,
            timeout=int(os.environ.get("SENTINEL_UPSTREAM_TIMEOUT_S", "1800")),
        )
        for dag_id, task_id in UPSTREAM.items()
    ]
    (
        sensors
        >> spark_task("aggregate_revenue", "pipelines.jobs.daily_revenue_agg")
        >> validate_task("daily_revenue")
    )
