"""Small factories so the four DAGs read as a list of steps.

Job code runs in a separate venv (/opt/venv/jobs) via BashOperator, so Spark/GX
dependencies never collide with Airflow's own pinned constraint set. Airflow passes
AIRFLOW_CTX_* env vars to the process, which the job uses to tag its events.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator

from sentinel.instrumentation.airflow_callbacks import on_failure, on_retry, on_success

JOBS_PYTHON = os.environ.get("SENTINEL_JOBS_PYTHON", "/opt/venv/jobs/bin/python")
SPARK_POOL = "spark"
START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")

DEFAULT_ARGS = {
    "owner": "data-platform",
    # No automatic retries: a failure should surface as an incident immediately. The
    # human-approved "Retry DAG" button (Phase 4) is the retry path.
    "retries": int(os.environ.get("SENTINEL_TASK_RETRIES", "0")),
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=20),
    # Every task attempt publishes a task_run event (Phase 2).
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry,
}

# Manual/triggered only: `make run-all` and the fault harness pick the logical date,
# which keeps runs deterministic. Set a cron here for a real deployment.
SCHEDULE = None
TAGS = ["pipeline-sentinel"]


def _cmd(module: str, *args: str) -> str:
    return " ".join([JOBS_PYTHON, "-m", module, *args])


def generate_task(dataset: str) -> BashOperator:
    return BashOperator(
        task_id=f"generate_{dataset}",
        bash_command=_cmd("pipelines.generators", "--dataset", dataset, "--ds", "{{ ds }}"),
        cwd="/opt/sentinel",
        append_env=True,
    )


def spark_task(task_id: str, module: str, *extra_args: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=_cmd(module, *extra_args, "--ds", "{{ ds }}"),
        cwd="/opt/sentinel",
        append_env=True,
        pool=SPARK_POOL,
    )


def validate_task(dataset: str) -> BashOperator:
    return BashOperator(
        task_id=f"validate_{dataset}",
        bash_command=_cmd("pipelines.quality", "--dataset", dataset, "--ds", "{{ ds }}"),
        cwd="/opt/sentinel",
        append_env=True,
    )
