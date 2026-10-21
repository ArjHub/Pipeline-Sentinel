from __future__ import annotations

import os

from sentinel.events.models import RunContext


def run_context_from_env(default_pipeline: str = "adhoc") -> RunContext:
    """Airflow's BashOperator exports AIRFLOW_CTX_* for every task; outside Airflow
    (tests, manual runs) we fall back to a named ad-hoc context."""
    env = os.environ
    try_number = env.get("AIRFLOW_CTX_TRY_NUMBER")
    return RunContext(
        pipeline=env.get("AIRFLOW_CTX_DAG_ID", default_pipeline),
        run_id=env.get("AIRFLOW_CTX_DAG_RUN_ID"),
        task_id=env.get("AIRFLOW_CTX_TASK_ID"),
        logical_date=env.get("AIRFLOW_CTX_EXECUTION_DATE"),
        try_number=int(try_number) if try_number and try_number.isdigit() else None,
        code_version=env.get("SENTINEL_CODE_VERSION", "unknown"),
    )
