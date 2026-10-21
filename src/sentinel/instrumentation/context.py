from __future__ import annotations

import os

from sentinel.events.models import RunContext


def code_version_for(pipeline: str) -> str:
    """An explicit SENTINEL_CODE_VERSION wins; otherwise use the version recorded by the
    last `make deploy` for this pipeline (see sentinel.deploy)."""
    explicit = os.environ.get("SENTINEL_CODE_VERSION")
    if explicit:
        return explicit
    from sentinel.deploy import current_code_version

    return current_code_version(pipeline) or "unknown"


def run_context_from_env(default_pipeline: str = "adhoc") -> RunContext:
    """Airflow's BashOperator exports AIRFLOW_CTX_* for every task; outside Airflow
    (tests, manual runs) we fall back to a named ad-hoc context."""
    env = os.environ
    try_number = env.get("AIRFLOW_CTX_TRY_NUMBER")
    pipeline = env.get("AIRFLOW_CTX_DAG_ID", default_pipeline)
    return RunContext(
        pipeline=pipeline,
        run_id=env.get("AIRFLOW_CTX_DAG_RUN_ID"),
        task_id=env.get("AIRFLOW_CTX_TASK_ID"),
        logical_date=env.get("AIRFLOW_CTX_EXECUTION_DATE"),
        try_number=int(try_number) if try_number and try_number.isdigit() else None,
        code_version=code_version_for(pipeline),
    )
