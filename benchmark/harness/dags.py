"""The four DAGs as the emulated scheduler sees them: tasks in order, their commands,
and which tasks are Spark (pool `spark`) or sensors.

Dependency-free on purpose: tests/dags/test_dag_integrity.py imports it with Airflow's
own interpreter and checks it against the real DagBag (task ids, order, rendered
bash commands, sensor targets), so the emulation can't drift from the DAG files.
"""

from __future__ import annotations

from dataclasses import dataclass

JOBS_PYTHON = "/opt/venv/jobs/bin/python"

# dataset -> the pipeline (DAG) that ingests it
PIPELINE_FOR = {
    "orders": "orders_ingest",
    "customers": "customers_dim",
    "payments": "payments_etl",
}
DATASET_FOR = {v: k for k, v in PIPELINE_FOR.items()}
SOURCES = tuple(PIPELINE_FOR.values())
DOWNSTREAM = "daily_revenue_agg"
ALL = (*SOURCES, DOWNSTREAM)


@dataclass(frozen=True)
class Task:
    dag_id: str
    task_id: str
    operator: str  # BashOperator | ExternalTaskSensor
    module: str = ""  # python -m <module> (bash tasks)
    args: tuple[str, ...] = ()
    spark: bool = False
    external_dag_id: str = ""  # sensors
    external_task_id: str = ""

    def command(self, ds: str) -> str:
        return " ".join([JOBS_PYTHON, "-m", self.module, *self.args, "--ds", ds])


def _gen(dag: str, dataset: str) -> Task:
    return Task(
        dag, f"generate_{dataset}", "BashOperator", "pipelines.generators", ("--dataset", dataset)
    )


def _val(dag: str, dataset: str) -> Task:
    return Task(
        dag, f"validate_{dataset}", "BashOperator", "pipelines.quality", ("--dataset", dataset)
    )


DAGS: dict[str, list[Task]] = {
    "orders_ingest": [
        _gen("orders_ingest", "orders"),
        Task(
            "orders_ingest",
            "ingest_orders",
            "BashOperator",
            "pipelines.jobs.orders_ingest",
            spark=True,
        ),
        _val("orders_ingest", "orders"),
    ],
    "customers_dim": [
        _gen("customers_dim", "customers"),
        Task(
            "customers_dim",
            "build_customers_dim",
            "BashOperator",
            "pipelines.jobs.customers_dim",
            spark=True,
        ),
        _val("customers_dim", "customers"),
    ],
    "payments_etl": [
        _gen("payments_etl", "payments"),
        Task(
            "payments_etl",
            "run_payments_glue_job",
            "BashOperator",
            "pipelines.jobs.payments_glue_job",
            ("--JOB_NAME", "payments_etl"),
            spark=True,
        ),
        _val("payments_etl", "payments"),
    ],
    "daily_revenue_agg": [
        *(
            Task(
                "daily_revenue_agg",
                f"wait_for_{dag}",
                "ExternalTaskSensor",
                external_dag_id=dag,
                external_task_id=f"validate_{DATASET_FOR[dag]}",
            )
            for dag in SOURCES
        ),
        Task(
            "daily_revenue_agg",
            "aggregate_revenue",
            "BashOperator",
            "pipelines.jobs.daily_revenue_agg",
            spark=True,
        ),
        Task(
            "daily_revenue_agg",
            "validate_daily_revenue",
            "BashOperator",
            "pipelines.quality",
            ("--dataset", "daily_revenue"),
        ),
    ],
}
TASKS: dict[str, Task] = {t.task_id: t for tasks in DAGS.values() for t in tasks}
FINAL_TASK = {dag: tasks[-1].task_id for dag, tasks in DAGS.items()}
