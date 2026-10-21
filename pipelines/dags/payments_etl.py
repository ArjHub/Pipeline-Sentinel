"""Payments ETL as an AWS Glue job.

Locally the Glue script runs as plain PySpark through glue_compat. On AWS this task
would be a GlueJobOperator pointing at the same script (see infra/terraform, Phase 6).
"""

from airflow import DAG

from pipelines.common.dag_helpers import (
    DEFAULT_ARGS,
    SCHEDULE,
    START_DATE,
    TAGS,
    generate_task,
    spark_task,
    validate_task,
)

with DAG(
    dag_id="payments_etl",
    description="Raw payments JSON -> curated Delta payments (Glue job, local shim)",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "glue"],
):
    (
        generate_task("payments")
        >> spark_task(
            "run_payments_glue_job",
            "pipelines.jobs.payments_glue_job",
            "--JOB_NAME",
            "payments_etl",
        )
        >> validate_task("payments")
    )
