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
    dag_id="customers_dim",
    description="Daily customer snapshot -> SCD1 Delta dimension via MERGE (PySpark)",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "spark"],
):
    (
        generate_task("customers")
        >> spark_task("build_customers_dim", "pipelines.jobs.customers_dim")
        >> validate_task("customers")
    )
