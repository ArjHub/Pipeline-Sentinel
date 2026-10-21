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
    dag_id="orders_ingest",
    description="Raw orders CSV -> curated Delta orders (PySpark)",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=[*TAGS, "spark"],
):
    (
        generate_task("orders")
        >> spark_task("ingest_orders", "pipelines.jobs.orders_ingest")
        >> validate_task("orders")
    )
