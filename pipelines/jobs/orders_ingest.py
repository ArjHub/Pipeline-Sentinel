"""Raw orders CSV -> curated Delta `orders`, partitioned by ds.

Duplicates are intentionally NOT dropped here: a duplicate order_id means something
upstream is broken, and the GX uniqueness check should fail loudly instead.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.config import curated_path, raw_dir
from pipelines.jobs.runner import run_spark_job
from pipelines.jobs.spark import observed_schema, require_columns, write_partition
from sentinel.instrumentation import JobStats

REQUIRED = ["order_id", "customer_id", "order_ts", "amount", "currency", "status"]
log = logging.getLogger("pipelines.jobs.orders_ingest")


def transform(raw: DataFrame, ds: str) -> DataFrame:
    return raw.select(
        F.col("order_id").cast("string"),
        F.col("customer_id").cast("long"),
        F.to_timestamp("order_ts").alias("order_ts"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.upper("currency").alias("currency"),
        F.lower("status").alias("status"),
        F.lit(ds).alias("ds"),
        F.current_timestamp().alias("ingested_at"),
    )


def run(spark: SparkSession, ds: str, stats: JobStats) -> None:
    src = raw_dir("orders", ds)
    raw = spark.read.option("header", True).option("inferSchema", True).csv(str(src))
    stats.input_schema = observed_schema(raw)
    stats.rows_in = raw.count()
    require_columns(raw, REQUIRED, "orders")

    out = transform(raw, ds)
    dest = curated_path("orders")
    write_partition(out, dest, ds)
    stats.rows_out = out.count()
    stats.output_path = str(dest)
    log.info("orders ds=%s rows_in=%d rows_out=%d", ds, stats.rows_in, stats.rows_out)


def main(argv: list[str] | None = None) -> int:
    return run_spark_job("orders_ingest", run, argv)


if __name__ == "__main__":
    sys.exit(main())
