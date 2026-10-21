"""Daily full customer snapshot -> SCD1 Delta dimension `customers` via MERGE.

A row is only updated when the snapshot has a newer updated_at, so re-running a day is
a no-op.
"""

from __future__ import annotations

import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.config import curated_path, raw_dir
from pipelines.jobs.runner import run_spark_job
from pipelines.jobs.spark import observed_schema, require_columns
from sentinel.instrumentation import JobStats

REQUIRED = ["customer_id", "email", "country", "tier", "signup_ts", "updated_at"]
log = logging.getLogger("pipelines.jobs.customers_dim")


def transform(raw: DataFrame, ds: str) -> DataFrame:
    return raw.select(
        F.col("customer_id").cast("long"),
        F.lower("email").alias("email"),
        F.upper("country").alias("country"),
        F.lower("tier").alias("tier"),
        F.to_timestamp("signup_ts").alias("signup_ts"),
        F.to_timestamp("updated_at").alias("updated_at"),
        F.lit(ds).alias("loaded_ds"),
    )


def run(spark: SparkSession, ds: str, stats: JobStats) -> None:
    src = raw_dir("customers", ds)
    raw = spark.read.option("header", True).option("inferSchema", True).csv(str(src))
    stats.input_schema = observed_schema(raw)
    stats.rows_in = raw.count()
    require_columns(raw, REQUIRED, "customers")

    snapshot = transform(raw, ds)
    dest = str(curated_path("customers"))
    if not DeltaTable.isDeltaTable(spark, dest):
        snapshot.write.format("delta").save(dest)
        stats.rows_out = stats.rows_in
    else:
        (
            DeltaTable.forPath(spark, dest)
            .alias("t")
            .merge(snapshot.alias("s"), "t.customer_id = s.customer_id")
            .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
            .whenNotMatchedInsertAll()
            .execute()
        )
        metrics = DeltaTable.forPath(spark, dest).history(1).first()["operationMetrics"]
        stats.rows_out = int(metrics.get("numTargetRowsInserted", 0)) + int(
            metrics.get("numTargetRowsUpdated", 0)
        )
        stats.extra["inserted"] = int(metrics.get("numTargetRowsInserted", 0))
        stats.extra["updated"] = int(metrics.get("numTargetRowsUpdated", 0))
    stats.output_path = dest
    log.info("customers ds=%s snapshot=%d changed=%d", ds, stats.rows_in, stats.rows_out)


def main(argv: list[str] | None = None) -> int:
    return run_spark_job("customers_dim", run, argv)


if __name__ == "__main__":
    sys.exit(main())
