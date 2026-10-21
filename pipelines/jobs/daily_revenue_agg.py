"""Join curated orders, payments and the customer dimension into `daily_revenue`
(one row per ds, country, currency)."""

from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.config import curated_path
from pipelines.jobs.runner import run_spark_job
from pipelines.jobs.spark import read_partition, write_partition
from sentinel.instrumentation import JobStats

log = logging.getLogger("pipelines.jobs.daily_revenue_agg")


def aggregate(orders: DataFrame, payments: DataFrame, customers: DataFrame, ds: str) -> DataFrame:
    completed = orders.where(F.col("status") == "completed")
    paid = (
        payments.where(F.col("status") == "captured")
        .groupBy("order_id")
        .agg(F.sum("amount").alias("paid_amount"))
    )
    joined = completed.join(paid, "order_id", "left").join(
        customers.select("customer_id", "country"), "customer_id", "left"
    )
    return (
        joined.groupBy(F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country"), "currency")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("gross_revenue"),
            F.coalesce(F.sum("paid_amount"), F.lit(0)).alias("paid_revenue"),
            F.sum(F.when(F.col("paid_amount").isNull(), 1).otherwise(0)).alias("unpaid_orders"),
        )
        .withColumn("ds", F.lit(ds))
    )


def run(spark: SparkSession, ds: str, stats: JobStats) -> None:
    orders = read_partition(spark, curated_path("orders"), ds)
    payments = read_partition(spark, curated_path("payments"), ds)
    customers = spark.read.format("delta").load(str(curated_path("customers")))
    stats.rows_in = orders.count()

    out = aggregate(orders, payments, customers, ds)
    dest = curated_path("daily_revenue")
    write_partition(out, dest, ds)
    stats.rows_out = out.count()
    stats.output_path = str(dest)
    log.info("daily_revenue ds=%s orders=%d groups=%d", ds, stats.rows_in, stats.rows_out)


def main(argv: list[str] | None = None) -> int:
    return run_spark_job("daily_revenue_agg", run, argv)


if __name__ == "__main__":
    sys.exit(main())
