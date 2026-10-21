"""AWS Glue job: raw payments JSON lines -> curated Delta `payments`, partitioned by ds.

Locally:  python -m pipelines.jobs.payments_glue_job --JOB_NAME payments_etl --ds 2026-10-20
On Glue:  same script, with --lake_root s3://<bucket>/lake and --datalake-formats delta.
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.config import curated_path, raw_dir
from pipelines.jobs.glue_compat import init_job
from pipelines.jobs.spark import observed_schema, require_columns, spark_info, write_partition
from sentinel.events import sink_from_env
from sentinel.instrumentation import (
    JobStats,
    configure_logging,
    instrumented_job,
    run_context_from_env,
)

REQUIRED = ["payment_id", "order_id", "amount", "currency", "method", "status", "paid_ts"]
log = logging.getLogger("pipelines.jobs.payments_glue_job")


def transform(raw: DataFrame, ds: str) -> DataFrame:
    return raw.select(
        F.col("payment_id").cast("string"),
        F.col("order_id").cast("string"),
        F.col("amount").cast("decimal(12,2)").alias("amount"),
        F.upper("currency").alias("currency"),
        F.lower("method").alias("method"),
        F.lower("status").alias("status"),
        F.to_timestamp("paid_ts").alias("paid_ts"),
        F.lit(ds).alias("ds"),
        F.current_timestamp().alias("ingested_at"),
    )


def run(spark: SparkSession, ds: str, stats: JobStats) -> None:
    raw = spark.read.json(str(raw_dir("payments", ds)))
    stats.input_schema = observed_schema(raw)
    stats.rows_in = raw.count()
    require_columns(raw, REQUIRED, "payments")

    out = transform(raw, ds)
    dest = curated_path("payments")
    write_partition(out, dest, ds)
    stats.rows_out = out.count()
    stats.output_path = str(dest)
    log.info("payments ds=%s rows_in=%d rows_out=%d", ds, stats.rows_in, stats.rows_out)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    ctx = run_context_from_env(default_pipeline="payments_etl")
    sink = sink_from_env()
    configure_logging(ctx, sink)

    try:
        with instrumented_job("payments_glue_job", ctx, sink) as stats:
            rt = init_job(argv, required=["ds"], optional=["lake_root"])
            stats.extra.update(spark_info(rt.spark))
            if "lake_root" in rt.args:
                # Paths are resolved from the env at call time, so this redirects to S3.
                os.environ["SENTINEL_LAKE_ROOT"] = rt.args["lake_root"]
            try:
                run(rt.spark, rt.args["ds"], stats)
                rt.commit()
            finally:
                rt.stop()
    finally:
        sink.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
