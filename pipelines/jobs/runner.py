"""Boilerplate every Spark job shares: args, logging, instrumentation, session."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from pyspark.sql import SparkSession

from sentinel.events import sink_from_env
from sentinel.instrumentation import (
    JobStats,
    configure_logging,
    instrumented_job,
    run_context_from_env,
)

JobBody = Callable[[SparkSession, str, JobStats], None]


def run_spark_job(job_name: str, body: JobBody, argv: list[str] | None = None) -> int:
    from pipelines.jobs.spark import build_spark, spark_info

    parser = argparse.ArgumentParser(prog=job_name)
    parser.add_argument("--ds", required=True)
    args = parser.parse_args(argv)

    ctx = run_context_from_env(default_pipeline=job_name)
    sink = sink_from_env()
    configure_logging(ctx, sink)

    try:
        with instrumented_job(job_name, ctx, sink) as stats:
            spark = build_spark(job_name)
            stats.extra.update(spark_info(spark))
            try:
                body(spark, args.ds, stats)
            finally:
                spark.stop()
    finally:
        sink.flush()
    return 0
