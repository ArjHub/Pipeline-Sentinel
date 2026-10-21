"""SparkSession factory and small shared helpers for all jobs."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from sentinel.events.models import ColumnSpec


class SchemaMismatchError(Exception):
    """Raised when required input columns are missing. The message lists both what was
    expected and what was observed, because that is exactly what a diagnosis needs."""


def build_spark(app_name: str) -> SparkSession:
    # Local mode inside the Airflow container (see docs/architecture.md, decision 3).
    # Delta jars are baked into the image's pyspark/jars, so no runtime downloads.
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
        .getOrCreate()
    )


def spark_info(spark: SparkSession) -> dict[str, str]:
    """Runtime facts worth having in a diagnosis (e.g. a version bump after a deploy)."""
    conf = spark.sparkContext.getConf()
    return {
        "spark_version": spark.version,
        "spark_app_id": spark.sparkContext.applicationId,
        "spark_master": spark.sparkContext.master,
        "driver_memory": conf.get("spark.driver.memory", "default"),
    }


def observed_schema(df: DataFrame) -> list[ColumnSpec]:
    return [
        ColumnSpec(name=f.name, type=f.dataType.simpleString(), nullable=f.nullable)
        for f in df.schema.fields
    ]


def require_columns(df: DataFrame, required: list[str], dataset: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SchemaMismatchError(
            f"{dataset}: missing required columns {missing}; observed columns {df.columns}"
        )


def write_partition(df: DataFrame, path: str | Path, ds: str) -> None:
    """Overwrite exactly one ds partition, so reruns are idempotent."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ds = '{ds}'")
        .partitionBy("ds")
        .save(str(path))
    )


def read_partition(spark: SparkSession, path: str | Path, ds: str) -> DataFrame:
    return spark.read.format("delta").load(str(path)).where(f"ds = '{ds}'")
