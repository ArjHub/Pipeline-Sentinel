"""Run the same Glue job script on real AWS Glue or as plain PySpark locally.

LocalStack's Glue emulation is a paid feature, so locally we skip `awsglue` entirely and
use the same SparkSession factory as the other jobs. The job body only uses the
DataFrame API, which behaves the same in both places. What this does NOT exercise
locally: DynamicFrames, job bookmarks, and Glue's own S3/catalog integration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession

try:  # pragma: no cover - only importable on Glue
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    ON_GLUE = True
except ImportError:
    ON_GLUE = False


class MissingJobArgumentError(Exception):
    pass


def parse_local_args(argv: list[str], required: list[str], optional: list[str]) -> dict[str, str]:
    """Mimics getResolvedOptions: `--name value` pairs; required names must be present."""
    wanted = set(required) | set(optional)
    out: dict[str, str] = {}
    i = 0
    while i < len(argv):
        token = argv[i]
        if token.startswith("--") and token[2:] in wanted and i + 1 < len(argv):
            out[token[2:]] = argv[i + 1]
            i += 2
        else:
            i += 1
    missing = [n for n in required if n not in out]
    if missing:
        raise MissingJobArgumentError(f"missing job arguments: {missing}")
    return out


@dataclass
class GlueRuntime:
    spark: SparkSession
    args: dict[str, str]
    _job: Any = None

    def commit(self) -> None:
        if self._job is not None:
            self._job.commit()

    def stop(self) -> None:
        # On Glue the service owns the session lifecycle.
        if self._job is None:
            self.spark.stop()


def init_job(argv: list[str], required: list[str], optional: list[str]) -> GlueRuntime:
    if ON_GLUE:  # pragma: no cover
        present = [n for n in optional if f"--{n}" in argv]
        args = getResolvedOptions(argv, ["JOB_NAME", *required, *present])
        glue_ctx = GlueContext(SparkContext.getOrCreate())
        job = Job(glue_ctx)
        job.init(args["JOB_NAME"], args)
        return GlueRuntime(glue_ctx.spark_session, args, job)

    from pipelines.jobs.spark import build_spark

    args = parse_local_args(argv, ["JOB_NAME", *required], optional)
    return GlueRuntime(build_spark(args["JOB_NAME"]), args)
