"""Validate one curated dataset partition with Great Expectations.

    python -m pipelines.quality --dataset orders --ds 2026-10-20

Reads the Delta table with delta-rs (no JVM start-up; partitions are small) and exits
non-zero if any expectation fails, so the Airflow task fails.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from decimal import Decimal

import pandas as pd
from deltalake import DeltaTable

from pipelines.common.config import DATASETS, curated_path
from pipelines.quality.checkpoint import validate
from sentinel.events import sink_from_env
from sentinel.instrumentation import configure_logging, run_context_from_env

# Tables that are not partitioned by ds (validated as a whole).
UNPARTITIONED = {"customers"}

log = logging.getLogger("pipelines.quality")


def load(dataset: str, ds: str) -> pd.DataFrame:
    table = DeltaTable(str(curated_path(dataset)))
    if dataset in UNPARTITIONED:
        df = table.to_pandas()
    else:
        df = table.to_pandas(partitions=[("ds", "=", ds)])
    # Delta decimals arrive as Python Decimal objects; GX range checks want floats.
    for col in df.columns:
        if df[col].dtype == object and len(df) and isinstance(df[col].dropna().iloc[0], Decimal):
            df[col] = df[col].astype(float)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=DATASETS)
    parser.add_argument("--ds", required=True)
    args = parser.parse_args(argv)

    os.environ.setdefault("TQDM_DISABLE", "1")  # GX progress bars clutter task logs
    ctx = run_context_from_env(default_pipeline=f"validate_{args.dataset}")
    sink = sink_from_env()
    configure_logging(ctx, sink)

    try:
        return _run(args.dataset, args.ds, ctx, sink)
    finally:
        sink.flush()


def _run(dataset: str, ds: str, ctx, sink) -> int:
    df = load(dataset, ds)
    result = validate(df, dataset, ds, ctx, sink)
    for outcome in result.results:
        if not outcome.success:
            log.error(
                "DQ check failed: %s column=%s observed=%s unexpected=%s",
                outcome.expectation_type,
                outcome.column,
                outcome.observed_value,
                outcome.unexpected_count,
            )
    log.info(
        "DQ %s ds=%s rows=%d evaluated=%d failed=%d",
        dataset,
        ds,
        result.row_count,
        result.evaluated,
        result.failed,
    )
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
