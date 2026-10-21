"""Generate one day of raw data for a dataset, simulating a source system drop.

python -m pipelines.generators --dataset orders --ds 2026-10-20
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from pipelines.common.config import base_seed, parse_ds, raw_dir
from pipelines.generators.base import write_atomic
from pipelines.generators.datasets import generate_customers, generate_orders, generate_payments
from sentinel.events import sink_from_env
from sentinel.instrumentation import configure_logging, instrumented_job, run_context_from_env

GENERATORS = {
    "orders": (generate_orders, "csv"),
    "customers": (generate_customers, "csv"),
    "payments": (generate_payments, "jsonl"),
}

log = logging.getLogger("pipelines.generators")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=sorted(GENERATORS))
    parser.add_argument("--ds", required=True)
    args = parser.parse_args(argv)

    ctx = run_context_from_env(default_pipeline=f"generate_{args.dataset}")
    sink = sink_from_env()
    configure_logging(ctx, sink)

    fn, fmt = GENERATORS[args.dataset]
    try:
        with instrumented_job(f"generate_{args.dataset}", ctx, sink) as stats:
            df = fn(parse_ds(args.ds), base_seed())
            path = write_atomic(df, raw_dir(args.dataset, args.ds) / f"part-0000.{fmt}", fmt)
            stats.rows_out = len(df)
            stats.output_path = str(path)
            stats.extra["arrived_at"] = datetime.now(UTC).isoformat()
            log.info("wrote %d %s rows to %s", len(df), args.dataset, path)
    finally:
        sink.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
