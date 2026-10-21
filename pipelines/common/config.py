"""Paths and dataset settings shared by generators, jobs, quality checks and DAGs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

DATASETS = ("orders", "customers", "payments", "daily_revenue")


def lake_root() -> Path:
    return Path(os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))


def raw_dir(dataset: str, ds: str) -> Path:
    return lake_root() / "raw" / dataset / f"ds={ds}"


def curated_path(dataset: str) -> Path:
    return lake_root() / "curated" / dataset


def base_seed() -> int:
    return int(os.environ.get("SENTINEL_SEED", "42"))


def parse_ds(ds: str) -> date:
    return date.fromisoformat(ds)


@dataclass(frozen=True)
class FreshnessSla:
    """Latest event must be no older than `max_lag` before the end of ds.

    `bounded_by_ds` adds an upper bound (nothing after the end of ds), which is right for
    ds-partitioned facts. A current-state SCD1 dimension has no such bound: after a later
    day has loaded, backfilling an earlier ds legitimately sees newer rows.
    """

    column: str
    max_lag: timedelta
    bounded_by_ds: bool = True


FRESHNESS: dict[str, FreshnessSla] = {
    "orders": FreshnessSla("order_ts", timedelta(hours=2)),
    "payments": FreshnessSla("paid_ts", timedelta(hours=2)),
    "customers": FreshnessSla("updated_at", timedelta(days=1), bounded_by_ds=False),
}
