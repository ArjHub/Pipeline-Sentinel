"""Deterministic helpers for synthetic data.

The same (seed, dataset, ds) always produces the same rows, which is what makes
replays and the benchmark reproducible.
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Day 0 of the synthetic business. Customers signed up before this are the initial base.
EPOCH = date(2026, 1, 1)

_GOLDEN = np.uint64(0x9E3779B97F4A7C15)


def rng_for(*parts: object) -> np.random.Generator:
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return np.random.default_rng(int.from_bytes(digest[:8], "big"))


def stable_hash(values: np.ndarray, salt: int) -> np.ndarray:
    """splitmix64 over an integer array: a per-row value that depends only on (value, salt),
    so an entity's attributes stay fixed as the population grows."""
    with np.errstate(over="ignore"):
        z = values.astype(np.uint64) + np.uint64(salt) * _GOLDEN
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        return z ^ (z >> np.uint64(31))


def unit_interval(values: np.ndarray, salt: int) -> np.ndarray:
    return (stable_hash(values, salt) >> np.uint64(11)).astype(np.float64) / float(1 << 53)


def day_index(d: date) -> int:
    return (d - EPOCH).days


def day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def to_iso(ts: pd.Series) -> pd.Series:
    return ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def seconds_after(start: datetime, seconds: np.ndarray) -> pd.Series:
    return pd.Series(pd.Timestamp(start) + pd.to_timedelta(seconds, unit="s"))


def write_atomic(df: pd.DataFrame, path: Path, fmt: str) -> Path:
    """Write to a temp file and rename, so readers never see a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    if fmt == "csv":
        df.to_csv(tmp, index=False)
    elif fmt == "jsonl":
        df.to_json(tmp, orient="records", lines=True)
    else:
        raise ValueError(f"unsupported format {fmt}")
    tmp.replace(path)
    return path


ONE_DAY = timedelta(days=1)
