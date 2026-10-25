"""Source-system extract for one dataset and day: what the upstream system delivered.

Installed by the benchmark harness into its isolated build tree as
`pipelines/generators/source.py`, and `pipelines.generators` calls `extract()` instead of
the generator directly. With no profile it returns the generator's output unchanged,
so clean runs are byte-identical to the normal stack.

SENTINEL_SOURCE_PROFILE (JSON, set by the harness for one generate_* task) describes
how the source misbehaves on that day. Everything is applied to the DataFrame *before*
it is written, so the generate task's own metrics (rows_out) and logs describe what
the source really delivered, as they would in production. Keys:

  days_old        deliver the extract of an earlier day (stale snapshot / old file)
  drop            [columns] removed
  rename          {old: new}
  add             {column, position, values}   values: codes | refs | phone | flag
  retype          {column, kind}   kind: eu_decimal | prefixed:<P> | quoted
  nulls           {column, fraction}
  duplicate       true: the whole extract appended twice
  scale           {column, factor}
  negate          {column, fraction}
  replace         {column, from, to}
  cutoff_hour     only rows whose event time is before HH:00 (the export stopped early)
  keep_fraction   keep this share of orders (by order_id hash; orders and payments agree)
  shift_minutes   {column, minutes}
  endpoint        {url, attempts}: fetch from a remote export that is unreachable
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import urllib.request
from collections.abc import Callable
from datetime import date, timedelta

import numpy as np
import pandas as pd

log = logging.getLogger("pipelines.generators.source")

TIME_COLUMN = {"orders": "order_ts", "payments": "paid_ts", "customers": "updated_at"}
_ADD_VALUES = {
    "codes": ["", "", "", "", "WELCOME10", "FESTIVE15", "FREESHIP"],
    "refs": None,  # gw_<hex>
    "phone": None,  # +91 9xxxxxxxxx
    "flag": [True, False, False],
}


def profile() -> dict:
    return json.loads(os.environ.get("SENTINEL_SOURCE_PROFILE") or "{}")


def _rng(seed: int, dataset: str, d: date) -> np.random.Generator:
    h = hashlib.sha256(f"{seed}|{dataset}|{d.isoformat()}|source".encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "big"))


def _unit(keys: pd.Series) -> np.ndarray:
    return np.array([int(hashlib.md5(str(k).encode()).hexdigest()[:8], 16) / 2**32 for k in keys])


def _fetch(dataset: str, d: date, spec: dict) -> pd.DataFrame:
    url = spec["url"].format(dataset=dataset, ds=d.isoformat())
    attempts = int(spec.get("attempts", 3))
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                return pd.read_csv(resp)
        except OSError as exc:
            if attempt == attempts:
                raise
            log.warning(
                "source export %s not available (attempt %d/%d): %s", url, attempt, attempts, exc
            )
            time.sleep(2)
    raise AssertionError("unreachable")


def apply(dataset: str, d: date, df: pd.DataFrame, p: dict, seed: int) -> pd.DataFrame:
    rng = _rng(seed, dataset, d)
    df = df.copy()
    if "keep_fraction" in p:
        df = df[_unit(df["order_id"]) < float(p["keep_fraction"])].reset_index(drop=True)
    if "cutoff_hour" in p:
        ts = pd.to_datetime(df[TIME_COLUMN[dataset]], utc=True)
        cutoff = pd.Timestamp(d.isoformat(), tz="UTC") + pd.Timedelta(hours=int(p["cutoff_hour"]))
        df = df[ts < cutoff].reset_index(drop=True)
    if s := p.get("shift_minutes"):
        ts = pd.to_datetime(df[s["column"]], utc=True) + pd.Timedelta(minutes=int(s["minutes"]))
        df[s["column"]] = ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if s := p.get("nulls"):
        col, mask = s["column"], rng.random(len(df)) < float(s["fraction"])
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].astype("Int64")
            df.loc[mask, col] = pd.NA
        else:
            df[col] = df[col].astype(object)
            df.loc[mask, col] = None
    if s := p.get("scale"):
        df[s["column"]] = (df[s["column"]] * float(s["factor"])).round(2)
    if s := p.get("negate"):
        mask = rng.random(len(df)) < float(s["fraction"])
        df.loc[mask, s["column"]] = -df.loc[mask, s["column"]]
    if s := p.get("replace"):
        df[s["column"]] = df[s["column"]].replace(s["from"], s["to"])
    if s := p.get("retype"):
        col, kind = s["column"], s["kind"]
        if kind == "eu_decimal":
            df[col] = [f"{v:.2f}".replace(".", ",") for v in df[col]]
        elif kind.startswith("prefixed:"):
            df[col] = [f"{kind.split(':', 1)[1]}{int(v):06d}" for v in df[col]]
        elif kind == "quoted":
            df[col] = [f"{v:.2f}" for v in df[col]]
    if s := p.get("add"):
        values = _ADD_VALUES[s["values"]]
        n = len(df)
        if s["values"] == "refs":
            col = [f"gw_{rng.integers(16**11, 16**12):012x}" for _ in range(n)]
        elif s["values"] == "phone":
            col = [f"+91 9{rng.integers(10**8, 10**9):09d}" for _ in range(n)]
        else:
            col = [values[i] for i in rng.integers(0, len(values), n)]
        df.insert(min(int(s["position"]), len(df.columns)), s["column"], col)
    if cols := p.get("drop"):
        df = df.drop(columns=cols)
    if mapping := p.get("rename"):
        df = df.rename(columns=mapping)
    if p.get("duplicate"):
        df = pd.concat([df, df], ignore_index=True)
    return df


def extract(
    dataset: str, d: date, generate: Callable[[date, int], pd.DataFrame], seed: int
) -> pd.DataFrame:
    p = profile()
    if "endpoint" in p:
        return _fetch(dataset, d, p["endpoint"])
    df = generate(d - timedelta(days=int(p.get("days_old", 0))), seed)
    return apply(dataset, d, df, p, seed) if p else df
