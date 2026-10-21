"""Synthetic orders, customers (daily full snapshot) and payments.

Shapes are deliberately simple but realistic enough to make detectors meaningful:
weekly seasonality and noise in order volume, a growing customer base with periodic
profile updates, and payments that lag orders by a few minutes.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from pipelines.generators.base import (
    EPOCH,
    day_index,
    day_start,
    rng_for,
    seconds_after,
    stable_hash,
    to_iso,
    unit_interval,
)

INITIAL_CUSTOMERS = 5000
NEW_CUSTOMERS_PER_DAY = 25
CUSTOMER_UPDATE_PERIOD_DAYS = 50

BASE_ORDERS_PER_DAY = 2000
# Monday..Sunday multipliers.
WEEKDAY_FACTOR = (1.0, 0.95, 0.97, 1.0, 1.1, 1.3, 1.25)
ORDER_VOLUME_NOISE = 0.03

COUNTRIES = ("IN", "US", "GB", "DE", "SG", "AE")
COUNTRY_WEIGHTS = (0.55, 0.15, 0.1, 0.08, 0.07, 0.05)
TIERS = ("bronze", "silver", "gold")
CURRENCIES = ("INR", "USD", "EUR")
CURRENCY_WEIGHTS = (0.7, 0.2, 0.1)
ORDER_STATUSES = ("completed", "cancelled", "pending")
ORDER_STATUS_WEIGHTS = (0.92, 0.05, 0.03)
PAYMENT_METHODS = ("card", "upi", "netbanking", "wallet")
PAYMENT_METHOD_WEIGHTS = (0.4, 0.4, 0.1, 0.1)
PAYMENT_STATUSES = ("captured", "failed")
PAYMENT_STATUS_WEIGHTS = (0.98, 0.02)
PAYMENT_COVERAGE = 0.97
MAX_AMOUNT = 5000.0

ORDER_COLUMNS = ["order_id", "customer_id", "order_ts", "amount", "currency", "status"]
CUSTOMER_COLUMNS = ["customer_id", "email", "country", "tier", "signup_ts", "updated_at"]
PAYMENT_COLUMNS = [
    "payment_id",
    "order_id",
    "amount",
    "currency",
    "method",
    "status",
    "paid_ts",
]


def customer_count(d: date) -> int:
    return INITIAL_CUSTOMERS + NEW_CUSTOMERS_PER_DAY * max(0, day_index(d))


def _weighted_pick(u: np.ndarray, options: tuple[str, ...], weights: tuple[float, ...]):
    idx = np.searchsorted(np.cumsum(weights), u, side="right")
    return np.asarray(options)[np.minimum(idx, len(options) - 1)]


def generate_customers(d: date, seed: int) -> pd.DataFrame:
    """Full snapshot of every customer known as of the end of day `d`."""
    n = customer_count(d)
    ids = np.arange(1, n + 1, dtype=np.int64)
    di = day_index(d)

    country = _weighted_pick(unit_interval(ids, seed + 1), COUNTRIES, COUNTRY_WEIGHTS)

    signup_secs = np.empty(n, dtype=np.int64)
    initial = ids <= INITIAL_CUSTOMERS
    tod = (stable_hash(ids, seed + 2) % np.uint64(86400)).astype(np.int64)
    # Initial base signed up at some point in the year before EPOCH.
    year_secs = (stable_hash(ids, seed + 3) % np.uint64(365 * 86400)).astype(np.int64)
    signup_secs[initial] = -365 * 86400 + year_secs[initial]
    signup_day = (ids[~initial] - INITIAL_CUSTOMERS - 1) // NEW_CUSTOMERS_PER_DAY
    signup_secs[~initial] = signup_day * 86400 + tod[~initial]

    # Each customer is updated every CUSTOMER_UPDATE_PERIOD_DAYS on a fixed phase.
    period = CUSTOMER_UPDATE_PERIOD_DAYS
    phase = (stable_hash(ids, seed + 4) % np.uint64(period)).astype(np.int64)
    last_update_day = di - np.mod(di - phase, period)
    update_tod = (stable_hash(ids * 1000 + last_update_day, seed + 5) % np.uint64(86400)).astype(
        np.int64
    )
    update_secs = last_update_day * 86400 + update_tod
    updated_secs = np.maximum(update_secs, signup_secs)
    end_of_day = (di + 1) * 86400 - 1
    updated_secs = np.minimum(updated_secs, end_of_day)

    tier_idx = (stable_hash(ids, seed + 6) + (last_update_day // period).astype(np.uint64)) % 3
    epoch_start = day_start(EPOCH)
    df = pd.DataFrame(
        {
            "customer_id": ids,
            "email": [f"customer_{i}@example.com" for i in ids],
            "country": country,
            "tier": np.asarray(TIERS)[tier_idx.astype(np.int64)],
            "signup_ts": to_iso(seconds_after(epoch_start, signup_secs)),
            "updated_at": to_iso(seconds_after(epoch_start, updated_secs)),
        }
    )
    return df[CUSTOMER_COLUMNS]


def generate_orders(d: date, seed: int) -> pd.DataFrame:
    rng = rng_for(seed, "orders", d.isoformat())
    noise = rng.normal(0.0, ORDER_VOLUME_NOISE)
    n = round(BASE_ORDERS_PER_DAY * WEEKDAY_FACTOR[d.weekday()] * (1.0 + noise))
    secs = np.sort(rng.integers(0, 86400, n))
    customers = rng.integers(1, customer_count(d) + 1, n)
    amount = np.round(np.clip(rng.lognormal(mean=4.0, sigma=0.7, size=n), 1.0, MAX_AMOUNT), 2)
    currency = _weighted_pick(rng.random(n), CURRENCIES, CURRENCY_WEIGHTS)
    status = _weighted_pick(rng.random(n), ORDER_STATUSES, ORDER_STATUS_WEIGHTS)
    stamp = d.strftime("%Y%m%d")
    df = pd.DataFrame(
        {
            "order_id": [f"o-{stamp}-{i:06d}" for i in range(n)],
            "customer_id": customers,
            "order_ts": to_iso(seconds_after(day_start(d), secs)),
            "amount": amount,
            "currency": currency,
            "status": status,
        }
    )
    return df[ORDER_COLUMNS]


def generate_payments(d: date, seed: int) -> pd.DataFrame:
    """One payment per completed order (minus a small uncovered fraction)."""
    orders = generate_orders(d, seed)
    orders = orders[orders["status"] == "completed"].reset_index(drop=True)
    rng = rng_for(seed, "payments", d.isoformat())
    covered = rng.random(len(orders)) < PAYMENT_COVERAGE
    orders = orders[covered].reset_index(drop=True)
    n = len(orders)

    order_ts = pd.to_datetime(orders["order_ts"], utc=True)
    delay = pd.to_timedelta(rng.exponential(300.0, n).round(), unit="s")
    end_of_day = pd.Timestamp(day_start(d)) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    paid_ts = (order_ts + delay).clip(upper=end_of_day)

    stamp = d.strftime("%Y%m%d")
    df = pd.DataFrame(
        {
            "payment_id": [f"p-{stamp}-{i:06d}" for i in range(n)],
            "order_id": orders["order_id"],
            "amount": orders["amount"],
            "currency": orders["currency"],
            "method": _weighted_pick(rng.random(n), PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS),
            "status": _weighted_pick(rng.random(n), PAYMENT_STATUSES, PAYMENT_STATUS_WEIGHTS),
            "paid_ts": to_iso(paid_ts),
        }
    )
    return df[PAYMENT_COLUMNS]
