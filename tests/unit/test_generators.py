from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from pipelines.generators import __main__ as gen_cli
from pipelines.generators.base import write_atomic
from pipelines.generators.datasets import (
    BASE_ORDERS_PER_DAY,
    CUSTOMER_COLUMNS,
    INITIAL_CUSTOMERS,
    NEW_CUSTOMERS_PER_DAY,
    ORDER_COLUMNS,
    PAYMENT_COLUMNS,
    WEEKDAY_FACTOR,
    customer_count,
    generate_customers,
    generate_orders,
    generate_payments,
)

D = date(2026, 10, 20)
SEED = 42


@pytest.mark.parametrize("fn", [generate_orders, generate_customers, generate_payments])
def test_same_seed_same_day_is_identical(fn) -> None:
    pd.testing.assert_frame_equal(fn(D, SEED), fn(D, SEED))


@pytest.mark.parametrize("fn", [generate_orders, generate_payments])
def test_different_seed_or_day_differs(fn) -> None:
    assert not fn(D, SEED).equals(fn(D, SEED + 1))
    assert not fn(D, SEED).equals(fn(D + timedelta(days=1), SEED))


def test_order_volume_follows_weekday_seasonality_within_noise() -> None:
    for offset in range(28):
        d = D + timedelta(days=offset)
        expected = BASE_ORDERS_PER_DAY * WEEKDAY_FACTOR[d.weekday()]
        # 3% Gaussian noise: 5 sigma is a generous bound that still catches regressions.
        assert abs(len(generate_orders(d, SEED)) - expected) <= 0.15 * expected


def test_orders_shape_and_ranges() -> None:
    df = generate_orders(D, SEED)
    assert list(df.columns) == ORDER_COLUMNS
    assert df["order_id"].is_unique
    assert df["amount"].between(1, 5000).all()
    assert df["customer_id"].between(1, customer_count(D)).all()
    assert df["order_ts"].str.startswith(D.isoformat()).all()


def test_payments_reference_completed_orders_only() -> None:
    orders = generate_orders(D, SEED)
    payments = generate_payments(D, SEED)
    assert list(payments.columns) == PAYMENT_COLUMNS
    completed = set(orders.loc[orders["status"] == "completed", "order_id"])
    assert set(payments["order_id"]) <= completed
    assert 0.9 * len(completed) < len(payments) <= len(completed)
    assert payments["payment_id"].is_unique
    # Payments never land before their order or after the end of the day.
    merged = payments.merge(orders[["order_id", "order_ts"]], on="order_id")
    assert (merged["paid_ts"] >= merged["order_ts"]).all()
    assert payments["paid_ts"].str.startswith(D.isoformat()).all()


def test_customer_base_grows_and_attributes_are_stable() -> None:
    today = generate_customers(D, SEED)
    tomorrow = generate_customers(D + timedelta(days=1), SEED)
    assert list(today.columns) == CUSTOMER_COLUMNS
    assert len(tomorrow) == len(today) + NEW_CUSTOMERS_PER_DAY
    assert today["customer_id"].is_unique
    same = tomorrow.head(len(today))
    pd.testing.assert_series_equal(today["country"], same["country"])
    pd.testing.assert_series_equal(today["signup_ts"], same["signup_ts"])


def test_customer_updates_are_bounded_by_the_snapshot_day() -> None:
    df = generate_customers(D, SEED)
    end = pd.Timestamp(D + timedelta(days=1), tz="UTC")
    updated = pd.to_datetime(df["updated_at"], utc=True)
    assert (updated < end).all()
    assert (updated >= pd.to_datetime(df["signup_ts"], utc=True)).all()
    # ~1/50 of customers get updated each day, which keeps freshness checks meaningful.
    assert (updated >= end - pd.Timedelta(days=1)).sum() > len(df) / 100


def test_customer_count_before_epoch_is_the_initial_base() -> None:
    assert customer_count(date(2025, 6, 1)) == INITIAL_CUSTOMERS


def test_write_atomic_leaves_no_temp_files(tmp_path) -> None:
    df = generate_orders(D, SEED).head(5)
    path = write_atomic(df, tmp_path / "x" / "part.csv", "csv")
    assert [p.name for p in path.parent.iterdir()] == ["part.csv"]
    with pytest.raises(ValueError):
        write_atomic(df, tmp_path / "y.parquet", "parquet")


def test_cli_writes_raw_partition(lake) -> None:
    assert gen_cli.main(["--dataset", "payments", "--ds", D.isoformat()]) == 0
    out = lake / "raw" / "payments" / f"ds={D.isoformat()}" / "part-0000.jsonl"
    assert len(pd.read_json(out, lines=True)) == len(generate_payments(D, SEED))
