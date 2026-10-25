"""The source shim: what the "source system" delivers for a run."""

from __future__ import annotations

import json
import urllib.error
from datetime import date, timedelta

import pandas as pd
import pytest

from benchmark.harness import source_shim as shim
from pipelines.generators.datasets import generate_customers, generate_orders, generate_payments
from tests.conftest import CLEAN_DS

D = date.fromisoformat(CLEAN_DS)


def _apply(dataset, gen, profile):
    return shim.apply(dataset, D, gen(D, 42), profile, 42)


def test_no_profile_is_the_generator_unchanged(monkeypatch) -> None:
    monkeypatch.delenv("SENTINEL_SOURCE_PROFILE", raising=False)
    pd.testing.assert_frame_equal(
        shim.extract("orders", D, generate_orders, 42), generate_orders(D, 42)
    )


def test_days_old_delivers_an_earlier_extract(monkeypatch) -> None:
    monkeypatch.setenv("SENTINEL_SOURCE_PROFILE", json.dumps({"days_old": 3}))
    got = shim.extract("customers", D, generate_customers, 42)
    pd.testing.assert_frame_equal(got, generate_customers(D - timedelta(days=3), 42))


def test_drop_rename_add() -> None:
    df = _apply("orders", generate_orders, {"drop": ["amount"]})
    assert "amount" not in df.columns
    df = _apply("orders", generate_orders, {"rename": {"amount": "order_amount"}})
    assert "order_amount" in df.columns and "amount" not in df.columns
    spec = {"column": "discount_code", "values": "codes", "position": 4}
    df = _apply("orders", generate_orders, {"add": spec})
    assert list(df.columns).index("discount_code") == 4


def test_retype_kinds() -> None:
    df = _apply("orders", generate_orders, {"retype": {"column": "amount", "kind": "eu_decimal"}})
    assert df["amount"].str.contains(",").all()
    df = _apply(
        "orders", generate_orders, {"retype": {"column": "customer_id", "kind": "prefixed:C-"}}
    )
    assert df["customer_id"].str.match(r"C-\d{6}").all()


def test_nulls_keep_integer_columns_integer_in_csv(tmp_path) -> None:
    df = _apply("orders", generate_orders, {"nulls": {"column": "customer_id", "fraction": 0.2}})
    share = df["customer_id"].isna().mean()
    assert 0.15 < share < 0.25
    df.to_csv(tmp_path / "x.csv", index=False)
    ids = pd.read_csv(tmp_path / "x.csv", dtype=str)["customer_id"].dropna()
    assert not ids.str.contains(".", regex=False).any()  # Int64: no 123.0 (a retype)


def test_duplicate_cutoff_and_shift() -> None:
    base = generate_orders(D, 42)
    assert len(_apply("orders", generate_orders, {"duplicate": True})) == 2 * len(base)
    cut = _apply("orders", generate_orders, {"cutoff_hour": 14})
    assert pd.to_datetime(cut["order_ts"]).max() < pd.Timestamp(f"{CLEAN_DS}T14:00:00Z")
    shifted = _apply(
        "orders", generate_orders, {"shift_minutes": {"column": "order_ts", "minutes": 330}}
    )
    assert pd.to_datetime(shifted["order_ts"]).max() > pd.Timestamp(f"{CLEAN_DS}T23:59:59Z")


def test_keep_fraction_drops_the_same_orders_from_orders_and_payments() -> None:
    o = _apply("orders", generate_orders, {"keep_fraction": 0.6})
    p = _apply("payments", generate_payments, {"keep_fraction": 0.6})
    assert 0.5 < len(o) / len(generate_orders(D, 42)) < 0.7
    assert set(p["order_id"]) <= set(o["order_id"])


def test_value_mutations() -> None:
    df = _apply("orders", generate_orders, {"scale": {"column": "amount", "factor": 100}})
    assert df["amount"].max() > 5000
    df = _apply("payments", generate_payments, {"negate": {"column": "amount", "fraction": 0.04}})
    assert (df["amount"] < 0).mean() > 0.02
    df = _apply(
        "customers",
        generate_customers,
        {"replace": {"column": "country", "from": "GB", "to": "UK"}},
    )
    assert "UK" in set(df["country"]) and "GB" not in set(df["country"])


def test_unreachable_endpoint_raises_a_real_network_error(monkeypatch) -> None:
    monkeypatch.setattr(shim.time, "sleep", lambda s: None)
    profile = {"endpoint": {"url": "http://127.0.0.1:9/exports/{dataset}?date={ds}", "attempts": 2}}
    monkeypatch.setenv("SENTINEL_SOURCE_PROFILE", json.dumps(profile))
    with pytest.raises(urllib.error.URLError):
        shim.extract("orders", D, generate_orders, 42)
