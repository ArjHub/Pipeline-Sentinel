"""GX suites must pass on clean data and fail on each class of bad data, with the
specific expectation that should catch it (not just "something failed")."""

from __future__ import annotations

import pandas as pd
import pytest

from pipelines.quality.__main__ import load
from pipelines.quality.checkpoint import validate
from sentinel.events import DQResult, MemorySink, RunContext
from tests.conftest import CLEAN_DS

pytestmark = pytest.mark.spark
CTX = RunContext(pipeline="test")


def _validate(df: pd.DataFrame, dataset: str) -> tuple[DQResult, MemorySink]:
    sink = MemorySink()
    return validate(df, dataset, CLEAN_DS, CTX, sink), sink


def _failed(result: DQResult) -> set[tuple[str, str | None]]:
    return {(r.expectation_type, r.column) for r in result.results if not r.success}


@pytest.mark.parametrize("dataset", ["orders", "customers", "payments", "daily_revenue"])
def test_clean_data_passes_and_emits_one_event(clean_lake, dataset) -> None:
    df = load(dataset, CLEAN_DS)
    result, sink = _validate(df, dataset)
    assert result.success, _failed(result)
    assert result.row_count == len(df) > 0
    assert len(sink.events) == 1
    assert sink.events[0].payload == result


def _orders(clean_lake) -> pd.DataFrame:
    return load("orders", CLEAN_DS)


def test_null_spike_is_caught(clean_lake) -> None:
    df = _orders(clean_lake)
    df.loc[df.sample(frac=0.2, random_state=0).index, "customer_id"] = None
    result, _ = _validate(df, "orders")
    assert _failed(result) == {("expect_column_values_to_not_be_null", "customer_id")}
    [outcome] = [r for r in result.results if not r.success]
    assert outcome.unexpected_count == round(0.2 * len(df))


def test_duplicate_load_is_caught(clean_lake) -> None:
    df = _orders(clean_lake)
    result, _ = _validate(pd.concat([df, df.head(50)]), "orders")
    assert _failed(result) == {("expect_column_values_to_be_unique", "order_id")}


def test_stale_partition_breaches_freshness(clean_lake) -> None:
    df = _orders(clean_lake)
    df = df[df["order_ts"] < pd.Timestamp(f"{CLEAN_DS}T18:00:00Z")]  # data stops at 18:00
    result, _ = _validate(df, "orders")
    assert _failed(result) == {("expect_column_max_to_be_between", "order_ts")}


def test_added_column_is_schema_drift(clean_lake) -> None:
    df = _orders(clean_lake).assign(discount_code="X")
    result, _ = _validate(df, "orders")
    assert _failed(result) == {("expect_table_columns_to_match_set", None)}


def test_out_of_range_and_unknown_category(clean_lake) -> None:
    df = _orders(clean_lake)
    df.loc[df.index[:3], "amount"] = -5.0
    df.loc[df.index[3:6], "currency"] = "XYZ"
    result, _ = _validate(df, "orders")
    assert _failed(result) == {
        ("expect_column_values_to_be_between", "amount"),
        ("expect_column_values_to_be_in_set", "currency"),
    }


def test_customers_bad_email_and_duplicate_ids(clean_lake) -> None:
    df = load("customers", CLEAN_DS)
    df.loc[df.index[:5], "email"] = "not-an-email"
    df = pd.concat([df, df.tail(3)])
    result, _ = _validate(df, "customers")
    assert _failed(result) == {
        ("expect_column_values_to_match_regex", "email"),
        ("expect_column_values_to_be_unique", "customer_id"),
    }


def test_dimension_backfill_after_a_later_load_passes(clean_lake) -> None:
    # Regression: the dim already holds 2026-10-20 rows; validating ds=2026-10-17 must
    # not treat those newer updated_at values as a freshness breach.
    df = load("customers", "2026-10-17")
    result = validate(df, "customers", "2026-10-17", CTX, MemorySink())
    assert result.success, _failed(result)


def test_stale_dimension_still_fails_freshness(clean_lake) -> None:
    df = load("customers", CLEAN_DS)
    result = validate(df, "customers", "2026-10-25", CTX, MemorySink())
    assert _failed(result) == {("expect_column_max_to_be_between", "updated_at")}


def test_revenue_paid_cannot_exceed_gross(clean_lake) -> None:
    df = load("daily_revenue", CLEAN_DS)
    df.loc[df.index[0], "paid_revenue"] = df.loc[df.index[0], "gross_revenue"] + 1
    result, _ = _validate(df, "daily_revenue")
    assert _failed(result) == {("expect_column_pair_values_a_to_be_greater_than_b", None)}
