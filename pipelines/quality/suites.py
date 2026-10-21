"""Great Expectations suites, defined in code because freshness bounds depend on ds.

Every suite covers schema, nulls, ranges/sets, uniqueness and freshness. Row-count
bounds here are only coarse sanity limits; the statistical volume check is a detector
(Phase 3), because a fixed range can't adapt to seasonality.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import great_expectations.expectations as gxe

from pipelines.common.config import FRESHNESS, parse_ds
from pipelines.generators.base import day_start
from pipelines.generators.datasets import (
    COUNTRIES,
    CURRENCIES,
    MAX_AMOUNT,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PAYMENT_STATUSES,
    TIERS,
)

SUITE_VERSION = "1"
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[a-z]+$"


def _freshness(dataset: str, ds: str) -> gxe.Expectation:
    sla = FRESHNESS[dataset]
    end: datetime = day_start(parse_ds(ds)) + timedelta(days=1)
    return gxe.ExpectColumnMaxToBeBetween(
        column=sla.column,
        min_value=end - sla.max_lag,
        max_value=end if sla.bounded_by_ds else None,
    )


def _not_null(*cols: str) -> list[gxe.Expectation]:
    return [gxe.ExpectColumnValuesToNotBeNull(column=c) for c in cols]


def _orders(ds: str) -> list[gxe.Expectation]:
    cols = ["order_id", "customer_id", "order_ts", "amount", "currency", "status"]
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=[*cols, "ds", "ingested_at"], exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=500, max_value=10_000),
        *_not_null(*cols),
        gxe.ExpectColumnValuesToBeUnique(column="order_id"),
        gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0, max_value=MAX_AMOUNT),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=list(CURRENCIES)),
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=list(ORDER_STATUSES)),
        _freshness("orders", ds),
    ]


def _customers(ds: str) -> list[gxe.Expectation]:
    cols = ["customer_id", "email", "country", "tier", "signup_ts", "updated_at"]
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=[*cols, "loaded_ds"], exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=1_000, max_value=1_000_000),
        *_not_null(*cols),
        gxe.ExpectColumnValuesToBeUnique(column="customer_id"),
        gxe.ExpectColumnValuesToMatchRegex(column="email", regex=EMAIL_REGEX),
        gxe.ExpectColumnValuesToBeInSet(column="country", value_set=list(COUNTRIES)),
        gxe.ExpectColumnValuesToBeInSet(column="tier", value_set=list(TIERS)),
        _freshness("customers", ds),
    ]


def _payments(ds: str) -> list[gxe.Expectation]:
    cols = ["payment_id", "order_id", "amount", "currency", "method", "status", "paid_ts"]
    return [
        gxe.ExpectTableColumnsToMatchSet(column_set=[*cols, "ds", "ingested_at"], exact_match=True),
        gxe.ExpectTableRowCountToBeBetween(min_value=500, max_value=10_000),
        *_not_null(*cols),
        gxe.ExpectColumnValuesToBeUnique(column="payment_id"),
        gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01, max_value=MAX_AMOUNT),
        gxe.ExpectColumnValuesToBeInSet(column="method", value_set=list(PAYMENT_METHODS)),
        gxe.ExpectColumnValuesToBeInSet(column="status", value_set=list(PAYMENT_STATUSES)),
        _freshness("payments", ds),
    ]


def _daily_revenue(ds: str) -> list[gxe.Expectation]:
    cols = ["country", "currency", "order_count", "gross_revenue", "paid_revenue"]
    return [
        gxe.ExpectTableColumnsToMatchSet(
            column_set=[*cols, "unpaid_orders", "ds"], exact_match=True
        ),
        gxe.ExpectTableRowCountToBeBetween(min_value=1, max_value=200),
        *_not_null(*cols),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["country", "currency"]),
        gxe.ExpectColumnValuesToBeBetween(column="order_count", min_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="gross_revenue", min_value=0),
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="gross_revenue", column_B="paid_revenue", or_equal=True
        ),
        gxe.ExpectColumnValuesToBeInSet(column="country", value_set=[*COUNTRIES, "UNKNOWN"]),
    ]


_BUILDERS = {
    "orders": _orders,
    "customers": _customers,
    "payments": _payments,
    "daily_revenue": _daily_revenue,
}


def suite_name(dataset: str) -> str:
    return f"{dataset}.v{SUITE_VERSION}"


def expectations_for(dataset: str, ds: str) -> list[gxe.Expectation]:
    """Plain expectation objects; GX needs an active context before they join a suite,
    so `checkpoint.validate` assembles the suite."""
    return _BUILDERS[dataset](ds)
