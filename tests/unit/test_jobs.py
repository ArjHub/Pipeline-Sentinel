from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import pytest
from deltalake import DeltaTable

from pipelines.common.config import curated_path, raw_dir
from pipelines.generators import __main__ as gen_cli
from pipelines.generators.datasets import (
    NEW_CUSTOMERS_PER_DAY,
    customer_count,
    generate_orders,
    generate_payments,
)
from pipelines.jobs import customers_dim, daily_revenue_agg, orders_ingest
from pipelines.jobs.spark import SchemaMismatchError
from sentinel.instrumentation import JobStats
from tests.conftest import CLEAN_DS

pytestmark = pytest.mark.spark
D = date.fromisoformat(CLEAN_DS)


def _rows(dataset: str, ds: str | None = CLEAN_DS) -> pd.DataFrame:
    table = DeltaTable(str(curated_path(dataset)))
    return table.to_pandas(partitions=[("ds", "=", ds)]) if ds else table.to_pandas()


def test_clean_pipeline_row_counts_match_generators(clean_lake) -> None:
    assert len(_rows("orders")) == len(generate_orders(D, 42))
    assert len(_rows("payments")) == len(generate_payments(D, 42))
    assert len(_rows("customers", None)) == customer_count(D)


def test_daily_revenue_reconciles_with_completed_orders(clean_lake) -> None:
    orders = generate_orders(D, 42)
    completed = orders[orders["status"] == "completed"]
    rev = _rows("daily_revenue")
    assert rev["order_count"].sum() == len(completed)
    expected_gross = Decimal(str(round(completed["amount"].sum(), 2)))
    assert rev["gross_revenue"].sum() == pytest.approx(expected_gross)
    assert (rev["paid_revenue"] <= rev["gross_revenue"]).all()
    assert rev[["country", "currency"]].duplicated().sum() == 0


def test_orders_types_are_cast(clean_lake) -> None:
    df = _rows("orders")
    assert str(df["order_ts"].dtype).startswith("datetime64")
    assert isinstance(df["amount"].iloc[0], Decimal)


def test_rerun_overwrites_only_its_partition(clean_lake, spark) -> None:
    before = len(_rows("orders"))
    version = DeltaTable(str(curated_path("orders"))).version()
    orders_ingest.run(spark, CLEAN_DS, JobStats())
    assert len(_rows("orders")) == before
    assert DeltaTable(str(curated_path("orders"))).version() == version + 1


def test_job_records_observed_input_schema(clean_lake, spark) -> None:
    stats = JobStats()
    orders_ingest.run(spark, CLEAN_DS, stats)
    names = [c.name for c in stats.input_schema]
    assert names == ["order_id", "customer_id", "order_ts", "amount", "currency", "status"]
    assert stats.rows_in == stats.rows_out == len(generate_orders(D, 42))


def test_missing_required_column_raises_schema_mismatch(lake, spark) -> None:
    path = raw_dir("orders", CLEAN_DS)
    path.mkdir(parents=True)
    generate_orders(D, 42).drop(columns=["amount"]).to_csv(path / "part-0000.csv", index=False)
    with pytest.raises(SchemaMismatchError, match=r"missing required columns \['amount'\]"):
        orders_ingest.run(spark, CLEAN_DS, JobStats())


def test_customers_merge_inserts_new_and_updates_changed_only(lake, spark) -> None:
    d1, d2 = D.isoformat(), (D + timedelta(days=1)).isoformat()
    for ds in (d1, d2):
        gen_cli.main(["--dataset", "customers", "--ds", ds])
    customers_dim.run(spark, d1, JobStats())

    stats = JobStats()
    customers_dim.run(spark, d2, stats)
    assert stats.extra["inserted"] == NEW_CUSTOMERS_PER_DAY
    assert 0 < stats.extra["updated"] < customer_count(D) // 10

    replay = JobStats()
    customers_dim.run(spark, d2, replay)
    assert replay.extra == {"inserted": 0, "updated": 0}


def test_aggregate_handles_unknown_customers_and_unpaid_orders(spark) -> None:
    orders = spark.createDataFrame(
        [
            ("o1", 1, Decimal("10.00"), "INR", "completed"),
            ("o2", 99, Decimal("5.00"), "INR", "completed"),
            ("o3", 1, Decimal("7.00"), "INR", "cancelled"),
        ],
        "order_id string, customer_id long, amount decimal(12,2), currency string, status string",
    )
    payments = spark.createDataFrame(
        [("o1", Decimal("10.00"), "captured")],
        "order_id string, amount decimal(12,2), status string",
    )
    customers = spark.createDataFrame([(1, "IN")], "customer_id long, country string")
    rows = {
        r["country"]: r.asDict()
        for r in daily_revenue_agg.aggregate(orders, payments, customers, "x").collect()
    }
    assert rows["IN"]["order_count"] == 1 and rows["IN"]["unpaid_orders"] == 0
    assert rows["UNKNOWN"]["unpaid_orders"] == 1
    assert rows["UNKNOWN"]["paid_revenue"] == 0
