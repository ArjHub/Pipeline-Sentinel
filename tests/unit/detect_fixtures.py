"""Synthetic bronze events for detector tests, shaped like the real producers' output.

`day_events(ds)` returns what one clean `make run-all` emits for a business date: a
task_run for every task, a job_metric for every job, a dq_result for every validate
task. Row counts, schemas and freshness values come from the real generators and from
real Phase 2 bronze rows. `fault=` reproduces the Phase 1 failure demos:

  missing_amount   orders CSV without `amount`: ingest_orders fails (SchemaMismatchError),
                   validate_orders never runs, daily_revenue_agg's sensor fails
  stale_customers  customers snapshot is days old: GX freshness fails validate_customers,
                   daily_revenue_agg's sensor fails
  volume_drop      orders file thinned to ~30% of its rows, still spread over the day:
                   everything succeeds with fewer rows (a truncated file is also stale;
                   see test_detect_real_producers)
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache

import pandas as pd

from pipelines.generators.datasets import generate_customers, generate_orders, generate_payments
from sentinel.events import (
    ColumnSpec,
    DQResult,
    Event,
    EventType,
    ExpectationOutcome,
    JobMetric,
    TaskRun,
)

SEED = 42
NS = uuid.UUID("6f1c3c56-0000-4000-8000-000000000000")
CODE_VERSION = "abc1234"

# Exactly as observed in Phase 2 bronze (Spark's inferred input schemas).
ORDERS_SCHEMA = [
    ("order_id", "string"),
    ("customer_id", "int"),
    ("order_ts", "timestamp"),
    ("amount", "double"),
    ("currency", "string"),
    ("status", "string"),
]
CUSTOMERS_SCHEMA = [
    ("customer_id", "int"),
    ("email", "string"),
    ("country", "string"),
    ("tier", "string"),
    ("signup_ts", "timestamp"),
    ("updated_at", "timestamp"),
]
PAYMENTS_SCHEMA = [
    ("amount", "double"),
    ("currency", "string"),
    ("method", "string"),
    ("order_id", "string"),
    ("paid_ts", "string"),
    ("payment_id", "string"),
    ("status", "string"),
]
SENSOR_ERROR = "The external task {task} in DAG {dag} failed."

_TYPES = {
    "task_run": EventType.TASK_RUN,
    "job_metric": EventType.JOB_METRIC,
    "dq_result": EventType.DQ_RESULT,
}


def _gx_ts(value) -> str:
    """GX reports a timestamp max as e.g. '2026-10-20 23:59:09+00:00'."""
    return str(pd.Timestamp(value).tz_convert("UTC"))


@lru_cache(maxsize=256)
def real_volumes(ds: str) -> dict:
    d = date.fromisoformat(ds)
    orders = generate_orders(d, SEED)
    payments = generate_payments(d, SEED)
    customers = generate_customers(d, SEED)
    return {
        "orders": len(orders),
        "payments": len(payments),
        "customers": len(customers),
        "orders_max": _gx_ts(orders["order_ts"].max()),
        "payments_max": _gx_ts(payments["paid_ts"].max()),
        "customers_max": _gx_ts(customers["updated_at"].max()),
    }


class Run:
    """Events for one DAG run, on a clock that advances 5 s per event."""

    def __init__(
        self, pipeline: str, ds: str, start: datetime, try_number: int = 1, run_id: str = ""
    ) -> None:
        self.pipeline, self.ds, self.clock = pipeline, ds, start
        self.try_number = try_number
        self.run_id = run_id or f"run_all__{ds}"
        self.events: list[Event] = []

    def _emit(self, task_id: str, payload) -> Event:
        self.clock += timedelta(seconds=5)
        ev = Event(
            event_id=str(
                uuid.uuid5(NS, f"{self.pipeline}|{self.ds}|{task_id}|{self.clock.isoformat()}")
            ),
            event_type=_TYPES[payload.kind],
            emitted_at=self.clock,
            pipeline=self.pipeline,
            run_id=self.run_id,
            task_id=task_id,
            logical_date=f"{self.ds}T00:00:00+00:00",
            try_number=self.try_number,
            code_version=CODE_VERSION,
            payload=payload,
        )
        self.events.append(ev)
        return ev

    def task(self, task_id: str, state: str = "success", exception: str | None = None) -> Event:
        return self._emit(
            task_id,
            TaskRun(
                task_id=task_id,
                operator="BashOperator",
                state=state,
                try_number=self.try_number,
                started_at=self.clock,
                finished_at=self.clock + timedelta(seconds=4),
                duration_s=4.0,
                exception=exception
                or (None if state == "success" else "Bash command failed. exit code 1."),
            ),
        )

    def job(
        self,
        task_id: str,
        job_name: str,
        *,
        status: str = "success",
        rows_in: int | None = None,
        rows_out: int | None = None,
        schema: list[tuple[str, str]] | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> Event:
        return self._emit(
            task_id,
            JobMetric(
                job_name=job_name,
                status=status,
                started_at=self.clock,
                finished_at=self.clock + timedelta(seconds=3),
                duration_s=3.0,
                rows_in=rows_in,
                rows_out=rows_out,
                input_schema=[ColumnSpec(name=n, type=t) for n, t in schema] if schema else [],
                error_type=error_type,
                error_message=error_message,
            ),
        )

    def dq(
        self,
        task_id: str,
        dataset: str,
        row_count: int,
        freshness: tuple[str, str, bool] | None = None,
        extra_failures: list[tuple[str, str | None]] = (),
    ) -> Event:
        results = [
            ExpectationOutcome(
                expectation_type="expect_table_row_count_to_be_between",
                success=True,
                observed_value=row_count,
            )
        ]
        if freshness:
            col, observed, ok = freshness
            results.append(
                ExpectationOutcome(
                    expectation_type="expect_column_max_to_be_between",
                    column=col,
                    success=ok,
                    observed_value=observed,
                )
            )
        for exp, col in extra_failures:
            results.append(ExpectationOutcome(expectation_type=exp, column=col, success=False))
        failed = sum(not r.success for r in results)
        return self._emit(
            task_id,
            DQResult(
                dataset=dataset,
                suite_name=f"{dataset}.v1",
                success=failed == 0,
                evaluated=len(results),
                failed=failed,
                row_count=row_count,
                results=results,
            ),
        )


def end_of(ds: str) -> datetime:
    return datetime.combine(date.fromisoformat(ds), datetime.min.time(), UTC) + timedelta(days=1)


def orders_run(r: Run, n: int, *, fault: str | None = None, factor: float = 0.3) -> int | None:
    """Returns the number of orders ingested (None if ingest failed)."""
    r.job("generate_orders", "generate_orders", rows_out=n)
    r.task("generate_orders")
    if fault == "missing_amount":
        cols = [c for c in ORDERS_SCHEMA if c[0] != "amount"]
        r.job(
            "ingest_orders",
            "orders_ingest",
            status="failed",
            rows_in=n,
            schema=cols,
            error_type="SchemaMismatchError",
            error_message=(
                "orders: missing required columns ['amount']; observed columns "
                f"{[c for c, _ in cols]}"
            ),
        )
        r.task("ingest_orders", "failed")
        return None  # validate_orders is upstream_failed: no callback, no events
    ingested = round(n * factor) if fault == "volume_drop" else n
    r.job(
        "ingest_orders", "orders_ingest", rows_in=ingested, rows_out=ingested, schema=ORDERS_SCHEMA
    )
    r.task("ingest_orders")
    r.dq(
        "validate_orders", "orders", ingested, ("order_ts", real_volumes(r.ds)["orders_max"], True)
    )
    r.task("validate_orders")
    return ingested


def customers_run(r: Run, *, fault: str | None = None, stale_days: int = 2) -> bool:
    v = real_volumes(r.ds)
    r.job("generate_customers", "generate_customers", rows_out=v["customers"])
    r.task("generate_customers")
    r.job(
        "build_customers_dim",
        "customers_dim",
        rows_in=v["customers"],
        rows_out=25,
        schema=CUSTOMERS_SCHEMA,
    )
    r.task("build_customers_dim")
    if fault == "stale_customers":
        old = end_of(r.ds) - timedelta(days=stale_days, minutes=5)
        r.dq("validate_customers", "customers", v["customers"], ("updated_at", _gx_ts(old), False))
        r.task("validate_customers", "failed")
        return False
    r.dq(
        "validate_customers", "customers", v["customers"], ("updated_at", v["customers_max"], True)
    )
    r.task("validate_customers")
    return True


def payments_run(r: Run) -> None:
    v = real_volumes(r.ds)
    r.job("generate_payments", "generate_payments", rows_out=v["payments"])
    r.task("generate_payments")
    r.job(
        "run_payments_glue_job",
        "payments_glue_job",
        rows_in=v["payments"],
        rows_out=v["payments"],
        schema=PAYMENTS_SCHEMA,
    )
    r.task("run_payments_glue_job")
    r.dq("validate_payments", "payments", v["payments"], ("paid_ts", v["payments_max"], True))
    r.task("validate_payments")


def revenue_run(r: Run, orders_ingested: int | None, upstream_ok: dict[str, bool]) -> None:
    validate = {
        "orders_ingest": "validate_orders",
        "customers_dim": "validate_customers",
        "payments_etl": "validate_payments",
    }
    for dag, ok in upstream_ok.items():
        if ok:
            r.task(f"wait_for_{dag}")
        else:
            r.task(
                f"wait_for_{dag}",
                "failed",
                SENSOR_ERROR.format(task=validate[dag], dag=dag),
            )
    if not all(upstream_ok.values()):
        return
    r.job("aggregate_revenue", "daily_revenue_agg", rows_in=orders_ingested, rows_out=18)
    r.task("aggregate_revenue")
    r.dq("validate_daily_revenue", "daily_revenue", 18)
    r.task("validate_daily_revenue")


def day_events(
    ds: str,
    *,
    fault: str | None = None,
    start: datetime | None = None,
    try_number: int = 1,
) -> list[Event]:
    """All events of one run-all for `ds` (runs start 30 min after the end of ds)."""
    start = start or end_of(ds) + timedelta(minutes=30)
    o = Run("orders_ingest", ds, start, try_number)
    c = Run("customers_dim", ds, start + timedelta(seconds=1), try_number)
    p = Run("payments_etl", ds, start + timedelta(seconds=2), try_number)
    ingested = orders_run(o, real_volumes(ds)["orders"], fault=fault)
    customers_ok = customers_run(c, fault=fault)
    payments_run(p)
    rev = Run("daily_revenue_agg", ds, start + timedelta(minutes=3), try_number)
    revenue_run(
        rev,
        ingested,
        {
            "orders_ingest": ingested is not None,
            "customers_dim": customers_ok,
            "payments_etl": True,
        },
    )
    return o.events + c.events + p.events + rev.events


def history(end_ds: str, days: int) -> list[Event]:
    """Clean runs for the `days` business dates before `end_ds`."""
    end = date.fromisoformat(end_ds)
    return [
        e for i in range(days, 0, -1) for e in day_events((end - timedelta(days=i)).isoformat())
    ]
