"""The Phase 1 faults through the *real* producers: real generators, the real Spark jobs
and GX suites, their real job_metric / dq_result events, landed by the real BronzeWriter
and read back with load_bronze. Only the history before the fault day is synthetic
(detect_fixtures, same shape), because 35 days of real Spark runs would take minutes.

Not exercised here: Airflow task callbacks (task_run events) and the ExternalTaskSensor
failures in daily_revenue_agg. A failed job's job_metric stands in for its missing
task_run (see observations.py); the downstream link is covered by test_detect_scenarios
and by the live-stack run in the README.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from pipelines.common.config import raw_dir
from pipelines.generators import __main__ as gen_cli
from pipelines.generators.datasets import generate_customers, generate_orders
from pipelines.jobs import customers_dim, orders_ingest, payments_glue_job
from pipelines.jobs.spark import SchemaMismatchError
from pipelines.quality.__main__ import load
from pipelines.quality.checkpoint import validate
from sentinel.detect import detect, load_bronze, load_config
from sentinel.events import MemorySink, RunContext
from sentinel.instrumentation import instrumented_job
from sentinel.lander.bronze import BronzeWriter, RawMessage
from tests.conftest import CLEAN_DS
from tests.unit.detect_fixtures import end_of, history

pytestmark = pytest.mark.spark
D = date.fromisoformat(CLEAN_DS)
CFG = load_config()


def _ctx(pipeline: str, task_id: str) -> RunContext:
    return RunContext(
        pipeline=pipeline,
        run_id=f"run_all__{CLEAN_DS}",
        task_id=task_id,
        logical_date=f"{CLEAN_DS}T00:00:00+00:00",
        try_number=1,
        code_version="test",
    )


def _job(spark, sink, pipeline: str, task_id: str, job_name: str, module) -> None:
    with instrumented_job(job_name, _ctx(pipeline, task_id), sink) as stats:
        module.run(spark, CLEAN_DS, stats)


def _validate(sink, pipeline: str, dataset: str) -> None:
    validate(
        load(dataset, CLEAN_DS), dataset, CLEAN_DS, _ctx(pipeline, f"validate_{dataset}"), sink
    )


def _produce(spark, fault: str) -> list:
    """One day's run of the three source pipelines with `fault` applied to the raw data."""
    sink = MemorySink()
    for dataset in ("orders", "customers", "payments"):
        gen_cli.main(["--dataset", dataset, "--ds", CLEAN_DS])
    orders_csv = raw_dir("orders", CLEAN_DS) / "part-0000.csv"
    customers_csv = raw_dir("customers", CLEAN_DS) / "part-0000.csv"
    if fault == "missing_amount":
        generate_orders(D, 42).drop(columns=["amount"]).to_csv(orders_csv, index=False)
    elif fault == "volume_drop":
        # Every 3rd row, still spread over the whole day (README: awk 'NR==1 || NR%3==0').
        pd.read_csv(orders_csv).iloc[1::3].to_csv(orders_csv, index=False)
    elif fault == "truncated_orders":
        # The first 30% of the file: a partial delivery that stops mid-morning.
        df = pd.read_csv(orders_csv)
        df.head(round(len(df) * 0.3)).to_csv(orders_csv, index=False)
    elif fault == "stale_customers":
        # The snapshot from two days earlier lands in today's folder.
        generate_customers(D - timedelta(days=2), 42).to_csv(customers_csv, index=False)

    if fault == "missing_amount":
        with pytest.raises(SchemaMismatchError):
            _job(spark, sink, "orders_ingest", "ingest_orders", "orders_ingest", orders_ingest)
    else:
        _job(spark, sink, "orders_ingest", "ingest_orders", "orders_ingest", orders_ingest)
        _validate(sink, "orders_ingest", "orders")
    _job(spark, sink, "customers_dim", "build_customers_dim", "customers_dim", customers_dim)
    _validate(sink, "customers_dim", "customers")
    _job(
        spark, sink, "payments_etl", "run_payments_glue_job", "payments_glue_job", payments_glue_job
    )
    _validate(sink, "payments_etl", "payments")
    return sink.events


def _land(root, events) -> None:
    BronzeWriter(root).land(
        [
            RawMessage(e.topic, 0, i, e.pipeline.encode(), e.model_dump_json().encode())
            for i, e in enumerate(events)
        ]
    )


@pytest.mark.parametrize(
    ("fault", "pipeline", "itype"),
    [
        ("missing_amount", "orders_ingest", "schema_change"),
        ("stale_customers", "customers_dim", "late_data"),
        ("volume_drop", "orders_ingest", "volume_anomaly"),
    ],
)
def test_real_producer_fault_creates_exactly_one_incident(
    lake, spark, fault, pipeline, itype
) -> None:
    real = _produce(spark, fault)
    assert {e.event_type.value for e in real} >= {"job_metric", "dq_result"}
    bronze = lake / "bronze"
    _land(bronze, history(CLEAN_DS, 35) + real)

    obs = load_bronze(bronze, D - timedelta(days=CFG.history_days))
    now = max(end_of(CLEAN_DS), *(e.emitted_at for e in real)) + timedelta(hours=1)
    [incident] = detect(obs, CFG, now)
    assert (incident.pipeline, incident.type, incident.role, incident.status) == (
        pipeline,
        itype,
        "primary",
        "open",
    )
    assert incident.evidence["affected_ds"] == [CLEAN_DS]
    head = incident.evidence["occurrences"][0]["signals"][0]["evidence"]
    if fault == "missing_amount":
        assert head["removed"] == ["amount"] and head["job_error"] == "SchemaMismatchError"
    elif fault == "stale_customers":
        assert head["column"] == "updated_at" and head["lag_hours"] > 24
        assert head["gx_success"] is False  # GX agrees
    else:
        assert head["method"] == "robust_z_same_weekday"
        assert head["change_pct"] == pytest.approx(-67, abs=1)


def test_real_truncated_file_is_one_incident_with_two_signals(lake, spark) -> None:
    """A file cut off mid-morning is both stale (latest order_ts hours before the end of
    ds) and short. Both are causes on the same run, so they merge into one incident;
    late_data ranks above volume_anomaly (CAUSE_PRIORITY) and names it."""
    real = _produce(spark, "truncated_orders")
    bronze = lake / "bronze"
    _land(bronze, history(CLEAN_DS, 35) + real)
    obs = load_bronze(bronze, D - timedelta(days=CFG.history_days))
    [incident] = detect(obs, CFG, max(e.emitted_at for e in real) + timedelta(hours=1))
    assert (incident.pipeline, incident.type, incident.role) == (
        "orders_ingest",
        "late_data",
        "primary",
    )
    signals = incident.evidence["occurrences"][0]["signals"]
    assert [s["type"] for s in signals] == ["late_data", "volume_anomaly"]
    assert signals[0]["evidence"]["lag_hours"] > 2
    assert signals[1]["evidence"]["change_pct"] == pytest.approx(-70, abs=1)


def test_real_producer_clean_day_creates_nothing(lake, spark) -> None:
    real = _produce(spark, "none")
    bronze = lake / "bronze"
    _land(bronze, history(CLEAN_DS, 35) + real)
    obs = load_bronze(bronze, D - timedelta(days=CFG.history_days))
    now = max(e.emitted_at for e in real) + timedelta(hours=1)
    assert detect(obs, CFG, now) == []
