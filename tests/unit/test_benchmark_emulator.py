"""The emulated scheduler: ordering, sensors, environment, log tails and the real
Airflow callbacks. Task processes are faked here; test_benchmark_build runs real ones."""

from __future__ import annotations

import random
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from benchmark.harness import airflow_emu as emu
from benchmark.harness.templates import Recipe
from sentinel.events.models import JobLog, JobMetric, RunContext, TaskRun

DS = "2026-08-12"
START = datetime(2026, 8, 13, 0, 5, tzinfo=UTC)
VERSIONS = dict.fromkeys(
    ("orders_ingest", "customers_dim", "payments_etl", "daily_revenue_agg"), "abc1234"
)


class FakeExec:
    def __init__(self, rc: dict[str, int] | None = None) -> None:
        self.rc = rc or {}
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, cmd: str, cwd: Path, env: dict[str, str]) -> emu.Exec:
        task = env["AIRFLOW_CTX_TASK_ID"]
        self.calls.append((task, env))
        t0 = time.time()
        rc = self.rc.get(task, 0)
        now = datetime.now(UTC)
        ctx = RunContext(
            pipeline=env["AIRFLOW_CTX_DAG_ID"],
            task_id=task,
            logical_date=env["AIRFLOW_CTX_EXECUTION_DATE"],
            run_id=env["AIRFLOW_CTX_DAG_RUN_ID"],
        )
        metric = JobMetric(
            job_name=task,
            status="failed" if rc else "success",
            started_at=now,
            finished_at=now,
            duration_s=0.1,
        )
        line = f'{{"ts": "{now.isoformat()}", "level": "INFO", "message": "job started: {task}"}}'
        lines = [(t0, line)] + ([(t0 + 0.01, "ValueError: boom in " + task)] if rc else [])
        return emu.Exec(rc, lines, t0, t0 + 0.05, [ctx.event(metric)])


def _run(tmp_path, recipe=None, rc=None):
    fake = FakeExec(rc)
    e = emu.Emulator(
        root=tmp_path / "root",
        scratch=tmp_path / "scratch",
        night=emu.Night(DS, VERSIONS, recipe or Recipe(), START),
        rng=random.Random(3),
        executor=fake,
        base_env={"PATH": "/usr/bin"},
    )
    events = e.run()
    return e, events, fake


def _task_runs(events):
    return {e.task_id: e.payload for e in events if isinstance(e.payload, TaskRun)}


def test_clean_night_runs_every_task_once_on_the_simulated_clock(tmp_path) -> None:
    _, events, fake = _run(tmp_path)
    runs = _task_runs(events)
    assert len(runs) == 14 and all(p.state == "success" for p in runs.values())
    assert [t for t, _ in fake.calls].count("aggregate_revenue") == 1
    assert all(START <= ev.emitted_at < START + timedelta(minutes=5) for ev in events)
    # the aggregate starts only after all three sensors succeeded
    agg = runs["aggregate_revenue"].started_at
    assert all(
        runs[f"wait_for_{d}"].finished_at <= agg for d in VERSIONS if d != "daily_revenue_agg"
    )
    assert all(ev.run_id == f"scheduled__{DS}T00:00:00+00:00" for ev in events)


def test_task_environment(tmp_path) -> None:
    recipe = Recipe(
        source={"orders": {"drop": ["amount"]}},
        task_env={"ingest_orders": {"JAVA_TOOL_OPTIONS": "-XX:MaxDirectMemorySize=64k"}},
    )
    _, _, fake = _run(tmp_path, recipe)
    env = dict(fake.calls)
    assert env["generate_orders"]["SENTINEL_SOURCE_PROFILE"] == '{"drop": ["amount"]}'
    assert "SENTINEL_SOURCE_PROFILE" not in env["generate_payments"]
    assert env["ingest_orders"]["JAVA_TOOL_OPTIONS"] == "-XX:MaxDirectMemorySize=64k"
    assert "JAVA_TOOL_OPTIONS" not in env["build_customers_dim"]
    assert env["ingest_orders"]["AIRFLOW_CTX_EXECUTION_DATE"] == f"{DS}T00:00:00+00:00"
    assert env["ingest_orders"]["SENTINEL_CODE_VERSION"] == "abc1234"


def test_failed_task_goes_through_the_real_failure_callback(tmp_path) -> None:
    _, events, fake = _run(tmp_path, rc={"ingest_orders": 1})
    runs = _task_runs(events)
    assert runs["ingest_orders"].state == "failed"
    assert runs["ingest_orders"].exception == (
        "Bash command failed. The command returned a non-zero exit code 1."
    )
    assert "validate_orders" not in runs  # upstream_failed: no callback, no events
    assert "validate_orders" not in [t for t, _ in fake.calls]
    sensor = runs["wait_for_orders_ingest"]
    assert sensor.state == "failed"
    assert (
        sensor.exception
        == "Some of the external tasks ['validate_orders'] in DAG orders_ingest failed."
    )
    assert "aggregate_revenue" not in runs
    logs = [
        ev.payload
        for ev in events
        if isinstance(ev.payload, JobLog)
        and ev.payload.logger == "airflow.task"
        and ev.task_id == "ingest_orders"
    ]
    tail = logs[0].traceback
    assert "{subprocess.py:106} INFO - ValueError: boom in ingest_orders" in tail
    assert "airflow.exceptions.AirflowException: Bash command failed." in tail
    assert "Marking task as FAILED. dag_id=orders_ingest" in tail
    assert logs[0].exc_type == "AirflowException"


def test_missing_upstream_dag_times_the_sensor_out(tmp_path) -> None:
    _, events, fake = _run(tmp_path, Recipe(skip_dags=("customers_dim",)))
    runs = _task_runs(events)
    assert not any(t.startswith(("generate_customers", "build_customers")) for t, _ in fake.calls)
    s = runs["wait_for_customers_dim"]
    assert s.state == "failed" and s.exception.startswith("Sensor has timed out; run duration of ")
    assert s.exception.endswith("seconds exceeds the specified timeout of 1800.")
    assert 1800 <= s.duration_s <= 1830
    assert runs["wait_for_orders_ingest"].state == "success"


def test_late_upstream_times_out_but_completes_later(tmp_path) -> None:
    _, events, _ = _run(tmp_path, Recipe(delay_minutes={"payments_etl": 45}))
    runs = _task_runs(events)
    assert runs["wait_for_payments_etl"].state == "failed"
    assert runs["validate_payments"].state == "success"
    assert runs["validate_payments"].finished_at > runs["wait_for_payments_etl"].finished_at


def test_spark_pool_never_runs_three_spark_tasks_at_once(tmp_path) -> None:
    _, events, _ = _run(tmp_path)
    runs = _task_runs(events)
    spark = [runs[t] for t in ("ingest_orders", "build_customers_dim", "run_payments_glue_job")]
    for p in spark:
        overlapping = sum(
            q.started_at < p.finished_at and p.started_at < q.finished_at for q in spark
        )
        assert overlapping <= 2


def test_shifter_moves_wall_clock_only() -> None:
    real = datetime(2026, 10, 25, 7, 0, 0, tzinfo=UTC)
    sh = emu.Shifter(
        START - real,
        real - timedelta(hours=1),
        real + timedelta(hours=1),
        "5a0cc4f85939",
        "abcdefabcdef",
    )
    text = (
        '{"ts": "2026-10-25T07:00:01.123456+00:00"} [2026-10-25T07:00:02.500+0000] '
        "26/10/25 07:00:03 WARN TaskSetManager: (5a0cc4f85939 executor driver) "
        "observed=2026-08-12 23:55:22+00:00 local-1792911601000"
    )
    out = sh.text(text)
    assert '"2026-08-13T00:05:01.123456+00:00"' in out
    assert "[2026-08-13T00:05:02.500+0000]" in out
    assert "26/08/13 00:05:03 WARN" in out
    assert "abcdefabcdef executor driver" in out
    assert "observed=2026-08-12 23:55:22+00:00" in out  # data, not wall clock
    assert "local-1792911601000" not in out


def test_real_subprocess_capture(tmp_path) -> None:
    (tmp_path / "ev").mkdir()
    ex = emu.run_bash(
        "echo one; echo two >&2; exit 3",
        tmp_path,
        {"PATH": "/usr/bin:/bin", "SENTINEL_EVENTS_DIR": str(tmp_path / "ev")},
    )
    assert ex.rc == 3 and [line for _, line in ex.lines] == ["one", "two"]
