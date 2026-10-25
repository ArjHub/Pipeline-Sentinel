"""Emulated Airflow for benchmark runs: the real task processes on a simulated clock.

Why not the real scheduler: every fault needs a clean, isolated lake and a single
nightly run of one date. Resetting the live stack (Airflow metadata, Redpanda, the lake)
per fault would take minutes each and make timings and ids non-reproducible.

What is real:
  - every task runs the exact `bash -c <command>` its DAG renders (benchmark.harness.dags,
    checked against the DagBag), in a subprocess with the AIRFLOW_CTX_* variables that
    BashOperator exports, so the real generators, Spark jobs and GX suites run and emit
    their own events;
  - task_run events and failure log tails come from the real callbacks
    (sentinel.instrumentation.airflow_callbacks). They get a task-instance stand-in and a
    task log written in Airflow's format, which the real `log_tail` reads.

What is simulated:
  - the clock. Tasks are placed as four parallel DAG runs with a 2-slot Spark pool would
    place them. Wall-clock timestamps in events and log lines are moved onto that night;
    measured durations are kept;
  - sensors: decided from the upstream outcome instead of polling;
  - the Airflow-side lines of the task log (header, failure traceback), copied from
    real task logs of this stack.
"""

from __future__ import annotations

import contextlib
import gzip
import heapq
import json
import os
import random
import re
import socket
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from benchmark.harness.dags import DAGS, DATASET_FOR, DOWNSTREAM, FINAL_TASK, SOURCES, Task
from benchmark.harness.templates import Recipe
from sentinel.events.models import Event, JobLog, JobMetric, parse_event

AF = "/home/airflow/.local/lib/python3.11/site-packages/airflow"
EXT = {"orders": "csv", "customers": "csv", "payments": "jsonl"}


class AirflowException(Exception):
    pass


class AirflowSensorTimeout(AirflowException):
    pass


# --- moving wall-clock time onto the simulated night ------------------------------

_ISO = re.compile(
    r"(?P<d>\d{4}-\d{2}-\d{2})(?P<sep>[T ])(?P<t>\d{2}:\d{2}:\d{2})(?:\.(?P<f>\d{1,6}))?"
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?"
)
_SPARK = re.compile(r"\b(\d{2})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})\b")
_APP_ID = re.compile(r"\blocal-(\d{13})\b")


@dataclass
class Shifter:
    """Moves timestamps that fall inside the real execution window by `delta`. Data
    timestamps (business dates months earlier) are outside the window and untouched."""

    delta: timedelta
    lo: datetime
    hi: datetime
    host_from: str = ""
    host_to: str = ""

    def dt(self, d: datetime) -> datetime:
        return d + self.delta if self.lo <= d <= self.hi else d

    def _iso(self, m: re.Match) -> str:
        try:
            d = datetime.fromisoformat(f"{m['d']}T{m['t']}").replace(tzinfo=UTC)
        except ValueError:
            return m[0]
        if m["f"]:
            d = d.replace(microsecond=int(m["f"].ljust(6, "0")))
        if not self.lo <= d <= self.hi:
            return m[0]
        d += self.delta
        out = d.strftime(f"%Y-%m-%d{m['sep']}%H:%M:%S")
        if m["f"]:
            out += "." + f"{d.microsecond:06d}"[: len(m["f"])]
        return out + (m["tz"] or "")

    def _spark(self, m: re.Match) -> str:
        yy, mo, dd, hh, mi, ss = (int(x) for x in m.groups())
        try:
            d = datetime(2000 + yy, mo, dd, hh, mi, ss, tzinfo=UTC)
        except ValueError:
            return m[0]
        if not self.lo <= d <= self.hi:
            return m[0]
        return (d + self.delta).strftime("%y/%m/%d %H:%M:%S")

    def _app(self, m: re.Match) -> str:
        d = datetime.fromtimestamp(int(m[1]) / 1000, UTC)
        return f"local-{int(self.dt(d).timestamp() * 1000)}" if self.lo <= d <= self.hi else m[0]

    def text(self, s: str | None) -> str | None:
        if not s:
            return s
        s = _APP_ID.sub(self._app, _SPARK.sub(self._spark, _ISO.sub(self._iso, s)))
        return s.replace(self.host_from, self.host_to) if self.host_from else s

    def value(self, v: Any) -> Any:
        if isinstance(v, str):
            return self.text(v)
        if isinstance(v, dict):
            return {k: self.value(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self.value(x) for x in v]
        return v

    def event(self, e: Event) -> Event:
        p = e.payload
        if isinstance(p, JobMetric):
            p = p.model_copy(
                update={
                    "started_at": self.dt(p.started_at),
                    "finished_at": self.dt(p.finished_at),
                    "output_path": self.text(p.output_path),
                    "error_message": self.text(p.error_message),
                    "extra": self.value(p.extra),
                }
            )
        elif isinstance(p, JobLog):
            p = p.model_copy(
                update={"message": self.text(p.message), "traceback": self.text(p.traceback)}
            )
        return e.model_copy(update={"emitted_at": self.dt(e.emitted_at), "payload": p})


# --- running one task process --------------------------------------------------------


@dataclass
class Exec:
    rc: int
    lines: list[tuple[float, str]]  # (epoch seconds when read, text)
    t0: float
    t1: float
    events: list[Event]


def read_events(directory: Path) -> list[Event]:
    events = []
    for path in sorted(directory.glob("*/*.jsonl")):
        events.extend(parse_event(line) for line in path.read_text().splitlines() if line)
    return sorted(events, key=lambda e: e.emitted_at)


def run_bash(cmd: str, cwd: Path, env: dict[str, str], timeout_s: float = 900) -> Exec:
    """Like Airflow's SubprocessHook: bash -c, own session, stderr merged into stdout."""
    events_dir = Path(env["SENTINEL_EVENTS_DIR"])
    t0 = time.time()
    proc = subprocess.Popen(
        ["/usr/bin/bash", "-c", cmd],
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    lines: list[tuple[float, str]] = []
    assert proc.stdout is not None
    for raw in iter(proc.stdout.readline, b""):
        lines.append((time.time(), raw.decode("utf-8", errors="backslashreplace").rstrip("\n")))
        if time.time() - t0 > timeout_s:
            proc.kill()
    rc = proc.wait()
    return Exec(rc, lines, t0, time.time(), read_events(events_dir))


# --- Airflow's side of the task log (copied from real logs of this stack) ------------

_TB_HEAD = f"""Traceback (most recent call last):
  File "{AF}/models/taskinstance.py", line 771, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "{AF}/models/taskinstance.py", line 742, in _execute_callable
    return ExecutionCallableRunner(
           ^^^^^^^^^^^^^^^^^^^^^^^^
  File "{AF}/utils/operator_helpers.py", line 252, in run
    return self.func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "{AF}/models/baseoperator.py", line 425, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
BASH_TB = (
    _TB_HEAD
    + f"""  File "{AF}/operators/bash.py", line 276, in execute
    raise AirflowException(
"""
)
_SENSOR_TB = (
    _TB_HEAD
    + f"""  File "{AF}/sensors/external_task.py", line 348, in execute
    super().execute(context)
  File "{AF}/models/baseoperator.py", line 425, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
)
SENSOR_FAILED_TB = (
    _SENSOR_TB
    + f"""  File "{AF}/sensors/base.py", line 309, in execute
    raise e
  File "{AF}/sensors/base.py", line 289, in execute
    poke_return = self.poke(context)
                  ^^^^^^^^^^^^^^^^^^
  File "{AF}/utils/session.py", line 97, in wrapper
    return func(*args, session=session, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "{AF}/sensors/external_task.py", line 296, in poke
    raise AirflowException(
"""
)
SENSOR_TIMEOUT_TB = (
    _SENSOR_TB
    + f"""  File "{AF}/sensors/base.py", line 326, in execute
    raise AirflowSensorTimeout(message)
"""
)


def _stamp(t: datetime) -> str:
    return f"[{t:%Y-%m-%dT%H:%M:%S}.{t.microsecond // 1000:03d}+0000]"


@dataclass
class TaskLog:
    lines: list[str] = field(default_factory=list)

    def add(self, t: datetime, src: str, level: str, msg: str) -> None:
        self.lines.append(f"{_stamp(t)} {{{src}}} {level} - {msg}")

    def raw(self, text: str) -> None:
        self.lines.extend(text.rstrip("\n").split("\n"))

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


@dataclass
class Result:
    state: str  # success | failed | upstream_failed
    start: datetime
    end: datetime


@dataclass
class Night:
    """What the emulator needs to know about one night's runs."""

    ds: str
    versions: dict[str, str]  # pipeline -> code_version
    recipe: Recipe
    start: datetime  # when the scheduler creates the DAG runs


@contextlib.contextmanager
def _env(**values: str) -> Iterator[None]:
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class Emulator:
    def __init__(
        self,
        *,
        root: Path,
        scratch: Path,
        night: Night,
        rng: random.Random,
        latency_s: tuple[float, float] = (0.8, 3.5),
        spark_slots: int = 2,
        sensor_timeout_s: int = 1800,
        poke_interval_s: int = 10,
        base_env: dict[str, str] | None = None,
        executor: Callable[[str, Path, dict[str, str]], Exec] = run_bash,
    ) -> None:
        self.root, self.scratch, self.night, self.rng = root, scratch, night, rng
        self.latency_s, self.spark_slots = latency_s, spark_slots
        self.sensor_timeout_s, self.poke_interval_s = sensor_timeout_s, poke_interval_s
        self.executor = executor
        self.ds = night.ds
        self.logical = f"{self.ds}T00:00:00+00:00"
        self.logical_dt = datetime.fromisoformat(self.logical)
        self.run_id = f"scheduled__{self.logical}"
        self.host = f"{rng.getrandbits(48):012x}"
        self.real_host = socket.gethostname()
        self.job_id = rng.randrange(200, 900)
        self.pid = rng.randrange(1500, 30000)
        self.logs = scratch / "airflow-logs"
        self.base_env = dict(base_env if base_env is not None else os.environ)
        self.results: dict[str, Result] = {}
        self.events: list[Event] = []
        self._seq = 0

    # --- helpers ---------------------------------------------------------------------

    def _lat(self) -> timedelta:
        return timedelta(seconds=self.rng.uniform(*self.latency_s))

    def _next_dir(self, kind: str) -> Path:
        self._seq += 1
        d = self.scratch / "events" / f"{self._seq:03d}-{kind}"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _header(self, log: TaskLog, task: Task, t: datetime) -> datetime:
        ti = f"<TaskInstance: {task.dag_id}.{task.task_id} {self.run_id}"
        self.job_id += 1
        self.pid += self.rng.randrange(20, 400)
        cfg = "".join(self.rng.choice("abcdefghijklmnopqrstuvwxyz0123456789_") for _ in range(8))
        ctx = (
            f"AIRFLOW_CTX_DAG_OWNER='data-platform' AIRFLOW_CTX_DAG_ID='{task.dag_id}' "
            f"AIRFLOW_CTX_TASK_ID='{task.task_id}' AIRFLOW_CTX_EXECUTION_DATE='{self.logical}' "
            f"AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='{self.run_id}'"
        )
        steps = [
            ("local_task_job_runner.py:123", "INFO", "::group::Pre task execution logs"),
            (
                "taskinstance.py:2632",
                "INFO",
                f"Dependencies all met for dep_context=non-requeueable deps ti={ti} [queued]>",
            ),
            (
                "taskinstance.py:2632",
                "INFO",
                f"Dependencies all met for dep_context=requeueable deps ti={ti} [queued]>",
            ),
            ("taskinstance.py:2885", "INFO", "Starting attempt 1 of 1"),
            (
                "taskinstance.py:2908",
                "INFO",
                f"Executing <Task({task.operator}): {task.task_id}> on {self.ds} 00:00:00+00:00",
            ),
            ("standard_task_runner.py:72", "INFO", f"Started process {self.pid} to run task"),
            (
                "standard_task_runner.py:104",
                "INFO",
                f"Running: ['airflow', 'tasks', 'run', '{task.dag_id}', '{task.task_id}', "
                f"'{self.run_id}', '--job-id', '{self.job_id}', '--raw', '--subdir', "
                f"'DAGS_FOLDER/{task.dag_id}.py', '--cfg-path', '/tmp/tmp{cfg}']",
            ),
            ("standard_task_runner.py:105", "INFO", f"Job {self.job_id}: Subtask {task.task_id}"),
            ("task_command.py:467", "INFO", f"Running {ti} [running]> on host {self.host}"),
            ("taskinstance.py:3158", "INFO", f"Exporting env vars: {ctx}"),
            ("taskinstance.py:740", "INFO", "::endgroup::"),
        ]
        for src, level, msg in steps:
            log.add(t, src, level, msg)
            t += timedelta(milliseconds=self.rng.uniform(1, 40))
        return t

    def _marking(self, log: TaskLog, task: Task, start: datetime, end: datetime, state: str):
        log.add(
            end,
            "taskinstance.py:1242",
            "INFO",
            f"Marking task as {state}. dag_id={task.dag_id}, task_id={task.task_id}, "
            f"run_id={self.run_id}, execution_date={self.ds.replace('-', '')}T000000, "
            f"start_date={start:%Y%m%dT%H%M%S}, end_date={end:%Y%m%dT%H%M%S}",
        )
        log.add(end, "taskinstance.py:1580", "INFO", "Executing callback at index 0: on_failure")

    def _callback(
        self,
        task: Task,
        state: str,
        start: datetime,
        end: datetime,
        exc: Exception | None,
        log: TaskLog | None,
    ) -> None:
        from sentinel.instrumentation import airflow_callbacks

        if log is not None:
            path = (
                self.logs
                / f"dag_id={task.dag_id}"
                / f"run_id={self.run_id}"
                / f"task_id={task.task_id}"
                / "attempt=1.log"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(log.text())
        ti = SimpleNamespace(
            dag_id=task.dag_id,
            task_id=task.task_id,
            run_id=self.run_id,
            try_number=1,
            start_date=start,
            end_date=end,
            duration=(end - start).total_seconds(),
            operator=task.operator,
            hostname=self.host,
        )
        out = self._next_dir("callback")
        with _env(
            SENTINEL_EVENT_SINK="file",
            SENTINEL_EVENTS_DIR=str(out),
            AIRFLOW__LOGGING__BASE_LOG_FOLDER=str(self.logs),
            SENTINEL_CODE_VERSION=self.night.versions[task.dag_id],
        ):
            fn = (
                airflow_callbacks.on_success if state == "success" else airflow_callbacks.on_failure
            )
            fn({"ti": ti, "exception": exc, "logical_date": self.logical_dt})
        t = end + timedelta(milliseconds=self.rng.uniform(8, 20))
        for e in read_events(out):
            self.events.append(e.model_copy(update={"emitted_at": t}))
            t += timedelta(milliseconds=self.rng.uniform(1.5, 4))

    def _after(self, task: Task) -> None:
        action = self.night.recipe.after_task.get(task.task_id)
        if not action:
            return
        dataset = action["dataset"]
        raw = self.root / "data" / "lake" / "raw" / dataset / f"ds={self.ds}"
        path = raw / f"part-0000.{EXT[dataset]}"
        if action["op"] == "chmod":
            path.chmod(0)
        elif action["op"] == "gzip":
            path.write_bytes(gzip.compress(path.read_bytes(), mtime=0))
        else:
            raise ValueError(f"unknown after-task op {action['op']!r}")

    # --- bash tasks ------------------------------------------------------------------

    def _task_env(self, task: Task, events_dir: Path) -> dict[str, str]:
        env = dict(self.base_env)
        env.pop("SENTINEL_SOURCE_PROFILE", None)
        env.update(
            AIRFLOW_CTX_DAG_OWNER="data-platform",
            AIRFLOW_CTX_DAG_ID=task.dag_id,
            AIRFLOW_CTX_TASK_ID=task.task_id,
            AIRFLOW_CTX_EXECUTION_DATE=self.logical,
            AIRFLOW_CTX_TRY_NUMBER="1",
            AIRFLOW_CTX_DAG_RUN_ID=self.run_id,
            SENTINEL_CODE_VERSION=self.night.versions[task.dag_id],
            SENTINEL_EVENT_SINK="file",
            SENTINEL_EVENTS_DIR=str(events_dir),
        )
        if task.task_id.startswith("generate_"):
            profile = self.night.recipe.source.get(DATASET_FOR[task.dag_id])
            if profile:
                env["SENTINEL_SOURCE_PROFILE"] = json.dumps(profile)
        env.update(self.night.recipe.task_env.get(task.task_id, {}))
        return env

    def _bash(self, task: Task, start: datetime) -> Result:
        cmd = task.command(self.ds)
        ex = self.executor(cmd, self.root, self._task_env(task, self._next_dir(task.task_id)))
        log = TaskLog()
        t = self._header(log, task, start)
        log.add(t, "subprocess.py:78", "INFO", "Tmp dir root location: /tmp")
        log.add(t, "subprocess.py:88", "INFO", f"Running command: ['/usr/bin/bash', '-c', '{cmd}']")
        log.add(t, "subprocess.py:99", "INFO", "Output:")
        proc_start = t + timedelta(milliseconds=self.rng.uniform(3, 9))
        real0 = datetime.fromtimestamp(ex.t0, UTC)
        sh = Shifter(
            proc_start - real0,
            real0 - timedelta(hours=1),
            datetime.fromtimestamp(ex.t1, UTC) + timedelta(hours=1),
            self.real_host,
            self.host,
        )
        for at, line in ex.lines:
            when = proc_start + timedelta(seconds=at - ex.t0)
            log.add(when, "subprocess.py:106", "INFO", sh.text(line) or "")
        end = proc_start + timedelta(seconds=ex.t1 - ex.t0)
        log.add(end, "subprocess.py:110", "INFO", f"Command exited with return code {ex.rc}")
        end += timedelta(milliseconds=self.rng.uniform(15, 45))
        self.events.extend(sh.event(e) for e in ex.events)
        if ex.rc == 0:
            self._callback(task, "success", start, end, None, None)
            self._after(task)
            return Result("success", start, end)
        msg = f"Bash command failed. The command returned a non-zero exit code {ex.rc}."
        log.add(end, "taskinstance.py:3337", "ERROR", "Task failed with exception")
        log.raw(BASH_TB + f"airflow.exceptions.AirflowException: {msg}")
        self._marking(log, task, start, end, "FAILED")
        self._callback(task, "failed", start, end, AirflowException(msg), log)
        return Result("failed", start, end)

    # --- sensors ---------------------------------------------------------------------

    def _sensor(self, task: Task, start: datetime) -> Result:
        up = self.results.get(FINAL_TASK[task.external_dag_id])
        deadline = start + timedelta(seconds=self.sensor_timeout_s)
        pokes = [start]
        while pokes[-1] <= deadline + timedelta(seconds=self.poke_interval_s):
            gap = self.poke_interval_s + self.rng.uniform(0.2, 1.4)
            pokes.append(pokes[-1] + timedelta(seconds=gap))
        if up is not None and up.end <= deadline:
            end = next(p for p in pokes if p >= up.end) + timedelta(seconds=0.3)
            state = "success" if up.state == "success" else "failed"
        else:
            end = next(p for p in pokes if p > deadline) + timedelta(seconds=0.3)
            state = "timeout"
        if state == "success":
            self._callback(task, "success", start, end, None, None)
            return Result("success", start, end)
        poke_line = (
            f"Poking for tasks ['{task.external_task_id}'] in dag {task.external_dag_id} on "
            f"{self.logical} ... "
        )
        log = TaskLog()
        cycles = [p for p in pokes if p < end][-8:]
        for i, p in enumerate(cycles):
            t = self._header(log, task, p)
            log.add(
                t,
                "baseoperator.py:424",
                "WARNING",
                "ExternalTaskSensor.execute cannot be called outside TaskInstance!",
            )
            log.add(t, "external_task.py:258", "INFO", poke_line)
            log.add(
                t,
                "logging_mixin.py:190",
                "WARNING",
                f"{AF}/utils/session.py:97 DeprecationWarning: This method is deprecated "
                "and will be removed in future.",
            )
            if i < len(cycles) - 1:
                log.add(
                    t,
                    "taskinstance.py:318",
                    "INFO",
                    "Rescheduling task, marking task as UP_FOR_RESCHEDULE",
                )
                log.add(t, "local_task_job_runner.py:243", "INFO", "Task exited with return code 0")
        if state == "failed":
            msg = (
                f"Some of the external tasks ['{task.external_task_id}'] in DAG "
                f"{task.external_dag_id} failed."
            )
            exc: Exception = AirflowException(msg)
            log.add(end, "taskinstance.py:3337", "ERROR", "Task failed with exception")
            log.raw(SENSOR_FAILED_TB + f"airflow.exceptions.AirflowException: {msg}")
        else:
            dur = (end - start).total_seconds() - self.rng.uniform(0.01, 0.2)
            msg = (
                f"Sensor has timed out; run duration of {dur} seconds exceeds the specified "
                f"timeout of {self.sensor_timeout_s}."
            )
            exc = AirflowSensorTimeout(msg)
            log.add(end, "taskinstance.py:3337", "ERROR", "Task failed with exception")
            log.raw(SENSOR_TIMEOUT_TB + f"airflow.exceptions.AirflowSensorTimeout: {msg}")
        self._marking(log, task, start, end, "FAILED")
        self._callback(task, "failed", start, end, exc, log)
        return Result("failed", start, end)

    # --- the night -------------------------------------------------------------------

    def run(self) -> list[Event]:
        r = self.night.recipe
        dag_start = {
            dag: self.night.start
            + timedelta(seconds=self.rng.uniform(0, 3), minutes=r.delay_minutes.get(dag, 0))
            for dag in DAGS
        }
        slots = [self.night.start] * self.spark_slots
        heap: list[tuple[datetime, int, str, int]] = []
        order = 0
        for dag in SOURCES:
            if dag not in r.skip_dags:
                heapq.heappush(heap, (dag_start[dag] + self._lat(), order, dag, 0))
                order += 1
        while heap:
            ready, _, dag, i = heapq.heappop(heap)
            res = self._run(DAGS[dag][i], ready, slots)
            self.results[DAGS[dag][i].task_id] = res
            if res.state == "success" and i + 1 < len(DAGS[dag]):
                heapq.heappush(heap, (res.end + self._lat(), order, dag, i + 1))
                order += 1
            elif res.state != "success":
                for later in DAGS[dag][i + 1 :]:
                    gone = res.end + timedelta(seconds=1)
                    self.results[later.task_id] = Result("upstream_failed", gone, gone)

        tasks = DAGS[DOWNSTREAM]
        sensors = [t for t in tasks if t.operator == "ExternalTaskSensor"]
        done = [self._run(s, dag_start[DOWNSTREAM] + self._lat(), slots) for s in sensors]
        for s, res in zip(sensors, done, strict=True):
            self.results[s.task_id] = res
        rest = [t for t in tasks if t.operator != "ExternalTaskSensor"]
        if all(d.state == "success" for d in done):
            t = max(d.end for d in done)
            for task in rest:
                res = self._run(task, t + self._lat(), slots)
                self.results[task.task_id] = res
                if res.state != "success":
                    break
                t = res.end
        failed_at = [d.end for d in done if d.state != "success"] + [
            x.end
            for k, x in self.results.items()
            if k in {t.task_id for t in rest} and x.state == "failed"
        ]
        for task in rest:
            if task.task_id not in self.results:
                gone = max(failed_at) + timedelta(seconds=1)
                self.results[task.task_id] = Result("upstream_failed", gone, gone)
        return sorted(self.events, key=lambda e: e.emitted_at)

    def _run(self, task: Task, ready: datetime, slots: list[datetime]) -> Result:
        if task.operator == "ExternalTaskSensor":
            return self._sensor(task, ready)
        if not task.spark:
            return self._bash(task, ready)
        i = min(range(len(slots)), key=slots.__getitem__)
        res = self._bash(task, max(ready, slots[i]))
        slots[i] = res.end
        return res
