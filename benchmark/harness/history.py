"""Synthetic clean history before each fault date.

Each history night is a clone of one real clean night (the *template*: the emulator
running the real jobs for `template_ds`), adapted to its own date:
  - logical date, run id, ds-dependent paths and log messages;
  - row counts (generators, jobs, GX row_count) and the GX freshness max, recomputed
    from the generators for that date, so the volume and freshness detectors see the
    same numbers a real run would produce (test: the template's own values equal
    `facts(template_ds)`);
  - timings: the whole night shifted onto ds + 1 day 00:05 (+ jitter) and stretched by
    a per-night factor (0.92-1.12), so durations vary;
  - code versions from a deploy history (deploy_history), with deploy_events at the
    times they happened.

Schemas, DQ outcomes, task order and log formats are the template's, i.e. real.
"""

from __future__ import annotations

import random
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Any

import pandas as pd

from benchmark.harness.dags import ALL
from pipelines.generators.datasets import generate_customers, generate_orders, generate_payments
from sentinel.events.models import (
    DeployEvent,
    DQResult,
    Event,
    JobLog,
    JobMetric,
    RunContext,
    TaskRun,
)

SEED = 42  # SENTINEL_SEED for every benchmark run (the stack's default)
SHORT = {
    "orders_ingest": "orders",
    "customers_dim": "customers",
    "payments_etl": "payments",
    "daily_revenue_agg": "revenue",
}
PIPELINE_FILES = {
    "orders_ingest": ["pipelines/jobs/orders_ingest.py", "pipelines/dags/orders_ingest.py"],
    "customers_dim": ["pipelines/jobs/customers_dim.py", "pipelines/dags/customers_dim.py"],
    "payments_etl": ["pipelines/jobs/payments_glue_job.py", "pipelines/dags/payments_etl.py"],
    "daily_revenue_agg": [
        "pipelines/jobs/daily_revenue_agg.py",
        "pipelines/dags/daily_revenue_agg.py",
    ],
}
SHARED_FILES = [
    ["requirements/jobs.lock.txt"],
    ["pipelines/common/dag_helpers.py"],
    ["pipelines/quality/suites.py"],
    ["src/sentinel/instrumentation/logging.py"],
    ["pipelines/jobs/spark.py"],
    ["pipelines/common/config.py", "pipelines/common/dag_helpers.py"],
]
PIPELINE_MESSAGES = [
    "{s}: add docstrings",
    "{s}: log row counts at INFO",
    "Refactor {s} transform for readability",
    "{s}: tidy imports",
    "{s}: rename local variables",
    "Add comments to the {s} job",
    "{s}: small cleanup",
]
SHARED_MESSAGES = [
    "Bump pyarrow in the jobs lockfile",
    "Refactor the Spark session builder",
    "Quality: clearer suite names in logs",
    "Logging: include run_id in JSON lines",
    "Pin the great-expectations patch release",
    "Shared helpers: type hints",
]
AUTHORS = ["Priya Raman", "Arjun Mehta", "Sam Okafor", "Lena Fischer", "Wei Zhang", "Nikhil Rao"]

# job_name -> (rows_in, rows_out) fact names
JOB_ROWS = {
    "generate_orders": (None, "orders"),
    "generate_customers": (None, "customers"),
    "generate_payments": (None, "payments"),
    "orders_ingest": ("orders", "orders"),
    "customers_dim": ("customers", "changed"),
    "payments_glue_job": ("payments", "payments"),
    "daily_revenue_agg": ("orders", "groups"),
}
DQ_ROWS = {
    "orders": "orders",
    "customers": "customers",
    "payments": "payments",
    "daily_revenue": "groups",
}
FRESHNESS_COLUMN = {"orders": "order_ts", "payments": "paid_ts", "customers": "updated_at"}


def new_id(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


@dataclass(frozen=True)
class Facts:
    orders: int
    payments: int
    customers: int
    inserted: int
    updated: int
    groups: int
    max_ts: dict[str, str]  # dataset -> the GX observed max, as GX reports it

    @property
    def changed(self) -> int:
        return self.inserted + self.updated

    def rows(self, name: str | None) -> int | None:
        return None if name is None else getattr(self, name)


def _gx_max(values: pd.Series) -> str:
    return str(pd.to_datetime(values, utc=True).max())


@lru_cache(maxsize=1024)
def facts(ds: str, seed: int = SEED) -> Facts:
    d = date.fromisoformat(ds)
    orders = generate_orders(d, seed)
    payments = generate_payments(d, seed)
    cust = generate_customers(d, seed)
    prev = generate_customers(d - timedelta(days=1), seed)
    both = cust.merge(prev[["customer_id", "updated_at"]], on="customer_id", suffixes=("", "_p"))
    updated = int((pd.to_datetime(both["updated_at"]) > pd.to_datetime(both["updated_at_p"])).sum())
    completed = orders[orders["status"] == "completed"]
    country = completed.merge(cust[["customer_id", "country"]], on="customer_id", how="left")
    groups = country.assign(country=country["country"].fillna("UNKNOWN"))
    return Facts(
        orders=len(orders),
        payments=len(payments),
        customers=len(cust),
        inserted=len(cust) - len(prev),
        updated=updated,
        groups=int(groups.groupby(["country", "currency"]).ngroups),
        max_ts={
            "orders": _gx_max(orders["order_ts"]),
            "payments": _gx_max(payments["paid_ts"]),
            "customers": _gx_max(cust["updated_at"]),
        },
    )


# --- deploys ------------------------------------------------------------------------


@dataclass(frozen=True)
class DeployAt:
    at: datetime
    components: tuple[str, ...]
    sha: str
    files: tuple[str, ...]
    message: str
    author: str


def _sha(rng: random.Random) -> str:
    return f"{rng.getrandbits(28):07x}"


def deploy_history(
    rng: random.Random,
    first: date,
    until: datetime,
    every: tuple[int, int],
    shared_every: tuple[int, int],
) -> tuple[dict[str, str], list[DeployAt]]:
    """Initial version per pipeline, and the deploys between `first` and `until`."""
    initial = {p: _sha(rng) for p in ALL}
    deploys: list[DeployAt] = []

    def when(d: date) -> datetime:
        return datetime(d.year, d.month, d.day, tzinfo=UTC) + timedelta(
            hours=rng.uniform(9.0, 19.5)
        )

    for p in ALL:
        d = first + timedelta(days=rng.randint(0, every[1]))
        while (at := when(d)) < until:
            files = rng.choice([[f] for f in PIPELINE_FILES[p]] + [PIPELINE_FILES[p]])
            msg = rng.choice(PIPELINE_MESSAGES).format(s=SHORT[p])
            deploys.append(DeployAt(at, (p,), _sha(rng), tuple(files), msg, rng.choice(AUTHORS)))
            d += timedelta(days=rng.randint(*every))
    d = first + timedelta(days=rng.randint(0, shared_every[1]))
    while (at := when(d)) < until:
        deploys.append(
            DeployAt(
                at,
                ALL,
                _sha(rng),
                tuple(rng.choice(SHARED_FILES)),
                rng.choice(SHARED_MESSAGES),
                rng.choice(AUTHORS),
            )
        )
        d += timedelta(days=rng.randint(*shared_every))
    return initial, sorted(deploys, key=lambda x: x.at)


def versions_at(initial: dict[str, str], deploys: list[DeployAt], t: datetime) -> dict[str, str]:
    v = dict(initial)
    for dep in deploys:
        if dep.at < t:
            for c in dep.components:
                v[c] = dep.sha
    return v


def deploy_events(
    initial: dict[str, str], deploys: list[DeployAt], rng: random.Random
) -> list[Event]:
    current = dict(initial)
    out = []
    for dep in deploys:
        for i, c in enumerate(dep.components):
            payload = DeployEvent(
                component=c,
                code_version=dep.sha,
                previous_version=current[c],
                changed_files=list(dep.files),
                author=dep.author,
                message=dep.message,
                git_sha=dep.sha,
                dirty=False,
            )
            e = RunContext(pipeline=c, code_version=dep.sha).event(payload)
            at = dep.at + timedelta(milliseconds=3 * i + rng.uniform(0, 2))
            out.append(e.model_copy(update={"event_id": new_id(rng), "emitted_at": at}))
            current[c] = dep.sha
    return out


# --- cloning the template night -------------------------------------------------------


@dataclass(frozen=True)
class Template:
    ds: str
    start: datetime  # the template night's start
    events: list[Event]


_GX_TMP = re.compile(r"/tmp/tmp[a-z0-9_]{8}")


def _message(p: JobLog, job: str | None, f: Facts, ds: str, tds: str, dur: float | None) -> str:
    msg = p.message.replace(tds, ds)
    if p.logger == "sentinel.job" and msg.startswith("job finished:") and job in JOB_ROWS:
        rin, rout = (f.rows(x) for x in JOB_ROWS[job])
        msg = f"job finished: {job} rows_in={rin} rows_out={rout} duration_s={dur}"
    elif p.logger == "pipelines.generators":
        msg = re.sub(r"^wrote \d+", f"wrote {f.rows(JOB_ROWS[job][1]) if job else 0}", msg)
    elif p.logger == "pipelines.jobs.orders_ingest":
        msg = f"orders ds={ds} rows_in={f.orders} rows_out={f.orders}"
    elif p.logger == "pipelines.jobs.payments_glue_job":
        msg = f"payments ds={ds} rows_in={f.payments} rows_out={f.payments}"
    elif p.logger == "pipelines.jobs.customers_dim":
        msg = f"customers ds={ds} snapshot={f.customers} changed={f.changed}"
    elif p.logger == "pipelines.jobs.daily_revenue_agg":
        msg = f"daily_revenue ds={ds} orders={f.orders} groups={f.groups}"
    elif p.logger == "pipelines.quality" and msg.startswith("DQ "):
        dataset = msg.split()[1]
        msg = re.sub(r"rows=\d+", f"rows={f.rows(DQ_ROWS[dataset])}", msg)
    return msg


def clone_night(
    t: Template,
    ds: str,
    start: datetime,
    factor: float,
    versions: dict[str, str],
    host: str,
    rng: random.Random,
) -> list[Event]:
    f = facts(ds)
    logical = f"{ds}T00:00:00+00:00"
    run_id = f"scheduled__{logical}"

    def at(x: datetime) -> datetime:
        return start + (x - t.start) * factor

    # the job each task's events belong to (to recompute "job finished" lines)
    job_of_task: dict[str, str] = {
        e.task_id: e.payload.job_name
        for e in t.events
        if isinstance(e.payload, JobMetric) and e.task_id
    }
    out: list[Event] = []
    for e in t.events:
        p: Any = e.payload
        if isinstance(p, JobMetric):
            rin, rout = (f.rows(x) for x in JOB_ROWS[p.job_name])
            extra = dict(p.extra)
            if "arrived_at" in extra:
                extra["arrived_at"] = at(datetime.fromisoformat(extra["arrived_at"])).isoformat()
            if "spark_app_id" in extra:
                extra["spark_app_id"] = f"local-{int(at(p.started_at).timestamp() * 1000)}"
            if p.job_name == "customers_dim":
                extra.update(inserted=f.inserted, updated=f.updated)
            p = p.model_copy(
                update={
                    "started_at": at(p.started_at),
                    "finished_at": at(p.finished_at),
                    "duration_s": round(p.duration_s * factor, 3),
                    "rows_in": rin,
                    "rows_out": rout,
                    "output_path": (p.output_path or "").replace(t.ds, ds) or None,
                    "extra": extra,
                }
            )
        elif isinstance(p, TaskRun):
            p = p.model_copy(
                update={
                    "started_at": at(p.started_at) if p.started_at else None,
                    "finished_at": at(p.finished_at) if p.finished_at else None,
                    "duration_s": round((p.duration_s or 0) * factor, 3),
                    "hostname": host,
                }
            )
        elif isinstance(p, DQResult):
            rows = f.rows(DQ_ROWS[p.dataset])
            results = []
            for r in p.results:
                if r.expectation_type == "expect_table_row_count_to_be_between":
                    r = r.model_copy(update={"observed_value": rows})
                elif r.expectation_type == "expect_column_max_to_be_between":
                    r = r.model_copy(update={"observed_value": f.max_ts[p.dataset]})
                results.append(r)
            p = p.model_copy(update={"row_count": rows, "results": results})
        elif isinstance(p, JobLog):
            job = job_of_task.get(e.task_id or "")
            dur = None
            if p.message.startswith("job finished:"):
                m = re.search(r"duration_s=([\d.]+)", p.message)
                dur = round(float(m[1]) * factor, 3) if m else None
            msg = _GX_TMP.sub(
                lambda _: "/tmp/tmp"
                + "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789_") for _ in range(8)),
                _message(p, job, f, ds, t.ds, dur),
            )
            p = p.model_copy(update={"message": msg})
        out.append(
            e.model_copy(
                update={
                    "event_id": new_id(rng),
                    "emitted_at": at(e.emitted_at),
                    "logical_date": logical,
                    "run_id": run_id,
                    "code_version": versions[e.pipeline],
                    "payload": p,
                }
            )
        )
    return out


def night_start(ds: str, after_midnight_s: float, jitter_s: float, rng: random.Random) -> datetime:
    d = date.fromisoformat(ds) + timedelta(days=1)
    return datetime(d.year, d.month, d.day, tzinfo=UTC) + timedelta(
        seconds=after_midnight_s + rng.uniform(0, jitter_s)
    )
