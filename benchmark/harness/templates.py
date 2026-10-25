"""Fault templates: the kinds of fault the benchmark injects, each with a pool of
parameters. `gen_catalog` draws concrete faults from them with a fixed seed and writes
the result, with its ground truth, to the sealed catalog (benchmark/sealed/).

Reading this file shows the *kinds* of fault (the category list was fixed before the
harness was written), not which opaque fault id is which or what was drawn for it.

Every fault is one realistic nightly run of one business date, with nothing run for that
date beforehand:
  source       the upstream system delivers something wrong (pipelines.generators
               goes through a source shim; the generate task's own metrics and logs
               describe what was really delivered)
  deploy       a real code regression in the isolated build copy, plus the
               deploy_event that shipped it (git SHA, changed file, author, message)
  task_env     the environment a task runs with (a JVM memory limit)
  after_task   something happens to a delivered file before the next task reads it
  skip / delay an upstream DAG run never starts, or starts late
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from benchmark.harness.dags import DATASET_FOR, PIPELINE_FOR, SOURCES, TASKS

TIME_COLUMN = {"orders": "order_ts", "payments": "paid_ts", "customers": "updated_at"}
JOB = {"orders": "orders_ingest", "payments": "payments_glue_job", "customers": "customers_dim"}


@dataclass(frozen=True)
class Patch:
    file: str
    old: str
    new: str


@dataclass(frozen=True)
class Deploy:
    pipeline: str
    patches: tuple[Patch, ...]
    git_sha: str
    author: str
    message: str
    hour_utc: float  # on the fault's business date, before the nightly run


@dataclass
class Recipe:
    source: dict[str, dict[str, Any]] = field(default_factory=dict)
    deploy: Deploy | None = None
    task_env: dict[str, dict[str, str]] = field(default_factory=dict)
    after_task: dict[str, dict[str, str]] = field(default_factory=dict)
    skip_dags: tuple[str, ...] = ()
    delay_minutes: dict[str, int] = field(default_factory=dict)
    # Where the primary incident should appear (None = no incident: a clean run). Used
    # only to check that a built scenario really contains its fault.
    expect_pipeline: str | None = None


@dataclass(frozen=True)
class Template:
    key: str
    category: str  # a diagnosis category, or "none" for a clean run
    acceptable: tuple[str, ...]  # other categories that also count as correct
    options: Callable[[], list[dict[str, Any]]]
    root_cause: Callable[[dict[str, Any]], str]
    recipe: Callable[[dict[str, Any]], Recipe]
    flavor: Callable[[random.Random], dict[str, Any]] | None = None


def _src(dataset: str, profile: dict[str, Any]) -> Recipe:
    return Recipe(source={dataset: profile}, expect_pipeline=PIPELINE_FOR[dataset])


# --- schema_drift ------------------------------------------------------------------

DROP = {
    "orders": ["amount", "currency", "customer_id", "status"],
    "payments": ["method", "currency", "paid_ts", "order_id"],
    "customers": ["tier", "country", "email", "updated_at"],
}
RENAME = {
    "orders": [("amount", "order_amount"), ("order_ts", "created_at"), ("customer_id", "cust_id")],
    "payments": [
        ("paid_ts", "payment_ts"),
        ("method", "payment_method"),
        ("order_id", "order_ref"),
    ],
    "customers": [
        ("tier", "loyalty_tier"),
        ("updated_at", "last_modified"),
        ("country", "country_code"),
    ],
}
RETYPE = [
    {"dataset": "orders", "column": "amount", "kind": "eu_decimal"},
    {"dataset": "orders", "column": "customer_id", "kind": "prefixed:C-"},
    {"dataset": "payments", "column": "amount", "kind": "quoted"},
    {"dataset": "customers", "column": "customer_id", "kind": "prefixed:CUST-"},
]
RETYPE_WHAT = {
    "eu_decimal": "a decimal-comma text format (e.g. '123,45'), so Spark infers a string and "
    "the decimal cast yields NULL",
    "prefixed:C-": "a prefixed string (C-000123), so the cast to long yields NULL",
    "prefixed:CUST-": "a prefixed string (CUST-000123), so the cast to long yields NULL",
    "quoted": "JSON strings instead of numbers; the job still casts it, so nothing fails, but "
    "the input type changed from double to string",
}
ADD = [
    {"dataset": "orders", "column": "discount_code", "values": "codes", "position": 4},
    {"dataset": "orders", "column": "gift_wrap", "values": "flag", "position": 6},
    {"dataset": "payments", "column": "gateway_ref", "values": "refs", "position": 2},
    {"dataset": "customers", "column": "phone", "values": "phone", "position": 2},
    {"dataset": "customers", "column": "marketing_opt_in", "values": "flag", "position": 5},
]

# --- data_quality ------------------------------------------------------------------

NULLS = [
    ("orders", "customer_id"),
    ("orders", "currency"),
    ("payments", "method"),
    ("customers", "email"),
]
OUT_OF_RANGE = [
    {"dataset": "orders", "column": "amount", "op": "scale", "value": 100},
    {"dataset": "payments", "column": "amount", "op": "scale", "value": 100},
    {"dataset": "orders", "column": "amount", "op": "negate", "value": 0.03},
    {"dataset": "payments", "column": "amount", "op": "negate", "value": 0.04},
]
INVALID = [
    {"dataset": "customers", "column": "country", "from": "GB", "to": "UK"},
    {"dataset": "orders", "column": "status", "from": "completed", "to": "COMPLETE"},
    {"dataset": "payments", "column": "method", "from": "upi", "to": "UPI-COLLECT"},
    {"dataset": "orders", "column": "currency", "from": "INR", "to": "RS"},
]

# --- bad_deploy: real code regressions ---------------------------------------------

REGRESSIONS: dict[str, dict[str, Any]] = {
    "orders_amount_precision": {
        "pipeline": "orders_ingest",
        "patch": Patch(
            "pipelines/jobs/orders_ingest.py",
            'F.col("amount").cast("decimal(12,2)")',
            'F.col("amount").cast("decimal(4,2)")',
        ),
        "messages": [
            "Align orders.amount with the finance mart type",
            "orders: tighten amount precision for the finance export",
        ],
        "effect": "casts amount to DECIMAL(4,2), so every amount of 100 or more overflows to "
        "NULL and validate_orders' not-null check fails",
    },
    "payments_required_column": {
        "pipeline": "payments_etl",
        "patch": Patch(
            "pipelines/jobs/payments_glue_job.py",
            '"method", "status", "paid_ts"]',
            '"method", "status", "paid_at"]',
        ),
        "messages": [
            "payments: validate the settlement timestamp column up front",
            "Fail fast on missing payment columns",
        ],
        "effect": "requires a column `paid_at` that the input never had (it is `paid_ts`), so "
        "every run fails the required-columns check although the input is unchanged",
    },
    "customers_tier_case": {
        "pipeline": "customers_dim",
        "patch": Patch(
            "pipelines/jobs/customers_dim.py",
            'F.lower("tier").alias("tier")',
            'F.upper("tier").alias("tier")',
        ),
        "messages": [
            "customers: normalise tier codes to upper case for the CRM sync",
            "Uppercase tier values (CRM-1182)",
        ],
        "effect": "upper-cases tier, so every tier fails validate_customers' allowed-values check",
    },
    "revenue_grain": {
        "pipeline": "daily_revenue_agg",
        "patch": Patch(
            "pipelines/jobs/daily_revenue_agg.py",
            'F.lit("UNKNOWN")).alias("country"), "currency")',
            'F.lit("UNKNOWN")).alias("country"))',
        ),
        "messages": [
            "revenue: roll daily revenue up to country level for the exec dashboard",
            "Simplify daily revenue grouping",
        ],
        "effect": "drops currency from the aggregate's grouping, so the output loses its "
        "currency column and validate_daily_revenue fails",
    },
    "orders_currency_case": {
        "pipeline": "orders_ingest",
        "patch": Patch(
            "pipelines/jobs/orders_ingest.py",
            'F.upper("currency").alias("currency")',
            'F.initcap("currency").alias("currency")',
        ),
        "messages": ["orders: title-case currency codes for display", "Normalise currency format"],
        "effect": "title-cases currency (Inr), so validate_orders' allowed-values check fails",
    },
}
AUTHORS = ["Priya Raman", "Arjun Mehta", "Sam Okafor", "Lena Fischer", "Wei Zhang", "Nikhil Rao"]


def _deploy_flavor(rng: random.Random) -> dict[str, Any]:
    return {
        "git_sha": f"{rng.getrandbits(28):07x}",
        "author": rng.choice(AUTHORS),
        "message_index": rng.randrange(2),
        "hour_utc": round(rng.uniform(10.5, 19.5), 2),
    }


def _deploy_recipe(p: dict[str, Any]) -> Recipe:
    r = REGRESSIONS[p["variant"]]
    return Recipe(
        deploy=Deploy(
            pipeline=r["pipeline"],
            patches=(r["patch"],),
            git_sha=p["git_sha"],
            author=p["author"],
            message=r["messages"][p["message_index"]],
            hour_utc=p["hour_utc"],
        ),
        expect_pipeline=r["pipeline"],
    )


# --- upstream_outage ---------------------------------------------------------------

EXPORT_HOSTS = [
    "exports.partner-gw.internal",
    "sftp-bridge.src-prod.internal",
    "extract-api.erp.internal",
]

TEMPLATES: dict[str, Template] = {}


def _t(t: Template) -> None:
    TEMPLATES[t.key] = t


_t(
    Template(
        "schema.drop",
        "schema_drift",
        (),
        lambda: [{"dataset": d, "column": c} for d, cols in DROP.items() for c in cols],
        lambda p: f"The {p['dataset']} source extract stopped including the `{p['column']}` "
        f"column; {JOB[p['dataset']]} requires it and fails.",
        lambda p: _src(p["dataset"], {"drop": [p["column"]]}),
    )
)
_t(
    Template(
        "schema.rename",
        "schema_drift",
        (),
        lambda: [
            {"dataset": d, "old": o, "new": n} for d, pairs in RENAME.items() for o, n in pairs
        ],
        lambda p: f"The {p['dataset']} source renamed column `{p['old']}` to `{p['new']}`; "
        f"{JOB[p['dataset']]} requires `{p['old']}` and fails.",
        lambda p: _src(p["dataset"], {"rename": {p["old"]: p["new"]}}),
    )
)
_t(
    Template(
        "schema.retype",
        "schema_drift",
        (),
        lambda: [dict(x) for x in RETYPE],
        lambda p: f"The {p['dataset']} source changed `{p['column']}` to {RETYPE_WHAT[p['kind']]}.",
        lambda p: _src(p["dataset"], {"retype": {"column": p["column"], "kind": p["kind"]}}),
    )
)
_t(
    Template(
        "schema.add",
        "schema_drift",
        (),
        lambda: [dict(x) for x in ADD],
        lambda p: f"The {p['dataset']} source added a new column `{p['column']}`; the job "
        "ignores it and succeeds, so only the input schema changed.",
        lambda p: _src(p["dataset"], {"add": {k: p[k] for k in ("column", "values", "position")}}),
    )
)
_t(
    Template(
        "late.stale_snapshot",
        "late_data",
        (),
        lambda: [{"dataset": "customers", "days_old": k} for k in (2, 3, 4)],
        lambda p: f"The customers source delivered a snapshot {p['days_old']} days old (it was "
        "not refreshed), so updated_at is past the 24 h freshness SLA.",
        lambda p: _src("customers", {"days_old": p["days_old"]}),
    )
)
_t(
    Template(
        "late.truncated",
        "late_data",
        (),
        lambda: [
            {"dataset": d, "cutoff_hour": h}
            for d in ("orders", "payments")
            for h in (11, 14, 17, 19)
        ],
        lambda p: f"The {p['dataset']} extract stopped early: it has only events before "
        f"{p['cutoff_hour']:02d}:00 UTC, so the latest {TIME_COLUMN[p['dataset']]} is "
        f"{24 - p['cutoff_hour']} h before the end of the day (SLA 2 h) and volume is low.",
        lambda p: _src(p["dataset"], {"cutoff_hour": p["cutoff_hour"]}),
    )
)
_t(
    Template(
        "late.previous_day_file",
        "late_data",
        (),
        lambda: [{"dataset": d} for d in ("orders", "payments")],
        lambda p: f"The {p['dataset']} source re-delivered the previous day's extract under "
        f"this date, so every {TIME_COLUMN[p['dataset']]} is a day old.",
        lambda p: _src(p["dataset"], {"days_old": 1}),
    )
)
_t(
    Template(
        "oom.direct_memory",
        "oom",
        (),
        lambda: [
            {"task": t, "limit_k": k}
            for t in (
                "ingest_orders",
                "run_payments_glue_job",
                "build_customers_dim",
                "aggregate_revenue",
            )
            for k in (32, 64, 96)
        ],
        lambda p: f"The Spark JVM of {p['task']} ran out of memory: its environment set "
        f"JAVA_TOOL_OPTIONS=-XX:MaxDirectMemorySize={p['limit_k']}k, the job hit "
        "java.lang.OutOfMemoryError (direct buffer memory) and the JVM died.",
        lambda p: Recipe(
            task_env={p["task"]: {"JAVA_TOOL_OPTIONS": f"-XX:MaxDirectMemorySize={p['limit_k']}k"}},
            expect_pipeline=TASKS[p["task"]].dag_id,
        ),
    )
)
_t(
    Template(
        "deploy.code_regression",
        "bad_deploy",
        (),
        lambda: [{"variant": v} for v in REGRESSIONS],
        lambda p: f"Deploy {p['git_sha']} of {REGRESSIONS[p['variant']]['pipeline']} changed "
        f"{REGRESSIONS[p['variant']]['patch'].file}: it {REGRESSIONS[p['variant']]['effect']}.",
        _deploy_recipe,
        _deploy_flavor,
    )
)
_t(
    Template(
        "upstream.dag_missing",
        "upstream_outage",
        (),
        lambda: [{"pipeline": p} for p in SOURCES],
        lambda p: f"The {p['pipeline']} run for the date never started (upstream outage), so "
        f"daily_revenue_agg's wait_for_{p['pipeline']} sensor timed out after 30 min.",
        lambda p: Recipe(skip_dags=(p["pipeline"],), expect_pipeline="daily_revenue_agg"),
    )
)
_t(
    Template(
        "upstream.dag_late",
        "upstream_outage",
        (),
        lambda: [{"pipeline": p, "minutes": m} for p in SOURCES for m in (40, 50, 65)],
        lambda p: f"The {p['pipeline']} run started {p['minutes']} min late, so "
        f"daily_revenue_agg's sensor timed out after 30 min; {p['pipeline']} completed "
        "later, so a re-run would now succeed.",
        lambda p: Recipe(
            delay_minutes={p["pipeline"]: p["minutes"]}, expect_pipeline="daily_revenue_agg"
        ),
    )
)
_t(
    Template(
        "upstream.source_unreachable",
        "upstream_outage",
        (),
        lambda: [{"dataset": d, "host": h} for d in PIPELINE_FOR for h in EXPORT_HOSTS],
        lambda p: f"The {p['dataset']} source system's export endpoint ({p['host']}) was "
        f"unreachable; generate_{p['dataset']} failed after its retries and nothing was "
        "delivered.",
        lambda p: _src(
            p["dataset"],
            {
                "endpoint": {
                    "url": f"http://{p['host']}/v2/exports/{{dataset}}?date={{ds}}",
                    "attempts": 3,
                }
            },
        ),
    )
)
_t(
    Template(
        "dq.null_spike",
        "data_quality",
        (),
        lambda: [
            {"dataset": d, "column": c, "fraction": f} for d, c in NULLS for f in (0.06, 0.18, 0.33)
        ],
        lambda p: f"About {p['fraction']:.0%} of {p['dataset']}.{p['column']} arrived NULL from "
        "the source (a content defect; the schema did not change).",
        lambda p: _src(p["dataset"], {"nulls": {"column": p["column"], "fraction": p["fraction"]}}),
    )
)
_t(
    Template(
        "dq.duplicate_load",
        "data_quality",
        (),
        lambda: [{"dataset": d} for d in ("orders", "payments")],
        lambda p: f"The {p['dataset']} extract contains every row twice (a duplicate load): "
        "the unique-id check fails and volume doubles.",
        lambda p: _src(p["dataset"], {"duplicate": True}),
    )
)
_t(
    Template(
        "dq.out_of_range",
        "data_quality",
        (),
        lambda: [dict(x) for x in OUT_OF_RANGE],
        lambda p: (
            f"The {p['dataset']} source sent {p['column']} in paise instead of rupees (x100), so "
            "many values exceed the allowed maximum."
            if p["op"] == "scale"
            else f"The {p['dataset']} source sent about {p['value']:.0%} of {p['column']} values "
            "as negative numbers (refunds mixed in), below the allowed minimum."
        ),
        lambda p: _src(
            p["dataset"],
            {
                p["op"]: {
                    "column": p["column"],
                    ("factor" if p["op"] == "scale" else "fraction"): p["value"],
                }
            },
        ),
    )
)
_t(
    Template(
        "dq.invalid_values",
        "data_quality",
        (),
        lambda: [dict(x) for x in INVALID],
        lambda p: f"The {p['dataset']} source started sending '{p['to']}' instead of "
        f"'{p['from']}' in {p['column']}, which is not an allowed value.",
        lambda p: _src(
            p["dataset"], {"replace": {"column": p["column"], "from": p["from"], "to": p["to"]}}
        ),
    )
)
_t(
    Template(
        "amb.permissions",
        "unknown",
        (),
        lambda: [{"dataset": d} for d in PIPELINE_FOR],
        lambda p: f"The {p['dataset']} raw file landed without read permission (mode 000), so "
        "the job cannot read it: an access problem outside the six categories.",
        lambda p: Recipe(
            after_task={f"generate_{p['dataset']}": {"op": "chmod", "dataset": p["dataset"]}},
            expect_pipeline=PIPELINE_FOR[p["dataset"]],
        ),
    )
)
_t(
    Template(
        "amb.business_dip",
        "unknown",
        (),
        lambda: [{"keep": k} for k in (0.58, 0.64, 0.70)],
        lambda p: f"Order volume really fell by about {1 - p['keep']:.0%} for the day (fewer "
        "orders were placed); the data is complete and valid and nothing in the pipelines is "
        "broken.",
        lambda p: Recipe(
            source={
                "orders": {"keep_fraction": p["keep"]},
                "payments": {"keep_fraction": p["keep"]},
            },
            expect_pipeline="orders_ingest",
        ),
    )
)
_t(
    Template(
        "amb.timezone_shift",
        "data_quality",
        ("unknown",),
        lambda: [{"dataset": d} for d in ("orders", "payments")],
        lambda p: f"The {p['dataset']} source wrote local IST times labelled as UTC (+5:30), so "
        f"late-evening {TIME_COLUMN[p['dataset']]} values fall after the end of the day and "
        "the freshness upper bound fails.",
        lambda p: _src(
            p["dataset"], {"shift_minutes": {"column": TIME_COLUMN[p["dataset"]], "minutes": 330}}
        ),
    )
)
_t(
    Template(
        "amb.gzip_as_csv",
        "unknown",
        ("schema_drift", "data_quality"),
        lambda: [{"dataset": d} for d in ("orders", "customers")],
        lambda p: f"The {p['dataset']} source delivered a gzip-compressed file under a .csv "
        "name; Spark reads binary as text and the required-columns check fails.",
        lambda p: Recipe(
            after_task={f"generate_{p['dataset']}": {"op": "gzip", "dataset": p["dataset"]}},
            expect_pipeline=PIPELINE_FOR[p["dataset"]],
        ),
    )
)
_t(
    Template(
        "clean.none",
        "none",
        (),
        lambda: [{}],
        lambda p: "No fault: a normal run.",
        lambda p: Recipe(),
    )
)

assert set(DATASET_FOR) == set(SOURCES)
