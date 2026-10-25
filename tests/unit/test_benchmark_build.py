"""End to end with the real jobs (slow, ~4 min; `make test-benchmark`): a template
night, then a clean scenario and a schema-drift scenario in a temp build root.

Checks: the clean night produces no incident and the volume detector ran on its robust
path; the fault night produces its incident; no harness text reached the lake; and the
task logs look like the stack's (paths, Airflow framing)."""

from __future__ import annotations

import dataclasses
import json
from datetime import date

import pytest

from benchmark.harness import scenario
from benchmark.harness.catalog import Fault, Label
from benchmark.harness.settings import load
from sentinel.detect import load_config
from sentinel.detect.observations import load_bronze
from sentinel.detect.volume import assess
from sentinel.diagnose.run import find_incident
from sentinel.diagnose.tools import Toolbox

pytestmark = [pytest.mark.spark, pytest.mark.benchmark_build]

CLEAN = Fault(
    "F-CLEAN2",
    "benchmark",
    "2026-07-15",
    "clean.none",
    {},
    Label("none", (), "No fault: a normal run."),
)
DROP = Fault(
    "F-DROP22",
    "benchmark",
    "2026-07-16",
    "schema.drop",
    {"dataset": "orders", "column": "amount"},
    Label("schema_drift", (), "orders lost amount"),
)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("bench")
    settings = dataclasses.replace(load(), work_dir=tmp / "work", template_ds="2026-06-01")
    root = tmp / "opt-sentinel"
    template = scenario.build_template(settings, root)
    out = {
        f.fault_id: scenario.build_scenario(f, settings, template, [CLEAN, DROP], root)
        for f in (CLEAN, DROP)
    }
    return settings, out


def test_clean_night_has_no_incident_and_robust_volume(built) -> None:
    settings, m = built
    c = m["F-CLEAN2"]
    assert c["expectation_met"] and c["targets"] == [] and c["leaks"] == 0
    assert set(c["task_states"].values()) == {"success"} and len(c["task_states"]) == 14
    obs = load_bronze(scenario.lake_path(settings, "F-CLEAN2") / "bronze", date(2026, 1, 1))
    hist = [
        (date.fromisoformat(j.ds), float(j.rows_in))
        for j in obs.jobs
        if j.job_name == "orders_ingest" and j.ds < CLEAN.ds
    ]
    today = next(j for j in obs.jobs if j.job_name == "orders_ingest" and j.ds == CLEAN.ds)
    a = assess(
        today.rows_in, date.fromisoformat(CLEAN.ds), hist, load_config().volume, "same_weekday"
    )
    assert a.method == "robust_z_same_weekday" and a.points == 6


def test_fault_night_produces_its_incident_with_stack_like_evidence(built) -> None:
    settings, m = built
    d = m["F-DROP22"]
    assert d["expectation_met"] and len(d["targets"]) == 1 and d["leaks"] == 0
    assert d["task_states"]["ingest_orders"] == "failed"
    lake = scenario.lake_path(settings, "F-DROP22")
    inc = find_incident(lake, d["targets"][0])
    assert inc["type"] == "schema_change" and inc["pipeline"] == "orders_ingest"
    box = Toolbox(lake, load_config(), inc)
    logs = json.loads(box.call("get_error_logs", {})[0])["untrusted_data"]
    tails = " ".join(x.get("traceback_tail") or "" for x in logs)
    assert "missing required columns ['amount']" in tails
    assert "opt-sentinel/pipelines/jobs/orders_ingest.py" in tails  # the build root's paths
    diff = json.loads(box.call("get_schema_diff", {})[0])["untrusted_data"]
    assert any("amount" in e.get("diff", {}).get("removed", []) for e in diff)
