from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sentinel.detect import load_config
from sentinel.detect.correlate import correlate, incident_id
from sentinel.detect.findings import Finding, IncidentType

CFG = load_config()
T = datetime(2026, 10, 21, 0, 30, tzinfo=UTC)
DS = "2026-10-20"
FJ, SC, LD, VA = (
    IncidentType.FAILED_JOB,
    IncidentType.SCHEMA_CHANGE,
    IncidentType.LATE_DATA,
    IncidentType.VOLUME_ANOMALY,
)


def F(type_, pipeline="orders_ingest", *, at=0, cleared=None, fp=None, task="t", ds=DS, sev="high"):
    return Finding(
        type=type_,
        pipeline=pipeline,
        ds=ds,
        fingerprint=fp or f"{task}:{type_.value}",
        severity=sev,
        title=f"{pipeline} {type_.value}",
        first_observed=T + timedelta(seconds=at),
        last_observed=T + timedelta(seconds=at),
        cleared_at=None if cleared is None else T + timedelta(seconds=cleared),
        task_id=task,
    )


def by_pipeline(incidents):
    return {i.pipeline: i for i in incidents}


def test_cause_absorbs_the_failure_it_explains() -> None:
    [inc] = correlate([F(SC, task="ingest"), F(FJ, task="ingest", at=1)], CFG)
    assert (inc.type, inc.role, inc.status) == ("schema_change", "primary", "open")
    assert len(inc.evidence["occurrences"][0]["signals"]) == 2


def test_failure_long_before_the_cause_is_its_own_incident() -> None:
    incidents = correlate([F(FJ, task="gen", at=0), F(VA, task="ingest", at=3600)], CFG)
    assert sorted(i.type for i in incidents) == ["failed_job", "volume_anomaly"]
    assert {i.role for i in incidents} == {"primary"}


def test_failure_after_the_cause_cleared_is_not_its_symptom() -> None:
    incidents = correlate([F(SC, task="ingest", cleared=600), F(FJ, task="validate", at=3600)], CFG)
    assert len(incidents) == 2


def test_most_specific_cause_names_the_incident_and_others_merge() -> None:
    [inc] = correlate([F(VA, at=0), F(LD, at=1), F(SC, at=2)], CFG)
    assert inc.type == "schema_change"
    assert "2 other cause signal(s)" in inc.evidence["occurrences"][0]["why"]


def test_distinct_failures_without_a_cause_stay_separate() -> None:
    incidents = correlate([F(FJ, task="a"), F(FJ, task="b", at=1)], CFG)
    assert len(incidents) == 2


def test_downstream_failure_links_to_upstream_root() -> None:
    up = F(SC, task="ingest_orders")
    down = F(FJ, "daily_revenue_agg", task="wait_for_orders_ingest", at=120)
    inc = by_pipeline(correlate([down, up], CFG))
    assert inc["orders_ingest"].role == "primary"
    assert inc["daily_revenue_agg"].role == "downstream"
    assert inc["daily_revenue_agg"].root_incident_id == inc["orders_ingest"].incident_id
    assert inc["daily_revenue_agg"].dedup_key.startswith(inc["orders_ingest"].dedup_key + " >> ")
    assert inc["daily_revenue_agg"].title.startswith("[downstream of INC-")


def test_downstream_problem_without_an_upstream_one_is_primary() -> None:
    [inc] = correlate([F(FJ, "daily_revenue_agg", task="aggregate_revenue")], CFG)
    assert (inc.role, inc.root_incident_id) == ("primary", None)


def test_upstream_problem_on_another_ds_does_not_link() -> None:
    incidents = correlate(
        [F(SC, ds="2026-10-19"), F(FJ, "daily_revenue_agg", task="aggregate_revenue")], CFG
    )
    assert {i.role for i in incidents} == {"primary"}


def test_upstream_problem_cleared_before_downstream_started_does_not_link() -> None:
    incidents = correlate(
        [F(SC, cleared=60), F(FJ, "daily_revenue_agg", task="aggregate_revenue", at=3600)], CFG
    )
    assert {i.role for i in incidents} == {"primary"}


def test_downstream_seen_before_upstream_does_not_link() -> None:
    incidents = correlate([F(SC, at=3600), F(FJ, "daily_revenue_agg", task="x", at=0)], CFG)
    assert {i.role for i in incidents} == {"primary"}


def test_sensor_named_upstream_wins_and_others_are_recorded() -> None:
    orders = F(SC, "orders_ingest", at=0)
    customers = F(LD, "customers_dim", at=5)
    down = F(FJ, "daily_revenue_agg", task="wait_for_customers_dim", at=60)
    inc = by_pipeline(correlate([orders, customers, down], CFG))
    rev = inc["daily_revenue_agg"]
    assert rev.root_incident_id == inc["customers_dim"].incident_id  # named, though later
    assert rev.evidence["occurrences"][0]["also_caused_by"] == [inc["orders_ingest"].incident_id]


def test_transitive_chain_links_to_the_top_root() -> None:
    cfg = load_config(overrides={"pipelines": {"payments_etl": {"upstream": ["orders_ingest"]}}})
    a = F(SC, "orders_ingest", at=0)
    b = F(FJ, "payments_etl", task="x", at=30)
    c = F(FJ, "daily_revenue_agg", task="y", at=60)
    inc = by_pipeline(correlate([c, b, a], cfg))
    root = inc["orders_ingest"].incident_id
    assert inc["payments_etl"].root_incident_id == root
    assert inc["daily_revenue_agg"].root_incident_id == root


def test_same_problem_while_still_open_adds_the_date_to_the_incident() -> None:
    [inc] = correlate([F(SC, ds="2026-10-19"), F(SC, ds="2026-10-20", at=86400)], CFG)
    assert inc.evidence["affected_ds"] == ["2026-10-19", "2026-10-20"]
    assert inc.status == "open" and inc.evidence["occurrence_number"] == 1


def test_overlapping_occurrences_stay_one_incident() -> None:
    # Day 2 broke before day 1 was fixed (cleared at 100 000 s): same incident.
    first = F(SC, ds="2026-10-19", cleared=100_000)
    [inc] = correlate([first, F(SC, ds="2026-10-20", at=86_400, cleared=100_010)], CFG)
    assert inc.evidence["occurrence_count"] == 2 and inc.status == "resolved"


def test_recurrence_after_resolution_is_a_new_numbered_incident() -> None:
    fp = "orders_ingest:-amount"
    one = F(SC, ds="2026-10-19", fp=fp, cleared=10)
    two = F(SC, ds="2026-10-20", fp=fp, at=86_400, cleared=86_500)
    three = F(SC, ds="2026-10-16", fp=fp, at=200_000)  # a backfill: earlier ds, later time
    a, b, c = correlate([three, two, one], CFG)
    base = "INC-c5962d3281"
    assert [i.incident_id for i in (a, b, c)] == [base, base + "#2", base + "#3"]
    assert [i.status for i in (a, b, c)] == ["resolved", "resolved", "open"]
    assert [i.evidence["affected_ds"] for i in (a, b, c)] == [
        ["2026-10-19"],
        ["2026-10-20"],
        ["2026-10-16"],
    ]
    assert (b.evidence["occurrence_number"], b.evidence["previous_incident_id"]) == (2, base)
    assert c.evidence["previous_incident_id"] == base + "#2"
    assert len({i.dedup_key for i in (a, b, c)}) == 3  # separate rows in the store


def test_downstream_of_a_recurrence_links_to_that_recurrence() -> None:
    def pair(ds, at, cleared=None):
        return [
            F(SC, "orders_ingest", fp="o:-amount", ds=ds, at=at, cleared=cleared),
            F(
                FJ,
                "daily_revenue_agg",
                task="wait_for_orders_ingest",
                ds=ds,
                at=at + 60,
                cleared=cleared,
            ),
        ]

    incs = correlate(pair("2026-10-19", 0, cleared=100) + pair("2026-10-20", 10_000), CFG)
    roots = [i for i in incs if i.role == "primary"]
    downs = [i for i in incs if i.role == "downstream"]
    assert [r.incident_id.endswith("#2") for r in roots] == [False, True]
    assert sorted(d.root_incident_id for d in downs) == sorted(r.incident_id for r in roots)
    assert {d.status for d in downs if d.root_incident_id.endswith("#2")} == {"open"}


def test_status_resolved_and_severity_is_the_max() -> None:
    [inc] = correlate([F(SC, sev="medium", cleared=10), F(FJ, at=1, cleared=20)], CFG)
    assert (inc.status, inc.severity) == ("resolved", "high")


def test_unknown_pipeline_is_kept_but_never_linked() -> None:
    [inc] = correlate([F(FJ, "ad_hoc_dag", task="x")], CFG)
    assert inc.role == "primary"


def test_incident_id_is_a_stable_hash_of_the_dedup_key() -> None:
    assert incident_id("a|b|c") == incident_id("a|b|c") != incident_id("a|b|d")
    assert incident_id("orders_ingest|schema_change|orders_ingest:-amount") == "INC-c5962d3281"
    assert incident_id("a|b|c", 2) == incident_id("a|b|c") + "#2"
