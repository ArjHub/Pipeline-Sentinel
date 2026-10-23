"""Button clicks: allowlist, retry-safe gate, Airflow calls, statuses, audit."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from sentinel.actions import audit
from sentinel.actions.handler import ActionHandler, Click, parse_click
from sentinel.airflow_client import AirflowApiError
from sentinel.detect import load_config as load_detection_config
from sentinel.detect.runner import run_once
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.diagnose import store
from sentinel.diagnose.agent import DiagnosisRun
from sentinel.diagnose.schema import Diagnosis
from sentinel.notify.slack import MockSlack
from tests.unit.diagnose_fixtures import DS, NOW, build_lake
from tests.unit.test_diagnose_agent import answer

ALL = ("orders_ingest", "customers_dim", "payments_etl", "daily_revenue_agg")


class FakeAirflow:
    def __init__(self, fail: bool = False) -> None:
        self.cleared: list[tuple[str, str, bool]] = []
        self.fail = fail

    def find_run(self, dag_id, logical_date):
        return {"dag_run_id": f"run_all__{logical_date[:10]}"}

    def clear_run(self, dag_id, run_id, only_failed):
        if self.fail:
            raise AirflowApiError(503, "webserver down")
        self.cleared.append((dag_id, run_id, only_failed))
        return {}


def add_diagnosis(lake, iid, retry_safe: bool) -> dict:
    d = Diagnosis.model_validate(answer(iid, retry_safe=retry_safe))
    row = store.to_row(DiagnosisRun(iid, "claude-haiku-5-5", "diagnosis_v1", diagnosis=d), "p")
    store.append(lake, row)
    return row


@pytest.fixture
def lake(tmp_path):
    build_lake(tmp_path)
    return tmp_path


def primary(lake):
    return next(r for r in read_incidents(incidents_path(lake)) if r["role"] == "primary")


def status_of(lake, iid):
    return next(
        r["status"] for r in read_incidents(incidents_path(lake)) if r["incident_id"] == iid
    )


def click(lake, action, iid, airflow=None, allowed=ALL, mock=None):
    h = ActionHandler(
        lake, airflow or FakeAirflow(), mock or MockSlack(lake / "slack.jsonl"), allowed
    )
    return h.handle(Click(action, iid, "U1:alice", "mock://respond/m1", "mock-slack", "123"))


def test_retry_refused_when_diagnosis_says_unsafe_and_offers_confirmation(lake) -> None:
    iid = primary(lake)["incident_id"]
    add_diagnosis(lake, iid, retry_safe=False)
    af, mock = FakeAirflow(), MockSlack(lake / "slack.jsonl")
    res = click(lake, "retry", iid, af, mock=mock)
    assert res.decision == "refused" and "retry_safe=false" in res.message
    assert af.cleared == [] and status_of(lake, iid) == "open"
    [reply] = mock.records()
    assert reply["blocks"][1]["elements"][0]["action_id"] == "retry_confirmed"


def test_retry_without_any_diagnosis_counts_as_unsafe(lake) -> None:
    res = click(lake, "retry", primary(lake)["incident_id"])
    assert res.decision == "refused" and "no diagnosis available" in res.message


def test_confirmed_retry_clears_primary_and_downstream_runs(lake) -> None:
    iid = primary(lake)["incident_id"]
    add_diagnosis(lake, iid, retry_safe=False)
    af = FakeAirflow()
    res = click(lake, "retry_confirmed", iid, af)
    assert res.decision == "accepted" and "confirmed despite retry_safe=false" in res.message
    assert af.cleared == [
        ("orders_ingest", f"run_all__{DS}", True),
        ("daily_revenue_agg", f"run_all__{DS}", True),
    ]
    assert status_of(lake, iid) == "retrying"


def test_safe_retry_is_accepted_directly(lake) -> None:
    iid = primary(lake)["incident_id"]
    add_diagnosis(lake, iid, retry_safe=True)
    res = click(lake, "retry", iid)
    assert res.decision == "accepted" and "confirmed" not in res.message


def test_allowlist_blocks_dags_not_listed(lake) -> None:
    iid = primary(lake)["incident_id"]
    add_diagnosis(lake, iid, retry_safe=True)
    af = FakeAirflow()
    res = click(lake, "retry", iid, af, allowed=("orders_ingest",))
    assert res.decision == "refused" and "daily_revenue_agg" in res.message
    assert af.cleared == []


def test_dismiss_sets_status_and_survives_the_next_detection_pass(lake) -> None:
    iid = primary(lake)["incident_id"]
    assert click(lake, "dismiss", iid).decision == "accepted"
    assert status_of(lake, iid) == "dismissed"
    run_once(lake, load_detection_config(), NOW)  # detector recomputes from bronze
    assert status_of(lake, iid) == "dismissed"


def test_downstream_resolved_unknown_and_missing_are_refused(lake) -> None:
    down = next(r for r in read_incidents(incidents_path(lake)) if r["role"] == "downstream")
    assert "act on its root" in click(lake, "retry", down["incident_id"]).message
    assert "unknown action" in click(lake, "drop_table", primary(lake)["incident_id"]).message
    assert click(lake, "retry", "INC-nope").message == "no such incident"


def test_airflow_failure_is_audited_as_error(lake) -> None:
    iid = primary(lake)["incident_id"]
    add_diagnosis(lake, iid, retry_safe=True)
    res = click(lake, "retry", iid, FakeAirflow(fail=True))
    assert res.decision == "error" and "503" in res.message
    assert status_of(lake, iid) == "open"


def test_every_click_is_audited(lake) -> None:
    iid = primary(lake)["incident_id"]
    add_diagnosis(lake, iid, retry_safe=False)
    click(lake, "retry", iid)
    click(lake, "retry_confirmed", iid)
    click(lake, "dismiss", iid)
    rows = audit.read_all(lake)
    assert [(r["action"], r["decision"]) for r in rows] == [
        ("retry", "refused"),
        ("retry_confirmed", "accepted"),
        ("dismiss", "accepted"),
    ]
    assert all(r["actor"] == "U1:alice" and r["source"] == "mock-slack" for r in rows)
    assert json.loads(rows[1]["targets"])[0]["run_id"] == f"run_all__{DS}"
    assert rows[1]["retry_safe"] is False and rows[1]["diagnosis_id"].startswith("DX-")
    assert audit.human_states(lake)[iid][0] == "dismissed"


def test_retrying_resolves_when_the_rerun_is_clean_and_reopens_on_a_new_failure() -> None:
    from sentinel.detect.store import overlay

    t = datetime(2026, 10, 21, 7, tzinfo=UTC)
    later = datetime(2026, 10, 21, 8, tzinfo=UTC)
    assert overlay("resolved", t, ("retrying", t)) == "resolved"
    assert overlay("open", t, ("retrying", later)) == "retrying"  # nothing new since
    assert overlay("open", later, ("retrying", t)) == "open"  # failed again after retry
    assert overlay("open", t, ("dismissed", t)) == "dismissed"
    assert overlay("open", t, None) == "open"


def test_parse_click_reads_only_ids_from_the_button() -> None:
    payload = {
        "type": "block_actions",
        "user": {"id": "U9", "username": "bob"},
        "response_url": "https://hooks.slack.com/x",
        "actions": [
            {"action_id": "retry", "value": json.dumps({"incident_id": "INC-1", "dag": "evil"})}
        ],
    }
    c = parse_click(payload, "slack", "1")
    assert (c.action, c.incident_id, c.actor) == ("retry", "INC-1", "U9:bob")
    with pytest.raises(ValueError):
        parse_click({"type": "view_submission"}, "slack", "1")
