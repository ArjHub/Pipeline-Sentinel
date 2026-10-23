"""The FastAPI app over HTTP (TestClient): signature enforcement, the mock Slack flow."""

from __future__ import annotations

import json
import time
import urllib.parse

import pytest
from fastapi.testclient import TestClient

from sentinel.actions import audit
from sentinel.api.app import create_app
from sentinel.api.signing import sign
from sentinel.notify.slack import MockSlack, incident_message
from tests.unit.diagnose_fixtures import build_lake
from tests.unit.test_actions import ALL, FakeAirflow, add_diagnosis

SECRET = "test-signing-secret"


@pytest.fixture
def ctx(tmp_path):
    incident = build_lake(tmp_path)
    row = add_diagnosis(tmp_path, incident["incident_id"], retry_safe=False)
    mock = MockSlack(tmp_path / "slack" / "messages.jsonl")
    mid = mock.post(incident_message(incident, row))
    af = FakeAirflow()
    app = create_app(tmp_path, af, mock, SECRET, ALL, mock=mock)
    return TestClient(app), incident, mock, mid, af, tmp_path


def body_for(action: str, iid: str) -> bytes:
    payload = {
        "type": "block_actions",
        "user": {"id": "U1", "username": "alice"},
        "actions": [{"action_id": action, "value": json.dumps({"incident_id": iid})}],
    }
    return urllib.parse.urlencode({"payload": json.dumps(payload)}).encode()


def post(client, body, ts=None, secret=SECRET, sig=None):
    ts = ts or str(int(time.time()))
    return client.post(
        "/slack/interactions",
        content=body,
        headers={
            "X-Slack-Request-Timestamp": ts,
            "X-Slack-Signature": sig or sign(secret, ts, body),
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )


def test_signed_click_is_handled(ctx) -> None:
    client, inc, *_ = ctx
    r = post(client, body_for("dismiss", inc["incident_id"]))
    assert r.status_code == 200 and r.json()["decision"] == "accepted"


def test_unsigned_wrong_and_replayed_requests_get_401_and_are_audited(ctx) -> None:
    client, inc, *_, lake = ctx
    body = body_for("retry_confirmed", inc["incident_id"])
    assert post(client, body, secret="attacker").status_code == 401
    assert client.post("/slack/interactions", content=body).status_code == 401
    old = str(int(time.time()) - 3600)
    assert post(client, body, ts=old).status_code == 401
    ts = str(int(time.time()))
    assert post(client, body, ts=ts).status_code == 200
    assert post(client, body, ts=ts).status_code == 401  # exact replay
    refused = [r for r in audit.read_all(lake) if r["actor"] == "unverified"]
    assert len(refused) == 4 and all(r["reason"].startswith("signature:") for r in refused)


def test_mock_slack_click_goes_through_the_signed_handler(ctx) -> None:
    client, inc, _, mid, af, _ = ctx
    val = json.dumps({"incident_id": inc["incident_id"]})
    r = client.post(
        "/mock-slack/click",
        data={"message_id": mid, "action_id": "retry", "value": val},
        headers={"Accept": "application/json"},
    )
    assert r.json()["decision"] == "refused"  # diagnosis said retry_safe=false
    r = client.post(
        "/mock-slack/click",
        data={"message_id": mid, "action_id": "retry_confirmed", "value": val},
        headers={"Accept": "application/json"},
    )
    assert r.json()["decision"] == "accepted" and len(af.cleared) == 2
    page = client.get("/mock-slack").text
    assert "MOCKED" in page and inc["incident_id"] in page and "Retry refused" in page
    assert client.get("/mock-slack/messages").json()[0]["id"] == mid


def test_bad_payload_is_400(ctx) -> None:
    client, *_ = ctx
    assert post(client, b"payload=not-json").status_code == 400


def test_health_and_metrics(ctx) -> None:
    client, inc, *_ = ctx
    post(client, body_for("dismiss", inc["incident_id"]))
    assert client.get("/healthz").json() == {"status": "ok"}
    assert (
        'sentinel_api_interactions_total{action="dismiss",decision="accepted"}'
        in client.get("/metrics/").text
    )
