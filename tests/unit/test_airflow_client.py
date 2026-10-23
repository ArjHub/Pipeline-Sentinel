"""Phase 1 gap: the REST client had no tests. A local HTTP stub records requests."""

from __future__ import annotations

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from sentinel.airflow_client import AirflowApiError, AirflowClient


class Stub(BaseHTTPRequestHandler):
    requests: list[dict] = []
    responses: dict[tuple[str, str], tuple[int, dict]] = {}

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else None
        Stub.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "body": body,
                "auth": self.headers.get("Authorization"),
            }
        )
        status, payload = Stub.responses.get((self.command, self.path.split("?")[0]), (200, {}))
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PATCH = _handle

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def client():
    Stub.requests, Stub.responses = [], {}
    server = HTTPServer(("127.0.0.1", 0), Stub)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield AirflowClient(f"http://127.0.0.1:{server.server_port}", "admin", "s3cret", timeout_s=5)
    server.shutdown()


def test_basic_auth_header(client) -> None:
    client.health()
    expected = "Basic " + base64.b64encode(b"admin:s3cret").decode()
    assert Stub.requests[0]["auth"] == expected
    assert Stub.requests[0]["path"] == "/api/v1/health"


def test_clear_run_body_retries_failed_tasks_in_place(client) -> None:
    client.clear_run("orders_ingest", "run_all__2026-10-20", only_failed=True)
    req = Stub.requests[0]
    assert (req["method"], req["path"]) == ("POST", "/api/v1/dags/orders_ingest/clearTaskInstances")
    assert req["body"] == {
        "dry_run": False,
        "dag_run_id": "run_all__2026-10-20",
        "only_failed": True,
        "reset_dag_runs": True,
    }


def test_trigger_and_unpause(client) -> None:
    client.trigger("payments_etl", "r1", "2026-10-20T00:00:00+00:00")
    client.unpause("payments_etl")
    trigger, patch = Stub.requests
    assert trigger["body"] == {
        "dag_run_id": "r1",
        "logical_date": "2026-10-20T00:00:00+00:00",
        "conf": {},
    }
    assert patch["method"] == "PATCH" and patch["body"] == {"is_paused": False}
    assert patch["path"].endswith("?update_mask=is_paused")


def test_run_ids_with_special_characters_are_quoted(client) -> None:
    client.get_run("orders_ingest", "manual__2026-10-20T00:00:00+00:00")
    assert "manual__2026-10-20T00%3A00%3A00%2B00%3A00" in Stub.requests[0]["path"]


def test_http_errors_raise_with_status(client) -> None:
    Stub.responses[("GET", "/api/v1/dags/nope/dagRuns/x")] = (404, {"title": "DAG not found"})
    with pytest.raises(AirflowApiError) as exc:
        client.get_run("nope", "x")
    assert exc.value.status == 404
    assert "DAG not found" in str(exc.value)


def test_find_run_filters_by_exact_logical_date(client) -> None:
    Stub.responses[("GET", "/api/v1/dags/d/dagRuns")] = (200, {"dag_runs": [{"dag_run_id": "a"}]})
    assert client.find_run("d", "2026-10-20T00:00:00+00:00") == {"dag_run_id": "a"}
    path = Stub.requests[0]["path"]
    assert "execution_date_gte=2026-10-20T00%3A00%3A00%2B00%3A00" in path
    assert "execution_date_lte=2026-10-20T00%3A00%3A00%2B00%3A00" in path


# --- Against a stateful fake Airflow (tests/fake_airflow.py) ---------------------------

from tests.fake_airflow import FakeAirflow  # noqa: E402


@pytest.fixture
def fake():
    fa = FakeAirflow()
    for dag in ("orders_ingest", "daily_revenue_agg"):
        fa.add_dag(dag)
    fa.add_run(
        "orders_ingest",
        "run_all__2026-10-20",
        "2026-10-20T00:00:00+00:00",
        {
            "generate_orders": "success",
            "ingest_orders": "failed",
            "validate_orders": "upstream_failed",
        },
    )
    server, url = fa.serve()
    yield fa, AirflowClient(url, "admin", "s3cret", timeout_s=5)
    server.shutdown()


def test_listing_dags_import_errors_and_health(fake) -> None:
    fa, c = fake
    assert c.list_dag_ids() == ["orders_ingest", "daily_revenue_agg"]
    assert c.import_errors() == []
    fa.import_errors.append({"filename": "x.py", "stack_trace": "SyntaxError"})
    assert c.import_errors()[0]["filename"] == "x.py"
    assert c.health()["scheduler"]["status"] == "healthy"


def test_find_get_and_task_instances(fake) -> None:
    _, c = fake
    run = c.find_run("orders_ingest", "2026-10-20T00:00:00+00:00")
    assert run["dag_run_id"] == "run_all__2026-10-20" and run["state"] == "failed"
    assert c.find_run("orders_ingest", "2026-10-21T00:00:00+00:00") is None
    assert c.get_run("orders_ingest", "run_all__2026-10-20")["state"] == "failed"
    states = {
        t["task_id"]: t["state"] for t in c.task_instances("orders_ingest", "run_all__2026-10-20")
    }
    assert states["ingest_orders"] == "failed"


def test_clear_only_failed_resets_just_the_failed_tasks(fake) -> None:
    fa, c = fake
    out = c.clear_run("orders_ingest", "run_all__2026-10-20", only_failed=True)
    assert sorted(t["task_id"] for t in out["task_instances"]) == [
        "ingest_orders",
        "validate_orders",
    ]
    assert fa.tasks[("orders_ingest", "run_all__2026-10-20")] == {
        "generate_orders": "success",
        "ingest_orders": "queued",
        "validate_orders": "queued",
    }
    assert fa.runs[("orders_ingest", "run_all__2026-10-20")]["state"] == "queued"


def test_trigger_then_conflict_and_unpause(fake) -> None:
    fa, c = fake
    c.unpause("daily_revenue_agg")
    assert fa.dags["daily_revenue_agg"]["is_paused"] is False
    c.trigger("daily_revenue_agg", "r1", "2026-10-20T00:00:00+00:00")
    with pytest.raises(AirflowApiError) as err:
        c.trigger("daily_revenue_agg", "r1", "2026-10-20T00:00:00+00:00")
    assert err.value.status == 409


def test_missing_objects_are_404(fake) -> None:
    _, c = fake
    for call in (
        lambda: c.get_run("orders_ingest", "nope"),
        lambda: c.clear_run("nope_dag", "r", only_failed=True),
        lambda: c.unpause("nope_dag"),
    ):
        with pytest.raises(AirflowApiError) as err:
            call()
        assert err.value.status == 404


def test_wrong_credentials_are_401(fake) -> None:
    _, c = fake
    bad = AirflowClient(c.base_url.removesuffix("/api/v1"), "admin", "wrong", timeout_s=5)
    with pytest.raises(AirflowApiError) as err:
        bad.list_dag_ids()
    assert err.value.status == 401


def test_from_env(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_API_URL", "http://airflow:8080/")
    monkeypatch.setenv("AIRFLOW_ADMIN_USER", "u")
    monkeypatch.setenv("AIRFLOW_ADMIN_PASSWORD", "p")
    c = AirflowClient.from_env()
    assert c.base_url == "http://airflow:8080/api/v1"


def test_slack_retry_reaches_airflow_over_http(fake, tmp_path) -> None:
    """ActionHandler -> real AirflowClient -> HTTP -> fake Airflow."""
    from sentinel.actions.handler import ActionHandler, Click
    from sentinel.notify.slack import MockSlack
    from tests.unit.diagnose_fixtures import build_lake
    from tests.unit.test_actions import ALL, add_diagnosis

    fa, c = fake
    fa.add_run(
        "daily_revenue_agg",
        "run_all__2026-10-20",
        "2026-10-20T00:00:00+00:00",
        {"wait_for_orders_ingest": "failed", "aggregate_revenue": "upstream_failed"},
    )
    inc = build_lake(tmp_path)
    add_diagnosis(tmp_path, inc["incident_id"], retry_safe=True)
    h = ActionHandler(tmp_path, c, MockSlack(tmp_path / "s.jsonl"), ALL)
    res = h.handle(Click("retry", inc["incident_id"], "U1:alice", None))
    assert res.decision == "accepted"
    cleared = [p for m, p, _ in fa.calls if p.endswith("clearTaskInstances")]
    assert cleared == [
        "/api/v1/dags/orders_ingest/clearTaskInstances",
        "/api/v1/dags/daily_revenue_agg/clearTaskInstances",
    ]
    assert fa.tasks[("orders_ingest", "run_all__2026-10-20")]["ingest_orders"] == "queued"
