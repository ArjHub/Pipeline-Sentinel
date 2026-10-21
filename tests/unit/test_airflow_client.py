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
