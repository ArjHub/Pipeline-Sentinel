"""A small stateful fake of Airflow's stable REST API (the subset sentinel uses), served
over real HTTP on localhost. It enforces basic auth and keeps DAG runs and task states,
so clearing a run really resets its failed tasks."""

from __future__ import annotations

import base64
import json
import re
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeAirflow:
    def __init__(self, user: str = "admin", password: str = "s3cret") -> None:
        self.auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self.dags: dict[str, dict] = {}
        self.runs: dict[tuple[str, str], dict] = {}
        self.tasks: dict[tuple[str, str], dict[str, str]] = {}
        self.import_errors: list[dict] = []
        self.calls: list[tuple[str, str, dict | None]] = []
        self.lock = threading.Lock()

    def add_dag(self, dag_id: str, paused: bool = True) -> None:
        self.dags[dag_id] = {"dag_id": dag_id, "is_paused": paused}

    def add_run(self, dag_id: str, run_id: str, logical_date: str, tasks: dict[str, str]) -> None:
        state = (
            "failed"
            if "failed" in tasks.values() or "upstream_failed" in tasks.values()
            else "success"
        )
        self.runs[(dag_id, run_id)] = {
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "logical_date": logical_date,
            "execution_date": logical_date,
            "state": state,
        }
        self.tasks[(dag_id, run_id)] = dict(tasks)

    # --- request handling -------------------------------------------------------------

    def handle(self, method: str, path: str, body: dict | None) -> tuple[int, dict]:
        url = urllib.parse.urlparse(path)
        p, q = urllib.parse.unquote(url.path), urllib.parse.parse_qs(url.query)
        self.calls.append((method, p, body))
        if p == "/api/v1/health":
            return 200, {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
        if p == "/api/v1/dags" and method == "GET":
            return 200, {"dags": list(self.dags.values()), "total_entries": len(self.dags)}
        if p == "/api/v1/importErrors":
            return 200, {"import_errors": self.import_errors}
        if m := re.fullmatch(r"/api/v1/dags/([^/]+)", p):
            if m[1] not in self.dags:
                return 404, {"title": "DAG not found"}
            if method == "PATCH":
                self.dags[m[1]]["is_paused"] = body["is_paused"]
            return 200, self.dags[m[1]]
        if m := re.fullmatch(r"/api/v1/dags/([^/]+)/dagRuns", p):
            dag = m[1]
            if dag not in self.dags:
                return 404, {"title": "DAG not found"}
            if method == "POST":
                key = (dag, body["dag_run_id"])
                if key in self.runs:
                    return 409, {"title": "DAGRun already exists"}
                self.add_run(dag, body["dag_run_id"], body["logical_date"], {})
                self.runs[key]["state"] = "queued"
                return 200, self.runs[key]
            lo = q.get("execution_date_gte", [None])[0]
            hi = q.get("execution_date_lte", [None])[0]
            runs = [
                r
                for (d, _), r in self.runs.items()
                if d == dag
                and (lo is None or r["logical_date"] >= lo)
                and (hi is None or r["logical_date"] <= hi)
            ]
            return 200, {"dag_runs": runs, "total_entries": len(runs)}
        if m := re.fullmatch(r"/api/v1/dags/([^/]+)/dagRuns/([^/]+)/taskInstances", p):
            key = (m[1], m[2])
            if key not in self.runs:
                return 404, {"title": "DAGRun not found"}
            tis = [{"task_id": t, "state": s} for t, s in self.tasks[key].items()]
            return 200, {"task_instances": tis, "total_entries": len(tis)}
        if m := re.fullmatch(r"/api/v1/dags/([^/]+)/dagRuns/([^/]+)", p):
            key = (m[1], m[2])
            return (200, self.runs[key]) if key in self.runs else (404, {"title": "not found"})
        if (m := re.fullmatch(r"/api/v1/dags/([^/]+)/clearTaskInstances", p)) and method == "POST":
            key = (m[1], body["dag_run_id"])
            if key not in self.runs:
                return 404, {"title": "DAGRun not found"}
            cleared = []
            for t, s in self.tasks[key].items():
                if not body["only_failed"] or s in ("failed", "upstream_failed"):
                    self.tasks[key][t] = "queued" if not body["dry_run"] else s
                    cleared.append({"task_id": t, "dag_run_id": key[1]})
            if body.get("reset_dag_runs") and not body["dry_run"]:
                self.runs[key]["state"] = "queued"
            return 200, {"task_instances": cleared}
        return 404, {"title": f"no route {method} {p}"}

    def serve(self) -> tuple[ThreadingHTTPServer, str]:
        fake = self

        class H(BaseHTTPRequestHandler):
            def _do(self) -> None:
                if self.headers.get("Authorization") != fake.auth:
                    status, payload = 401, {"title": "Unauthorized"}
                else:
                    n = int(self.headers.get("Content-Length") or 0)
                    body = json.loads(self.rfile.read(n)) if n else None
                    with fake.lock:
                        status, payload = fake.handle(self.command, self.path, body)
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_PATCH = _do

            def log_message(self, *a) -> None:
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return server, f"http://127.0.0.1:{server.server_port}"
