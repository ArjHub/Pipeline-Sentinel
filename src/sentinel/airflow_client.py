"""Minimal Airflow 2.x stable REST API client (stdlib only, so host scripts need no deps).

Phase 4 adds a fake implementing the same methods for Slack-handler tests.
"""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

TERMINAL_STATES = {"success", "failed"}


class AirflowApiError(Exception):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"Airflow API {status}: {body[:500]}")
        self.status = status


class AirflowClient:
    def __init__(self, base_url: str, username: str, password: str, timeout_s: float = 30) -> None:
        self.base_url = base_url.rstrip("/") + "/api/v1"
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        self._headers = {"Authorization": f"Basic {token}", "Content-Type": "application/json"}
        self.timeout_s = timeout_s

    @classmethod
    def from_env(cls) -> AirflowClient:
        return cls(
            os.environ.get("AIRFLOW_API_URL", "http://localhost:8080"),
            os.environ["AIRFLOW_ADMIN_USER"],
            os.environ["AIRFLOW_ADMIN_PASSWORD"],
        )

    def _request(self, method: str, path: str, body: dict | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base_url + path, data=data, method=method, headers=self._headers
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            raise AirflowApiError(exc.code, exc.read().decode(errors="replace")) from exc
        return json.loads(raw) if raw else None

    def health(self) -> dict:
        return self._request("GET", "/health")

    def list_dag_ids(self) -> list[str]:
        return [d["dag_id"] for d in self._request("GET", "/dags?limit=100")["dags"]]

    def import_errors(self) -> list[dict]:
        return self._request("GET", "/importErrors")["import_errors"]

    def unpause(self, dag_id: str) -> None:
        self._request("PATCH", f"/dags/{dag_id}?update_mask=is_paused", {"is_paused": False})

    def find_run(self, dag_id: str, logical_date: str) -> dict | None:
        q = urllib.parse.urlencode(
            {"execution_date_gte": logical_date, "execution_date_lte": logical_date}
        )
        runs = self._request("GET", f"/dags/{dag_id}/dagRuns?{q}")["dag_runs"]
        return runs[0] if runs else None

    def get_run(self, dag_id: str, run_id: str) -> dict:
        return self._request("GET", f"/dags/{dag_id}/dagRuns/{urllib.parse.quote(run_id)}")

    def trigger(
        self, dag_id: str, run_id: str, logical_date: str, conf: dict | None = None
    ) -> dict:
        body = {"dag_run_id": run_id, "logical_date": logical_date, "conf": conf or {}}
        return self._request("POST", f"/dags/{dag_id}/dagRuns", body)

    def clear_run(self, dag_id: str, run_id: str, only_failed: bool) -> dict:
        """Re-run tasks of an existing run in place (keeps its logical date and lineage)."""
        body = {
            "dry_run": False,
            "dag_run_id": run_id,
            "only_failed": only_failed,
            "reset_dag_runs": True,
        }
        return self._request("POST", f"/dags/{dag_id}/clearTaskInstances", body)

    def task_instances(self, dag_id: str, run_id: str) -> list[dict]:
        path = f"/dags/{dag_id}/dagRuns/{urllib.parse.quote(run_id)}/taskInstances"
        return self._request("GET", path)["task_instances"]
