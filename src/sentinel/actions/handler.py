"""What a Slack button click does. Pure logic over injected dependencies (lake, Airflow
client, notifier), so every rule is unit-tested without Slack or Airflow.

Retry rules:
  - The button value only names the incident. The incident, its pipeline, the dates to
    re-run and the diagnosis are all re-read from the lake.
  - Only primary incidents that are open or retrying can be retried.
  - Targets = the incident's DAG + the DAGs of its open downstream incidents (their
    sensors failed because of it). Every target must be in config `allowed_dags`.
  - If the latest diagnosis says retry_safe = false (or there is no diagnosis), `retry`
    is refused and the user is offered `retry_confirmed`, an explicit "I fixed it".
  - Accepted: Airflow clearTaskInstances(only_failed) for each target run, status
    `retrying`. The model never gets here: only a signed human click does.
Every request, accepted or refused, writes an audit record.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sentinel.actions import audit
from sentinel.airflow_client import AirflowApiError
from sentinel.detect.store import incidents_path, read_incidents, set_status
from sentinel.diagnose import store as diagnoses
from sentinel.notify.slack import Notifier, confirm_retry_message

log = logging.getLogger("sentinel.actions")
ACTIONS = ("retry", "retry_confirmed", "dismiss")
RETRYABLE = ("open", "retrying")


@dataclass
class Click:
    action: str
    incident_id: str
    actor: str
    response_url: str | None
    source: str = "slack"
    request_ts: str | None = None


@dataclass
class Result:
    decision: str  # accepted | refused | error
    message: str
    audit_row: dict[str, Any]


def parse_click(payload: dict[str, Any], source: str, request_ts: str | None) -> Click:
    if payload.get("type") != "block_actions" or not payload.get("actions"):
        raise ValueError("not a block_actions payload")
    act = payload["actions"][0]
    value = json.loads(act.get("value") or "{}")
    user = payload.get("user") or {}
    return Click(
        action=act.get("action_id", ""),
        incident_id=str(value.get("incident_id", "")),
        actor=f"{user.get('id', '?')}:{user.get('username') or user.get('name') or '?'}",
        response_url=payload.get("response_url"),
        source=source,
        request_ts=request_ts,
    )


class ActionHandler:
    def __init__(
        self, lake: Path, airflow: Any, notifier: Notifier, allowed_dags: tuple[str, ...]
    ) -> None:
        self.lake, self.airflow, self.notifier = lake, airflow, notifier
        self.allowed_dags = allowed_dags

    def handle(self, click: Click) -> Result:
        incidents = read_incidents(incidents_path(self.lake))
        inc = next((i for i in incidents if i["incident_id"] == click.incident_id), None)
        dx = diagnoses.latest(self.lake, click.incident_id)
        base = {
            "source": click.source,
            "actor": click.actor,
            "action": click.action,
            "incident_id": click.incident_id,
            "request_ts": click.request_ts,
            "diagnosis_id": dx["diagnosis_id"] if dx else None,
            "retry_safe": dx["retry_safe"] if dx else None,
        }

        def done(
            decision: str,
            reason: str,
            *,
            status: str | None = None,
            targets=None,
            reply: dict[str, Any] | None = None,
        ) -> Result:
            row = audit.record(
                self.lake,
                decision=decision,
                reason=reason,
                new_status=status,
                targets=targets or [],
                **base,
            )
            log.info(
                "%s %s %s by %s: %s", decision, click.action, click.incident_id, click.actor, reason
            )
            if click.response_url:
                self.notifier.respond(
                    click.response_url,
                    reply
                    or {
                        "replace_original": False,
                        "text": f"{decision}: {click.action} {click.incident_id}: {reason}",
                    },
                )
            return Result(decision, reason, row)

        if click.action not in ACTIONS:
            return done("refused", f"unknown action {click.action!r}")
        if inc is None:
            return done("refused", "no such incident")
        if inc["role"] != "primary":
            root = inc["root_incident_id"]
            return done("refused", f"downstream incident; act on its root {root}")

        if click.action == "dismiss":
            set_status(
                incidents_path(self.lake), inc["incident_id"], "dismissed", datetime.now(UTC)
            )
            return done("accepted", "dismissed", status="dismissed")

        if inc["status"] not in RETRYABLE:
            return done("refused", f"incident is {inc['status']}; nothing to retry")
        targets = self._targets(inc, incidents)
        blocked = sorted({t["dag_id"] for t in targets} - set(self.allowed_dags))
        if blocked:
            return done("refused", f"DAG(s) not in the retry allowlist: {blocked}", targets=targets)
        safe = bool(dx and dx["retry_safe"])
        if click.action == "retry" and not safe:
            why = (
                json.loads(dx["result"])["root_causes"][0]["retry_safe_reason"]
                if dx and dx["result"]
                else "no diagnosis available"
            )
            return done(
                "refused",
                f"retry_safe=false: {why}",
                targets=targets,
                reply=confirm_retry_message(inc["incident_id"], base["diagnosis_id"], why),
            )
        try:
            for t in targets:
                run = self.airflow.find_run(t["dag_id"], f"{t['ds']}T00:00:00+00:00")
                if run is None:
                    raise LookupError(f"no {t['dag_id']} run for ds={t['ds']}")
                t["run_id"] = run["dag_run_id"]
                self.airflow.clear_run(t["dag_id"], t["run_id"], only_failed=True)
                t["cleared"] = True
        except (AirflowApiError, LookupError, OSError) as exc:
            return done("error", f"Airflow call failed: {exc}", targets=targets)
        set_status(incidents_path(self.lake), inc["incident_id"], "retrying", datetime.now(UTC))
        confirmed = " (confirmed despite retry_safe=false)" if not safe else ""
        cleared = ", ".join(f"{t['dag_id']}/{t['run_id']}" for t in targets)
        return done(
            "accepted",
            f"cleared failed tasks: {cleared}{confirmed}",
            status="retrying",
            targets=targets,
        )

    @staticmethod
    def _targets(inc: dict[str, Any], incidents: list[dict[str, Any]]) -> list[dict[str, Any]]:
        def active_ds(i: dict[str, Any]) -> list[str]:
            ev = json.loads(i["evidence"])
            ds = sorted({o["ds"] for o in ev["occurrences"] if o["active"]})
            return ds or ev["affected_ds"][-1:]

        targets = [{"dag_id": inc["pipeline"], "ds": d, "cleared": False} for d in active_ds(inc)]
        for d in incidents:
            if d["root_incident_id"] == inc["incident_id"] and d["status"] in RETRYABLE:
                targets += [
                    {"dag_id": d["pipeline"], "ds": x, "cleared": False} for x in active_ds(d)
                ]
        seen, out = set(), []
        for t in targets:
            if (t["dag_id"], t["ds"]) not in seen:
                seen.add((t["dag_id"], t["ds"]))
                out.append(t)
        return out
