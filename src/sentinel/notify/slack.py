"""Slack notifications: Block Kit message per diagnosed primary incident.

  Notifier         interface: post(message) -> message id; respond(response_url, body)
  MockSlack        default locally. Appends messages and responses to a JSONL file
                   (data/slack/messages.jsonl). The API serves them at /mock-slack, and
                   its buttons send *signed* Slack-format requests to the real handler
  SlackWebNotifier real Slack (chat.postMessage + response_url) when SLACK_BOT_TOKEN and
                   SLACK_CHANNEL are set. Not exercised against real Slack in this repo

Buttons carry only ids (incident and diagnosis). The handler re-reads both from the
lake, so nothing in a button value is trusted.
"""

from __future__ import annotations

import fcntl
import json
import os
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

MOCK_RESPONSE_PREFIX = "mock://respond/"
MAX_EVIDENCE = 3


class Notifier(Protocol):
    def post(self, message: dict[str, Any]) -> str: ...

    def respond(self, response_url: str, body: dict[str, Any]) -> None: ...


def _confidence(c: float | None) -> str:
    return "n/a" if c is None else f"{c:.0%}"


def _mrkdwn_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def incident_message(incident: dict[str, Any], diagnosis_row: dict[str, Any] | None) -> dict:
    """Block Kit payload for one primary incident and its latest diagnosis."""
    iid = incident["incident_id"]
    ev = json.loads(incident["evidence"])
    header = f"{iid}: {incident['type']} on {incident['pipeline']} ({incident['severity']})"
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": header[:150]}},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{_mrkdwn_escape(incident['title'][:500])}*\n"
                f"Affected ds: {', '.join(ev['affected_ds'])}",
            },
        },
    ]
    d = json.loads(diagnosis_row["result"]) if diagnosis_row and diagnosis_row["result"] else None
    if d:
        top = d["root_causes"][0]
        blocks += [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": _mrkdwn_escape(d["summary"][:1500])},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Top cause*\n`{top['category']}`"},
                    {"type": "mrkdwn", "text": f"*Confidence*\n{_confidence(top['confidence'])}"},
                    {
                        "type": "mrkdwn",
                        "text": "*Retry safe*\n"
                        + ("yes" if top["retry_safe"] else "*no*")
                        + f": {_mrkdwn_escape(top['retry_safe_reason'][:200])}",
                    },
                    {"type": "mrkdwn", "text": f"*Diagnosis*\n{diagnosis_row['status']}"},
                ],
            },
        ]
        for e in top["evidence"][:MAX_EVIDENCE]:
            quote = _mrkdwn_escape(e["quote"][:280]).replace("`", "'")
            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"_{e['kind']}_ ({e['source_tool']}): `{quote}`",
                        }
                    ],
                }
            )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Suggested fix*\n{_mrkdwn_escape(top['suggested_fix'][:1500])}",
                },
            }
        )
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"{diagnosis_row['model']} · {diagnosis_row['prompt_version']} · "
                        f"{diagnosis_row['input_tokens']}+{diagnosis_row['output_tokens']} tok · "
                        f"${diagnosis_row['cost_usd']:.4f} · {diagnosis_row['diagnosis_id']}",
                    }
                ],
            }
        )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "_No diagnosis available (see logs)._"},
            }
        )
    value = json.dumps(
        {
            "incident_id": iid,
            "diagnosis_id": diagnosis_row["diagnosis_id"] if diagnosis_row else None,
        }
    )
    retry_safe = bool(d and d["root_causes"][0]["retry_safe"])
    retry: dict[str, Any] = {
        "type": "button",
        "action_id": "retry",
        "text": {"type": "plain_text", "text": "Retry DAG"},
        "value": value,
    }
    if retry_safe:
        retry["style"] = "primary"
    blocks.append(
        {
            "type": "actions",
            "block_id": f"incident:{iid}",
            "elements": [
                retry,
                {
                    "type": "button",
                    "action_id": "dismiss",
                    "text": {"type": "plain_text", "text": "Dismiss"},
                    "style": "danger",
                    "value": value,
                },
            ],
        }
    )
    top_text = d["root_causes"][0]["category"] if d else "no diagnosis"
    return {"text": f"{header}: {top_text}", "blocks": blocks, "incident_id": iid}


def confirm_retry_message(incident_id: str, diagnosis_id: str | None, reason: str) -> dict:
    """Shown when Retry is refused because the diagnosis says it isn't safe."""
    value = json.dumps({"incident_id": incident_id, "diagnosis_id": diagnosis_id})
    return {
        "replace_original": False,
        "text": f"Retry refused for {incident_id}: {reason}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":no_entry: *Retry refused* for {incident_id}: the diagnosis "
                    f"says it is not safe ({_mrkdwn_escape(reason[:300])}). Fix the cause "
                    "first. If you already have, confirm below.",
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "action_id": "retry_confirmed",
                        "style": "danger",
                        "text": {"type": "plain_text", "text": "I fixed it, retry anyway"},
                        "value": value,
                        "confirm": {
                            "title": {"type": "plain_text", "text": "Retry anyway?"},
                            "text": {
                                "type": "plain_text",
                                "text": "The diagnosis marked this retry unsafe.",
                            },
                            "confirm": {"type": "plain_text", "text": "Retry"},
                            "deny": {"type": "plain_text", "text": "Cancel"},
                        },
                    }
                ],
            },
        ],
    }


@dataclass
class MockSlack:
    path: Path

    def _append(self, record: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            fh.write(json.dumps(record, default=str) + "\n")

    def post(self, message: dict[str, Any]) -> str:
        mid = uuid.uuid4().hex[:12]
        self._append({"kind": "message", "id": mid, "at": datetime.now(UTC).isoformat(), **message})
        return mid

    def respond(self, response_url: str, body: dict[str, Any]) -> None:
        self._append(
            {
                "kind": "response",
                "to": response_url.removeprefix(MOCK_RESPONSE_PREFIX),
                "at": datetime.now(UTC).isoformat(),
                **body,
            }
        )

    def records(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(line) for line in self.path.read_text().splitlines() if line.strip()]


class SlackWebNotifier:
    """Real Slack. Uses only chat.postMessage and the interaction's response_url."""

    def __init__(
        self,
        token: str,
        channel: str,
        timeout_s: float = 10,
        api_base: str = "https://slack.com/api",
    ) -> None:
        self._token, self.channel, self.timeout_s = token, channel, timeout_s
        self.api_base = api_base

    def _post_json(self, url: str, body: dict[str, Any], auth: bool) -> dict[str, Any]:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        if auth:
            headers["Authorization"] = f"Bearer {self._token}"
        req = urllib.request.Request(url, json.dumps(body).encode(), headers, method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
            raw = resp.read()
        return json.loads(raw) if raw[:1] == b"{" else {"ok": True}

    def post(self, message: dict[str, Any]) -> str:
        body = {"channel": self.channel, "text": message["text"], "blocks": message["blocks"]}
        out = self._post_json(f"{self.api_base}/chat.postMessage", body, auth=True)
        if not out.get("ok"):
            raise RuntimeError(f"Slack chat.postMessage failed: {out.get('error')}")
        return out["ts"]

    def respond(self, response_url: str, body: dict[str, Any]) -> None:
        if not response_url.startswith("https://hooks.slack.com/"):
            raise ValueError("refusing a response_url outside hooks.slack.com")
        self._post_json(response_url, body, auth=False)


def mock_path(lake: Path) -> Path:
    default = lake.parent / "slack" / "messages.jsonl"
    return Path(os.environ.get("SENTINEL_MOCK_SLACK_PATH", default))


def notifier_from_env(lake: Path) -> Notifier:
    token, channel = os.environ.get("SLACK_BOT_TOKEN"), os.environ.get("SLACK_CHANNEL")
    if token and channel:
        return SlackWebNotifier(token, channel)
    return MockSlack(mock_path(lake))
