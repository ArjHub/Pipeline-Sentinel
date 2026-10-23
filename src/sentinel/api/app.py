"""Slack interactivity API (the `sentinel-api` service, http://localhost:8000).

  POST /slack/interactions   Slack's interactivity request URL. Signature + timestamp
                             verified, then ActionHandler (allowlist, retry-safe gate,
                             Airflow, audit). Unverified requests get 401 and are audited.
  GET  /mock-slack           the local mock Slack: every recorded message with its
                             buttons
  POST /mock-slack/click     a mock button click: builds the same block_actions payload
                             Slack would send, SIGNS it with the signing secret, and runs
                             it through the same verification as /slack/interactions
  GET  /healthz, /metrics

Signing secret: SLACK_SIGNING_SECRET. If it is empty (local, mock only) a random one is
generated per process, so only the mock can produce valid signatures.
"""

from __future__ import annotations

import html
import json
import logging
import os
import secrets
import time
import urllib.parse
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from prometheus_client import Counter, make_asgi_app

from sentinel.actions import audit
from sentinel.actions.handler import ActionHandler, parse_click
from sentinel.airflow_client import AirflowClient
from sentinel.api.signing import SignatureError, Verifier, sign
from sentinel.diagnose.config import load_config
from sentinel.notify.slack import MOCK_RESPONSE_PREFIX, MockSlack, Notifier, notifier_from_env

log = logging.getLogger("sentinel.api")
INTERACTIONS = Counter("sentinel_api_interactions", "Slack interactions", ["action", "decision"])


def create_app(
    lake: Path,
    airflow: Any,
    notifier: Notifier,
    signing_secret: str,
    allowed_dags: tuple[str, ...],
    max_age_s: int = 300,
    mock: MockSlack | None = None,
    clock=time.time,
) -> FastAPI:
    app = FastAPI(title="PipelineSentinel actions", docs_url=None, redoc_url=None)
    verifier = Verifier(signing_secret, max_age_s, clock)
    handler = ActionHandler(lake, airflow, notifier, allowed_dags)
    app.mount("/metrics", make_asgi_app())

    def interact(body: bytes, ts: str | None, sig: str | None, source: str) -> JSONResponse:
        try:
            verifier.verify(ts, sig, body)
        except SignatureError as exc:
            audit.record(
                lake,
                source=source,
                actor="unverified",
                action="?",
                incident_id="?",
                decision="refused",
                reason=f"signature: {exc}",
                request_ts=ts,
            )
            INTERACTIONS.labels("?", "unauthorized").inc()
            return JSONResponse({"ok": False, "error": str(exc)}, status_code=401)
        try:
            form = urllib.parse.parse_qs(body.decode())
            click = parse_click(json.loads(form["payload"][0]), source, ts)
        except (KeyError, ValueError) as exc:
            INTERACTIONS.labels("?", "bad_request").inc()
            return JSONResponse({"ok": False, "error": f"bad payload: {exc}"}, status_code=400)
        result = handler.handle(click)
        INTERACTIONS.labels(click.action, result.decision).inc()
        return JSONResponse({"ok": True, "decision": result.decision, "message": result.message})

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/slack/interactions")
    async def slack_interactions(request: Request) -> JSONResponse:
        body = await request.body()
        return interact(
            body,
            request.headers.get("X-Slack-Request-Timestamp"),
            request.headers.get("X-Slack-Signature"),
            "slack",
        )

    if mock is not None:

        @app.post("/mock-slack/click")
        async def mock_click(request: Request):
            # Parsed with the stdlib (FastAPI's Form() would need python-multipart).
            form = {
                k: v[0] for k, v in urllib.parse.parse_qs((await request.body()).decode()).items()
            }
            try:
                message_id, action_id, value = (
                    form[k] for k in ("message_id", "action_id", "value")
                )
            except KeyError as exc:
                return JSONResponse({"ok": False, "error": f"missing {exc}"}, status_code=400)
            user = form.get("user", "U_LOCAL")
            payload = {
                "type": "block_actions",
                "user": {"id": user, "username": "local-operator"},
                "actions": [{"action_id": action_id, "value": value}],
                "response_url": MOCK_RESPONSE_PREFIX + message_id,
            }
            body = urllib.parse.urlencode({"payload": json.dumps(payload)}).encode()
            ts = str(int(clock()))
            resp = interact(body, ts, sign(signing_secret, ts, body), "mock-slack")
            # A browser goes back to the page; a script (Accept: json) gets the result.
            if "application/json" in request.headers.get("accept", "") or resp.status_code >= 400:
                return resp
            return RedirectResponse("/mock-slack", status_code=303)

        @app.get("/mock-slack/messages")
        def mock_messages() -> list[dict[str, Any]]:
            return mock.records()

        @app.get("/mock-slack", response_class=HTMLResponse)
        def mock_page() -> str:
            return render_mock(mock.records())

    return app


def _text(obj: dict[str, Any] | None) -> str:
    return html.escape((obj or {}).get("text", "")).replace("\n", "<br>")


def render_mock(records: list[dict[str, Any]]) -> str:
    out = []
    replies: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        if r["kind"] == "response":
            replies.setdefault(r["to"], []).append(r)
    for r in reversed([r for r in records if r["kind"] == "message"]):
        parts = [
            f"<div class='msg'><div class='meta'>#{html.escape(r['id'])} · "
            f"{html.escape(r['at'])}</div>"
        ]
        for b in (
            r["blocks"]
            + [b for x in replies.get(r["id"], []) for b in x.get("blocks", [])]
            + [
                {"type": "reply", "text": x["text"]}
                for x in replies.get(r["id"], [])
                if not x.get("blocks")
            ]
        ):
            t = b["type"]
            if t == "header":
                parts.append(f"<h3>{_text(b['text'])}</h3>")
            elif t == "section":
                parts.append(f"<p>{_text(b.get('text'))}</p>")
                if b.get("fields"):
                    parts.append(
                        "<div class='fields'>"
                        + "".join(f"<div>{_text(f)}</div>" for f in b["fields"])
                        + "</div>"
                    )
            elif t == "context":
                parts.append(
                    "<div class='ctx'>" + " ".join(_text(e) for e in b["elements"]) + "</div>"
                )
            elif t == "actions":
                for e in b["elements"]:
                    parts.append(
                        "<form method='post' action='/mock-slack/click' class='btn'>"
                        f"<input type='hidden' name='message_id' value='{html.escape(r['id'])}'>"
                        "<input type='hidden' name='action_id' "
                        f"value='{html.escape(e['action_id'])}'>"
                        f"<input type='hidden' name='value' value='{html.escape(e['value'])}'>"
                        f"<button class='{html.escape(e.get('style', ''))}'>"
                        f"{_text(e['text'])}</button></form>"
                    )
            elif t == "reply":
                parts.append(f"<div class='reply'>↳ {html.escape(b['text'])}</div>")
        parts.append("</div>")
        out.append("".join(parts))
    body = "".join(out) or "<p>No messages yet.</p>"
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Mock Slack</title><style>
:root{{--bg:#fff;--fg:#1d1c1d;--muted:#616061;--card:#f8f8f8;--line:#ddd}}
@media (prefers-color-scheme:dark){{:root{{--bg:#1a1d21;--fg:#d1d2d3;--muted:#9a9b9c;
--card:#222529;--line:#35373b}}}}
body{{font:14px/1.45 system-ui,sans-serif;background:var(--bg);color:var(--fg);
margin:0 auto;max-width:760px;padding:16px}}
.msg{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:12px 14px;
margin:12px 0}}.meta,.ctx{{color:var(--muted);font-size:12px}}h3{{margin:4px 0}}
.fields{{display:grid;grid-template-columns:1fr 1fr;gap:6px}}.btn{{display:inline}}
button{{margin:8px 6px 0 0;padding:6px 12px;border-radius:4px;border:1px solid var(--line);
background:var(--bg);color:var(--fg);cursor:pointer}}button.primary{{background:#007a5a;color:#fff}}
button.danger{{background:#e01e5a;color:#fff}}.reply{{margin-top:8px;color:var(--muted)}}
</style></head><body><h2>Mock Slack (MOCKED: not real Slack)</h2>
<p class="meta">Buttons send signed Slack-format requests to the real handler.</p>{body}
</body></html>"""


def app_from_env() -> FastAPI:
    """uvicorn --factory sentinel.api.app:app_from_env"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    lake = Path(os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))
    cfg = load_config()
    secret = os.environ.get("SLACK_SIGNING_SECRET") or secrets.token_hex(32)
    notifier = notifier_from_env(lake)
    return create_app(
        lake,
        AirflowClient.from_env(),
        notifier,
        secret,
        cfg.actions.allowed_dags,
        cfg.actions.signature_max_age_seconds,
        mock=notifier if isinstance(notifier, MockSlack) else None,
    )
