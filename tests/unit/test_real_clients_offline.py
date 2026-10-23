"""The REAL client code paths (anthropic SDK, Slack Web API) against local fake HTTP
servers: request shape, response parsing, round-tripping SDK content blocks into the
next turn, error mapping. No network, no spend."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from sentinel.detect import load_config as load_detection_config
from sentinel.diagnose.agent import Agent
from sentinel.diagnose.config import load_config
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import AnthropicLLM, LLMError
from sentinel.diagnose.tools import Toolbox
from sentinel.notify.slack import SlackWebNotifier, notifier_from_env
from tests.unit.diagnose_fixtures import build_lake
from tests.unit.test_diagnose_agent import answer


class Server:
    def __init__(self, replies: list[tuple[int, dict]]) -> None:
        self.replies, self.requests = replies, []
        srv = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                n = int(self.headers.get("Content-Length") or 0)
                srv.requests.append(
                    {
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                        "body": json.loads(self.rfile.read(n) or b"{}"),
                    }
                )
                status, payload = srv.replies.pop(0)
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("request-id", "req_test123")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a) -> None:
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.http.server_port}"


def message(content, stop, usage) -> dict:
    return {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-haiku-5-5",
        "content": content,
        "stop_reason": stop,
        "stop_sequence": None,
        "usage": usage,
    }


@pytest.fixture
def fake_api(monkeypatch):
    def start(replies):
        srv = Server(replies)
        monkeypatch.setenv("ANTHROPIC_BASE_URL", srv.url)
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-not-a-real-key-000000")
        return srv

    yield start


def test_full_diagnosis_through_the_real_sdk(fake_api, tmp_path) -> None:
    incident = build_lake(tmp_path)
    iid = incident["incident_id"]
    srv = fake_api(
        [
            (
                200,
                message(
                    [
                        {"type": "thinking", "thinking": "", "signature": "sig-abc"},
                        {
                            "type": "tool_use",
                            "id": "toolu_01",
                            "name": "get_error_logs",
                            "input": {},
                        },
                    ],
                    "tool_use",
                    {
                        "input_tokens": 2500,
                        "output_tokens": 120,
                        "cache_creation_input_tokens": 2400,
                        "cache_read_input_tokens": 0,
                    },
                ),
            ),
            (
                200,
                message(
                    [{"type": "text", "text": json.dumps(answer(iid))}],
                    "end_turn",
                    {
                        "input_tokens": 300,
                        "output_tokens": 400,
                        "cache_creation_input_tokens": 1500,
                        "cache_read_input_tokens": 2400,
                    },
                ),
            ),
        ]
    )
    cfg = load_config()
    ledger = Ledger(tmp_path / "ledger.json")
    run = Agent(cfg, AnthropicLLM("claude-haiku-5-5"), ledger).diagnose(
        incident, Toolbox(tmp_path, load_detection_config(), incident)
    )
    assert run.status == "ok" and run.diagnosis.top.category == "schema_drift"
    first, second = (r["body"] for r in srv.requests)
    assert srv.requests[0]["path"] == "/v1/messages"
    assert first["model"] == "claude-haiku-5-5" and first["output_config"]["effort"] == "medium"
    assert first["cache_control"] == {"type": "ephemeral"} and len(first["tools"]) == 6
    # The thinking block (with its signature) and the tool_use go back unchanged.
    assert second["messages"][1]["content"][0] == {
        "type": "thinking",
        "thinking": "",
        "signature": "sig-abc",
    }
    assert second["messages"][2]["content"][0]["tool_use_id"] == "toolu_01"
    # Ledger: real usage of both calls, priced per call.
    calls = json.loads(ledger.path.read_text())["calls"]
    assert [c["request_id"] for c in calls] == ["req_test123", "req_test123"]
    expected = (
        2500 * 0.10
        + 120 * 0.50
        + 2400 * 0.125
        + 300 * 0.10
        + 400 * 0.50
        + 1500 * 0.125
        + 2400 * 0.01
    ) / 1e6
    assert run.cost_usd == pytest.approx(expected)
    # The key goes out as the x-api-key header, and nowhere else.
    assert srv.requests[0]["headers"]["x-api-key"].startswith("sk-ant-test")
    assert "sk-ant-test" not in ledger.path.read_text()


@pytest.mark.parametrize(
    ("status", "msg"),
    [(401, "authentication failed"), (400, "bad request"), (404, "API error 404")],
)
def test_api_errors_are_mapped_and_become_unknown(fake_api, tmp_path, status, msg) -> None:
    fake_api([(status, {"type": "error", "error": {"type": "x", "message": "nope"}})])
    with pytest.raises(LLMError, match=msg):
        AnthropicLLM("claude-haiku-5-5").create(
            max_tokens=10, messages=[{"role": "user", "content": "hi"}]
        )


def test_slack_web_post_and_response_url_guard(tmp_path) -> None:
    srv = Server(
        [(200, {"ok": True, "ts": "1729.0001"}), (200, {"ok": False, "error": "channel_not_found"})]
    )
    n = SlackWebNotifier("xoxb-test-token", "#alerts", api_base=srv.url)
    assert n.post({"text": "t", "blocks": []}) == "1729.0001"
    req = srv.requests[0]
    assert req["path"] == "/chat.postMessage" and req["body"]["channel"] == "#alerts"
    assert req["headers"]["authorization"] == "Bearer xoxb-test-token"
    with pytest.raises(RuntimeError, match="channel_not_found"):
        n.post({"text": "t", "blocks": []})
    with pytest.raises(ValueError, match=r"hooks\.slack\.com"):
        n.respond("http://evil.example.com/hook", {"text": "x"})


def test_notifier_from_env_picks_real_slack_only_when_configured(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    assert type(notifier_from_env(tmp_path)).__name__ == "MockSlack"
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-1")
    monkeypatch.setenv("SLACK_CHANNEL", "#c")
    assert isinstance(notifier_from_env(tmp_path), SlackWebNotifier)
