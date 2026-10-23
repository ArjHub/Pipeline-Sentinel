"""LLM clients behind one small interface, so the agent can be tested without the API.

  AnthropicLLM   the real Claude API (anthropic SDK). The key comes from the
                 environment (ANTHROPIC_API_KEY) and is never logged or stored.
  ScriptedLLM    tests: returns canned responses in order and records every request.
  RuleBasedLLM   MOCK for demos without a key: deterministic rules over real tool
                 output. Its diagnoses are labelled model="mock-rule-based" everywhere
                 and cost nothing. It is not an LLM and not a benchmark baseline.

`llm_from_env()` picks one: SENTINEL_LLM=anthropic | mock | auto (default: anthropic
if ANTHROPIC_API_KEY is set, otherwise none, and diagnosis is skipped).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from sentinel.diagnose.pricing import Usage

MOCK_MODEL = "mock-rule-based"


class LLMError(RuntimeError):
    pass


@dataclass
class LLMResponse:
    content: list[dict[str, Any]]  # plain dicts, for the agent's logic
    raw_content: list[Any]  # what gets sent back as the assistant turn (SDK blocks as-is)
    stop_reason: str
    usage: Usage
    model: str
    request_id: str | None = None

    @property
    def text(self) -> str:
        return "".join(b.get("text", "") for b in self.content if b.get("type") == "text")

    @property
    def tool_uses(self) -> list[dict[str, Any]]:
        return [b for b in self.content if b.get("type") == "tool_use"]


class LLMClient(Protocol):
    model: str
    billable: bool

    def create(self, **params: Any) -> LLMResponse: ...


class AnthropicLLM:
    billable = True

    def __init__(self, model: str, timeout_s: float = 120.0) -> None:
        import anthropic

        self._anthropic = anthropic
        # Credentials resolve from the environment; we never read or pass the key.
        self._client = anthropic.Anthropic(timeout=timeout_s, max_retries=2)
        self.model = model

    def create(self, **params: Any) -> LLMResponse:
        a = self._anthropic
        try:
            resp = self._client.messages.create(model=self.model, **params)
        except a.AuthenticationError as exc:
            raise LLMError("authentication failed (check ANTHROPIC_API_KEY)") from exc
        except a.RateLimitError as exc:
            raise LLMError("rate limited after SDK retries") from exc
        except a.BadRequestError as exc:
            raise LLMError(f"bad request: {exc.message}") from exc
        except a.APIStatusError as exc:
            raise LLMError(f"API error {exc.status_code}") from exc
        except a.APIConnectionError as exc:
            raise LLMError("could not reach the API") from exc
        return LLMResponse(
            content=[b.model_dump(mode="json", exclude_none=True) for b in resp.content],
            raw_content=list(resp.content),
            stop_reason=resp.stop_reason or "",
            usage=Usage.of(resp.usage),
            model=resp.model,
            request_id=getattr(resp, "_request_id", None),
        )


def response(
    content: list[dict[str, Any]],
    stop_reason: str = "end_turn",
    usage: dict[str, int] | None = None,
    model: str = "claude-haiku-5-5",
) -> LLMResponse:
    """Build a response from plain dicts (fakes and tests)."""
    return LLMResponse(
        content=content,
        raw_content=content,
        stop_reason=stop_reason,
        usage=Usage.of(usage or {"input_tokens": 1000, "output_tokens": 200}),
        model=model,
    )


def tool_use(name: str, args: dict[str, Any] | None = None, id_: str | None = None) -> dict:
    return {"type": "tool_use", "id": id_ or f"toolu_{name}", "name": name, "input": args or {}}


def text(s: str) -> dict:
    return {"type": "text", "text": s}


@dataclass
class ScriptedLLM:
    """Replays `script` (responses, or callables taking the request params)."""

    script: list[LLMResponse | Callable[[dict[str, Any]], LLMResponse]]
    model: str = "claude-haiku-5-5"
    billable: bool = True
    requests: list[dict[str, Any]] = field(default_factory=list)

    def create(self, **params: Any) -> LLMResponse:
        self.requests.append(json.loads(json.dumps(params, default=str)))
        if not self.script:
            raise LLMError("ScriptedLLM: script exhausted")
        step = self.script.pop(0)
        return step(params) if callable(step) else step


def _tool_results(params: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """name -> parsed output of every tool result so far."""
    names = {}
    out = {}
    for m in params["messages"]:
        if not isinstance(m["content"], list):
            continue
        for b in m["content"]:
            b = b if isinstance(b, dict) else b.model_dump()
            if b.get("type") == "tool_use":
                names[b["id"]] = b["name"]
            elif b.get("type") == "tool_result":
                out[names.get(b["tool_use_id"], "?")] = json.loads(b["content"])
    return out


class RuleBasedLLM:
    """MOCK. Turn 1 calls three tools; turn 2 maps what they returned to a category.
    Quotes are copied from the tool output, so they pass the evidence check."""

    billable = False
    model = MOCK_MODEL

    def create(self, **params: Any) -> LLMResponse:
        results = _tool_results(params)
        if not results:
            calls = ["get_error_logs", "get_schema_diff", "get_dq_failures", "get_metric_history"]
            return response(
                [tool_use(n, id_=f"toolu_mock_{i}") for i, n in enumerate(calls)],
                "tool_use",
                {"input_tokens": 0, "output_tokens": 0},
                MOCK_MODEL,
            )
        incident_id = re.search(r'"incident_id": "([^"]+)"', params["messages"][0]["content"])
        return response(
            [
                text(
                    json.dumps(
                        self._diagnose(incident_id.group(1) if incident_id else "?", results)
                    )
                )
            ],
            "end_turn",
            {"input_tokens": 0, "output_tokens": 0},
            MOCK_MODEL,
        )

    @staticmethod
    def _diagnose(incident_id: str, r: dict[str, dict[str, Any]]) -> dict[str, Any]:
        def cause(category, confidence, kind, tool, quote, why, fix, safe, safe_why):
            return {
                "incident_id": incident_id,
                "summary": f"[MOCK rule-based diagnosis] {why}",
                "root_causes": [
                    {
                        "category": category,
                        "confidence": confidence,
                        "explanation": why,
                        "evidence": [{"kind": kind, "source_tool": tool, "quote": quote}],
                        "suggested_fix": fix,
                        "retry_safe": safe,
                        "retry_safe_reason": safe_why,
                    }
                ],
            }

        for entry in r.get("get_schema_diff", {}).get("untrusted_data", []):
            removed = (entry.get("diff") or {}).get("removed")
            if removed:
                logs = r.get("get_error_logs", {}).get("untrusted_data", [])
                lines = [
                    ln.strip()
                    for x in logs
                    for ln in (
                        x.get("message", "") + "\n" + x.get("traceback_tail", "")
                    ).splitlines()
                ]
                msg = next((ln for ln in lines if "missing required columns" in ln), None)
                quote = msg or json.dumps({"removed": removed})[1:-1]
                return cause(
                    "schema_drift",
                    0.9,
                    "log_line" if msg else "schema_diff",
                    "get_error_logs" if msg else "get_schema_diff",
                    quote,
                    f"{entry['job_name']} input lost column(s) {removed}.",
                    "Restore the missing column(s) in the source file (or regenerate it), "
                    "then re-run the failed tasks.",
                    False,
                    "The input file is still missing the column; a retry fails again.",
                )
        for dq in r.get("get_dq_failures", {}).get("untrusted_data", []):
            for e in dq.get("failed_expectations", []):
                if e["expectation_type"] == "expect_column_max_to_be_between":
                    return cause(
                        "late_data",
                        0.85,
                        "dq_failure",
                        "get_dq_failures",
                        e["expectation_type"],
                        f"{dq['dataset']}.{e['column']} is older than its freshness SLA.",
                        "Refresh the upstream snapshot for this date, then re-run.",
                        False,
                        "The snapshot is still stale; a retry fails the same check.",
                    )
        hist = r.get("get_metric_history", {}).get("untrusted_data", [])
        if len(hist) >= 2 and hist[0].get("rows_in") and hist[1].get("rows_in"):
            now, before = hist[0]["rows_in"], max(h["rows_in"] or 0 for h in hist[1:])
            if before and now < 0.6 * before:
                return cause(
                    "data_quality",
                    0.6,
                    "metric",
                    "get_metric_history",
                    f'"rows_in": {now}',
                    f"Row count dropped to {now} (recent max {before}).",
                    "Check the source extract for missing rows and reload it.",
                    False,
                    "Re-running on the same short file reproduces the drop.",
                )
        return {
            "incident_id": incident_id,
            "summary": "[MOCK rule-based diagnosis] No rule matched.",
            "root_causes": [
                {
                    "category": "unknown",
                    "confidence": 0.0,
                    "explanation": "No rule matched the tool output.",
                    "evidence": [],
                    "suggested_fix": "Investigate manually.",
                    "retry_safe": False,
                    "retry_safe_reason": "Cause unknown.",
                }
            ],
        }


def llm_from_env(model: str) -> LLMClient | None:
    mode = os.environ.get("SENTINEL_LLM", "auto")
    if mode == "mock":
        return RuleBasedLLM()
    if mode == "anthropic" or (mode == "auto" and os.environ.get("ANTHROPIC_API_KEY")):
        return AnthropicLLM(model)
    return None
