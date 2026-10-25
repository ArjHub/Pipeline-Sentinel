"""LLM judge for the free-text root cause (used by Phase 5b scoring).

The category is scored exactly (string match against the label). Whether the
explanation names the *right specific cause* (the renamed column, the deploy, the
memory limit) needs judgement, so a model grades it against the ground-truth text:
match | partial | mismatch. Prompt frozen in prompts/judge_v1.md; output constrained by a
JSON schema; every call is ledger-gated (worst case reserved first) and recorded with
purpose "judge".
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sentinel.diagnose.agent import ESTIMATE_SAFETY
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import LLMClient
from sentinel.diagnose.pricing import worst_case_usd
from sentinel.diagnose.redact import clean

VERDICTS = ("match", "partial", "mismatch")
SCHEMA = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": list(VERDICTS)},
        "reason": {"type": "string"},
    },
    "required": ["verdict", "reason"],
    "additionalProperties": False,
}
MAX_INPUT_TOKENS = 6000  # prompt + both blocks, generously


@dataclass(frozen=True)
class Verdict:
    verdict: str  # match | partial | mismatch | error
    reason: str
    cost_usd: float


def user_message(root_cause: str, diagnosis: dict[str, Any] | None) -> str:
    top = (diagnosis or {}).get("root_causes", [{}])[0] if diagnosis else {}
    shown = {k: top.get(k) for k in ("category", "explanation", "suggested_fix")}
    return (
        f"<ground_truth>\n{root_cause}\n</ground_truth>\n\n"
        f'<diagnosis format="json">\n{clean(json.dumps(shown), 4000)}\n</diagnosis>'
    )


def judge(
    llm: LLMClient,
    ledger: Ledger,
    *,
    prompt: str,
    root_cause: str,
    diagnosis: dict[str, Any] | None,
    ref: str,
    max_tokens: int = 1024,
) -> Verdict:
    if llm.billable:
        worst = ESTIMATE_SAFETY * worst_case_usd(llm.model, MAX_INPUT_TOKENS, max_tokens)
        ledger.reserve(worst, f"judge of {ref} ({llm.model})")
    resp = llm.create(
        max_tokens=max_tokens,
        system=prompt,
        messages=[{"role": "user", "content": user_message(root_cause, diagnosis)}],
        output_config={"format": {"type": "json_schema", "schema": SCHEMA}},
    )
    cost = 0.0
    if llm.billable:
        cost = ledger.record(
            model=llm.model,
            usage=resp.usage,
            purpose="judge",
            ref=ref,
            request_id=resp.request_id,
        )
    try:
        out = json.loads(resp.text)
        if out.get("verdict") not in VERDICTS:
            raise ValueError(out.get("verdict"))
        return Verdict(out["verdict"], str(out.get("reason", "")), cost)
    except (ValueError, TypeError) as exc:
        return Verdict("error", f"unparseable judge output: {exc}", cost)
