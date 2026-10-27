"""LLM judges for Phase 5b scoring.

The category is scored exactly (string match against the label). Two things need
judgement, so a model grades them against the ground-truth text:

- root cause (`judge`): does the explanation name the *right specific cause* (the renamed
  column, the deploy, the memory limit)? match | partial | mismatch. prompts/judge_v1.md.
- fix quality (`judge_fix`): the suggested fix on a 0-3 rubric. prompts/judge_fix_v1.md;
  the human sample (benchmark.harness.human) uses the same rubric text.

Output constrained by a JSON schema; every call is ledger-gated (worst case reserved
first) and recorded with purpose "judge".
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
FIX_SCORES = (0, 1, 2, 3)
FIX_SCHEMA = {
    "type": "object",
    "properties": {
        "score": {"type": "integer", "enum": list(FIX_SCORES)},
        "reason": {"type": "string"},
    },
    "required": ["score", "reason"],
    "additionalProperties": False,
}
MAX_INPUT_TOKENS = 6000  # prompt + both blocks, generously


@dataclass(frozen=True)
class Verdict:
    verdict: str  # match | partial | mismatch | error
    reason: str
    cost_usd: float


ROOT_CAUSE_FIELDS = ("category", "explanation", "suggested_fix")
FIX_FIELDS = ("category", "explanation", "suggested_fix", "retry_safe", "retry_safe_reason")


def user_message(
    root_cause: str, diagnosis: dict[str, Any] | None, fields: tuple[str, ...] = ROOT_CAUSE_FIELDS
) -> str:
    top = (diagnosis or {}).get("root_causes", [{}])[0] if diagnosis else {}
    shown = {k: top.get(k) for k in fields}
    return (
        f"<ground_truth>\n{root_cause}\n</ground_truth>\n\n"
        f'<diagnosis format="json">\n{clean(json.dumps(shown), 4000)}\n</diagnosis>'
    )


def _call(
    llm: LLMClient,
    ledger: Ledger,
    *,
    prompt: str,
    content: str,
    schema: dict,
    ref: str,
    max_tokens: int,
) -> tuple[str, float]:
    if llm.billable:
        worst = ESTIMATE_SAFETY * worst_case_usd(llm.model, MAX_INPUT_TOKENS, max_tokens)
        ledger.reserve(worst, f"judge of {ref} ({llm.model})")
    resp = llm.create(
        max_tokens=max_tokens,
        system=prompt,
        messages=[{"role": "user", "content": content}],
        output_config={"format": {"type": "json_schema", "schema": schema}},
    )
    cost = 0.0
    if llm.billable:
        cost = ledger.record(
            model=llm.model, usage=resp.usage, purpose="judge", ref=ref, request_id=resp.request_id
        )
    return resp.text, cost


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
    text, cost = _call(
        llm,
        ledger,
        prompt=prompt,
        content=user_message(root_cause, diagnosis),
        schema=SCHEMA,
        ref=ref,
        max_tokens=max_tokens,
    )
    try:
        out = json.loads(text)
        if out.get("verdict") not in VERDICTS:
            raise ValueError(out.get("verdict"))
        return Verdict(out["verdict"], str(out.get("reason", "")), cost)
    except (ValueError, TypeError) as exc:
        return Verdict("error", f"unparseable judge output: {exc}", cost)


@dataclass(frozen=True)
class FixScore:
    score: int | None  # 0-3; None = the judge's output was unparseable
    reason: str
    cost_usd: float


def judge_fix(
    llm: LLMClient,
    ledger: Ledger,
    *,
    prompt: str,
    root_cause: str,
    diagnosis: dict[str, Any] | None,
    ref: str,
    max_tokens: int = 1024,
) -> FixScore:
    text, cost = _call(
        llm,
        ledger,
        prompt=prompt,
        content=user_message(root_cause, diagnosis, FIX_FIELDS),
        schema=FIX_SCHEMA,
        ref=ref,
        max_tokens=max_tokens,
    )
    try:
        out = json.loads(text)
        if out.get("score") not in FIX_SCORES:
            raise ValueError(out.get("score"))
        return FixScore(int(out["score"]), str(out.get("reason", "")), cost)
    except (ValueError, TypeError) as exc:
        return FixScore(None, f"unparseable judge output: {exc}", cost)
