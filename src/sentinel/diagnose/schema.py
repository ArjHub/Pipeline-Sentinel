"""The diagnosis the agent must return: validated JSON, nothing else.

Two layers:
  OUTPUT_JSON_SCHEMA  sent as `output_config.format`, so the API constrains the final
                      answer to this shape (types, enums, required keys).
  Diagnosis           the Pydantic model it is validated against afterwards. It adds
                      what a JSON schema can't express: confidence in [0, 1], causes
                      ranked by confidence, "unknown" rules, and that every cited piece
                      of evidence is a verbatim excerpt of what a tool actually returned
                      (check_evidence), so the model can't invent log lines or SHAs.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Category = Literal[
    "schema_drift",
    "late_data",
    "oom",
    "bad_deploy",
    "upstream_outage",
    "data_quality",
    "unknown",
]
CATEGORIES: tuple[str, ...] = Category.__args__  # type: ignore[attr-defined]
EvidenceKind = Literal[
    "log_line", "schema_diff", "deploy", "metric", "dq_failure", "upstream_status", "incident"
]
EVIDENCE_KINDS: tuple[str, ...] = EvidenceKind.__args__  # type: ignore[attr-defined]
MIN_QUOTE_CHARS = 4
GUARDRAIL_NOTE = "[guardrail: top cause is unknown, so retry_safe forced to false] Model said: "


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: EvidenceKind
    source_tool: str = Field(min_length=1)
    quote: str = Field(min_length=MIN_QUOTE_CHARS, max_length=600)


class RootCause(BaseModel):
    model_config = ConfigDict(extra="forbid")
    category: Category
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: str = Field(min_length=1, max_length=1500)
    evidence: list[Evidence] = Field(max_length=8)
    suggested_fix: str = Field(min_length=1, max_length=1500)
    retry_safe: bool
    retry_safe_reason: str = Field(min_length=1, max_length=600)

    @model_validator(mode="after")
    def _known_causes_cite_evidence(self) -> RootCause:
        if self.category != "unknown" and not self.evidence:
            raise ValueError(f"a {self.category} cause must cite at least one piece of evidence")
        return self


class Diagnosis(BaseModel):
    model_config = ConfigDict(extra="forbid")
    incident_id: str
    summary: str = Field(min_length=1, max_length=1000)
    root_causes: list[RootCause] = Field(min_length=1, max_length=5)

    @field_validator("root_causes")
    @classmethod
    def _ranked(cls, causes: list[RootCause]) -> list[RootCause]:
        conf = [c.confidence for c in causes]
        if conf != sorted(conf, reverse=True):
            raise ValueError("root_causes must be ranked by confidence, highest first")
        return causes

    @model_validator(mode="after")
    def _unknown_is_never_retry_safe(self) -> Diagnosis:
        """Guardrail (Phase 5c): if the top cause is unknown, nobody knows a retry won't
        repeat the failure, so retry_safe is forced to false whatever the model said. In
        the v1 benchmark every unsafe "safe to retry" was an `unknown` OOM diagnosis."""
        top = self.root_causes[0]
        if top.category == "unknown" and top.retry_safe:
            top.retry_safe = False
            top.retry_safe_reason = (GUARDRAIL_NOTE + top.retry_safe_reason)[:600]
        return self

    @property
    def guardrail_applied(self) -> bool:
        return self.top.retry_safe_reason.startswith(GUARDRAIL_NOTE)

    @property
    def top(self) -> RootCause:
        return self.root_causes[0]

    @property
    def retry_safe(self) -> bool:
        return self.top.retry_safe

    def check_evidence(self, tool_outputs: list[str]) -> list[str]:
        """Problems with cited evidence: every quote must appear verbatim in some tool
        output the agent actually received (whitespace-normalised)."""
        corpus = [" ".join(t.split()) for t in tool_outputs]
        problems = []
        for i, cause in enumerate(self.root_causes):
            for ev in cause.evidence:
                quote = " ".join(ev.quote.split())
                if not any(quote in t for t in corpus):
                    problems.append(
                        f"root_causes[{i}] cites a {ev.kind} quote that no tool returned: "
                        f"{ev.quote[:120]!r}"
                    )
        return problems


def unknown(incident_id: str, why: str) -> Diagnosis:
    """The honest answer when there isn't enough to go on (also the fallback)."""
    return Diagnosis(
        incident_id=incident_id,
        summary=why,
        root_causes=[
            RootCause(
                category="unknown",
                confidence=0.0,
                explanation=why,
                evidence=[],
                suggested_fix="Investigate manually; the automated diagnosis could not "
                "determine a cause.",
                retry_safe=False,
                retry_safe_reason="Cause unknown, so a retry may repeat the failure.",
            )
        ],
    )


def _obj(props: dict, required: list[str] | None = None) -> dict:
    return {
        "type": "object",
        "properties": props,
        "required": required or list(props),
        "additionalProperties": False,
    }


_S = {"type": "string"}
OUTPUT_JSON_SCHEMA = _obj(
    {
        "incident_id": _S,
        "summary": _S,
        "root_causes": {
            "type": "array",
            "items": _obj(
                {
                    "category": {"type": "string", "enum": list(CATEGORIES)},
                    "confidence": {"type": "number"},
                    "explanation": _S,
                    "evidence": {
                        "type": "array",
                        "items": _obj(
                            {
                                "kind": {"type": "string", "enum": list(EVIDENCE_KINDS)},
                                "source_tool": _S,
                                "quote": _S,
                            }
                        ),
                    },
                    "suggested_fix": _S,
                    "retry_safe": {"type": "boolean"},
                    "retry_safe_reason": _S,
                }
            ),
        },
    }
)
