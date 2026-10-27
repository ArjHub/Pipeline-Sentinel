from __future__ import annotations

import copy

import pytest
from pydantic import ValidationError

from sentinel.diagnose.schema import (
    CATEGORIES,
    GUARDRAIL_NOTE,
    OOM_GUARDRAIL_NOTE,
    OUTPUT_JSON_SCHEMA,
    Diagnosis,
    unknown,
)

GOOD = {
    "incident_id": "INC-1",
    "summary": "orders lost its amount column",
    "root_causes": [
        {
            "category": "schema_drift",
            "confidence": 0.9,
            "explanation": "ingest_orders failed on a missing column",
            "evidence": [
                {
                    "kind": "log_line",
                    "source_tool": "get_error_logs",
                    "quote": "missing required columns ['amount']",
                }
            ],
            "suggested_fix": "restore the column",
            "retry_safe": False,
            "retry_safe_reason": "file still broken",
        },
        {
            "category": "bad_deploy",
            "confidence": 0.1,
            "explanation": "unlikely",
            "evidence": [
                {"kind": "deploy", "source_tool": "get_recent_deploys", "quote": "abc1234"}
            ],
            "suggested_fix": "n/a",
            "retry_safe": False,
            "retry_safe_reason": "n/a",
        },
    ],
}


def bad(**patch):
    d = copy.deepcopy(GOOD)
    for path, value in patch.items():
        target = d
        *head, last = path.split("__")
        for k in head:
            target = target[int(k)] if k.isdigit() else target[k]
        target[int(last) if last.isdigit() else last] = value
    return d


def test_good_diagnosis_validates() -> None:
    d = Diagnosis.model_validate(GOOD)
    assert d.top.category == "schema_drift" and not d.retry_safe


@pytest.mark.parametrize(
    "payload",
    [
        bad(root_causes__0__category="cosmic_rays"),
        bad(root_causes__0__confidence=1.5),
        bad(root_causes__0__confidence=-0.1),
        bad(root_causes=[]),
        bad(root_causes__0__evidence=[]),  # a known category must cite evidence
        bad(root_causes__1__confidence=0.95),  # not ranked
        bad(root_causes__0__evidence__0__kind="vibes"),
        bad(root_causes__0__evidence__0__quote="ab"),  # too short to be a real excerpt
        {**GOOD, "action": "retry_dag"},  # extra keys: the model can't smuggle actions in
        bad(root_causes__0__retry_safe="yes please"),
    ],
)
def test_malformed_diagnoses_are_rejected(payload) -> None:
    with pytest.raises(ValidationError):
        Diagnosis.model_validate(payload)


def test_unknown_is_always_allowed_without_evidence() -> None:
    d = Diagnosis.model_validate(
        bad(
            root_causes=[
                {
                    "category": "unknown",
                    "confidence": 0.0,
                    "explanation": "not enough data",
                    "evidence": [],
                    "suggested_fix": "look manually",
                    "retry_safe": False,
                    "retry_safe_reason": "unknown cause",
                }
            ]
        )
    )
    assert d.top.category == "unknown"
    assert unknown("INC-2", "why").top.retry_safe is False


def test_evidence_must_be_verbatim_from_tool_output() -> None:
    d = Diagnosis.model_validate(GOOD)
    outputs = [
        '{"message": "orders: missing required columns   [\'amount\']"}',
        "deploy abc1234def",
    ]
    assert d.check_evidence(outputs) == []  # whitespace-normalised substring
    problems = d.check_evidence(["something else"])
    assert len(problems) == 2 and "no tool returned" in problems[0]


def test_json_schema_matches_the_model() -> None:
    cause = OUTPUT_JSON_SCHEMA["properties"]["root_causes"]["items"]
    assert cause["properties"]["category"]["enum"] == list(CATEGORIES)
    assert set(cause["required"]) == set(
        Diagnosis.model_fields["root_causes"].annotation.__args__[0].model_fields
    )
    assert OUTPUT_JSON_SCHEMA["additionalProperties"] is False


UNKNOWN_SAFE = {
    "category": "unknown",
    "confidence": 0.5,
    "explanation": "the JVM gateway went away; the tools don't show why",
    "evidence": [],
    "suggested_fix": "check executor memory settings",
    "retry_safe": True,
    "retry_safe_reason": "looks transient",
}


def test_guardrail_forces_retry_unsafe_when_the_top_cause_is_unknown() -> None:
    # Phase 5c: in the v1 benchmark all 12 unsafe "safe to retry" calls were unknown OOMs.
    d = Diagnosis.model_validate(bad(root_causes=[UNKNOWN_SAFE]))
    assert d.retry_safe is False and d.guardrail_applied
    assert d.top.retry_safe_reason == GUARDRAIL_NOTE + "looks transient"
    again = Diagnosis.model_validate(d.model_dump())  # idempotent: no double note
    assert again.top.retry_safe_reason == d.top.retry_safe_reason and not again.retry_safe


def test_guardrail_leaves_known_top_causes_and_lower_unknowns_alone() -> None:
    safe_drift = {**GOOD["root_causes"][0], "retry_safe": True, "retry_safe_reason": "fixed"}
    lower_unknown = {**UNKNOWN_SAFE, "confidence": 0.1}
    d = Diagnosis.model_validate(bad(root_causes=[safe_drift, lower_unknown]))
    assert d.retry_safe is True and not d.guardrail_applied
    assert d.root_causes[1].retry_safe is True  # only the top cause drives the retry button
    honest = Diagnosis.model_validate(bad(root_causes=[{**UNKNOWN_SAFE, "retry_safe": False}]))
    assert not honest.guardrail_applied and honest.top.retry_safe_reason == "looks transient"


def test_guardrail_note_fits_the_reason_limit() -> None:
    d = Diagnosis.model_validate(
        bad(root_causes=[{**UNKNOWN_SAFE, "retry_safe_reason": "x" * 600}])
    )
    assert len(d.top.retry_safe_reason) == 600 and d.guardrail_applied


OOM_SAFE = {
    **UNKNOWN_SAFE,
    "category": "oom",
    "explanation": "java.lang.OutOfMemoryError: Cannot reserve direct buffer memory",
    "evidence": [
        {"kind": "log_line", "source_tool": "get_error_logs", "quote": "OutOfMemoryError"}
    ],
    "retry_safe_reason": "transient memory pressure",
}


def test_guardrail_forces_retry_unsafe_when_the_top_cause_is_oom() -> None:
    # Phase 5d: after the 5c tool fixes, 2 of 9 correct oom diagnoses still said "safe".
    d = Diagnosis.model_validate(bad(root_causes=[OOM_SAFE]))
    assert d.retry_safe is False and d.guardrail_applied
    assert d.top.retry_safe_reason == OOM_GUARDRAIL_NOTE + "transient memory pressure"
    again = Diagnosis.model_validate(d.model_dump())
    assert again.top.retry_safe_reason == d.top.retry_safe_reason


def test_oom_guardrail_only_looks_at_the_top_cause_and_keeps_honest_answers() -> None:
    safe_drift = {**GOOD["root_causes"][0], "retry_safe": True, "retry_safe_reason": "fixed"}
    lower_oom = {**OOM_SAFE, "confidence": 0.1}
    d = Diagnosis.model_validate(bad(root_causes=[safe_drift, lower_oom]))
    assert d.retry_safe is True and not d.guardrail_applied
    honest = Diagnosis.model_validate(bad(root_causes=[{**OOM_SAFE, "retry_safe": False}]))
    assert (
        not honest.guardrail_applied and honest.top.retry_safe_reason == "transient memory pressure"
    )
    for cat in ("schema_drift", "late_data", "bad_deploy", "upstream_outage", "data_quality"):
        other = Diagnosis.model_validate(bad(root_causes=[{**OOM_SAFE, "category": cat}]))
        assert other.retry_safe is True and not other.guardrail_applied  # only unknown, oom
