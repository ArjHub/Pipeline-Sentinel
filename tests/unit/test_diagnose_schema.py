from __future__ import annotations

import copy

import pytest
from pydantic import ValidationError

from sentinel.diagnose.schema import CATEGORIES, OUTPUT_JSON_SCHEMA, Diagnosis, unknown

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
