"""Make tool output safe to send to the LLM: redact secrets and PII, then truncate.

Redacted (replaced by a typed placeholder, so the model still sees *that* something was
there): email addresses, Anthropic / Slack / GitHub / AWS keys, bearer and basic
credentials in headers, JWTs, `password=`/`token=`/`secret=`-style assignments, and
long hex or base64 blobs that look like keys. Truncation keeps the head and the tail,
because errors usually sit at the end of a traceback.
"""

from __future__ import annotations

import re

_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("ANTHROPIC_KEY", re.compile(r"sk-ant-[A-Za-z0-9_\-]{8,}")),
    ("SLACK_TOKEN", re.compile(r"xox[abposr]-[A-Za-z0-9\-]{8,}")),
    ("GITHUB_TOKEN", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b")),
    ("AWS_KEY_ID", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("JWT", re.compile(r"\beyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+")),
    ("AUTH_HEADER", re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=\-]{8,}")),
    (
        "SECRET",
        re.compile(
            r"(?i)\b([a-z_]*(?:password|passwd|secret|token|api[_-]?key|fernet[_-]?key)[a-z_]*)"
            r"(['\"]?\s*[=:]\s*)(['\"]?)[^\s'\",;]{4,}\3"
        ),
    ),
    ("EMAIL", re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")),
    ("HEX_BLOB", re.compile(r"\b[a-fA-F0-9]{40,}\b")),
    ("B64_BLOB", re.compile(r"\b[A-Za-z0-9+/]{48,}={0,2}")),
]


def redact(text: str) -> str:
    for name, pattern in _RULES:
        if name == "SECRET":
            text = pattern.sub(lambda m: f"{m.group(1)}{m.group(2)}[REDACTED:SECRET]", text)
        else:
            text = pattern.sub(f"[REDACTED:{name}]", text)
    return text


def truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    marker = f"\n...[truncated {len(text) - max_chars} chars]...\n"
    keep = max(0, max_chars - len(marker))
    head = keep // 3
    return text[:head] + marker + text[len(text) - (keep - head) :]


def clean(text: str | None, max_chars: int) -> str:
    """Redact first (a secret cut in half by truncation would no longer match)."""
    return truncate(redact(text or ""), max_chars)
