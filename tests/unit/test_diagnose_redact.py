from __future__ import annotations

import pytest

from sentinel.diagnose.redact import clean, redact, truncate


@pytest.mark.parametrize(
    ("text", "secret", "label"),
    [
        ("contact customer_17@example.com now", "customer_17@example.com", "EMAIL"),
        ("key=sk-ant-api03-AbCdEf0123456789xyz", "sk-ant-api03-AbCdEf0123456789xyz", "ANTHROPIC"),
        ("Authorization: Bearer abcdef1234567890", "abcdef1234567890", "AUTH_HEADER"),
        ("slack xoxb-1234-5678-abcdefgh", "xoxb-1234-5678-abcdefgh", "SLACK_TOKEN"),
        ("aws AKIAABCDEFGHIJKLMNOP", "AKIAABCDEFGHIJKLMNOP", "AWS_KEY_ID"),
        ("jwt eyJhbGciOi.eyJzdWIiOi.c2lnbmF0dXJl", "eyJhbGciOi.eyJzdWIiOi.c2lnbmF0dXJl", "JWT"),
        ("POSTGRES_PASSWORD=hunter2hunter2", "hunter2hunter2", "SECRET"),
        ('{"api_key": "abcd1234efgh"}', "abcd1234efgh", "SECRET"),
        ("ghp_" + "a" * 36, "ghp_" + "a" * 36, "GITHUB_TOKEN"),
        ("blob " + "f" * 64, "f" * 64, "HEX_BLOB"),
    ],
)
def test_secrets_and_pii_are_redacted(text, secret, label) -> None:
    out = redact(text)
    assert secret not in out and "[REDACTED:" in out


def test_ordinary_log_text_is_untouched() -> None:
    line = "orders: missing required columns ['amount']; observed columns ['order_id']"
    assert redact(line) == line


def test_truncate_keeps_head_and_tail() -> None:
    text = "HEAD" + "x" * 5000 + "TAIL: SchemaMismatchError"
    out = truncate(text, 300)
    assert len(out) <= 300 and out.startswith("HEAD") and out.endswith("SchemaMismatchError")
    assert "[truncated" in out


def test_clean_redacts_before_truncating() -> None:
    # A secret straddling the cut would survive truncate-then-redact.
    text = "a" * 100 + " password=topsecretvalue " + "b" * 100
    assert "topsecret" not in clean(text, 60)
