from __future__ import annotations

import pytest

from sentinel.api.signing import SignatureError, Verifier, sign

SECRET = "8f742231b10e8888abcd99yyyzzz85a5"  # the example secret from Slack's docs
BODY = b"payload=%7B%22type%22%3A%22block_actions%22%7D"
NOW = 1_792_736_000


def verifier(now=NOW):
    return Verifier(SECRET, 300, clock=lambda: now)


def test_valid_signature_is_accepted_once() -> None:
    v = verifier()
    ts = str(NOW - 10)
    v.verify(ts, sign(SECRET, ts, BODY), BODY)


def test_known_vector() -> None:
    # Slack's documented example request.
    body = (
        b"token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&"
        b"channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&"
        b"command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2F"
        b"commands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&"
        b"trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c"
    )
    assert sign(SECRET, "1531420618", body) == (
        "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503"
    )


@pytest.mark.parametrize(
    ("ts", "sig", "body", "msg"),
    [
        (str(NOW), "v0=" + "0" * 64, BODY, "bad signature"),
        (str(NOW), sign("wrong-secret", str(NOW), BODY), BODY, "bad signature"),
        (str(NOW), sign(SECRET, str(NOW), BODY), BODY + b"&tampered=1", "bad signature"),
        (str(NOW - 301), sign(SECRET, str(NOW - 301), BODY), BODY, "outside the 300s window"),
        (str(NOW + 301), sign(SECRET, str(NOW + 301), BODY), BODY, "outside the 300s window"),
        (None, "v0=x", BODY, "missing"),
        (str(NOW), None, BODY, "missing"),
        ("not-a-number", "v0=x", BODY, "not an integer"),
    ],
)
def test_invalid_requests_are_refused(ts, sig, body, msg) -> None:
    with pytest.raises(SignatureError, match=msg):
        verifier().verify(ts, sig, body)


def test_replayed_request_is_refused_even_inside_the_window() -> None:
    v = verifier()
    ts = str(NOW)
    sig = sign(SECRET, ts, BODY)
    v.verify(ts, sig, BODY)
    with pytest.raises(SignatureError, match="replayed"):
        v.verify(ts, sig, BODY)


def test_replay_cache_forgets_after_the_window() -> None:
    clock = {"t": NOW}
    v = Verifier(SECRET, 300, clock=lambda: clock["t"])
    ts = str(NOW)
    sig = sign(SECRET, ts, BODY)
    v.verify(ts, sig, BODY)
    clock["t"] = NOW + 301  # the old signature is now refused as stale, and evicted
    with pytest.raises(SignatureError, match="window"):
        v.verify(ts, sig, BODY)
    assert v._seen == {}


def test_empty_secret_is_a_config_error() -> None:
    with pytest.raises(ValueError):
        Verifier("")
