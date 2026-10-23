"""Slack request signing (https://api.slack.com/authentication/verifying-requests-from-slack).

  basestring = "v0:" + X-Slack-Request-Timestamp + ":" + raw body
  signature  = "v0=" + hex(HMAC-SHA256(signing secret, basestring))

Checks, in order: both headers present; timestamp within `max_age_s` of now (stale or
future requests are refused, which bounds replays); constant-time signature compare;
and the same signature is accepted only once inside that window (an exact replay of a
fresh request is refused too). The replay cache is in memory: one API process.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
import time
from collections.abc import Callable


class SignatureError(Exception):
    pass


def sign(secret: str, timestamp: str, body: bytes) -> str:
    base = b"v0:" + timestamp.encode() + b":" + body
    return "v0=" + hmac.new(secret.encode(), base, hashlib.sha256).hexdigest()


class Verifier:
    def __init__(
        self, secret: str, max_age_s: int = 300, clock: Callable[[], float] = time.time
    ) -> None:
        if not secret:
            raise ValueError("a signing secret is required")
        self._secret, self.max_age_s, self._clock = secret, max_age_s, clock
        self._seen: dict[str, float] = {}
        self._lock = threading.Lock()

    def verify(self, timestamp: str | None, signature: str | None, body: bytes) -> None:
        if not timestamp or not signature:
            raise SignatureError("missing X-Slack-Request-Timestamp or X-Slack-Signature")
        try:
            ts = int(timestamp)
        except ValueError as exc:
            raise SignatureError("timestamp is not an integer") from exc
        now = self._clock()
        with self._lock:  # evict first, so the cache stays bounded whatever happens next
            for sig, at in list(self._seen.items()):
                if now - at > self.max_age_s:
                    del self._seen[sig]
        if abs(now - ts) > self.max_age_s:
            raise SignatureError(f"timestamp outside the {self.max_age_s}s window (replay?)")
        if not hmac.compare_digest(sign(self._secret, timestamp, body), signature):
            raise SignatureError("bad signature")
        with self._lock:
            if signature in self._seen:
                raise SignatureError("replayed request (signature already used)")
            self._seen[signature] = now
