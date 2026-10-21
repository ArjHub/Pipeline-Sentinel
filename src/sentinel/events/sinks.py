"""Where events go.

Default in the stack (`SENTINEL_EVENT_SINK=kafka`): every event is first appended to the
local JSONL journal, then published to Redpanda. The journal is the durable record:
if the broker is down, nothing is lost, and `make replay-events` re-publishes later.
Replay doesn't need to know which events made it, because the bronze lander dedupes
on event_id.

Publishing is best-effort and time-bounded: a dead broker costs a job at most
SENTINEL_KAFKA_TIMEOUT_MS at flush time and never raises into pipeline code.
"""

from __future__ import annotations

import logging
import os
import socket
import sys
from pathlib import Path
from typing import Any, Protocol

from sentinel.events.models import Event

# Sink diagnostics use their own non-propagating logger. If they reached the root logger,
# the job's EventLogHandler would turn "can't reach Kafka" into yet another event headed
# for Kafka.
log = logging.getLogger("sentinel.events.sink")
log.propagate = False
if not log.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    log.addHandler(_h)
    log.setLevel(logging.INFO)


class EventSink(Protocol):
    def emit(self, event: Event) -> None: ...

    def flush(self, timeout_s: float | None = None) -> int:
        """Block until buffered events are sent; return how many were NOT delivered."""
        ...


class NullSink:
    def emit(self, event: Event) -> None:
        return None

    def flush(self, timeout_s: float | None = None) -> int:
        return 0


class MemorySink:
    """Collects events in a list; used by tests."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)

    def flush(self, timeout_s: float | None = None) -> int:
        return 0


class JsonlFileSink:
    """Appends one JSON line per event to <base>/<topic>/<YYYY-MM-DD>.jsonl (emitted date).

    Each event is written with a single write() on a file opened in append mode, so
    lines from concurrent task processes don't interleave in practice (lines are small).
    """

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def emit(self, event: Event) -> None:
        day = event.emitted_at.strftime("%Y-%m-%d")
        path = self.base_dir / event.topic / f"{day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        line = event.model_dump_json() + "\n"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)

    def flush(self, timeout_s: float | None = None) -> int:
        return 0


def _kafka_config(bootstrap: str, timeout_ms: int) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": f"sentinel-{socket.gethostname()}",
        # acks=all + idempotence: the broker drops duplicate retries of the same message.
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "message.timeout.ms": timeout_ms,
        "socket.connection.setup.timeout.ms": min(timeout_ms, 3000),
        "reconnect.backoff.max.ms": 1000,
        "logger": log,
    }


class KafkaSink:
    """Publishes events to their topic. Key = pipeline, so one pipeline's events keep
    their order within a partition."""

    def __init__(
        self,
        bootstrap: str,
        timeout_ms: int = 5000,
        producer: Any = None,
    ) -> None:
        if producer is None:
            from confluent_kafka import Producer

            producer = Producer(_kafka_config(bootstrap, timeout_ms))
        self._producer = producer
        self.bootstrap = bootstrap
        self.timeout_s = timeout_ms / 1000
        self.failed = 0
        self.delivered = 0
        self._last_error: str | None = None

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            self._last_error = str(err)
        else:
            self.delivered += 1

    def emit(self, event: Event) -> None:
        try:
            self._producer.produce(
                event.topic,
                key=event.pipeline.encode(),
                value=event.model_dump_json().encode(),
                headers={
                    "schema_version": event.schema_version,
                    "event_type": event.event_type.value,
                },
                on_delivery=self._on_delivery,
            )
            self._producer.poll(0)  # serve delivery callbacks without blocking
        except Exception as exc:  # BufferError (queue full), KafkaException, ...
            self.failed += 1
            self._last_error = f"{type(exc).__name__}: {exc}"

    def flush(self, timeout_s: float | None = None) -> int:
        remaining = self._producer.flush(self.timeout_s if timeout_s is None else timeout_s)
        undelivered = remaining + self.failed
        if undelivered:
            log.warning(
                "%d event(s) not delivered to Kafka at %s (%s). They are safe in the JSONL "
                "journal; run `make replay-events` once the broker is back.",
                undelivered,
                self.bootstrap,
                self._last_error or "timed out",
            )
        self.failed = 0
        return undelivered


class TeeSink:
    """Journal first (durable, local), then the publisher (best effort)."""

    def __init__(self, journal: EventSink, publisher: EventSink) -> None:
        self.journal = journal
        self.publisher = publisher

    def emit(self, event: Event) -> None:
        try:
            self.journal.emit(event)
        except Exception:
            log.exception("failed to write event %s to the journal", event.event_id)
        self.publisher.emit(event)

    def flush(self, timeout_s: float | None = None) -> int:
        return self.publisher.flush(timeout_s)


def sink_from_env() -> EventSink:
    kind = os.environ.get("SENTINEL_EVENT_SINK", "file")
    if kind == "none":
        return NullSink()
    journal = JsonlFileSink(os.environ.get("SENTINEL_EVENTS_DIR", "data/events"))
    if kind == "file":
        return journal
    if kind == "kafka":
        bootstrap = os.environ.get("SENTINEL_KAFKA_BOOTSTRAP", "localhost:19092")
        timeout_ms = int(os.environ.get("SENTINEL_KAFKA_TIMEOUT_MS", "5000"))
        try:
            return TeeSink(journal, KafkaSink(bootstrap, timeout_ms))
        except Exception:
            # e.g. confluent-kafka not installed: degrade to the journal, never fail the job.
            log.exception("Kafka sink unavailable; writing events to the JSONL journal only")
            return journal
    raise ValueError(f"Unknown SENTINEL_EVENT_SINK={kind!r} (expected 'kafka', 'file' or 'none')")
