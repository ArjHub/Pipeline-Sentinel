"""Kafka consume loop around BronzeWriter.

Delivery semantics: offsets are committed only after a batch is safely in Delta. If a
write fails, the exception propagates and the process exits; the container restarts
and resumes from the last committed offset. We deliberately don't skip ahead: an
uncommitted batch is re-delivered, and the event_id MERGE makes that harmless.

We use a plain consumer rather than Spark Structured Streaming: volume is hundreds of
events a day, and a plain loop is lighter (no JVM), easier to test, and easier to reason
about than a streaming checkpoint.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from sentinel.lander.bronze import BronzeWriter, RawMessage

log = logging.getLogger("sentinel.lander")

EVENTS = Counter("sentinel_lander_events_total", "Events inserted into bronze", ["topic"])
DUPLICATES = Counter(
    "sentinel_lander_duplicates_total", "Events skipped as already landed", ["topic"]
)
DEAD = Counter("sentinel_lander_dead_letter_total", "Messages sent to dead-letter", ["topic"])
BATCH_SECONDS = Histogram("sentinel_lander_batch_seconds", "Time to land one batch")
LAST_SUCCESS = Gauge("sentinel_lander_last_success_timestamp", "Unix time of last landed batch")


def consumer_config(
    bootstrap: str, group_id: str, instance_id: str | None = None
) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # we commit after the Delta write, never before
        # Static membership: after a broker restart Redpanda restores the group with the
        # lander's old member, and a plain rejoin waited ~30 s for it to expire (measured
        # with 90 s and 150 s outages). With a fixed instance id the rejoining lander
        # replaces its old self at once. Safe because there is exactly one lander.
        "group.instance.id": instance_id or f"{group_id}-0",
        # Shorter than the librdkafka defaults (30 s) so a dead broker or a stale
        # container IP is noticed sooner; the broker is on the same Docker network.
        "session.timeout.ms": 10000,
        "socket.connection.setup.timeout.ms": 5000,
        "reconnect.backoff.max.ms": 2000,
    }


def run(
    consumer: Any,
    writer: BronzeWriter,
    *,
    batch_size: int = 500,
    poll_timeout_s: float = 2.0,
    stop: threading.Event | None = None,
    idle_exit_s: float | None = None,
) -> int:
    """Consume and land until `stop` is set (or until idle for `idle_exit_s`, the drain
    mode used by tests and one-off catch-up). Returns the number of events inserted."""
    stop = stop or threading.Event()
    total = 0
    idle_since = time.monotonic()
    while not stop.is_set():
        msgs = consumer.consume(num_messages=batch_size, timeout=poll_timeout_s)
        batch: list[RawMessage] = []
        for m in msgs:
            err = m.error()
            if err is not None:
                log.warning("consumer error: %s", err)
                continue
            batch.append(RawMessage(m.topic(), m.partition(), m.offset(), m.key(), m.value()))

        if batch:
            t0 = time.monotonic()
            result = writer.land(batch)  # raises -> no commit -> redelivery after restart
            consumer.commit(asynchronous=False)
            BATCH_SECONDS.observe(time.monotonic() - t0)
            LAST_SUCCESS.set_to_current_time()
            for topic, n in result.inserted.items():
                EVENTS.labels(topic).inc(n)
                total += n
            for topic, n in result.duplicates.items():
                DUPLICATES.labels(topic).inc(n)
            for topic, n in result.dead_letters.items():
                DEAD.labels(topic).inc(n)
            log.info(
                "landed batch of %d: inserted=%s duplicates=%s dead_letter=%s",
                len(batch),
                dict(result.inserted),
                dict(result.duplicates),
                dict(result.dead_letters),
            )
            idle_since = time.monotonic()
        elif idle_exit_s is not None and time.monotonic() - idle_since >= idle_exit_s:
            break
    return total
