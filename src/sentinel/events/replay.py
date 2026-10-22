"""Re-publish events from the local JSONL journal to Redpanda.

    python -m sentinel.events.replay [--date YYYY-MM-DD] [--topic job_logs]

Use it after a broker outage, or to rebuild bronze from scratch. Lines are sent as they
are, without re-validation: a corrupt line should end up in the lander's dead-letter
table, where it's visible, not be silently skipped here. Replaying events that were
already delivered is safe, because the lander dedupes on event_id.

After publishing, it waits (default up to 180 s) until the lander's consumer group has
committed every offset on all four topics, so the next `make query-bronze` sees the
replayed rows. Lag is read from the broker (committed offset vs high watermark), the
same numbers `rpk group describe sentinel-lander` shows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sentinel.events.models import TOPIC_FOR
from sentinel.events.sinks import _kafka_config

TOPICS = sorted(set(TOPIC_FOR.values()))
LANDER_GROUP = "sentinel-lander"


def journal_files(base: Path, date: str | None, topic: str | None) -> list[Path]:
    pattern = f"{topic or '*'}/{date or '*'}.jsonl"
    return sorted(p for p in base.glob(pattern) if not p.parent.name.startswith("_"))


def _key(line: str) -> bytes | None:
    try:
        return str(json.loads(line)["pipeline"]).encode()
    except Exception:
        return None


def replay(files: list[Path], producer: Any, timeout_s: float = 30.0) -> tuple[Counter, int]:
    sent: Counter[str] = Counter()
    errors: list[str] = []

    def on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            errors.append(str(err))

    for path in files:
        topic = path.parent.name
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            producer.produce(topic, key=_key(line), value=line.encode(), on_delivery=on_delivery)
            producer.poll(0)
            sent[topic] += 1
    remaining = producer.flush(timeout_s)
    return sent, remaining + len(errors)


def consumer_lag(consumer: Any, topics: list[str], timeout_s: float = 5.0) -> dict[str, int]:
    """Messages per topic that the consumer's group has not committed yet.

    `consumer` must be created with the group's group.id but never subscribe: reading
    committed offsets doesn't join the group, so the lander is not rebalanced."""
    from confluent_kafka import TopicPartition

    meta = consumer.list_topics(timeout=timeout_s)
    tps = [
        TopicPartition(t, p) for t in topics if t in meta.topics for p in meta.topics[t].partitions
    ]
    committed = {(tp.topic, tp.partition): tp.offset for tp in consumer.committed(tps, timeout_s)}
    lag = dict.fromkeys(topics, 0)
    for tp in tps:
        low, high = consumer.get_watermark_offsets(tp, timeout=timeout_s)
        offset = committed[(tp.topic, tp.partition)]
        # No commit yet (-1001): the lander starts from the earliest offset.
        lag[tp.topic] += max(0, high - (offset if offset >= 0 else low))
    return lag


def wait_caught_up(
    lag_fn: Callable[[], dict[str, int]],
    timeout_s: float,
    poll_s: float = 2.0,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    report_every_s: float = 15.0,
) -> tuple[bool, dict[str, int]]:
    """Poll `lag_fn` until every topic's lag is 0 or `timeout_s` passes."""
    t0 = clock()
    last_report = t0
    lag: dict[str, int] = {}
    while True:
        try:
            lag = lag_fn()
        except Exception as exc:  # broker still starting, metadata timeout, ...
            lag = {"<broker unreachable>": -1}
            err = f" ({type(exc).__name__}: {exc})"
        else:
            err = ""
            if all(v == 0 for v in lag.values()):
                return True, lag
        now = clock()
        if now - t0 >= timeout_s:
            return False, lag
        if now - last_report >= report_every_s:
            print(f"  waiting for lander... {int(now - t0)}s, lag {_fmt(lag)}{err}", flush=True)
            last_report = now
        sleep(poll_s)


def _fmt(lag: dict[str, int]) -> str:
    return " ".join(f"{t}={n}" for t, n in sorted(lag.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="emitted date of the journal files (default: all)")
    parser.add_argument("--topic")
    parser.add_argument(
        "--events-dir", default=os.environ.get("SENTINEL_EVENTS_DIR", "data/events")
    )
    parser.add_argument("--bootstrap", default=os.environ.get("SENTINEL_KAFKA_BOOTSTRAP"))
    parser.add_argument(
        "--wait-timeout",
        type=float,
        default=180.0,
        help="seconds to wait for the lander to catch up (0 = don't wait)",
    )
    parser.add_argument("--group", default=LANDER_GROUP)
    args = parser.parse_args(argv)

    files = journal_files(Path(args.events_dir), args.date, args.topic)
    if not files:
        print(f"no journal files under {args.events_dir} for date={args.date} topic={args.topic}")
        return 1
    from confluent_kafka import Producer

    producer = Producer(_kafka_config(args.bootstrap, 30000))
    sent, failed = replay(files, producer)
    for topic, n in sorted(sent.items()):
        print(f"replayed {n:>6} lines to {topic}")
    if failed:
        print(f"{failed} message(s) NOT delivered: is Redpanda up?")
        return 1
    print("published; the lander skips any events it already has (dedupe on event_id)")
    if args.wait_timeout <= 0:
        return 0
    return _wait_for_lander(args.bootstrap, args.group, args.wait_timeout)


def _wait_for_lander(bootstrap: str, group: str, timeout_s: float) -> int:
    from confluent_kafka import Consumer

    print(f"waiting up to {timeout_s:.0f}s for consumer group {group!r} to reach lag 0...")
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False}
    )
    t0 = time.monotonic()
    try:
        ok, lag = wait_caught_up(lambda: consumer_lag(consumer, TOPICS), timeout_s)
    finally:
        consumer.close()
    if ok:
        print(f"lander caught up (lag 0 on {', '.join(TOPICS)}) after {time.monotonic() - t0:.0f}s")
        return 0
    print(
        f"WARNING: lander has NOT caught up after {timeout_s:.0f}s; remaining lag: {_fmt(lag)}\n"
        "  Bronze is incomplete until it does. Check:\n"
        "    docker compose ps sentinel-lander                   # running? restarting?\n"
        "    docker compose logs --tail=50 sentinel-lander       # Delta write / broker errors\n"
        "    docker compose exec redpanda rpk group describe sentinel-lander  # lag per partition\n"
        "  If the broker was just restarted, the lander may still be reconnecting; re-run\n"
        "  `make replay-events` (replays are idempotent) or wait and run `make query-bronze`."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
