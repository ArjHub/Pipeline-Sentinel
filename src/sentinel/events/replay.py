"""Re-publish events from the local JSONL journal to Redpanda.

    python -m sentinel.events.replay [--date YYYY-MM-DD] [--topic job_logs]

Use it after a broker outage, or to rebuild bronze from scratch. Lines are sent as they
are, without re-validation: a corrupt line should end up in the lander's dead-letter
table, where it's visible, not be silently skipped here. Replaying events that were
already delivered is safe, because the lander dedupes on event_id.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from sentinel.events.sinks import _kafka_config


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="emitted date of the journal files (default: all)")
    parser.add_argument("--topic")
    parser.add_argument(
        "--events-dir", default=os.environ.get("SENTINEL_EVENTS_DIR", "data/events")
    )
    parser.add_argument("--bootstrap", default=os.environ.get("SENTINEL_KAFKA_BOOTSTRAP"))
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
    print("done; the lander will skip any events it already has (dedupe on event_id)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
