"""python -m sentinel.lander [--drain]

Long-running by default (the `sentinel-lander` compose service). `--drain` consumes
until the topics have been idle for a few seconds, then exits.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading

from confluent_kafka import Consumer
from prometheus_client import start_http_server

from sentinel.cli import exit_now
from sentinel.lander.bronze import TOPICS, BronzeWriter
from sentinel.lander.service import consumer_config, run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.environ.get("SENTINEL_KAFKA_BOOTSTRAP"))
    parser.add_argument("--group", default="sentinel-lander")
    parser.add_argument(
        "--instance-id",
        help="static group member id (default <group>-0). A second process in the same "
        "group needs its own, or the broker fences one of them",
    )
    parser.add_argument("--drain", action="store_true", help="exit after topics go idle")
    parser.add_argument("--idle-seconds", type=float, default=8.0)
    parser.add_argument("--metrics-port", type=int, default=int(os.environ.get("METRICS_PORT", 0)))
    args = parser.parse_args(argv)
    if not args.bootstrap:
        parser.error("--bootstrap or SENTINEL_KAFKA_BOOTSTRAP is required")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if args.metrics_port:
        start_http_server(args.metrics_port)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    consumer = Consumer(consumer_config(args.bootstrap, args.group, args.instance_id))
    consumer.subscribe(TOPICS)
    logging.getLogger("sentinel.lander").info("consuming %s from %s", TOPICS, args.bootstrap)
    try:
        total = run(
            consumer,
            BronzeWriter(),
            stop=stop,
            idle_exit_s=args.idle_seconds if args.drain else None,
        )
    finally:
        consumer.close()  # commits nothing extra; leaves the group cleanly
    logging.getLogger("sentinel.lander").info("stopped; inserted %d events", total)
    return 0


if __name__ == "__main__":
    exit_now(main())
