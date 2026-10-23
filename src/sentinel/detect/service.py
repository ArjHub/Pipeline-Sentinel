"""python -m sentinel.detect.service   (the `sentinel-detector` compose service)

Runs one detection pass (runner.run_once) every `--interval` seconds (env
SENTINEL_DETECT_INTERVAL_S, default 60) until SIGTERM. A failed pass is logged and
counted, and the loop carries on: a bad pass must not stop detection, and the next pass
recomputes everything from bronze anyway. `make detect` remains the manual equivalent.

Metrics (port METRICS_PORT, scraped by Prometheus):
  sentinel_detector_runs_total{outcome=success|error}
  sentinel_detector_incidents_new_total{role, type}
  sentinel_detector_open_incidents{role}
  sentinel_detector_run_seconds
  sentinel_detector_last_success_timestamp
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from sentinel.cli import exit_now
from sentinel.detect.config import load_config
from sentinel.detect.correlate import Incident
from sentinel.detect.runner import PassResult, run_once

log = logging.getLogger("sentinel.detector")

RUNS = Counter("sentinel_detector_runs", "Detection passes", ["outcome"])
NEW = Counter("sentinel_detector_incidents_new", "Incidents created", ["role", "type"])
OPEN = Gauge("sentinel_detector_open_incidents", "Open incidents after the last pass", ["role"])
RUN_SECONDS = Histogram("sentinel_detector_run_seconds", "Duration of one detection pass")
LAST_SUCCESS = Gauge("sentinel_detector_last_success_timestamp", "Unix time of last good pass")

OnNewPrimary = Callable[[Incident], None]


def one_pass(
    lake: Path,
    config_path: Path | None,
    now: datetime | None = None,
    on_new_primary: OnNewPrimary | None = None,
) -> PassResult | None:
    """Run and record one pass. Never raises: errors are logged and counted."""
    t0 = time.monotonic()
    try:
        # Reloaded every pass, so a threshold change applies without a restart.
        cfg = load_config(config_path)
        r = run_once(lake, cfg, now or datetime.now(UTC))
    except Exception:
        RUNS.labels("error").inc()
        log.exception("detection pass failed")
        return None
    elapsed = time.monotonic() - t0
    RUN_SECONDS.observe(elapsed)
    RUNS.labels("success").inc()
    LAST_SUCCESS.set_to_current_time()
    by_id = {i.incident_id: i for i in r.incidents}
    for iid in r.written.new:
        NEW.labels(by_id[iid].role, by_id[iid].type).inc()
    for role in ("primary", "downstream"):
        OPEN.labels(role).set(sum(i.status == "open" and i.role == role for i in r.incidents))
    w = r.written
    log.info(
        "pass ok in %.2fs: findings=%d incidents=%d new=%d updated=%d superseded=%d",
        elapsed,
        len(r.findings),
        len(r.incidents),
        len(w.new),
        len(w.updated),
        len(w.superseded),
    )
    for inc in r.new_primaries:
        log.warning(
            "NEW INCIDENT %s %s %s %s: %s",
            inc.incident_id,
            inc.severity,
            inc.type,
            inc.pipeline,
            inc.title,
        )
        if on_new_primary:
            try:
                on_new_primary(inc)
            except Exception:
                log.exception("new-incident hook failed for %s", inc.incident_id)
    return r


def serve(
    lake: Path,
    config_path: Path | None,
    interval_s: float,
    stop: threading.Event,
    on_new_primary: OnNewPrimary | None = None,
) -> int:
    passes = 0
    while not stop.is_set():
        one_pass(lake, config_path, on_new_primary=on_new_primary)
        passes += 1
        stop.wait(interval_s)
    return passes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="sentinel.detect.service")
    parser.add_argument("--lake", default=os.environ.get("SENTINEL_LAKE_ROOT", "data/lake"))
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("SENTINEL_DETECT_INTERVAL_S", "60")),
        help="seconds between passes (env SENTINEL_DETECT_INTERVAL_S)",
    )
    parser.add_argument("--metrics-port", type=int, default=int(os.environ.get("METRICS_PORT", 0)))
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    if args.metrics_port:
        start_http_server(args.metrics_port)

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    log.info("detecting every %ss over %s", args.interval, args.lake)
    passes = serve(Path(args.lake), args.config, args.interval, stop)
    log.info("stopped after %d passes", passes)
    return 0


if __name__ == "__main__":
    exit_now(main())
