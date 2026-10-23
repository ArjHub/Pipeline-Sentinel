"""sentinel-detector: one pass is recorded in metrics and logs, new primaries reach the
hook exactly once, and nothing (a broken pass or a broken hook) stops the loop."""

from __future__ import annotations

import logging
import threading
from datetime import datetime

from prometheus_client import REGISTRY

from sentinel.detect import service
from tests.unit.detect_fixtures import day_events, history
from tests.unit.test_detect_cli import DS, _land

NOW = datetime.fromisoformat("2026-10-21T06:00:00+00:00")


def metric(name: str, **labels) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_pass_counts_new_incidents_and_calls_the_hook_once(tmp_path, caplog) -> None:
    _land(tmp_path, history(DS, 21) + day_events(DS, fault="missing_amount"))
    seen = []
    ok = metric("sentinel_detector_runs_total", outcome="success")
    new_primary = metric(
        "sentinel_detector_incidents_new_total", role="primary", type="schema_change"
    )
    with caplog.at_level(logging.INFO, "sentinel.detector"):
        r = service.one_pass(tmp_path, None, NOW, on_new_primary=seen.append)
    assert [i.incident_id for i in seen] == ["INC-c5962d3281"]
    assert len(r.written.new) == 2
    assert metric("sentinel_detector_runs_total", outcome="success") == ok + 1
    assert (
        metric("sentinel_detector_incidents_new_total", role="primary", type="schema_change")
        == new_primary + 1
    )
    assert metric("sentinel_detector_open_incidents", role="primary") == 1
    assert "NEW INCIDENT INC-c5962d3281" in caplog.text

    # Second pass over the same bronze: nothing new, hook not called again.
    service.one_pass(tmp_path, None, NOW, on_new_primary=seen.append)
    assert len(seen) == 1


def test_a_failing_pass_is_counted_and_does_not_raise(tmp_path, monkeypatch, caplog) -> None:
    def boom(*a, **k):
        raise RuntimeError("bronze unreadable")

    monkeypatch.setattr(service, "run_once", boom)
    before = metric("sentinel_detector_runs_total", outcome="error")
    with caplog.at_level(logging.ERROR, "sentinel.detector"):
        assert service.one_pass(tmp_path, None, NOW) is None
    assert metric("sentinel_detector_runs_total", outcome="error") == before + 1
    assert "bronze unreadable" in caplog.text


def test_a_failing_hook_does_not_fail_the_pass(tmp_path) -> None:
    _land(tmp_path, history(DS, 21) + day_events(DS, fault="stale_customers"))

    def hook(_):
        raise RuntimeError("diagnosis down")

    assert service.one_pass(tmp_path, None, NOW, on_new_primary=hook) is not None


def test_serve_loops_until_stopped(tmp_path, monkeypatch) -> None:
    stop = threading.Event()
    calls = []

    def fake_pass(*a, **k):
        calls.append(1)
        if len(calls) == 3:
            stop.set()

    monkeypatch.setattr(service, "one_pass", fake_pass)
    assert service.serve(tmp_path, None, 0.01, stop) == 3


def test_main_reads_interval_from_env_and_serves(tmp_path, monkeypatch) -> None:
    got = {}

    def fake_serve(lake, config_path, interval_s, stop, on_new_primary=None):
        got.update(lake=lake, interval=interval_s, stopped=stop.is_set())
        return 2

    monkeypatch.setattr(service, "serve", fake_serve)
    monkeypatch.setenv("SENTINEL_DETECT_INTERVAL_S", "15")
    assert service.main(["--lake", str(tmp_path)]) == 0
    assert got == {"lake": tmp_path, "interval": 15.0, "stopped": False}
