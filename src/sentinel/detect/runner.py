"""One detection pass, shared by `make detect` (sentinel.detect) and the
`sentinel-detector` service (sentinel.detect.service)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from sentinel.detect import correlate, load_bronze, run_detectors
from sentinel.detect.config import DetectionConfig
from sentinel.detect.correlate import Incident
from sentinel.detect.findings import Finding
from sentinel.detect.observations import Observations
from sentinel.detect.store import UpsertResult, incidents_path, upsert


@dataclass
class PassResult:
    since: date
    obs: Observations
    findings: list[Finding]
    incidents: list[Incident]
    written: UpsertResult | None  # None on a dry run

    @property
    def new_primaries(self) -> list[Incident]:
        new = set(self.written.new) if self.written else set()
        return [i for i in self.incidents if i.incident_id in new and i.role == "primary"]


def run_once(lake: Path, cfg: DetectionConfig, now: datetime, dry_run: bool = False) -> PassResult:
    since = (now - timedelta(days=cfg.history_days)).date()
    obs = load_bronze(lake / "bronze", since)
    findings = run_detectors(obs, cfg, now)
    incidents = correlate(findings, cfg)
    written = None
    if not dry_run:
        window_start = datetime.combine(since, time.min, UTC)
        written = upsert(incidents_path(lake), incidents, now, window_start)
    return PassResult(since, obs, findings, incidents, written)
