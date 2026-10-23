"""Phase 3 detection: bronze events -> findings -> correlated incidents.

    findings = run_detectors(observations, config, now)
    incidents = correlate(findings, config)

Every step is a pure function of its inputs (bronze + config + `now`), so detection can
be re-run or replayed over the same bronze and gives the same incidents.
"""

from __future__ import annotations

from datetime import datetime

from sentinel.detect import failed_jobs, late_data, schema_drift, volume
from sentinel.detect.config import DetectionConfig, load_config
from sentinel.detect.correlate import Incident, correlate
from sentinel.detect.findings import Finding, IncidentType
from sentinel.detect.observations import Observations, from_events, load_bronze


def run_detectors(obs: Observations, cfg: DetectionConfig, now: datetime) -> list[Finding]:
    return [
        *failed_jobs.detect(obs, cfg),
        *schema_drift.detect(obs, cfg),
        *late_data.detect(obs, cfg, now),
        *volume.detect(obs, cfg),
    ]


def detect(obs: Observations, cfg: DetectionConfig, now: datetime) -> list[Incident]:
    return correlate(run_detectors(obs, cfg, now), cfg)


__all__ = [
    "DetectionConfig",
    "Finding",
    "Incident",
    "IncidentType",
    "Observations",
    "correlate",
    "detect",
    "from_events",
    "load_bronze",
    "load_config",
    "run_detectors",
]
