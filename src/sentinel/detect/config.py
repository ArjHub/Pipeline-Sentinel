"""Typed view of config/detection.toml.

The TOML file is the single source of thresholds: every key is required and there are
no fallback values here, so what you read in the file is what runs.
"""

from __future__ import annotations

import copy
import os
import tomllib
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

DEFAULT_PATH = Path(__file__).resolve().parents[3] / "config" / "detection.toml"
SEVERITIES = ("low", "medium", "high")
BASELINES = ("same_weekday", "recent")


@dataclass(frozen=True)
class VolumeMetric:
    job_name: str
    field: str  # rows_in | rows_out

    @property
    def label(self) -> str:
        return f"{self.job_name}.{self.field}"


@dataclass(frozen=True)
class FreshnessRule:
    column: str
    max_staleness: timedelta


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    upstream: tuple[str, ...]
    final_task: str
    volume_metrics: tuple[VolumeMetric, ...]
    freshness: FreshnessRule | None
    arrival_sla: timedelta | None
    volume_baseline: str  # same_weekday | recent


@dataclass(frozen=True)
class VolumeConfig:
    z_threshold: float
    min_relative_change: float
    mad_floor_fraction: float
    min_baseline_points: int
    recent_days: int
    cold_start_min_change: float
    high_severity_change: float


@dataclass(frozen=True)
class DetectionConfig:
    history_days: int
    correlation_tolerance: timedelta
    failed_job_severity: str
    schema_breaking_severity: str
    schema_added_severity: str
    late_high_severity_multiple: float
    arrival_lookback_days: int
    volume: VolumeConfig
    pipelines: dict[str, PipelineConfig]

    def upstream_closure(self, pipeline: str) -> set[str]:
        """Every pipeline `pipeline` reads from, directly or transitively."""
        seen: set[str] = set()
        stack = list(self.pipelines[pipeline].upstream) if pipeline in self.pipelines else []
        while stack:
            p = stack.pop()
            if p not in seen:
                seen.add(p)
                stack.extend(self.pipelines[p].upstream if p in self.pipelines else ())
        return seen

    def depth(self, pipeline: str) -> int:
        """0 for a source pipeline, 1 + max(upstream depth) otherwise."""
        ups = self.pipelines[pipeline].upstream if pipeline in self.pipelines else ()
        return 1 + max(map(self.depth, ups)) if ups else 0


def _merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _severity(value: str) -> str:
    if value not in SEVERITIES:
        raise ValueError(f"severity must be one of {SEVERITIES}, got {value!r}")
    return value


def _pipeline(name: str, raw: dict[str, Any]) -> PipelineConfig:
    metrics = []
    for spec in raw["volume_metrics"]:
        job, _, field = spec.rpartition(".")
        if field not in ("rows_in", "rows_out") or not job:
            raise ValueError(f"{name}: bad volume metric {spec!r} (want <job>.rows_in|rows_out)")
        metrics.append(VolumeMetric(job, field))
    if raw["volume_baseline"] not in BASELINES:
        raise ValueError(f"{name}: volume_baseline must be one of {BASELINES}")
    fresh = raw.get("freshness")
    sla = raw.get("arrival_sla_hours")
    return PipelineConfig(
        name=name,
        upstream=tuple(raw["upstream"]),
        final_task=raw["final_task"],
        volume_metrics=tuple(metrics),
        freshness=(
            FreshnessRule(fresh["column"], timedelta(hours=fresh["max_staleness_hours"]))
            if fresh
            else None
        ),
        arrival_sla=timedelta(hours=sla) if sla is not None else None,
        volume_baseline=raw["volume_baseline"],
    )


def from_dict(raw: dict[str, Any]) -> DetectionConfig:
    v = raw["volume"]
    pipelines = {name: _pipeline(name, p) for name, p in raw["pipelines"].items()}
    for p in pipelines.values():
        unknown = set(p.upstream) - set(pipelines)
        if unknown:
            raise ValueError(f"{p.name}: unknown upstream pipeline(s) {sorted(unknown)}")
    return DetectionConfig(
        history_days=int(raw["detector"]["history_days"]),
        correlation_tolerance=timedelta(seconds=raw["detector"]["correlation_tolerance_seconds"]),
        failed_job_severity=_severity(raw["failed_job"]["severity"]),
        schema_breaking_severity=_severity(raw["schema_change"]["removed_or_retyped_severity"]),
        schema_added_severity=_severity(raw["schema_change"]["added_severity"]),
        late_high_severity_multiple=float(raw["late_data"]["high_severity_multiple"]),
        arrival_lookback_days=int(raw["late_data"]["arrival_lookback_days"]),
        volume=VolumeConfig(
            z_threshold=float(v["z_threshold"]),
            min_relative_change=float(v["min_relative_change"]),
            mad_floor_fraction=float(v["mad_floor_fraction"]),
            min_baseline_points=int(v["min_baseline_points"]),
            recent_days=int(v["recent_days"]),
            cold_start_min_change=float(v["cold_start_min_change"]),
            high_severity_change=float(v["high_severity_change"]),
        ),
        pipelines=pipelines,
    )


def config_path() -> Path:
    return Path(os.environ.get("SENTINEL_DETECTION_CONFIG", DEFAULT_PATH))


def load_raw(path: Path | None = None) -> dict[str, Any]:
    with (path or config_path()).open("rb") as fh:
        return tomllib.load(fh)


def load_config(
    path: Path | None = None, overrides: dict[str, Any] | None = None
) -> DetectionConfig:
    """Load the TOML file; `overrides` (a nested dict) is deep-merged on top (tests)."""
    raw = load_raw(path)
    return from_dict(_merge(raw, overrides) if overrides else raw)
