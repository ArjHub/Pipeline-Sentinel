"""Typed view of config/benchmark.toml (every key required, like the other configs)."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATH = ROOT / "config" / "benchmark.toml"


@dataclass(frozen=True)
class Settings:
    catalog_path: Path
    catalog_seed: int
    first_ds: str
    last_ds: str
    history_days: int
    template_ds: str
    pipeline_deploy_every: tuple[int, int]
    shared_deploy_every: tuple[int, int]
    start_after_midnight_s: float
    start_jitter_s: float
    scheduler_latency_s: tuple[float, float]
    spark_pool_slots: int
    sensor_timeout_s: int
    sensor_poke_interval_s: int
    detect_after_s: float
    work_dir: Path
    results_dir: Path
    manual_dir: Path
    repeats: dict[str, int]
    mean_usd: dict[str, float]
    max_usd: dict[str, float]
    high_multiple: float
    judge_model: str
    judge_prompt: Path
    judge_max_tokens: int
    judge_estimate_usd: float
    trial_seed: int
    trial_count: int


def load(path: Path | None = None) -> Settings:
    path = path or Path(os.environ.get("SENTINEL_BENCHMARK_CONFIG", DEFAULT_PATH))
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    c, h, s, p = raw["catalog"], raw["history"], raw["schedule"], raw["paths"]
    e, j, m = raw["estimate"], raw["judge"], raw["manual_trial"]
    work = Path(os.environ.get("SENTINEL_BENCHMARK_WORK", ROOT / p["work_dir"]))
    return Settings(
        catalog_path=ROOT / c["path"],
        catalog_seed=int(c["seed"]),
        first_ds=c["first_ds"],
        last_ds=c["last_ds"],
        history_days=int(h["days"]),
        template_ds=h["template_ds"],
        pipeline_deploy_every=tuple(h["pipeline_deploy_every_days"]),
        shared_deploy_every=tuple(h["shared_deploy_every_days"]),
        start_after_midnight_s=float(s["start_after_midnight_s"]),
        start_jitter_s=float(s["start_jitter_s"]),
        scheduler_latency_s=tuple(s["scheduler_latency_s"]),
        spark_pool_slots=int(s["spark_pool_slots"]),
        sensor_timeout_s=int(s["sensor_timeout_s"]),
        sensor_poke_interval_s=int(s["sensor_poke_interval_s"]),
        detect_after_s=float(s["detect_after_s"]),
        work_dir=work,
        results_dir=Path(os.environ.get("SENTINEL_BENCHMARK_RESULTS", ROOT / p["results_dir"])),
        manual_dir=Path(os.environ.get("SENTINEL_BENCHMARK_MANUAL", ROOT / p["manual_dir"])),
        repeats=dict(raw["run"]["repeats"]),
        mean_usd=dict(e["mean_usd"]),
        max_usd=dict(e["max_usd"]),
        high_multiple=float(e["high_multiple"]),
        judge_model=os.environ.get("SENTINEL_JUDGE_MODEL") or j["model"],
        judge_prompt=ROOT / j["prompt_file"],
        judge_max_tokens=int(j["max_tokens"]),
        judge_estimate_usd=float(j["estimate_usd"]),
        trial_seed=int(m["seed"]),
        trial_count=int(m["count"]),
    )
