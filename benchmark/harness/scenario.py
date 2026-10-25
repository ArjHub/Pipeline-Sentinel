"""Build scenario lakes: 6 weeks of clean history + one nightly run of the fault date.

One scenario per fault, each in its own lake (data/benchmark/scenarios/<fault_id>/lake),
so every fault starts from the same clean state and nothing carries over between faults.

  1. build tree   a fresh copy of pipelines/ and src/ in an isolated root
                  (/opt/sentinel inside the `benchmark` container, so paths in logs and
                  tracebacks are the stack's), with the source shim installed and the
                  fault's code regression applied, if it has one
  2. pre-state    the real customers_dim job for ds - 1, so the dimension already exists
                  (the fault night's MERGE then behaves like any other night)
  3. history      clean nights for ds - 42 .. ds - 1, cloned from the template night
                  (history.py), plus deploy events
  4. the night    the emulator runs all four DAGs for ds with the fault's recipe
  5. land         every event goes through the real BronzeWriter into bronze
  6. detect       one detection pass (the real detectors and correlation), 30 min after
                  the night's last task, writes the incidents table
  7. check        no label, fault id or template text in bronze or incidents (leaks.py);
                  the incident appeared where the recipe says (or none, for a clean run)

The manifest records what was built (no labels). Re-running `build` skips scenarios
whose manifest exists; `--rebuild` rebuilds them.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from benchmark.harness import history, leaks
from benchmark.harness.airflow_emu import Emulator, Night
from benchmark.harness.catalog import Fault
from benchmark.harness.dags import JOBS_PYTHON
from benchmark.harness.settings import Settings
from benchmark.harness.templates import Patch, Recipe
from sentinel.detect.config import load_config as load_detection_config
from sentinel.detect.runner import run_once
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.events.models import Event
from sentinel.lander.bronze import BronzeWriter, RawMessage

REPO = Path(__file__).resolve().parents[2]
MARKER = ".benchmark-build-root"
GENERATOR_CALL = "df = fn(parse_ds(args.ds), base_seed())"
SHIM_CALL = "df = source.extract(args.dataset, parse_ds(args.ds), fn, base_seed())"
# What a scenario's content depends on (the diagnosis agent is not part of it).
HASHED = [
    "pipelines/**/*.py",
    "src/sentinel/detect/**/*.py",
    "src/sentinel/events/**/*.py",
    "src/sentinel/instrumentation/**/*.py",
    "src/sentinel/lander/**/*.py",
    "benchmark/harness/*.py",
    "config/detection.toml",
    "config/benchmark.toml",
]


def build_hash(extra: str = "") -> str:
    h = hashlib.sha256(extra.encode())
    for pattern in HASHED:
        for p in sorted(REPO.glob(pattern)):
            if "__pycache__" not in p.parts:
                h.update(p.relative_to(REPO).as_posix().encode() + p.read_bytes())
    return h.hexdigest()[:16]


def build_root() -> Path:
    return Path(os.environ.get("SENTINEL_BENCHMARK_BUILD_ROOT", "/opt/sentinel"))


# --- 1. build tree --------------------------------------------------------------------


def prepare_tree(root: Path, patches: tuple[Patch, ...] = ()) -> None:
    root = root.resolve()
    if root == REPO or (root / ".git").exists() or root == Path("/"):
        raise RuntimeError(f"refusing to use {root} as a build root (it is a repository)")
    if root.exists() and any(root.iterdir()) and not (root / MARKER).exists():
        raise RuntimeError(f"refusing to wipe {root}: not empty and not a build root")
    root.mkdir(parents=True, exist_ok=True)
    for child in root.iterdir():
        if child.name != MARKER:
            _rm(child)
    (root / MARKER).write_text("PipelineSentinel benchmark build root (wiped per scenario)\n")
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    shutil.copytree(REPO / "pipelines", root / "pipelines", ignore=ignore)
    shutil.copytree(REPO / "src", root / "src", ignore=ignore)
    shutil.copy(Path(__file__).with_name("source_shim.py"), root / "pipelines/generators/source.py")
    _patch(root, Patch("pipelines/generators/__main__.py", GENERATOR_CALL, SHIM_CALL))
    _patch(
        root,
        Patch(
            "pipelines/generators/__main__.py",
            "from pipelines.generators.base import write_atomic",
            "from pipelines.generators import source\nfrom pipelines.generators.base import "
            "write_atomic",
        ),
    )
    for p in patches:
        _patch(root, p)


def _rm(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        for p in path.rglob("*"):
            if not p.is_symlink():
                p.chmod(0o755 if p.is_dir() else 0o644)
        shutil.rmtree(path)
    else:
        path.unlink()


def _patch(root: Path, p: Patch) -> None:
    path = root / p.file
    text = path.read_text()
    if text.count(p.old) != 1:
        raise RuntimeError(f"patch for {p.file} expects its target text exactly once")
    path.write_text(text.replace(p.old, p.new))


def base_env(root: Path) -> dict[str, str]:
    """The task environment of the stack (docker-compose x-airflow-common), with the
    lake at root/data/lake and events going to a file sink the emulator collects."""
    drop = ("ANTHROPIC", "SLACK", "AIRFLOW_CTX", "SENTINEL_", "JAVA_TOOL_OPTIONS")
    env = {k: v for k, v in os.environ.items() if not k.startswith(drop)}
    env.update(
        PYTHONPATH=f"{root}:{root}/src",
        SENTINEL_LAKE_ROOT=f"{root}/data/lake",
        SENTINEL_SEED=str(history.SEED),
        SPARK_DRIVER_MEMORY="1g",
        TQDM_DISABLE="1",
        HADOOP_USER_NAME="airflow",
        SENTINEL_EVENT_SINK="none",
    )
    return env


# --- 2. pre-state -----------------------------------------------------------------------


def prestate(root: Path, ds: str) -> None:
    """customers_dim as of ds - 1 (the real job; its events are not kept)."""
    prev = (date.fromisoformat(ds) - timedelta(days=1)).isoformat()
    env = base_env(root)
    for module, args in (
        ("pipelines.generators", ["--dataset", "customers", "--ds", prev]),
        ("pipelines.jobs.customers_dim", ["--ds", prev]),
    ):
        r = subprocess.run(
            [JOBS_PYTHON, "-m", module, *args], cwd=root, env=env, capture_output=True, text=True
        )
        if r.returncode:
            raise RuntimeError(f"pre-state {module} failed:\n{r.stdout[-2000:]}")


def run_night(root: Path, scratch: Path, night: Night, settings: Settings, rng) -> list[Event]:
    prepare_scratch(scratch)
    prestate(root, night.ds)
    return Emulator(
        root=root,
        scratch=scratch,
        night=night,
        rng=rng,
        latency_s=settings.scheduler_latency_s,
        spark_slots=settings.spark_pool_slots,
        sensor_timeout_s=settings.sensor_timeout_s,
        poke_interval_s=settings.sensor_poke_interval_s,
        base_env=base_env(root),
    ).run()


def prepare_scratch(scratch: Path) -> None:
    if scratch.exists():
        _rm(scratch)
    scratch.mkdir(parents=True)


# --- the template night ---------------------------------------------------------------


def template_dir(settings: Settings) -> Path:
    return settings.work_dir / "template"


def build_template(settings: Settings, root: Path | None = None) -> history.Template:
    """One real clean night for template_ds (all tasks must succeed)."""
    root = root or build_root()
    rng = random.Random(f"template|{settings.template_ds}")
    prepare_tree(root)
    start = history.night_start(settings.template_ds, settings.start_after_midnight_s, 0, rng)
    versions = {p: "0000000" for p in history.ALL}
    events = run_night(
        root,
        settings.work_dir / "tmp" / "template",
        Night(settings.template_ds, versions, Recipe(), start),
        settings,
        rng,
    )
    failed = [e.task_id for e in events if getattr(e.payload, "state", None) == "failed"]
    if failed:
        raise RuntimeError(f"template night had failed tasks: {failed}")
    out = template_dir(settings)
    out.mkdir(parents=True, exist_ok=True)
    (out / "events.jsonl").write_text("".join(e.model_dump_json() + "\n" for e in events))
    meta = {"ds": settings.template_ds, "start": start.isoformat(), "build_hash": build_hash()}
    (out / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    return history.Template(settings.template_ds, start, events)


def load_template(settings: Settings) -> history.Template:
    d = template_dir(settings)
    meta = json.loads((d / "meta.json").read_text())
    events = read_events_file(d / "events.jsonl")
    return history.Template(meta["ds"], datetime.fromisoformat(meta["start"]), events)


def read_events_file(path: Path) -> list[Event]:
    from sentinel.events.models import parse_event

    return [parse_event(line) for line in path.read_text().splitlines() if line]


# --- one scenario ---------------------------------------------------------------------


def scenario_dir(settings: Settings, fault_id: str) -> Path:
    return settings.work_dir / "scenarios" / fault_id


def lake_path(settings: Settings, fault_id: str) -> Path:
    return scenario_dir(settings, fault_id) / "lake"


def manifest(settings: Settings, fault_id: str) -> dict[str, Any] | None:
    path = scenario_dir(settings, fault_id) / "manifest.json"
    return json.loads(path.read_text()) if path.exists() else None


def history_events(
    fault: Fault, settings: Settings, template: history.Template, rng: random.Random
) -> tuple[list[Event], dict[str, str], datetime, str]:
    """(history events incl. deploys, versions for the fault night, its start, host)."""
    ds = date.fromisoformat(fault.ds)
    first = ds - timedelta(days=settings.history_days)
    start = history.night_start(
        fault.ds, settings.start_after_midnight_s, settings.start_jitter_s, rng
    )
    initial, deploys = history.deploy_history(
        rng, first, start, settings.pipeline_deploy_every, settings.shared_deploy_every
    )
    dep = fault.recipe.deploy
    if dep:
        at = datetime(ds.year, ds.month, ds.day, tzinfo=UTC) + timedelta(hours=dep.hour_utc)
        files = tuple(p.file for p in dep.patches)
        deploys.append(
            history.DeployAt(at, (dep.pipeline,), dep.git_sha, files, dep.message, dep.author)
        )
        deploys.sort(key=lambda x: x.at)
    host = f"{rng.getrandbits(48):012x}"
    events = history.deploy_events(initial, deploys, rng)
    for i in range(settings.history_days, 0, -1):
        d = (ds - timedelta(days=i)).isoformat()
        s = history.night_start(d, settings.start_after_midnight_s, settings.start_jitter_s, rng)
        v = history.versions_at(initial, deploys, s)
        events += history.clone_night(template, d, s, rng.uniform(0.92, 1.12), v, host, rng)
    return events, history.versions_at(initial, deploys, start), start, host


def land(lake: Path, events: list[Event], now: datetime) -> None:
    msgs = [
        RawMessage(e.topic, 0, i, e.pipeline.encode(), e.model_dump_json().encode())
        for i, e in enumerate(events)
    ]
    result = BronzeWriter(lake / "bronze").land(msgs, now=now)
    if result.dead_letters:
        raise RuntimeError(f"events rejected by the lander: {dict(result.dead_letters)}")


def build_scenario(
    fault: Fault,
    settings: Settings,
    template: history.Template,
    all_faults: list[Fault],
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or build_root()
    rng = random.Random(f"{settings.catalog_seed}|{fault.fault_id}")
    recipe = fault.recipe
    past, versions, start, host = history_events(fault, settings, template, rng)

    prepare_tree(root, recipe.deploy.patches if recipe.deploy else ())
    scratch = settings.work_dir / "tmp" / fault.fault_id
    em_rng = random.Random(f"{settings.catalog_seed}|{fault.fault_id}|night")
    night = run_night(root, scratch, Night(fault.ds, versions, recipe, start), settings, em_rng)
    night = [e.model_copy(update={"event_id": history.new_id(rng)}) for e in night]

    out = scenario_dir(settings, fault.fault_id)
    if out.exists():
        _rm(out)
    out.mkdir(parents=True)
    lake = out / "lake"
    src = root / "data" / "lake"
    for p in src.rglob("*"):  # undo a chmod fault so the lake can be copied and read
        if p.is_file() and not os.access(p, os.R_OK):
            p.chmod(0o644)
    shutil.copytree(src, lake)
    last = max(e.emitted_at for e in night)
    land(lake, sorted(past, key=lambda e: e.emitted_at), now=start - timedelta(minutes=1))
    land(lake, night, now=last + timedelta(seconds=5))
    detect_now = last + timedelta(seconds=settings.detect_after_s)
    run_once(lake, load_detection_config(), detect_now)

    hits = leaks.scan(lake, leaks.tokens(all_faults))
    incidents = read_incidents(incidents_path(lake))
    primaries = [
        i
        for i in incidents
        if i["role"] == "primary" and fault.ds in json.loads(i["evidence"])["affected_ds"]
    ]
    expect = recipe.expect_pipeline
    ok = (not primaries) if expect is None else any(i["pipeline"] == expect for i in primaries)
    states = {
        e.task_id: e.payload.state for e in night if e.event_type.value == "task_run" and e.task_id
    }
    m = {
        "fault_id": fault.fault_id,
        "ds": fault.ds,
        "build_hash": build_hash(
            fault.template + json.dumps(fault.params, sort_keys=True) + fault.ds
        ),
        "built_at": datetime.now(UTC).isoformat(),
        "night_start": start.isoformat(),
        "detect_now": detect_now.isoformat(),
        "host": host,
        "events": {"history": len(past), "night": len(night)},
        "task_states": states,
        "incidents": [
            {k: str(i[k]) for k in ("incident_id", "pipeline", "type", "role", "status")}
            | {"affected_ds": json.loads(i["evidence"])["affected_ds"]}
            for i in incidents
        ],
        "targets": [i["incident_id"] for i in primaries],
        "expectation_met": ok,
        "leaks": len(hits),
    }
    (out / "manifest.json").write_text(json.dumps(m, indent=2) + "\n")
    shutil.rmtree(scratch, ignore_errors=True)
    if hits:
        raise RuntimeError(f"{fault.fault_id}: {len(hits)} leak(s) of benchmark text into the lake")
    return m
