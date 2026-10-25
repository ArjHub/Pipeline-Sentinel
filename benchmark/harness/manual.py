"""The manual baseline: a person diagnoses 10 faults by hand, timed, label hidden.

Selection: 10 benchmark faults with at least one incident, stratified over the seven
fault families (schema, late, oom, deploy, upstream, data quality, ambiguous): one from
each, then three more from families picked at random, so about 2 per family. Clean runs
are left out (nothing to diagnose). The choice and the order are seeded and stored in
trial_plan.json as opaque ids only.

Per trial:
  1. the scenario lake is copied fresh to data/benchmark/trial/lake (the same lake the
     agent reads, reset for every trial);
  2. you press a key: the clock starts and the incident alert is shown;
  3. you investigate in another terminal with the same six read-only tools the agent
     has (`make trial-inspect TOOL=...`), the incident record (`make trial-incident`)
     and raw bronze (`make trial-bronze DS=...`);
  4. back here you press Enter, then give a category, a root cause and retry-safe;
     the clock stops when you submit;
  5. the result is written (and fsync'ed) first, and only then is the label shown.

`--reveal end` keeps all labels until the last trial, so an early reveal can't hint at
what is left. Re-running continues with the first trial not yet recorded.
"""

from __future__ import annotations

import json
import random
import shutil
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmark.harness import scenario
from benchmark.harness.catalog import Fault
from benchmark.harness.runner import append_result, load_results
from benchmark.harness.settings import Settings
from sentinel.diagnose.schema import CATEGORIES
from sentinel.diagnose.tools import Toolbox

FAMILIES = ("schema", "late", "oom", "deploy", "upstream", "dq", "amb")


def family(f: Fault) -> str:
    return f.template.split(".", 1)[0]


def select(faults: list[Fault], settings: Settings) -> list[Fault]:
    rng = random.Random(settings.trial_seed)
    eligible = [
        f
        for f in faults
        if f.split == "benchmark"
        and not f.is_clean
        and (m := scenario.manifest(settings, f.fault_id))
        and m["targets"]
    ]
    by_family = {
        k: sorted((f for f in eligible if family(f) == k), key=lambda f: f.fault_id)
        for k in FAMILIES
    }
    for pool in by_family.values():
        rng.shuffle(pool)
    picked = [pool.pop() for pool in by_family.values() if pool]
    extra = [k for k in FAMILIES if by_family[k]]
    while len(picked) < settings.trial_count and extra:
        k = rng.choice(extra)
        picked.append(by_family[k].pop())
        if not by_family[k] or sum(family(f) == k for f in picked) >= 2:
            extra.remove(k)
    rng.shuffle(picked)
    return picked[: settings.trial_count]


def trial_dir(settings: Settings) -> Path:
    return settings.work_dir / "trial"


def trial_lake(settings: Settings) -> Path:
    return trial_dir(settings) / "lake"


def current(settings: Settings) -> dict[str, Any]:
    path = trial_dir(settings) / "current.json"
    if not path.exists():
        raise SystemExit("no trial in progress: start `make manual-trial` first")
    return json.loads(path.read_text())


def prepare(settings: Settings, f: Fault, n: int) -> dict[str, Any]:
    d = trial_dir(settings)
    if trial_lake(settings).exists():
        shutil.rmtree(trial_lake(settings))
    d.mkdir(parents=True, exist_ok=True)
    shutil.copytree(scenario.lake_path(settings, f.fault_id), trial_lake(settings))
    m = scenario.manifest(settings, f.fault_id) or {}
    cur = {"trial": n, "incidents": m["targets"], "lake": str(trial_lake(settings))}
    (d / "current.json").write_text(json.dumps(cur, indent=2) + "\n")
    return cur


def getkey() -> str:
    if not sys.stdin.isatty():
        return sys.stdin.readline()
    import termios
    import tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        return sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


@dataclass
class IO:
    out: Callable[[str], None] = print
    ask: Callable[[str], str] = input
    key: Callable[[], str] = getkey
    clock: Callable[[], float] = time.monotonic


def _alert(lake: Path, incident_ids: list[str]) -> list[str]:
    from sentinel.diagnose.run import find_incident

    lines = []
    for iid in incident_ids:
        i = find_incident(lake, iid)
        ev = json.loads(i["evidence"])
        lines += [
            f"  {i['incident_id']}  severity={i['severity']}  type={i['type']}  "
            f"pipeline={i['pipeline']}  ds={','.join(ev['affected_ds'])}",
            f"    {i['title']}",
        ]
    return lines


def _ask_category(io: IO) -> str:
    menu = "  ".join(f"{n}={c}" for n, c in enumerate(CATEGORIES, 1))
    while True:
        raw = io.ask(f"category ({menu}): ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(CATEGORIES):
            return CATEGORIES[int(raw) - 1]
        if raw in CATEGORIES:
            return raw


def _label(f: Fault) -> list[str]:
    also = f" (also accepted: {', '.join(f.label.acceptable)})" if f.label.acceptable else ""
    return [f"  label: {f.label.category}{also}", f"  root cause: {f.label.root_cause}"]


def run(faults: list[Fault], settings: Settings, reveal: str = "each", io: IO | None = None) -> int:
    io = io or IO()
    out_dir = settings.manual_dir
    plan_path = out_dir / "trial_plan.json"
    by_id = {f.fault_id: f for f in faults}
    if plan_path.exists():
        order = json.loads(plan_path.read_text())["fault_ids"]
    else:
        order = [f.fault_id for f in select(faults, settings)]
        out_dir.mkdir(parents=True, exist_ok=True)
        plan_path.write_text(json.dumps({"fault_ids": order}, indent=2) + "\n")
    results_path = out_dir / "trials.jsonl"
    done = {r["fault_id"] for r in load_results(results_path) if r.get("kind") == "diagnosis"}
    lake = trial_lake(settings)
    for n, fid in enumerate(order, 1):
        if fid in done:
            continue
        f = by_id[fid]
        cur = prepare(settings, f, n)
        io.out(f"\n=== Trial {n} of {len(order)}")
        io.out("Investigate in a second terminal (same evidence the agent has):")
        io.out("  make trial-incident                      # the incident record")
        io.out("  make trial-inspect TOOL=get_error_logs   # any of the six tools; optional")
        io.out('       PIPELINE=... DS=YYYY-MM-DD ARGS=\'{"task_id": "..."}\'')
        io.out("  make trial-bronze DS=YYYY-MM-DD          # raw bronze rows")
        io.out("Press any key to start the clock and show the alert.")
        io.key()
        t0 = io.clock()
        started = datetime.now(UTC)
        io.out("\nINCIDENT")
        for line in _alert(lake, cur["incidents"]):
            io.out(line)
        io.ask("\nPress Enter here when you have a diagnosis. ")
        category = _ask_category(io)
        root_cause = io.ask("root cause (one line): ").strip()
        retry = io.ask("retry safe? (y/n/?): ").strip().lower()[:1] or "?"
        seconds = round(io.clock() - t0, 1)
        append_result(
            results_path,
            {
                "kind": "diagnosis",
                "trial": n,
                "fault_id": fid,
                "incident_ids": cur["incidents"],
                "started_at": started.isoformat(),
                "seconds": seconds,
                "category": category,
                "root_cause": root_cause,
                "retry_safe": {"y": True, "n": False}.get(retry),
            },
        )
        io.out(f"recorded: {category}, {seconds:.0f} s")
        if reveal == "each":
            _reveal(io, results_path, f)
    if reveal == "end":
        shown = {r["fault_id"] for r in load_results(results_path) if r.get("kind") == "label"}
        for fid in order:
            if fid not in shown:
                io.out(f"\nTrial {order.index(fid) + 1}:")
                _reveal(io, results_path, by_id[fid])
    io.out("\nAll trials recorded.")
    return 0


def _reveal(io: IO, results_path: Path, f: Fault) -> None:
    for line in _label(f):
        io.out(line)
    append_result(
        results_path,
        {
            "kind": "label",
            "fault_id": f.fault_id,
            "category": f.label.category,
            "acceptable": list(f.label.acceptable),
            "root_cause": f.label.root_cause,
        },
    )


# --- the human's tools ------------------------------------------------------------------


def inspect(settings: Settings, tool: str, args: dict[str, Any], incident: str | None) -> str:
    from sentinel.detect.config import load_config as load_detection_config
    from sentinel.diagnose.config import load_config
    from sentinel.diagnose.run import find_incident

    cur = current(settings)
    lake = Path(cur["lake"])
    inc = find_incident(lake, incident or cur["incidents"][0])
    t = load_config().tools
    box = Toolbox(
        lake,
        load_detection_config(),
        inc,
        ds_window_days=t.ds_window_days,
        max_items=t.max_items,
        item_max_chars=t.item_max_chars,
        traceback_max_chars=t.traceback_max_chars,
        output_max_chars=t.output_max_chars,
    )
    text, _ = box.call(tool, args)
    return json.dumps(json.loads(text), indent=2, ensure_ascii=False)


def incident_record(settings: Settings, incident: str | None) -> str:
    from sentinel.diagnose.agent import incident_message
    from sentinel.diagnose.config import load_config
    from sentinel.diagnose.run import find_incident

    cur = current(settings)
    inc = find_incident(Path(cur["lake"]), incident or cur["incidents"][0])
    return incident_message(inc, load_config().tools.incident_evidence_max_chars)
