"""python -m benchmark.harness <command>   (the make targets wrap these)

  gen-catalog [--check]      draw the faults (seeded) into the sealed catalog; prints counts
  build [--split] [--only]   build scenario lakes (real jobs, no API); --rebuild to redo
  run --model --repeats      run the agent over the scenarios (--dry-run, --resume)
  status                     what is built and what has run (counts only)
  manual-trial               the timed, blind manual baseline
  inspect / incident         the human's view of the current trial (same tools as the agent)

Never prints labels, categories or root causes, except manual-trial after a diagnosis
has been recorded.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter

from benchmark.harness import catalog, gen_catalog, manual, runner, scenario
from benchmark.harness.settings import load as load_settings


def _faults(settings, split: str):
    return catalog.load(settings.catalog_path, split)


def cmd_gen_catalog(args, settings) -> int:
    data = catalog.encode(
        gen_catalog.generate(settings.catalog_seed, settings.first_ds, settings.last_ds)
    )
    path = settings.catalog_path
    if args.check:
        same = path.exists() and path.read_bytes() == data
        print(f"{path}: {'identical to a fresh draw' if same else 'DIFFERS from a fresh draw'}")
        return 0 if same else 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    splits = Counter(e["split"] for e in catalog.decode(data))
    print(f"wrote {path}: {dict(splits)} faults, sha256 {catalog.sha256(path)[:16]}")
    return 0


def cmd_build(args, settings) -> int:
    everything = catalog.load(settings.catalog_path)
    faults = [f for f in everything if args.split in ("all", f.split)]
    if args.only:
        faults = [f for f in faults if f.fault_id in set(args.only)]
    if args.rebuild or not (scenario.template_dir(settings) / "meta.json").exists():
        print(f"template night {settings.template_ds} (real jobs) ...", flush=True)
        scenario.build_template(settings)
    template = scenario.load_template(settings)
    bad = []
    for n, f in enumerate(faults, 1):
        if scenario.manifest(settings, f.fault_id) and not args.rebuild:
            print(f"[{n}/{len(faults)}] {f.fault_id} already built", flush=True)
            continue
        m = scenario.build_scenario(f, settings, template, everything)
        print(
            f"[{n}/{len(faults)}] {f.fault_id} built: {len(m['targets'])} target incident(s), "
            f"leaks={m['leaks']}",
            flush=True,
        )
        if not m["expectation_met"]:
            bad.append(f.fault_id)
    if bad:
        print(f"{len(bad)} scenario(s) did not produce the expected incident: {bad}")
        return 1
    return 0


def cmd_status(args, settings) -> int:
    for split in catalog.SPLITS:
        faults = _faults(settings, split)
        ms = [scenario.manifest(settings, f.fault_id) for f in faults]
        built = [m for m in ms if m]
        print(
            f"{split}: {len(faults)} faults, {len(built)} built, "
            f"{sum(bool(m['targets']) for m in built)} with >=1 target incident, "
            f"{sum(len(m['targets']) for m in built)} target incidents, "
            f"{sum(not m['expectation_met'] for m in built)} expectation mismatches, "
            f"{sum(m['leaks'] for m in built)} leaks"
        )
    for path in sorted(settings.results_dir.glob("*/results.jsonl")):
        rows = runner.load_results(path)
        print(f"{path.parent.name}: {len(rows)} results, ${sum(r['cost_usd'] for r in rows):.4f}")
    return 0


def cmd_run(args, settings) -> int:
    repeats = args.repeats or settings.repeats.get(args.model, 1)
    return runner.run(
        _faults(settings, args.split),
        settings,
        split=args.split,
        model=args.model,
        repeats=repeats,
        prompt=args.prompt,
        dry_run=args.dry_run,
        resume=args.resume,
        retry_errors=args.retry_errors,
        judge=args.judge,
    )


def cmd_manual(args, settings) -> int:
    return manual.run(_faults(settings, "benchmark"), settings, reveal=args.reveal)


def cmd_inspect(args, settings) -> int:
    extra = json.loads(args.args) if args.args else {}
    extra |= {k: v for k, v in (("pipeline", args.pipeline), ("ds", args.ds)) if v}
    print(manual.inspect(settings, args.tool, extra, args.incident))
    return 0


def cmd_incident(args, settings) -> int:
    print(manual.incident_record(settings, args.incident))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="benchmark.harness")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("gen-catalog")
    g.add_argument("--check", action="store_true")
    b = sub.add_parser("build")
    b.add_argument("--split", default="all", choices=["all", *catalog.SPLITS])
    b.add_argument("--only", nargs="*")
    b.add_argument("--rebuild", action="store_true")
    sub.add_parser("status")
    r = sub.add_parser("run")
    r.add_argument("--model", default="claude-haiku-5-5")
    r.add_argument("--repeats", type=int, default=0)
    r.add_argument("--prompt", default="diagnosis_v1")
    r.add_argument("--split", default="benchmark", choices=catalog.SPLITS)
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--resume", action="store_true")
    r.add_argument("--retry-errors", action="store_true")
    r.add_argument("--judge", action="store_true", help="include the judge in the estimate")
    m = sub.add_parser("manual-trial")
    m.add_argument("--reveal", default="each", choices=["each", "end"])
    i = sub.add_parser("inspect")
    i.add_argument("--tool", required=True)
    i.add_argument("--pipeline")
    i.add_argument("--ds")
    i.add_argument("--args")
    i.add_argument("--incident")
    c = sub.add_parser("incident")
    c.add_argument("--incident")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.WARNING)
    settings = load_settings()
    handlers = {
        "gen-catalog": cmd_gen_catalog,
        "build": cmd_build,
        "status": cmd_status,
        "run": cmd_run,
        "manual-trial": cmd_manual,
        "inspect": cmd_inspect,
        "incident": cmd_incident,
    }
    return handlers[args.cmd](args, settings)


if __name__ == "__main__":
    sys.exit(main())
