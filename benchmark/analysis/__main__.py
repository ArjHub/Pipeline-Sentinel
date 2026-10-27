"""python -m benchmark.analysis <command>   (Phase 5b scoring; the make targets wrap these)

detection                  replay the detector over the scenario lakes (no API)
judge --model M [--dry-run] run both judges over one model's run (paid, resumable)
score                      metrics.json, CSVs and SVG charts (no API)
human-sample               draw the blind 15-diagnosis fix-quality sheet
agreement                  judge vs. human agreement from the filled-in sheet
posthoc --label L --model M  Phase 5c: a labelled run vs. v1 (its own lakes; no API)
    [--prior L1 ...]          Phase 5d: v1 vs. each prior round vs. L, into report/L/
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import tomllib

from benchmark.analysis import charts, detection, human, judging, posthoc, score
from benchmark.harness import catalog
from benchmark.harness.runner import results_path
from benchmark.harness.settings import DEFAULT_PATH, ROOT
from benchmark.harness.settings import load as load_settings

MODELS = ["claude-haiku-5-5", "claude-sonnet-5-5"]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="benchmark.analysis")
    p.add_argument("--prompt", default="diagnosis_v1")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("detection")
    j = sub.add_parser("judge")
    j.add_argument("--model", required=True)
    j.add_argument("--dry-run", action="store_true")
    j.add_argument("--label", default="")
    sub.add_parser("score")
    sub.add_parser("human-sample")
    sub.add_parser("agreement")
    ph = sub.add_parser("posthoc")
    ph.add_argument("--label", required=True)
    ph.add_argument("--model", default="claude-haiku-5-5")
    ph.add_argument("--prior", action="append", default=[], help="earlier round labels, in order")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.WARNING)
    settings = load_settings()
    faults = catalog.load(settings.catalog_path, "benchmark")
    report = settings.results_dir.parent / "report"
    human_dir = settings.results_dir.parent / "human"
    if args.cmd == "detection":
        rows = detection.run(faults, settings)
        detection.write(rows, report / "detection.json")
        print(json.dumps(score.detection_metrics(rows), indent=2))
        return 0
    if args.cmd == "judge":
        run_dir = results_path(settings, "benchmark", args.model, args.prompt, args.label).parent
        return judging.run(faults, settings, run_dir, dry_run=args.dry_run)
    if args.cmd == "score":
        metrics, items, manual = score.run(faults, settings, MODELS, args.prompt, report)
        for path in charts.write_all(metrics, items, manual, report / "charts"):
            print(f"wrote {path}")
        return 0
    if args.cmd == "human-sample":
        targets = score.fault_targets(settings, faults)
        return human.sample(faults, settings, MODELS, args.prompt, targets, human_dir)
    if args.cmd == "agreement":
        res = human.agreement(settings, human_dir)
        print(json.dumps({k: v for k, v in res.items() if k != "rows"}, indent=2))
        return 0
    if args.cmd == "posthoc":
        with DEFAULT_PATH.open("rb") as fh:
            v1_work = ROOT / tomllib.load(fh)["paths"]["work_dir"]
        if args.prior:
            m = posthoc.rounds(
                faults,
                settings,
                model=args.model,
                prompt=args.prompt,
                label=args.label,
                priors=args.prior,
                v1_work=v1_work,
                report_dir=report,
            )
            print(json.dumps({k: m[k] for k in ("note", "rescored_equals_committed")}, indent=2))
            return 0
        m = posthoc.run(
            faults,
            settings,
            model=args.model,
            prompt=args.prompt,
            label=args.label,
            v1_work=v1_work,
            report_dir=report,
        )
        print(json.dumps({k: m[k] for k in ("note", "v1_rescored_equals_committed")}, indent=2))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
