"""Record a "deploy" of pipeline code and emit one `deploy_event` per changed pipeline.

    python -m sentinel.deploy --git-sha <sha> --dirty <0|1> [--author ..] [--message ..]

`make deploy` runs this (and `make up` / `make run-all` call it first). Each pipeline's
code is its own DAG + job files plus the shared modules it imports. We hash file
*contents* instead of trusting the git SHA alone, so uncommitted edits (exactly what a
"bad deploy" fault injection does in Phase 5) are still detected and attributed.

The resulting per-pipeline version is written to a state file, and every event a
pipeline emits afterwards carries it as `code_version`. That's what lets diagnosis line
up "failures started at version X" with "version X changed jobs/orders_ingest.py".
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sentinel.events.models import DeployEvent, RunContext
from sentinel.events.sinks import EventSink, sink_from_env

SHARED = [
    "pipelines/common/*.py",
    "pipelines/generators/*.py",
    "pipelines/quality/*.py",
    "pipelines/jobs/spark.py",
    "pipelines/jobs/runner.py",
    "pipelines/jobs/glue_compat.py",
    "src/sentinel/**/*.py",
    "requirements/jobs.lock.txt",
]

PIPELINES: dict[str, list[str]] = {
    "orders_ingest": ["pipelines/dags/orders_ingest.py", "pipelines/jobs/orders_ingest.py"],
    "customers_dim": ["pipelines/dags/customers_dim.py", "pipelines/jobs/customers_dim.py"],
    "payments_etl": ["pipelines/dags/payments_etl.py", "pipelines/jobs/payments_glue_job.py"],
    "daily_revenue_agg": [
        "pipelines/dags/daily_revenue_agg.py",
        "pipelines/jobs/daily_revenue_agg.py",
    ],
}


def state_path() -> Path:
    return Path(os.environ.get("SENTINEL_DEPLOY_STATE", "data/deploy/state.json"))


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pipeline_files(root: Path, pipeline: str) -> dict[str, str]:
    """{relative path: sha256} for every file that makes up a pipeline."""
    out: dict[str, str] = {}
    for pattern in [*PIPELINES[pipeline], *SHARED]:
        for p in sorted(root.glob(pattern)):
            if p.is_file() and "__pycache__" not in p.parts:
                out[p.relative_to(root).as_posix()] = _hash_file(p)
    return out


def _content_version(files: dict[str, str]) -> str:
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    return digest[:8]


def version_for(git_sha: str, dirty: bool, files: dict[str, str]) -> str:
    # A clean tree is identified by its commit. A dirty tree also gets a content hash,
    # so two different uncommitted edits on the same commit get different versions.
    return f"{git_sha}-dirty.{_content_version(files)}" if dirty else git_sha


def load_state(path: Path | None = None) -> dict:
    path = path or state_path()
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {"pipelines": {}}


def current_code_version(pipeline: str, path: Path | None = None) -> str | None:
    return load_state(path).get("pipelines", {}).get(pipeline, {}).get("version")


@dataclass
class DeployResult:
    pipeline: str
    event: DeployEvent | None  # None = unchanged, nothing emitted


def record(
    root: Path,
    sink: EventSink,
    git_sha: str,
    dirty: bool,
    author: str | None = None,
    message: str | None = None,
    path: Path | None = None,
) -> list[DeployResult]:
    path = path or state_path()
    state = load_state(path)
    known = state.setdefault("pipelines", {})
    results: list[DeployResult] = []
    now = datetime.now(UTC).isoformat()

    for pipeline in PIPELINES:
        files = pipeline_files(root, pipeline)
        prev = known.get(pipeline)
        prev_files: dict[str, str] = prev["files"] if prev else {}
        changed = sorted(
            f for f in set(files) | set(prev_files) if files.get(f) != prev_files.get(f)
        )
        if prev and not changed:
            results.append(DeployResult(pipeline, None))
            continue

        version = version_for(git_sha, dirty, files)
        event = DeployEvent(
            component=pipeline,
            code_version=version,
            previous_version=prev["version"] if prev else None,
            changed_files=changed,
            author=author,
            message=message,
            git_sha=git_sha,
            dirty=dirty,
        )
        sink.emit(RunContext(pipeline=pipeline, code_version=version).event(event))
        known[pipeline] = {"version": version, "deployed_at": now, "files": files}
        results.append(DeployResult(pipeline, event))

    sink.flush()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--dirty", type=int, default=0, help="number of uncommitted changes")
    parser.add_argument("--author")
    parser.add_argument("--message")
    parser.add_argument("--root", default=".")
    args = parser.parse_args(argv)

    results = record(
        Path(args.root),
        sink_from_env(),
        git_sha=args.git_sha,
        dirty=args.dirty > 0,
        author=args.author,
        message=args.message,
    )
    for r in results:
        if r.event is None:
            print(f"{r.pipeline:<20} unchanged")
        else:
            prev = r.event.previous_version or "(first deploy)"
            print(
                f"{r.pipeline:<20} {prev} -> {r.event.code_version}  "
                f"({len(r.event.changed_files)} file(s) changed)"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
