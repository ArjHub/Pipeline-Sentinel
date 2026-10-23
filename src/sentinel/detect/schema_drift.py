"""Schema changes: a job's observed input schema differs from its last known good one.

"Last known good" = the input schema of the most recent *successful* run of the same job
before this one (by emitted time, across all ds). A failed run never becomes the
baseline, so a broken file keeps being compared against the schema that worked. A
successful run with a change (e.g. an added column the job ignores) becomes the new
baseline, so the drift is reported once rather than every day after.

Columns are compared by name and Spark type. Nullability is ignored: Spark marks every
CSV/JSON column nullable, so it carries no signal here.

Cold start: a job's first observed schema has nothing to compare against and only
becomes the baseline.

A finding clears at the job's next run whose schema matches its own baseline again (the
file was fixed and re-run, or the change was accepted by the next successful run).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from sentinel.detect.config import DetectionConfig
from sentinel.detect.findings import Finding, IncidentType
from sentinel.detect.observations import JobRun, Observations


@dataclass(frozen=True)
class SchemaDiff:
    removed: tuple[str, ...]
    added: tuple[str, ...]
    retyped: tuple[tuple[str, str, str], ...]  # (column, old type, new type)

    def __bool__(self) -> bool:
        return bool(self.removed or self.added or self.retyped)

    @property
    def breaking(self) -> bool:
        return bool(self.removed or self.retyped)

    @property
    def key(self) -> str:
        parts = [f"-{c}" for c in self.removed] + [f"+{c}" for c in self.added]
        parts += [f"~{c}:{a}->{b}" for c, a, b in self.retyped]
        return ",".join(parts)


def diff(baseline: tuple[tuple[str, str], ...], current: tuple[tuple[str, str], ...]) -> SchemaDiff:
    old, new = dict(baseline), dict(current)
    return SchemaDiff(
        removed=tuple(sorted(set(old) - set(new))),
        added=tuple(sorted(set(new) - set(old))),
        retyped=tuple(sorted((c, old[c], new[c]) for c in set(old) & set(new) if old[c] != new[c])),
    )


def _run_ref(j: JobRun) -> dict:
    return {
        "ds": j.ds,
        "run_id": j.run_id,
        "try_number": j.try_number,
        "status": j.status,
        "code_version": j.code_version,
        "emitted_at": j.at.isoformat(),
    }


def detect(obs: Observations, cfg: DetectionConfig) -> list[Finding]:
    by_job: dict[str, list[JobRun]] = defaultdict(list)
    for j in obs.jobs:  # already time-ordered
        if j.input_schema:
            by_job[j.job_name].append(j)

    findings: list[Finding] = []
    for job_name, runs in by_job.items():
        baseline: JobRun | None = None
        compared: list[tuple[JobRun, JobRun | None, SchemaDiff | None]] = []
        for run in runs:
            d = diff(baseline.input_schema, run.input_schema) if baseline else None
            compared.append((run, baseline, d))
            if run.status == "success":
                baseline = run

        # One finding per (pipeline, ds, diff); repeated attempts with the same diff merge.
        groups: dict[tuple[str, str, str], list[int]] = defaultdict(list)
        for i, (run, _, d) in enumerate(compared):
            if d:
                groups[(run.pipeline, run.ds, d.key)].append(i)
        for (pipeline, ds, key), idx in groups.items():
            first, last = compared[idx[0]], compared[idx[-1]]
            run, base, d = last
            clean_after = [r for r, _, dd in compared[idx[-1] + 1 :] if dd is not None and not dd]
            findings.append(
                Finding(
                    type=IncidentType.SCHEMA_CHANGE,
                    pipeline=pipeline,
                    ds=ds,
                    fingerprint=f"{job_name}:{key}",
                    severity=(
                        cfg.schema_breaking_severity if d.breaking else cfg.schema_added_severity
                    ),
                    title=f"{job_name} input schema changed vs last good run: {key}",
                    first_observed=first[0].at,
                    last_observed=run.at,
                    cleared_at=clean_after[0].at if clean_after else None,
                    task_id=run.task_id,
                    evidence={
                        "job_name": job_name,
                        "removed": list(d.removed),
                        "added": list(d.added),
                        "retyped": [{"column": c, "from": a, "to": b} for c, a, b in d.retyped],
                        "baseline_run": _run_ref(base),
                        "current_run": _run_ref(run),
                        "current_columns": [f"{n}:{t}" for n, t in run.input_schema],
                        "baseline_columns": [f"{n}:{t}" for n, t in base.input_schema],
                        "job_error": run.error_type,
                    },
                )
            )
    return findings
