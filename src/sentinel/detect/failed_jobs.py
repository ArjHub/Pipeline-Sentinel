"""Failed jobs: a task attempt that ended `failed`.

One finding per (pipeline, ds, task, error signature). Repeated attempts that fail the
same way are one finding (first/last observed, attempt count). The finding clears when a
later attempt of that task doesn't fail that way (it succeeded, or failed differently).

The error signature is what makes two failures "the same problem":
  1. the job's own exception class (job_metric.error_type, e.g. SchemaMismatchError), else
  2. the failed GX expectations of that attempt (validate_* tasks), else
  3. the Airflow exception text with digits masked (sensors, bash errors, OOM kills).
"""

from __future__ import annotations

import re
from collections import defaultdict

from sentinel.detect.config import DetectionConfig
from sentinel.detect.findings import Finding, IncidentType
from sentinel.detect.observations import DQRun, Observations, TaskAttempt

_MAX_TEXT = 500


def _dq_for(attempt: TaskAttempt, dq_runs: list[DQRun]) -> DQRun | None:
    """The DQ result this attempt produced: same try if known, else the latest before it."""
    same = [
        d
        for d in dq_runs
        if (d.pipeline, d.ds, d.task_id) == (attempt.pipeline, attempt.ds, attempt.task_id)
        and d.at <= attempt.at
    ]
    exact = [d for d in same if d.try_number is not None and d.try_number == attempt.try_number]
    return (exact or same or [None])[-1]


def signature(attempt: TaskAttempt, dq: DQRun | None) -> str:
    if attempt.error_type:
        return attempt.error_type
    if dq is not None and not dq.success:
        return "dq:" + ",".join(dq.failed_expectations)
    text = (attempt.exception or "unknown error").strip().splitlines()[0]
    return re.sub(r"\d+", "#", text)[:120]


def detect(obs: Observations, cfg: DetectionConfig) -> list[Finding]:
    by_task: dict[tuple[str, str, str], list[TaskAttempt]] = defaultdict(list)
    for a in obs.attempts:
        if a.state in ("success", "failed"):  # up_for_retry is not terminal
            by_task[(a.pipeline, a.ds, a.task_id)].append(a)

    findings: list[Finding] = []
    for (pipeline, ds, task_id), attempts in by_task.items():
        sigs = []
        for a in attempts:
            dq = _dq_for(a, obs.dq) if a.state == "failed" else None
            sigs.append((a, signature(a, dq) if a.state == "failed" else None, dq))
        for sig in dict.fromkeys(s for _, s, _ in sigs if s is not None):
            idx = [i for i, (_, s, _) in enumerate(sigs) if s == sig]
            first, last = sigs[idx[0]][0], sigs[idx[-1]][0]
            later = [a for a, s, _ in sigs[idx[-1] + 1 :] if s != sig]
            dq = sigs[idx[-1]][2]
            evidence = {
                "task_id": task_id,
                "attempts_failed": len(idx),
                "try_numbers": [sigs[i][0].try_number for i in idx],
                "run_id": last.run_id,
                "error_type": last.error_type,
                "error_message": (last.error_message or "")[:_MAX_TEXT] or None,
                "exception": (last.exception or "")[:_MAX_TEXT] or None,
                "failed_expectations": dq.failed_expectations if dq and not dq.success else [],
                "event_ids": [sigs[i][0].event_id for i in idx][-5:],
            }
            findings.append(
                Finding(
                    type=IncidentType.FAILED_JOB,
                    pipeline=pipeline,
                    ds=ds,
                    fingerprint=f"{task_id}:{sig}",
                    severity=cfg.failed_job_severity,
                    title=f"{pipeline}.{task_id} failed for ds={ds}: {sig}",
                    first_observed=first.at,
                    last_observed=last.at,
                    cleared_at=later[0].at if later else None,
                    task_id=task_id,
                    evidence=evidence,
                )
            )
    return findings
