"""Findings -> incidents: decide what is one root problem and which pipeline owns it.

Three steps.

1. Within one pipeline run (pipeline, ds): a cause explains its symptoms.
   All cause findings (schema change, late data, volume) of that run become ONE
   occurrence, named after the most specific cause (CAUSE_PRIORITY). A failed task in
   the same run is attached to it as a symptom if it happened at or after the cause was
   first seen (minus `correlation_tolerance`) and before the cause cleared. Example: the
   missing `amount` column (schema_change) and ingest_orders failing on it
   (failed_job) are one occurrence, typed schema_change. A failed task with no cause is
   its own occurrence, one per distinct error signature.

2. Across pipelines: upstream owns the problem. An occurrence in pipeline P is
   *downstream* if a pipeline P reads from (transitively, per the `upstream` lineage in
   config) has a primary occurrence for the same ds that started first (within
   tolerance) and hadn't cleared yet. Pipelines are processed in lineage order, so a
   root is always a primary. If several upstreams qualify, the one a sensor names
   (`wait_for_<pipeline>`) wins, then the earliest. The rest are recorded as
   `also_caused_by`.

3. Across days: same problem, same incident. Occurrences are grouped by dedup_key
   = pipeline|type|fingerprint (downstream: <root key> >> pipeline|type|fingerprint).
   The fingerprint describes *what* broke, not when, so the same broken column
   tomorrow updates the existing incident instead of opening a new one.
   incident_id is a hash of dedup_key, so re-running detection over the same bronze
   always produces the same ids.

Status: `open` while any of an incident's signals is still active, else `resolved`.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from sentinel.detect.config import DetectionConfig
from sentinel.detect.findings import (
    CAUSE_PRIORITY,
    SEVERITY_RANK,
    Finding,
    max_severity,
)

MAX_OCCURRENCES_IN_EVIDENCE = 10


@dataclass
class Occurrence:
    head: Finding
    signals: list[Finding]
    reason: str
    root: Occurrence | None = None
    also_caused_by: list[Occurrence] = field(default_factory=list)

    @property
    def pipeline(self) -> str:
        return self.head.pipeline

    @property
    def ds(self) -> str:
        return self.head.ds

    @property
    def first_seen(self) -> datetime:
        return min(s.first_observed for s in self.signals)

    @property
    def last_seen(self) -> datetime:
        return max(s.last_observed for s in self.signals)

    @property
    def active(self) -> bool:
        return any(s.active for s in self.signals)

    @property
    def cleared_at(self) -> datetime | None:
        return None if self.active else max(s.cleared_at for s in self.signals)

    @property
    def severity(self) -> str:
        return max_severity([s.severity for s in self.signals])

    @property
    def role(self) -> str:
        return "downstream" if self.root else "primary"

    @property
    def key(self) -> str:
        local = f"{self.pipeline}|{self.head.type.value}|{self.head.fingerprint}"
        return f"{self.root.key} >> {local}" if self.root else local


@dataclass(frozen=True)
class Incident:
    incident_id: str
    dedup_key: str
    pipeline: str
    type: str
    severity: str
    status: str  # open | resolved   (store.py adds `superseded`)
    role: str  # primary | downstream
    root_incident_id: str | None
    title: str
    first_seen: datetime
    last_seen: datetime
    evidence: dict[str, Any]


def incident_id(dedup_key: str) -> str:
    return "INC-" + hashlib.sha256(dedup_key.encode()).hexdigest()[:10]


def explains(cause_first: datetime, cause_cleared: datetime | None, effect: datetime, tol) -> bool:
    """Did `effect` happen while the cause was present? (with clock tolerance)"""
    return cause_first - tol <= effect and (cause_cleared is None or effect < cause_cleared + tol)


def _pick_head(causes: list[Finding]) -> Finding:
    return max(
        causes,
        key=lambda f: (
            CAUSE_PRIORITY[f.type],
            SEVERITY_RANK[f.severity],
            -f.first_observed.timestamp(),
            f.fingerprint,
        ),
    )


def _within_run(findings: list[Finding], tol: timedelta) -> list[Occurrence]:
    causes = [f for f in findings if f.is_cause]
    failures = sorted(
        (f for f in findings if not f.is_cause), key=lambda f: (f.first_observed, f.fingerprint)
    )
    occurrences: list[Occurrence] = []
    leftovers = failures
    if causes:
        head = _pick_head(causes)
        symptoms = [
            f
            for f in failures
            if any(explains(c.first_observed, c.cleared_at, f.first_observed, tol) for c in causes)
        ]
        leftovers = [f for f in failures if f not in symptoms]
        others = sorted((c for c in causes if c is not head), key=lambda f: f.first_observed)
        reason = f"named after the most specific cause ({head.type.value})"
        if others:
            reason += f"; {len(others)} other cause signal(s) in the same run merged"
        if symptoms:
            reason += f"; {len(symptoms)} failed task(s) attached as symptoms: " + ", ".join(
                sorted({s.task_id or "?" for s in symptoms})
            )
        occurrences.append(Occurrence(head, [head, *others, *symptoms], reason))
    for f in leftovers:
        occurrences.append(Occurrence(f, [f], "failed task with no cause signal in the same run"))
    return occurrences


def _link_downstream(occurrences: list[Occurrence], cfg: DetectionConfig, tol) -> None:
    def depth(o: Occurrence) -> int:
        return cfg.depth(o.pipeline) if o.pipeline in cfg.pipelines else 0

    by_ds: dict[str, list[Occurrence]] = defaultdict(list)
    for o in occurrences:
        by_ds[o.ds].append(o)
    for same_day in by_ds.values():
        for o in sorted(same_day, key=lambda o: (depth(o), o.first_seen)):
            if o.pipeline not in cfg.pipelines:
                continue
            ups = cfg.upstream_closure(o.pipeline)
            candidates = [
                u
                for u in same_day
                if u.pipeline in ups
                and u.root is None
                and explains(u.first_seen, u.cleared_at, o.first_seen, tol)
            ]
            if not candidates:
                continue
            named = o.head.task_id or ""
            candidates.sort(
                key=lambda u: (f"wait_for_{u.pipeline}" != named, u.first_seen, u.pipeline)
            )
            o.root, o.also_caused_by = candidates[0], candidates[1:]
            o.reason = (
                f"{o.pipeline} reads {candidates[0].pipeline} (lineage), which had a problem "
                f"for the same ds first seen at {candidates[0].first_seen.isoformat()}"
            )


def _ds_key(o: Occurrence) -> tuple[str, datetime]:
    return (o.ds, o.first_seen)


def correlate(findings: list[Finding], cfg: DetectionConfig) -> list[Incident]:
    tol = cfg.correlation_tolerance
    by_run: dict[tuple[str, str], list[Finding]] = defaultdict(list)
    for f in findings:
        by_run[(f.pipeline, f.ds)].append(f)
    occurrences = [o for fs in by_run.values() for o in _within_run(fs, tol)]
    _link_downstream(occurrences, cfg, tol)

    groups: dict[str, list[Occurrence]] = defaultdict(list)
    for o in occurrences:
        groups[o.key].append(o)

    incidents = []
    for key, occs in groups.items():
        occs.sort(key=_ds_key)
        latest = occs[-1]
        root_id = incident_id(latest.root.key) if latest.root else None
        title = latest.head.title
        if root_id:
            title = f"[downstream of {root_id}] {title}"
        incidents.append(
            Incident(
                incident_id=incident_id(key),
                dedup_key=key,
                pipeline=latest.pipeline,
                type=latest.head.type.value,
                severity=max_severity([o.severity for o in occs]),
                status="open" if any(o.active for o in occs) else "resolved",
                role=latest.role,
                root_incident_id=root_id,
                title=title,
                first_seen=min(o.first_seen for o in occs),
                last_seen=max(o.last_seen for o in occs),
                evidence=_evidence(title, occs),
            )
        )
    incidents.sort(key=lambda i: (i.first_seen, i.dedup_key))
    return incidents


def _evidence(title: str, occs: list[Occurrence]) -> dict[str, Any]:
    shown = occs[-MAX_OCCURRENCES_IN_EVIDENCE:][::-1]  # newest first
    return {
        "summary": title,
        "affected_ds": sorted({o.ds for o in occs}),
        "occurrence_count": len(occs),
        "occurrences": [
            {
                "ds": o.ds,
                "active": o.active,
                "first_seen": o.first_seen.isoformat(),
                "last_seen": o.last_seen.isoformat(),
                "why": o.reason,
                "root_incident_id": incident_id(o.root.key) if o.root else None,
                "also_caused_by": [incident_id(u.key) for u in o.also_caused_by],
                "signals": [s.summary() for s in o.signals],
            }
            for o in shown
        ],
    }
