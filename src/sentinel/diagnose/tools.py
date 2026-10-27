"""The agent's tools. All read-only, all over bronze and the incidents table.

Scoping: every tool defaults to the incident's pipeline and its latest affected ds. A
caller may name another *configured* pipeline (e.g. an upstream) and a ds within
`ds_window_days` of the incident's dates; anything else is refused with an error result,
not silently widened.

Output: JSON text, redacted and capped at `tool_output_max_chars`. Everything that came
from a pipeline (log text, error messages, deploy messages, column names) is under
`untrusted_data`: it is data to analyse, never instructions (the system prompt says so,
and nothing the model reads can reach an action anyway).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.dataset as pads
from deltalake import DeltaTable

from sentinel.detect.config import DetectionConfig
from sentinel.detect.schema_drift import diff
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.diagnose.redact import clean
from sentinel.instrumentation.terminal_errors import terminal_error_lines

LOG_LEVELS = ("ERROR", "CRITICAL", "WARNING")


class ToolError(ValueError):
    """A bad tool call: returned to the model as an is_error tool result."""


@dataclass
class Scope:
    incident: dict[str, Any]
    pipelines: tuple[str, ...]
    ds_window_days: int

    @property
    def pipeline(self) -> str:
        return self.incident["pipeline"]

    @property
    def affected_ds(self) -> list[str]:
        return json.loads(self.incident["evidence"])["affected_ds"]

    def resolve(self, args: dict[str, Any]) -> tuple[str, str]:
        pipeline = args.get("pipeline") or self.pipeline
        if pipeline not in self.pipelines:
            raise ToolError(f"unknown pipeline {pipeline!r}; known: {list(self.pipelines)}")
        ds = args.get("ds") or self.affected_ds[-1]
        try:
            d = date.fromisoformat(ds)
        except (TypeError, ValueError) as exc:
            raise ToolError(f"ds must be YYYY-MM-DD, got {ds!r}") from exc
        dates = [date.fromisoformat(x) for x in self.affected_ds]
        lo = min(dates) - timedelta(days=self.ds_window_days)
        hi = max(dates) + timedelta(days=self.ds_window_days)
        if not lo <= d <= hi:
            raise ToolError(f"ds {ds} is outside this incident's window [{lo}, {hi}]")
        return pipeline, ds


def _read(root: Path, topic: str, since: date) -> list[dict[str, Any]]:
    path = root / topic
    if not DeltaTable.is_deltatable(str(path)):
        return []
    ds = DeltaTable(str(path)).to_pyarrow_dataset()
    rows = ds.to_table(filter=pads.field("event_date") >= since.isoformat()).to_pylist()
    for r in rows:
        r["payload"] = json.loads(r["payload"])
        r["ds"] = (r.get("logical_date") or "")[:10]
    rows.sort(key=lambda r: r["emitted_at"])
    return rows


@dataclass
class Bronze:
    """Bronze rows for one diagnosis, read once (a few hundred rows a day here)."""

    root: Path
    since: date
    _cache: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def rows(self, topic: str) -> list[dict[str, Any]]:
        if topic not in self._cache:
            self._cache[topic] = _read(self.root, topic, self.since)
        return self._cache[topic]


class Toolbox:
    def __init__(
        self,
        lake: Path,
        cfg: DetectionConfig,
        incident: dict[str, Any],
        *,
        ds_window_days: int = 7,
        max_items: int = 20,
        item_max_chars: int = 600,
        traceback_max_chars: int = 1500,
        output_max_chars: int = 8000,
    ) -> None:
        self.lake, self.cfg = lake, cfg
        self.scope = Scope(incident, tuple(cfg.pipelines), ds_window_days)
        first = min(date.fromisoformat(x) for x in self.scope.affected_ds)
        self.bronze = Bronze(lake / "bronze", first - timedelta(days=cfg.history_days))
        self.max_items, self.item_max_chars = max_items, item_max_chars
        self.traceback_max_chars, self.output_max_chars = traceback_max_chars, output_max_chars
        self.handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
            "get_error_logs": self.get_error_logs,
            "get_schema_diff": self.get_schema_diff,
            "get_recent_deploys": self.get_recent_deploys,
            "get_upstream_status": self.get_upstream_status,
            "get_metric_history": self.get_metric_history,
            "get_dq_failures": self.get_dq_failures,
        }

    # --- dispatch ------------------------------------------------------------------

    def call(self, name: str, args: dict[str, Any]) -> tuple[str, bool]:
        """Returns (JSON text, is_error). Never raises for a bad call."""
        handler = self.handlers.get(name)
        if handler is None:
            return self._render(
                {"error": f"no such tool {name!r}; tools are read-only: {sorted(self.handlers)}"}
            ), True
        try:
            return self._render({"tool": name, **handler(args or {})}), False
        except ToolError as exc:
            return self._render({"tool": name, "error": str(exc)}), True

    def _render(self, out: dict[str, Any]) -> str:
        text = json.dumps(out, default=str, ensure_ascii=False)
        items = out.get("untrusted_data")
        while len(text) > self.output_max_chars and isinstance(items, list) and items:
            items.pop()
            out["truncated_items"] = out.get("truncated_items", 0) + 1
            text = json.dumps(out, default=str, ensure_ascii=False)
        return text[: self.output_max_chars]

    def _c(self, text: str | None, limit: int | None = None) -> str:
        return clean(text, limit or self.item_max_chars)

    # --- tools ---------------------------------------------------------------------

    def get_error_logs(self, args: dict[str, Any]) -> dict[str, Any]:
        pipeline, ds = self.scope.resolve(args)
        task = args.get("task_id")
        limit = min(int(args.get("limit") or self.max_items), self.max_items)
        items = []
        for r in self.bronze.rows("job_logs"):
            p = r["payload"]
            if (r["pipeline"], r["ds"]) != (pipeline, ds) or p.get("level") not in LOG_LEVELS:
                continue
            if task and r["task_id"] != task:
                continue
            item = {
                "at": r["emitted_at"],
                "task_id": r["task_id"],
                "try_number": r["try_number"],
                "level": p.get("level"),
                "logger": p.get("logger"),
                "exc_type": p.get("exc_type"),
                "message": self._c(p.get("message")),
            }
            # Scanned before truncation, so a fatal JVM error deep in the traceback text
            # (or kept by the callback ahead of the tail) is never cut away.
            text = "\n".join(filter(None, (p.get("message"), p.get("traceback"))))
            fatal = terminal_error_lines(text.splitlines())
            if fatal:
                item["terminal_errors"] = [self._c(line) for line in fatal]
            item["traceback_tail"] = self._c(p.get("traceback"), self.traceback_max_chars)
            items.append(item)
        for r in self.bronze.rows("job_metrics"):
            p = r["payload"]
            if (r["pipeline"], r["ds"]) != (pipeline, ds) or (task and r["task_id"] != task):
                continue
            if p.get("kind") == "task_run" and p.get("state") == "failed":
                items.append(
                    {
                        "at": r["emitted_at"],
                        "task_id": r["task_id"],
                        "try_number": r["try_number"],
                        "level": "TASK_FAILED",
                        "message": self._c(p.get("exception")),
                    }
                )
        items.sort(key=lambda x: x["at"], reverse=True)
        return {
            "scope": {"pipeline": pipeline, "ds": ds, "task_id": task},
            "count": len(items),
            "untrusted_data": items[:limit],
        }

    def _jobs(self, pipeline: str) -> list[dict[str, Any]]:
        return [
            r
            for r in self.bronze.rows("job_metrics")
            if r["pipeline"] == pipeline and r["payload"].get("kind") == "job_metric"
        ]

    def get_schema_diff(self, args: dict[str, Any]) -> dict[str, Any]:
        pipeline, ds = self.scope.resolve(args)
        out = []
        by_job: dict[str, list[dict[str, Any]]] = {}
        for r in self._jobs(pipeline):
            if r["payload"].get("input_schema"):
                by_job.setdefault(r["payload"]["job_name"], []).append(r)
        for job, runs in by_job.items():
            for run in (r for r in runs if r["ds"] == ds):
                good = [
                    r
                    for r in runs
                    if r["payload"]["status"] == "success" and r["emitted_at"] < run["emitted_at"]
                ]
                cols = tuple((c["name"], c["type"]) for c in run["payload"]["input_schema"])
                entry = {
                    "job_name": job,
                    "run_at": run["emitted_at"],
                    "status": run["payload"]["status"],
                    "code_version": run["code_version"],
                    "columns": [f"{n}:{t}" for n, t in cols],
                }
                if good:
                    base = good[-1]
                    bcols = tuple((c["name"], c["type"]) for c in base["payload"]["input_schema"])
                    d = diff(bcols, cols)
                    entry["baseline"] = {
                        "ds": base["ds"],
                        "run_at": base["emitted_at"],
                        "code_version": base["code_version"],
                        "columns": [f"{n}:{t}" for n, t in bcols],
                    }
                    entry["diff"] = {
                        "removed": list(d.removed),
                        "added": list(d.added),
                        "retyped": [f"{c}:{a}->{b}" for c, a, b in d.retyped],
                    }
                else:
                    entry["baseline"] = None
                out.append(entry)
        return {"scope": {"pipeline": pipeline, "ds": ds}, "untrusted_data": out[-self.max_items :]}

    def get_recent_deploys(self, args: dict[str, Any]) -> dict[str, Any]:
        pipeline, ds = self.scope.resolve(args)
        days = max(1, min(int(args.get("days") or 14), 30))
        cutoff: datetime = self.scope.incident["last_seen"]
        lo = cutoff - timedelta(days=days)
        deploys = [
            {
                "at": r["emitted_at"],
                "code_version": r["payload"].get("code_version"),
                "previous_version": r["payload"].get("previous_version"),
                "git_sha": r["payload"].get("git_sha"),
                "dirty": r["payload"].get("dirty"),
                "author": self._c(r["payload"].get("author"), 80),
                "message": self._c(r["payload"].get("message"), 200),
                "changed_files": r["payload"].get("changed_files", [])[:30],
            }
            for r in self.bronze.rows("deploy_events")
            if r["payload"].get("component") == pipeline and lo <= r["emitted_at"] <= cutoff
        ]
        versions = sorted(
            {(r["ds"], r["code_version"]) for r in self._jobs(pipeline) if r["ds"] <= ds},
            reverse=True,
        )[:10]
        return {
            "scope": {"pipeline": pipeline, "days": days, "until": cutoff},
            "untrusted_data": deploys[-self.max_items :][::-1],
            "code_versions_by_ds": [{"ds": d, "code_version": v} for d, v in versions],
        }

    def get_upstream_status(self, args: dict[str, Any]) -> dict[str, Any]:
        pipeline, ds = self.scope.resolve(args)
        incidents = read_incidents(incidents_path(self.lake))
        out = []
        for up in self.cfg.pipelines[pipeline].upstream:
            final = self.cfg.pipelines[up].final_task
            runs = [
                r
                for r in self.bronze.rows("job_metrics")
                if (r["pipeline"], r["ds"]) == (up, ds) and r["payload"].get("kind") == "task_run"
            ]
            latest: dict[str, dict[str, Any]] = {}
            for r in runs:
                latest[r["task_id"]] = r
            out.append(
                {
                    "pipeline": up,
                    "final_task": final,
                    "final_task_state": latest[final]["payload"]["state"]
                    if final in latest
                    else "no run seen",
                    "tasks": {t: r["payload"]["state"] for t, r in sorted(latest.items())},
                    "open_incidents": [
                        {
                            "incident_id": i["incident_id"],
                            "type": i["type"],
                            "title": self._c(i["title"], 300),
                        }
                        for i in incidents
                        if i["pipeline"] == up and i["status"] in ("open", "retrying")
                    ],
                }
            )
        return {
            "scope": {"pipeline": pipeline, "ds": ds},
            "upstream": list(self.cfg.pipelines[pipeline].upstream),
            "untrusted_data": out,
        }

    def get_metric_history(self, args: dict[str, Any]) -> dict[str, Any]:
        pipeline, ds = self.scope.resolve(args)
        days = max(1, min(int(args.get("days") or 28), self.cfg.history_days))
        lo = (date.fromisoformat(ds) - timedelta(days=days)).isoformat()
        rows = []
        for r in self._jobs(pipeline):
            if not lo <= r["ds"] <= ds:
                continue
            p = r["payload"]
            rows.append(
                {
                    "ds": r["ds"],
                    "job_name": p["job_name"],
                    "status": p["status"],
                    "rows_in": p.get("rows_in"),
                    "rows_out": p.get("rows_out"),
                    "duration_s": p.get("duration_s"),
                    "error_type": p.get("error_type"),
                    "code_version": r["code_version"],
                    "at": r["emitted_at"],
                }
            )
        rows.sort(key=lambda x: x["at"], reverse=True)
        return {
            "scope": {"pipeline": pipeline, "ds": ds, "days": days},
            "untrusted_data": rows[: self.max_items * 2],
        }

    def get_dq_failures(self, args: dict[str, Any]) -> dict[str, Any]:
        pipeline, ds = self.scope.resolve(args)
        out = []
        for r in self.bronze.rows("dq_results"):
            if (r["pipeline"], r["ds"]) != (pipeline, ds):
                continue
            p = r["payload"]
            out.append(
                {
                    "at": r["emitted_at"],
                    "dataset": p["dataset"],
                    "success": p["success"],
                    "row_count": p["row_count"],
                    "failed_expectations": [
                        self._expectation(e) for e in p["results"] if not e["success"]
                    ],
                }
            )
        return {
            "scope": {"pipeline": pipeline, "ds": ds},
            "untrusted_data": out[-self.max_items :][::-1],
        }

    def _expectation(self, e: dict[str, Any]) -> dict[str, Any]:
        """One failed expectation. Column-map expectations (values in set, between, ...)
        have no single observed value: GX reports None, which used to reach the model as
        the string "null" and read as null data. Report what was actually there instead:
        a sample of the unexpected values and the null count (events from 1.2 on)."""
        out: dict[str, Any] = {
            "expectation_type": e["expectation_type"],
            "column": e.get("column"),
        }
        if e.get("observed_value") is not None:
            out["observed_value"] = self._c(json.dumps(e["observed_value"], default=str), 200)
        out["unexpected_count"] = e.get("unexpected_count")
        if e.get("unexpected_values") is not None:
            out["unexpected_values_sample"] = [
                {"value": self._c(json.dumps(v.get("value"), default=str), 80), "count": v["count"]}
                for v in e["unexpected_values"]
            ]
        if e.get("missing_count") is not None:
            out["null_count"] = e["missing_count"]
        return out


_SCOPE_PROPS = {
    "pipeline": {
        "type": "string",
        "description": "Pipeline (DAG id). Default: the incident's pipeline.",
    },
    "ds": {
        "type": "string",
        "description": "Business date YYYY-MM-DD. Default: the incident's latest affected ds.",
    },
}


def _tool(name: str, description: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "description": description,
        "input_schema": {
            "type": "object",
            "properties": {**_SCOPE_PROPS, **(extra or {})},
            "additionalProperties": False,
        },
    }


TOOL_DEFS: list[dict[str, Any]] = [
    _tool(
        "get_error_logs",
        "ERROR/WARNING log lines and failed task attempts (with traceback tails) for one "
        "pipeline run. Newest first. Redacted and truncated. `terminal_errors`, when present, "
        "lists fatal JVM/process errors (e.g. java.lang.OutOfMemoryError) found anywhere in "
        "the log, even outside the tail.",
        {
            "task_id": {"type": "string", "description": "Only this task."},
            "limit": {"type": "integer", "description": "Max items (<= 20)."},
        },
    ),
    _tool(
        "get_schema_diff",
        "Observed input schema of each job run for the ds, compared with the job's last "
        "successful run before it: removed, added and retyped columns.",
    ),
    _tool(
        "get_recent_deploys",
        "Deploys of this pipeline before the incident (code version, git SHA, changed "
        "files, dirty flag), plus which code version ran for each recent ds.",
        {"days": {"type": "integer", "description": "Look-back in days (<= 30)."}},
    ),
    _tool(
        "get_upstream_status",
        "For each upstream pipeline (from lineage): task states for the ds and any open "
        "incidents on it.",
    ),
    _tool(
        "get_metric_history",
        "Per-run job metrics for recent business dates: rows_in, rows_out, duration, "
        "status, error type, code version. Newest first.",
        {"days": {"type": "integer", "description": "Look-back in days (<= 56)."}},
    ),
    _tool(
        "get_dq_failures",
        "Great Expectations results for the ds: which expectations failed, on which "
        "column, with the observed value (table-level checks) or a sample of the "
        "unexpected values and the null count (column-value checks).",
    ),
]
