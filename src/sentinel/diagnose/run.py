"""Incident id -> diagnosis -> diagnoses table -> Slack. Used by `make diagnose` and by
the detector service (DiagnosisWorker, for every new primary incident)."""

from __future__ import annotations

import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sentinel.detect.config import load_config as load_detection_config
from sentinel.detect.correlate import Incident
from sentinel.detect.store import incidents_path, read_incidents
from sentinel.diagnose import store
from sentinel.diagnose.agent import Agent, DiagnosisRun
from sentinel.diagnose.config import DiagnosisConfig, load_config
from sentinel.diagnose.ledger import Ledger
from sentinel.diagnose.llm import LLMClient, llm_from_env
from sentinel.diagnose.tools import Toolbox
from sentinel.notify.slack import Notifier, incident_message, notifier_from_env

log = logging.getLogger("sentinel.diagnose")


class NotDiagnosable(ValueError):
    pass


@dataclass
class Outcome:
    run: DiagnosisRun | None
    row: dict[str, Any] | None
    message_id: str | None


def find_incident(lake: Path, incident_id: str) -> dict[str, Any]:
    for r in read_incidents(incidents_path(lake)):
        if r["incident_id"] == incident_id:
            if r["role"] != "primary":
                raise NotDiagnosable(
                    f"{incident_id} is a downstream incident; diagnose its root "
                    f"{r['root_incident_id']} instead"
                )
            return r
    raise NotDiagnosable(f"no incident {incident_id!r} in {incidents_path(lake)}")


def diagnose_incident(
    lake: Path,
    incident_id: str,
    *,
    cfg: DiagnosisConfig | None = None,
    llm: LLMClient | None = None,
    notifier: Notifier | None = None,
    ledger: Ledger | None = None,
    notify: bool = True,
    dry_run: bool = False,
) -> Outcome:
    cfg = cfg or load_config()
    incident = find_incident(lake, incident_id)
    llm = llm or llm_from_env(cfg.agent.model)
    row = run = None
    if llm is None:
        log.warning(
            "no LLM configured (ANTHROPIC_API_KEY unset and SENTINEL_LLM != mock); "
            "%s is posted without a diagnosis",
            incident_id,
        )
    else:
        t = cfg.tools
        toolbox = Toolbox(
            lake,
            load_detection_config(),
            incident,
            ds_window_days=t.ds_window_days,
            max_items=t.max_items,
            item_max_chars=t.item_max_chars,
            traceback_max_chars=t.traceback_max_chars,
            output_max_chars=t.output_max_chars,
        )
        agent = Agent(cfg, llm, ledger or Ledger(budget_usd=cfg.budget_usd))
        run = agent.diagnose(incident, toolbox, dry_run=dry_run)
        if dry_run:
            return Outcome(run, None, None)
        row = store.to_row(run, cfg.prompt_text)
        store.append(lake, row)
    mid = None
    if notify:
        mid = (notifier or notifier_from_env(lake)).post(incident_message(incident, row))
        log.info("posted %s to Slack (%s)", incident_id, mid)
    return Outcome(run, row, mid)


class DiagnosisWorker:
    """Runs diagnoses one at a time in the background, so a slow API call never delays
    the next detection pass. Errors are logged; the detector carries on."""

    def __init__(self, lake: Path) -> None:
        self.lake = lake
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="diagnose")

    def submit(self, incident: Incident) -> Future:
        return self._pool.submit(self._run, incident.incident_id)

    def _run(self, incident_id: str) -> None:
        try:
            diagnose_incident(self.lake, incident_id)
        except Exception:
            log.exception("diagnosis of %s failed", incident_id)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)


def auto_diagnose_enabled() -> bool:
    return os.environ.get("SENTINEL_AUTO_DIAGNOSE", "1") != "0"
