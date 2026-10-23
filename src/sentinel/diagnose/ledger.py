"""The LLM cost ledger: benchmark/results/ledger.json. The $10 cap is enforced here.

Every real API call is recorded with its real token usage and cost. Before a
diagnosis (or any other LLM job) starts, `reserve()` refuses to proceed if
spent + worst-case estimate > budget. The estimate is the worst case under the job's
token caps, so a started job can never push spend past the budget.

The file is shared by the detector service, `make diagnose` and (Phase 5) the
benchmark, so every read-modify-write holds an exclusive flock.
"""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sentinel.diagnose.pricing import Usage, cost_usd

DEFAULT_PATH = Path(__file__).resolve().parents[3] / "benchmark" / "results" / "ledger.json"


class BudgetExceededError(RuntimeError):
    pass


def ledger_path() -> Path:
    return Path(os.environ.get("SENTINEL_LEDGER_PATH", DEFAULT_PATH))


class Ledger:
    def __init__(self, path: Path | None = None, budget_usd: float = 10.0) -> None:
        self.path = path or ledger_path()
        self.budget_usd = budget_usd

    @contextmanager
    def _locked(self) -> Iterator[dict[str, Any]]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path.with_suffix(".lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            data = (
                json.loads(self.path.read_text())
                if self.path.exists() and self.path.stat().st_size
                else {"budget_usd": self.budget_usd, "spent_usd": 0.0, "calls": []}
            )
            yield data
            data["spent_usd"] = round(sum(c["cost_usd"] for c in data["calls"]), 8)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2) + "\n")
            tmp.replace(self.path)

    def spent(self) -> float:
        if not self.path.exists():
            return 0.0
        return float(json.loads(self.path.read_text()).get("spent_usd", 0.0))

    def reserve(self, estimate_usd: float, what: str) -> None:
        """Refuse to start a job whose worst case would cross the budget."""
        spent = self.spent()
        if spent + estimate_usd > self.budget_usd:
            raise BudgetExceededError(
                f"refusing {what}: spent ${spent:.4f} + worst-case ${estimate_usd:.4f} "
                f"> budget ${self.budget_usd:.2f} (ledger {self.path})"
            )

    def record(self, *, model: str, usage: Usage, purpose: str, ref: str, **extra: Any) -> float:
        cost = cost_usd(model, usage)
        with self._locked() as data:
            data["calls"].append(
                {
                    "at": datetime.now(UTC).isoformat(),
                    "model": model,
                    "purpose": purpose,
                    "ref": ref,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                    "cost_usd": round(cost, 8),
                    **extra,
                }
            )
        return cost
