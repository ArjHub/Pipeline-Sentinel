"""Typed view of config/diagnosis.toml (same rules as detection: every key required)."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PATH = ROOT / "config" / "diagnosis.toml"


@dataclass(frozen=True)
class AgentConfig:
    model: str
    prompt_file: Path
    effort: str
    max_tokens_per_call: int
    max_llm_calls: int
    max_tool_calls: int
    max_input_tokens: int
    max_output_tokens: int
    malformed_retries: int


@dataclass(frozen=True)
class ToolsConfig:
    ds_window_days: int
    max_items: int
    item_max_chars: int
    traceback_max_chars: int
    output_max_chars: int
    incident_evidence_max_chars: int


@dataclass(frozen=True)
class ActionsConfig:
    allowed_dags: tuple[str, ...]
    signature_max_age_seconds: int


@dataclass(frozen=True)
class DiagnosisConfig:
    agent: AgentConfig
    tools: ToolsConfig
    budget_usd: float
    actions: ActionsConfig

    @property
    def prompt_text(self) -> str:
        return self.agent.prompt_file.read_text()

    @property
    def prompt_version(self) -> str:
        """`diagnosis_v1` (file stem) - the version stored with every diagnosis."""
        return self.agent.prompt_file.stem


def prompt_path(version: str) -> Path:
    """`diagnosis_v1` (or `v1`) -> prompts/diagnosis_v1.md. Refuses a missing file."""
    stem = version if version.startswith("diagnosis_") else f"diagnosis_{version}"
    path = ROOT / "prompts" / f"{stem}.md"
    if not path.is_file():
        raise FileNotFoundError(f"no prompt {path} (versions: prompts/diagnosis_*.md)")
    return path


def load_config(
    path: Path | None = None, model: str | None = None, prompt: str | None = None
) -> DiagnosisConfig:
    """`model` and `prompt` (a version such as `diagnosis_v1`) override the file."""
    path = path or Path(os.environ.get("SENTINEL_DIAGNOSIS_CONFIG", DEFAULT_PATH))
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    a = dict(raw["agent"])
    a["model"] = model or os.environ.get("SENTINEL_MODEL") or a["model"]
    a["prompt_file"] = prompt_path(prompt) if prompt else ROOT / a["prompt_file"]
    return DiagnosisConfig(
        agent=AgentConfig(**a),
        tools=ToolsConfig(**raw["tools"]),
        budget_usd=float(raw["budget"]["total_usd"]),
        actions=ActionsConfig(
            allowed_dags=tuple(raw["actions"]["allowed_dags"]),
            signature_max_age_seconds=int(raw["actions"]["signature_max_age_seconds"]),
        ),
    )
