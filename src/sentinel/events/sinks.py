"""Where events go. Phase 1 writes JSONL files; Phase 2 adds a Kafka sink behind the
same interface, so producers never change."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

from sentinel.events.models import Event


class EventSink(Protocol):
    def emit(self, event: Event) -> None: ...


class NullSink:
    def emit(self, event: Event) -> None:
        return None


class MemorySink:
    """Collects events in a list; used by tests."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def emit(self, event: Event) -> None:
        self.events.append(event)


class JsonlFileSink:
    """Appends one JSON line per event to <base>/<topic>/<YYYY-MM-DD>.jsonl.

    Each event is written with a single write() on a file opened in append mode, so
    lines from concurrent task processes don't interleave in practice (lines are small).
    """

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)

    def emit(self, event: Event) -> None:
        day = event.emitted_at.strftime("%Y-%m-%d")
        path = self.base_dir / event.topic / f"{day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        line = event.model_dump_json() + "\n"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)


def sink_from_env() -> EventSink:
    kind = os.environ.get("SENTINEL_EVENT_SINK", "file")
    if kind == "none":
        return NullSink()
    if kind == "file":
        return JsonlFileSink(os.environ.get("SENTINEL_EVENTS_DIR", "data/events"))
    raise ValueError(f"Unknown SENTINEL_EVENT_SINK={kind!r} (expected 'file' or 'none')")
