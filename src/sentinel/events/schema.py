"""Export the event contract as JSON Schema: `python -m sentinel.events.schema`.

The committed file (schemas/event.schema.json) is for consumers that don't use Python.
A test fails if it drifts from the Pydantic models, so the two can't disagree silently.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from sentinel.events.models import SCHEMA_VERSION, Event

SCHEMA_FILE = Path(__file__).resolve().parents[3] / "schemas" / "event.schema.json"


def json_schema() -> dict:
    schema = Event.model_json_schema()
    schema["$id"] = f"https://github.com/ArjHub/Pipeline-Sentinel/schemas/event/{SCHEMA_VERSION}"
    schema["title"] = f"PipelineSentinel event v{SCHEMA_VERSION}"
    return schema


def render() -> str:
    return json.dumps(json_schema(), indent=2, sort_keys=True) + "\n"


def main() -> int:
    SCHEMA_FILE.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_FILE.write_text(render())
    print(f"wrote {SCHEMA_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
