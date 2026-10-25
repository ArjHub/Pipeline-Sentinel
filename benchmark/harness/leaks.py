"""Check that nothing the agent can read gives away the answer.

The agent's tools read bronze and the incidents table, nothing else. This scans every
row of both (all columns, payloads included) for:
  - every fault id, template key and ground-truth root-cause text in the catalog;
  - words that only the harness would write: fault, inject, benchmark, sealed, label,
    ground truth, scenario, harness, catalog, break-it.
Category names are not on the list: the detector legitimately writes `late_data` and
`schema_change` as incident types, and a category name is not an answer.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator
from pathlib import Path

from deltalake import DeltaTable

from benchmark.harness.catalog import Fault
from sentinel.detect.store import incidents_path
from sentinel.lander.bronze import DEAD_LETTER, TOPICS

WORDS = re.compile(
    r"\b(faults?|inject(ed|ion)?|benchmarks?|sealed|labels?|ground[ _-]?truth|scenarios?"
    r"|harness|catalog|break[_-]it)\b",
    re.IGNORECASE,
)


def tokens(faults: Iterable[Fault]) -> list[str]:
    out: set[str] = set()
    for f in faults:
        out |= {f.fault_id, f.template, f.label.root_cause}
    # Anything shorter than a fault id would match ordinary text by chance.
    return sorted((t for t in out if len(t) >= 8), key=len, reverse=True)


def _rows(lake: Path) -> Iterator[tuple[str, str]]:
    tables = [lake / "bronze" / t for t in (*TOPICS, DEAD_LETTER)] + [incidents_path(lake)]
    for path in tables:
        if DeltaTable.is_deltatable(str(path)):
            for row in DeltaTable(str(path)).to_pyarrow_table().to_pylist():
                yield path.name, json.dumps(row, default=str, ensure_ascii=False)


def scan(lake: Path, needles: list[str]) -> list[str]:
    """`table: what` for every hit (empty = clean)."""
    hits = []
    for table, text in _rows(lake):
        for n in needles:
            if n in text:
                hits.append(f"{table}: catalog text")
                break
        if m := WORDS.search(text):
            hits.append(f"{table}: word {m[0]!r}")
    return hits
