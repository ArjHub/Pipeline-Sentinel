"""Draw the concrete faults from the templates with a fixed seed.

The slot plan says how many faults of which template go in each split. Within a slot
the parameters are drawn at random without repeats, and no (template, parameters) pair
is used twice anywhere, so the dev set never contains a benchmark fault. Business dates
are distinct and fault ids are random opaque strings, assigned after shuffling.

Prints counts only, never the drawn faults.
"""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from typing import Any

from benchmark.harness.templates import TEMPLATES

ID_ALPHABET = "ABCDEFGHJKMNPQRSTVWXYZ23456789"

# (template keys to choose from, how many, parameter that must differ within the slot)
BENCHMARK_SLOTS: list[tuple[tuple[str, ...], int, str | None]] = [
    (("schema.drop",), 1, None),
    (("schema.rename",), 1, None),
    (("schema.retype",), 1, None),
    (("schema.add",), 1, None),
    (("schema.drop", "schema.rename", "schema.retype"), 1, None),
    (("late.stale_snapshot",), 1, None),
    (("late.truncated",), 1, None),
    (("late.previous_day_file",), 1, None),
    (("oom.direct_memory",), 3, "task"),
    (("deploy.code_regression",), 4, "variant"),
    (("upstream.dag_missing",), 1, None),
    (("upstream.dag_late",), 1, None),
    (("upstream.source_unreachable",), 1, None),
    (("dq.null_spike",), 1, None),
    (("dq.duplicate_load",), 1, None),
    (("dq.out_of_range",), 1, None),
    (("dq.invalid_values",), 1, None),
    (("amb.permissions",), 1, None),
    (("amb.business_dip",), 1, None),
    (("amb.timezone_shift",), 1, None),
    (("clean.none",), 5, None),
]
DEV_SLOTS: list[tuple[tuple[str, ...], int, str | None]] = [
    (("schema.drop", "schema.rename", "schema.retype", "schema.add"), 1, None),
    (("late.stale_snapshot", "late.truncated", "late.previous_day_file"), 1, None),
    (("oom.direct_memory",), 1, None),
    (("deploy.code_regression",), 1, None),
    (("upstream.dag_missing", "upstream.dag_late", "upstream.source_unreachable"), 1, None),
    (("dq.null_spike", "dq.duplicate_load", "dq.out_of_range", "dq.invalid_values"), 1, None),
    (("amb.gzip_as_csv",), 1, None),
    (("clean.none",), 2, None),
]
# Kept out of the benchmark so the dev set has a regression of its own.
DEV_ONLY_VARIANTS = {"orders_currency_case"}


def _key(template: str, params: dict[str, Any]) -> str:
    return template + json.dumps(params, sort_keys=True)


def _draw(
    rng: random.Random,
    slots: list[tuple[tuple[str, ...], int, str | None]],
    used: set[str],
    split: str,
) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    for keys, count, distinct in slots:
        seen: set[Any] = set()
        for _ in range(count):
            pool = [
                (k, p)
                for k in keys
                for p in TEMPLATES[k].options()
                if (k == "clean.none" or _key(k, p) not in used)
                and (distinct is None or p.get(distinct) not in seen)
                and not (
                    k == "deploy.code_regression"
                    and (p["variant"] in DEV_ONLY_VARIANTS) != (split == "dev")
                )
            ]
            if not pool:
                raise ValueError(f"no parameters left for slot {keys}")
            k, p = rng.choice(pool)
            used.add(_key(k, p))
            if distinct:
                seen.add(p[distinct])
            flavor = TEMPLATES[k].flavor
            out.append((k, {**p, **(flavor(rng) if flavor else {})}))
    return out


def _fault_id(rng: random.Random, taken: set[str]) -> str:
    while True:
        fid = "F-" + "".join(rng.choice(ID_ALPHABET) for _ in range(6))
        if fid not in taken:
            taken.add(fid)
            return fid


def generate(seed: int, first_ds: str, last_ds: str) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    used: set[str] = set()
    drawn = [("benchmark", k, p) for k, p in _draw(rng, BENCHMARK_SLOTS, used, "benchmark")]
    drawn += [("dev", k, p) for k, p in _draw(rng, DEV_SLOTS, used, "dev")]
    lo, hi = date.fromisoformat(first_ds), date.fromisoformat(last_ds)
    days = [lo + timedelta(days=i) for i in range((hi - lo).days + 1)]
    dates = rng.sample(days, len(drawn))
    rng.shuffle(drawn)
    ids: set[str] = set()
    entries = []
    for (split, key, params), d in zip(drawn, dates, strict=True):
        t = TEMPLATES[key]
        entries.append(
            {
                "fault_id": _fault_id(rng, ids),
                "split": split,
                "ds": d.isoformat(),
                "template": key,
                "params": params,
                "category": t.category,
                "acceptable": list(t.acceptable),
                "root_cause": t.root_cause(params),
            }
        )
    return entries
