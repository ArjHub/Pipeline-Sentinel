"""The sealed fault catalog: reproducible, complete, opaque.

These assertions compare counts and booleans only, so a failure never prints a label
(pytest shows the values in a failed comparison). Don't "improve" them into
`assert a == b` over catalog entries.
"""

from __future__ import annotations

import base64
import re
from collections import Counter
from datetime import date

from benchmark.harness import catalog, gen_catalog
from benchmark.harness.settings import load
from benchmark.harness.templates import TEMPLATES
from sentinel.diagnose.schema import CATEGORIES

S = load()
FAULTS = catalog.load(S.catalog_path)
BENCH = [f for f in FAULTS if f.split == "benchmark"]
DEV = [f for f in FAULTS if f.split == "dev"]


def test_committed_catalog_is_a_fresh_seeded_draw() -> None:
    fresh = catalog.encode(gen_catalog.generate(S.catalog_seed, S.first_ds, S.last_ds))
    same = S.catalog_path.read_bytes() == fresh
    assert same, "benchmark/sealed/catalog.b64 differs from `make benchmark-catalog`"


def test_sizes() -> None:
    n_bench, n_dev = len(BENCH), len(DEV)
    assert (n_bench, n_dev) == (30, 9)


def test_required_coverage_of_the_benchmark() -> None:
    sub = Counter(f.template for f in BENCH)
    missing = sum(
        sub[k] == 0
        for k in (
            "schema.drop",
            "schema.rename",
            "schema.retype",
            "schema.add",
            "late.stale_snapshot",
            "upstream.dag_missing",
            "dq.null_spike",
            "dq.duplicate_load",
            "dq.out_of_range",
        )
    )
    cat = Counter(f.label.category for f in BENCH)
    families = Counter(f.template.split(".")[0] for f in BENCH)
    unknown_ok = sum("unknown" in (f.label.category, *f.label.acceptable) for f in BENCH)
    ok = (
        missing == 0
        and all(cat[c] >= 1 for c in CATEGORIES if c != "unknown")
        and families["amb"] >= 3
        and unknown_ok >= 3
        and cat["none"] >= 5
    )
    assert ok, "benchmark coverage requirements not met"


def test_dev_set_is_disjoint_from_the_benchmark() -> None:
    def key(f):
        return (f.template, tuple(sorted((k, str(v)) for k, v in f.params.items())))

    bench_keys = {key(f) for f in BENCH if not f.is_clean}
    overlap = sum(key(f) in bench_keys for f in DEV if not f.is_clean)
    ids_overlap = len({f.fault_id for f in BENCH} & {f.fault_id for f in DEV})
    assert (overlap, ids_overlap) == (0, 0)
    assert 8 <= len(DEV) <= 10


def test_ids_are_opaque_and_dates_distinct() -> None:
    pattern = re.compile(r"F-[A-Z2-9]{6}")
    bad_ids = sum(not pattern.fullmatch(f.fault_id) for f in FAULTS)
    assert bad_ids == 0
    assert len({f.fault_id for f in FAULTS}) == len(FAULTS)
    assert len({f.ds for f in FAULTS}) == len(FAULTS)
    lo, hi = date.fromisoformat(S.first_ds), date.fromisoformat(S.last_ds)
    assert sum(not lo <= date.fromisoformat(f.ds) <= hi for f in FAULTS) == 0
    # history (42 days) must end before today, and fault dates must be in the past
    assert date.fromisoformat(S.last_ds) < date.today()


def test_every_entry_has_a_valid_label_and_recipe() -> None:
    valid = {*CATEGORIES, "none"}
    bad = 0
    for f in FAULTS:
        t = TEMPLATES[f.template]
        r = f.recipe  # builds without error
        bad += f.label.category not in valid
        bad += any(a not in CATEGORIES for a in f.label.acceptable)
        bad += not f.label.root_cause
        bad += f.label.category != t.category
        bad += (r.expect_pipeline is None) != f.is_clean
    assert bad == 0


def test_sealed_file_has_no_readable_labels() -> None:
    raw = S.catalog_path.read_bytes()
    plain = raw.decode()
    leaked = sum(c in plain for c in (*CATEGORIES, "schema", "deploy", "root_cause", "F-"))
    assert leaked == 0
    base64.b64decode(b"".join(raw.split()))  # valid base64


def test_load_by_split() -> None:
    assert len(catalog.load(S.catalog_path, "benchmark")) == 30
    assert len(catalog.load(S.catalog_path, "all")) == 39
