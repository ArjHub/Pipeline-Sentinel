from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.conftest import CLEAN_DS


@pytest.fixture(scope="session")
def clean_lake(spark, tmp_path_factory) -> Iterator[Path]:
    """Run the whole Phase 1 pipeline once, on clean data, into a temp lake."""
    from pipelines.generators import __main__ as gen_cli
    from pipelines.jobs import customers_dim, daily_revenue_agg, orders_ingest, payments_glue_job
    from sentinel.instrumentation import JobStats

    root = tmp_path_factory.mktemp("clean") / "lake"
    mp = pytest.MonkeyPatch()
    mp.setenv("SENTINEL_LAKE_ROOT", str(root))
    mp.setenv("SENTINEL_EVENT_SINK", "none")
    for dataset in ("orders", "customers", "payments"):
        gen_cli.main(["--dataset", dataset, "--ds", CLEAN_DS])
    for job in (orders_ingest, customers_dim, payments_glue_job, daily_revenue_agg):
        job.run(spark, CLEAN_DS, JobStats())
    yield root
    mp.undo()
