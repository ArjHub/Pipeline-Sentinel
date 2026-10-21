from __future__ import annotations

import os
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest

CLEAN_DS = "2026-10-20"  # a Tuesday


@pytest.fixture
def lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "lake"
    monkeypatch.setenv("SENTINEL_LAKE_ROOT", str(root))
    monkeypatch.setenv("SENTINEL_EVENT_SINK", "none")
    return root


@pytest.fixture(scope="session")
def spark() -> Iterator:
    pytest.importorskip("pyspark")
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("Java not available")
    from pipelines.jobs.spark import build_spark

    session = build_spark("pytest")
    yield session
    session.stop()
