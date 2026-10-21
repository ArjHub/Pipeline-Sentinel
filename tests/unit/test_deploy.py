from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from sentinel import deploy
from sentinel.events import MemorySink
from sentinel.instrumentation import run_context_from_env

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture
def tree(tmp_path) -> Path:
    """A copy of the real pipeline code, so tests can edit files freely."""
    for rel in ("pipelines", "src/sentinel", "requirements"):
        shutil.copytree(REPO / rel, tmp_path / rel, ignore=shutil.ignore_patterns("__pycache__"))
    return tmp_path


def _record(tree: Path, state: Path, sha="aaa1111", dirty=False) -> dict[str, list[str]]:
    sink = MemorySink()
    deploy.record(tree, sink, git_sha=sha, dirty=dirty, author="dev", message="m", path=state)
    return {e.pipeline: e.payload.changed_files for e in sink.events}


def test_first_deploy_emits_one_event_per_pipeline(tree, tmp_path) -> None:
    emitted = _record(tree, tmp_path / "state.json")
    assert set(emitted) == set(deploy.PIPELINES)
    assert "pipelines/jobs/orders_ingest.py" in emitted["orders_ingest"]
    assert "pipelines/jobs/orders_ingest.py" not in emitted["payments_etl"]


def test_redeploy_without_changes_emits_nothing(tree, tmp_path) -> None:
    state = tmp_path / "state.json"
    _record(tree, state)
    assert _record(tree, state, sha="bbb2222") == {}


def test_job_change_is_attributed_only_to_its_pipeline(tree, tmp_path) -> None:
    state = tmp_path / "state.json"
    _record(tree, state)
    job = tree / "pipelines/jobs/orders_ingest.py"
    job.write_text(job.read_text() + "\n# regression\n")

    sink = MemorySink()
    deploy.record(tree, sink, git_sha="aaa1111", dirty=True, path=state)
    [ev] = sink.events
    assert ev.event_type.value == "deploy_event"
    assert ev.topic == "deploy_events"
    assert ev.pipeline == "orders_ingest"
    assert ev.payload.changed_files == ["pipelines/jobs/orders_ingest.py"]
    assert ev.payload.previous_version == "aaa1111"
    assert ev.payload.code_version.startswith("aaa1111-dirty.")
    assert ev.payload.dirty is True
    assert deploy.current_code_version("orders_ingest", state) == ev.payload.code_version
    assert deploy.current_code_version("payments_etl", state) == "aaa1111"


def test_shared_module_change_redeploys_every_pipeline(tree, tmp_path) -> None:
    state = tmp_path / "state.json"
    _record(tree, state)
    shared = tree / "pipelines/jobs/spark.py"
    shared.write_text(shared.read_text() + "\n# tweak\n")
    emitted = _record(tree, state, sha="ccc3333")
    assert set(emitted) == set(deploy.PIPELINES)
    assert all(files == ["pipelines/jobs/spark.py"] for files in emitted.values())


def test_deleted_file_counts_as_a_change(tree, tmp_path) -> None:
    state = tmp_path / "state.json"
    _record(tree, state)
    (tree / "pipelines/jobs/daily_revenue_agg.py").unlink()
    assert _record(tree, state)["daily_revenue_agg"] == ["pipelines/jobs/daily_revenue_agg.py"]


def test_two_different_dirty_edits_get_different_versions(tree, tmp_path) -> None:
    files = deploy.pipeline_files(tree, "orders_ingest")
    v1 = deploy.version_for("aaa1111", True, files)
    files["pipelines/jobs/orders_ingest.py"] = "different"
    assert deploy.version_for("aaa1111", True, files) != v1
    assert deploy.version_for("aaa1111", False, files) == "aaa1111"


def test_events_carry_the_deployed_code_version(tree, tmp_path, monkeypatch) -> None:
    state = tmp_path / "state.json"
    _record(tree, state, sha="ddd4444")
    monkeypatch.setenv("SENTINEL_DEPLOY_STATE", str(state))
    monkeypatch.delenv("SENTINEL_CODE_VERSION", raising=False)
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "customers_dim")
    assert run_context_from_env().code_version == "ddd4444"
    monkeypatch.setenv("AIRFLOW_CTX_DAG_ID", "never_deployed")
    assert run_context_from_env().code_version == "unknown"


def test_missing_or_corrupt_state_is_treated_as_no_deploys(tmp_path) -> None:
    bad = tmp_path / "state.json"
    bad.write_text("{not json")
    assert deploy.current_code_version("orders_ingest", bad) is None
    assert deploy.current_code_version("orders_ingest", tmp_path / "missing.json") is None
