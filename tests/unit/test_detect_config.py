from __future__ import annotations

from datetime import timedelta

import pytest

from sentinel.detect.config import from_dict, load_config, load_raw


def test_real_config_file_loads_with_expected_lineage() -> None:
    cfg = load_config()
    assert set(cfg.pipelines) == {
        "orders_ingest",
        "customers_dim",
        "payments_etl",
        "daily_revenue_agg",
    }
    assert cfg.upstream_closure("daily_revenue_agg") == {
        "orders_ingest",
        "customers_dim",
        "payments_etl",
    }
    assert cfg.upstream_closure("orders_ingest") == set()
    assert (cfg.depth("orders_ingest"), cfg.depth("daily_revenue_agg")) == (0, 1)
    assert cfg.pipelines["customers_dim"].freshness.max_staleness == timedelta(hours=24)
    assert {n: p.volume_baseline for n, p in cfg.pipelines.items()} == {
        "orders_ingest": "same_weekday",
        "customers_dim": "recent",
        "payments_etl": "same_weekday",
        "daily_revenue_agg": "same_weekday",
    }
    # Disabled locally (schedule=None), see the comment in the TOML file.
    assert all(p.arrival_sla is None for p in cfg.pipelines.values())


def test_overrides_are_deep_merged() -> None:
    cfg = load_config(overrides={"volume": {"z_threshold": 9}})
    assert cfg.volume.z_threshold == 9
    assert cfg.volume.min_relative_change == load_config().volume.min_relative_change


def test_missing_key_is_an_error_not_a_silent_default() -> None:
    raw = load_raw()
    del raw["volume"]["z_threshold"]
    with pytest.raises(KeyError):
        from_dict(raw)


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"failed_job": {"severity": "urgent"}}, "severity must be one of"),
        (
            {"pipelines": {"orders_ingest": {"volume_metrics": ["orders_ingest.rows"]}}},
            "bad volume metric",
        ),
        ({"pipelines": {"orders_ingest": {"upstream": ["nope"]}}}, "unknown upstream"),
        ({"pipelines": {"orders_ingest": {"volume_baseline": "weekly"}}}, "volume_baseline"),
    ],
)
def test_invalid_values_are_rejected(patch, message) -> None:
    with pytest.raises(ValueError, match=message):
        load_config(overrides=patch)


def test_transitive_upstream_closure() -> None:
    cfg = load_config(overrides={"pipelines": {"payments_etl": {"upstream": ["orders_ingest"]}}})
    assert "orders_ingest" in cfg.upstream_closure("daily_revenue_agg")
    assert cfg.depth("daily_revenue_agg") == 2
