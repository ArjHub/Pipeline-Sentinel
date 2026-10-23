from __future__ import annotations

import statistics
from datetime import date, timedelta

import pytest

from pipelines.generators.datasets import customer_count, generate_orders, generate_payments
from sentinel.detect import from_events, load_config, volume
from sentinel.detect.volume import MAD_TO_SIGMA, assess
from tests.unit.detect_fixtures import Run, end_of, history

CFG = load_config()
V = CFG.volume
SAT = date(2026, 10, 24)
WEEKDAY = (1.0, 0.95, 0.97, 1.0, 1.1, 1.3, 1.25)


def weeks(n: int, target: date = SAT, noise=(0.99, 1.01, 1.0, 0.98, 1.02, 1.0, 0.99, 1.01)):
    """n weeks of history before `target` with the generator's weekly cycle."""
    return [
        (d, 2000 * WEEKDAY[d.weekday()] * noise[i % len(noise)])
        for i in range(n * 7, 0, -1)
        if (d := target - timedelta(days=i))
    ]


def test_normal_saturday_passes_against_same_weekday_history() -> None:
    a = assess(2600, SAT, weeks(5), V)
    assert a.method == "robust_z_same_weekday" and a.points == 5
    assert not a.anomalous


def _all_days_z(value: float, hist) -> float:
    values = [v for _, v in hist]
    med = statistics.median(values)
    return (value - med) / (MAD_TO_SIGMA * statistics.median(abs(v - med) for v in values))


def test_why_same_weekday_pooled_history_hides_a_real_saturday_drop() -> None:
    """The design rationale, made executable: pooled over all days, the weekly cycle
    puts a normal Saturday near the threshold and hides a 20% Saturday drop."""
    hist = weeks(5)
    assert _all_days_z(2600, hist) > 2  # normal day, already most of the way to 3.5
    dropped = 2600 * 0.8
    assert abs(_all_days_z(dropped, hist)) < 1  # pooled: the drop is invisible
    assert assess(dropped, SAT, hist, V).anomalous  # same weekday: caught


def test_drop_is_flagged_with_robust_z() -> None:
    a = assess(800, SAT, weeks(5), V)
    assert a.anomalous and a.direction == "drop" and a.z < -V.z_threshold


def test_a_past_incident_in_history_does_not_mask_the_next_one() -> None:
    hist = weeks(5)
    hist = [(d, v * 0.3) if d == SAT - timedelta(days=7) else (d, v) for d, v in hist]
    assert assess(1300, SAT, hist, V).anomalous  # -50% still caught; mean/std would widen


def test_relative_change_floor_ignores_tiny_but_significant_shifts() -> None:
    tight = [(SAT - timedelta(days=7 * i), 2600.0) for i in range(1, 6)]
    a = assess(2600 * 1.10, SAT, tight, V)  # z is huge (MAD floor), but only +10%
    assert abs(a.z) > V.z_threshold and not a.anomalous


def test_spike_is_flagged() -> None:
    a = assess(2600 * 2, SAT, weeks(5), V)
    assert a.anomalous and a.direction == "spike"


@pytest.mark.parametrize(("value", "flag"), [(1300, False), (1000, True), (6000, True)])
def test_cold_start_flags_only_big_changes(value, flag) -> None:
    a = assess(value, SAT, weeks(1)[-2:], V)  # two days of history
    assert a.method == "cold_start_ratio" and a.anomalous is flag


@pytest.mark.parametrize(("value", "flag"), [(0, True), (5, False), (2000, False)])
def test_no_history_flags_only_empty_output(value, flag) -> None:
    a = assess(value, SAT, [], V)
    assert (a.method, a.anomalous) == ("no_history", flag)


def test_zero_baseline() -> None:
    zeros = [(SAT - timedelta(days=7 * i), 0.0) for i in range(1, 4)]
    assert not assess(0, SAT, zeros, V).anomalous
    a = assess(50, SAT, zeros, V)
    assert a.anomalous and a.change == float("inf")


# --- "recent" baseline (cumulative snapshots) ------------------------------------------


def snapshot(target: date, days: int, start: date = date(2026, 1, 1)):
    """customers-like history: flat at 5000 until `start`, then +25 a day."""
    return [
        (d, 5000.0 + 25 * max(0, (d - start).days))
        for i in range(days, 0, -1)
        if (d := target - timedelta(days=i))
    ]


def test_growing_snapshot_is_a_false_positive_against_same_weekday_but_not_recent() -> None:
    """The case that made "recent" necessary (measured on the generator: 5 clean days)."""
    t = date(2026, 2, 3)
    value = 5000.0 + 25 * (t - date(2026, 1, 1)).days
    hist = snapshot(t, 56)
    assert assess(value, t, hist, V, "same_weekday").anomalous
    a = assess(value, t, hist, V, "recent")
    assert a.method == "robust_z_recent" and a.points == 7 and not a.anomalous


def test_recent_baseline_still_catches_a_dropped_snapshot() -> None:
    t = date(2026, 10, 20)
    a = assess(12000 * 0.7, t, snapshot(t, 56), V, "recent")
    assert a.anomalous and a.direction == "drop"


def _year_of(series: dict[date, float], baseline: str) -> list:
    year = [d for d in series if d.year == 2026]
    return [
        (t, a)
        for t in year
        if (
            a := assess(
                series[t],
                t,
                [(d, v) for d, v in series.items() if t - timedelta(CFG.history_days) <= d < t],
                V,
                baseline,
            )
        ).anomalous
    ]


def test_a_year_of_clean_generator_volumes_raises_nothing() -> None:
    """Every day of 2026 (history from Nov 2025, before the generator's epoch), every
    configured metric on its configured baseline: zero anomalies. Same-weekday |z| does
    pass 3.5 on a few normal days; the 15% relative-change floor is what holds them."""
    days = [date(2025, 11, 1) + timedelta(i) for i in range(426)]
    orders = {d: float(len(generate_orders(d, 42))) for d in days}
    series = {
        "orders_ingest": orders,
        "payments_etl": {d: float(len(generate_payments(d, 42))) for d in days},
        "customers_dim": {d: float(customer_count(d)) for d in days},
        "daily_revenue_agg": orders,
    }
    for pipeline, values in series.items():
        assert _year_of(values, CFG.pipelines[pipeline].volume_baseline) == [], pipeline


# --- detect(): history handling --------------------------------------------------------

DS = "2026-10-20"


def _ingest(ds: str, rows: int, status: str = "success", minutes: int = 0, try_number: int = 1):
    r = Run("orders_ingest", ds, end_of(ds) + timedelta(minutes=minutes), try_number)
    r.job("ingest_orders", "orders_ingest", status=status, rows_in=rows, rows_out=rows)
    return r.events


def orders_findings(events):
    return [f for f in volume.detect(from_events(events), CFG) if f.pipeline == "orders_ingest"]


def test_normal_rerun_clears_the_anomaly() -> None:
    events = history(DS, 21) + _ingest(DS, 500) + _ingest(DS, 1900, minutes=60, try_number=2)
    [f] = orders_findings(events)
    assert not f.active and f.evidence["value"] == 500


def test_failed_runs_and_the_judged_day_are_not_history() -> None:
    # A failed run's rows never enter history, and today's anomalous value is not
    # compared against itself.
    events = history(DS, 21) + _ingest(DS, 10, status="failed") + _ingest(DS, 500, minutes=5)
    [f] = orders_findings(events)
    assert f.evidence["history_points"] == 3 and f.evidence["expected"] > 1500


def test_only_configured_metrics_are_checked() -> None:
    # customers_dim.rows_out is "rows changed by MERGE" (0 on a re-run): not a volume signal.
    r = Run("customers_dim", DS, end_of(DS))
    r.job("build_customers_dim", "customers_dim", rows_in=12000, rows_out=0)
    assert volume.detect(from_events(history(DS, 21) + r.events), CFG) == []
