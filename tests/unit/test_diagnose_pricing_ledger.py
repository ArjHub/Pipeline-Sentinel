"""Prices are the ones on the pricing page (2026-10-23); the ledger enforces the cap."""

from __future__ import annotations

import json
import threading

import pytest

from sentinel.diagnose.ledger import BudgetExceededError, Ledger
from sentinel.diagnose.pricing import UnknownModelError, Usage, cost_usd, worst_case_usd


def test_haiku_standard_prices() -> None:
    # 100,000 is the top of the standard tier ("prompts up to 100,000 tokens").
    assert cost_usd("claude-haiku-5-5", Usage(input_tokens=100_000)) == pytest.approx(0.01)
    assert cost_usd("claude-haiku-5-5", Usage(output_tokens=1_000_000)) == pytest.approx(0.50)
    assert cost_usd("claude-haiku-5-5", Usage(cache_read_input_tokens=50_000)) == pytest.approx(
        50_000 * 0.01 / 1e6
    )
    assert cost_usd("claude-haiku-5-5", Usage(cache_creation_input_tokens=80_000)) == pytest.approx(
        80_000 * 0.125 / 1e6
    )


def test_haiku_long_prompt_tier_counts_cache_tokens_too() -> None:
    # 60k uncached + 50k cache read = 110k prompt -> the > 100K tier for the whole request.
    u = Usage(input_tokens=60_000, cache_read_input_tokens=50_000, output_tokens=1_000)
    assert cost_usd("claude-haiku-5-5", u) == pytest.approx(
        (60_000 * 0.50 + 50_000 * 0.05 + 1_000 * 2.50) / 1e6
    )


def test_sonnet_prices() -> None:
    u = Usage(
        input_tokens=10_000,
        output_tokens=2_000,
        cache_creation_input_tokens=4_000,
        cache_read_input_tokens=20_000,
    )
    assert cost_usd("claude-sonnet-5-5", u) == pytest.approx(
        (10_000 * 2.0 + 2_000 * 10.0 + 4_000 * 2.5 + 20_000 * 0.10) / 1e6
    )


def test_unknown_model_has_no_price_instead_of_a_guess() -> None:
    with pytest.raises(UnknownModelError):
        cost_usd("claude-opus-5-5", Usage(input_tokens=1))


def test_worst_case_uses_the_dearest_input_rate() -> None:
    assert worst_case_usd("claude-haiku-5-5", 90_000, 16_000) == pytest.approx(
        (90_000 * 0.125 + 16_000 * 0.50) / 1e6
    )


def test_usage_from_sdk_like_object_and_sum() -> None:
    class U:
        input_tokens, output_tokens = 10, 5
        cache_creation_input_tokens = None
        cache_read_input_tokens = 3

    total = Usage.of(U()) + Usage.of({"input_tokens": 1})
    assert total == Usage(11, 5, 0, 3) and total.prompt_tokens == 14


def test_ledger_records_real_usage_and_refuses_past_the_budget(tmp_path) -> None:
    ledger = Ledger(tmp_path / "ledger.json", budget_usd=0.01)
    ledger.reserve(0.009, "first")  # fits
    cost = ledger.record(
        model="claude-haiku-5-5",
        usage=Usage(input_tokens=50_000, output_tokens=4_000),
        purpose="diagnosis",
        ref="INC-1",
    )
    assert cost == pytest.approx(0.007) and ledger.spent() == pytest.approx(0.007)
    with pytest.raises(BudgetExceededError, match=r"spent \$0.0070 \+ worst-case \$0.0040"):
        ledger.reserve(0.004, "second")
    data = json.loads((tmp_path / "ledger.json").read_text())
    assert data["calls"][0]["input_tokens"] == 50_000 and data["budget_usd"] == 0.01


def test_ledger_is_safe_under_concurrent_writers(tmp_path) -> None:
    path = tmp_path / "ledger.json"

    def write():
        for _ in range(10):
            Ledger(path).record(
                model="claude-haiku-5-5", usage=Usage(output_tokens=1000), purpose="test", ref="x"
            )

    threads = [threading.Thread(target=write) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    data = json.loads(path.read_text())
    assert len(data["calls"]) == 40
    assert data["spent_usd"] == pytest.approx(40 * 1000 * 0.5 / 1e6)
