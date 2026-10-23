"""Claude API prices, USD per million tokens.

Source: https://platform.claude.com/docs/en/about-claude/pricing (read 2026-10-23).
Standard (global) routing, no batch discount. 5-minute cache writes (the only TTL used
here). Thinking tokens are billed as output tokens and are included in
`usage.output_tokens`.

  Claude Haiku 5.5, prompt <= 100,000 tokens: in 0.10, 5m write 0.125, read 0.01, out 0.50
  Claude Haiku 5.5, prompt  > 100,000 tokens: in 0.50, 5m write 0.625, read 0.05, out 2.50
  Claude Sonnet 5.5 (any prompt length):      in 2.00, 5m write 2.50,  read 0.10, out 10.00

"Prompt length" for the Haiku tier counts every input token of the request, cache reads
and writes included.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PRICING_SOURCE = "https://platform.claude.com/docs/en/about-claude/pricing (2026-10-23)"
HAIKU_LONG_PROMPT_TOKENS = 100_000


@dataclass(frozen=True)
class Price:
    input: float
    cache_write: float
    cache_read: float
    output: float


PRICES: dict[str, tuple[Price, Price | None]] = {
    # model: (standard price, price above HAIKU_LONG_PROMPT_TOKENS or None)
    "claude-haiku-5-5": (Price(0.10, 0.125, 0.01, 0.50), Price(0.50, 0.625, 0.05, 2.50)),
    "claude-sonnet-5-5": (Price(2.00, 2.50, 0.10, 10.00), None),
}


class UnknownModelError(ValueError):
    pass


@dataclass(frozen=True)
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0

    @classmethod
    def of(cls, usage: Any) -> Usage:
        """From an SDK `usage` object or a dict (the fake client)."""
        get = usage.get if isinstance(usage, dict) else lambda k: getattr(usage, k, None)
        return cls(*(int(get(f) or 0) for f in cls.__dataclass_fields__))

    @property
    def prompt_tokens(self) -> int:
        return self.input_tokens + self.cache_creation_input_tokens + self.cache_read_input_tokens

    def __add__(self, other: Usage) -> Usage:
        return Usage(*(getattr(self, f) + getattr(other, f) for f in self.__dataclass_fields__))


def price_for(model: str, prompt_tokens: int = 0) -> Price:
    if model not in PRICES:
        raise UnknownModelError(
            f"no price for {model!r}; known: {sorted(PRICES)} (add it from {PRICING_SOURCE})"
        )
    standard, long = PRICES[model]
    return long if long and prompt_tokens > HAIKU_LONG_PROMPT_TOKENS else standard


def cost_usd(model: str, usage: Usage) -> float:
    """Cost of ONE request (the Haiku tier is decided per request)."""
    p = price_for(model, usage.prompt_tokens)
    return (
        usage.input_tokens * p.input
        + usage.cache_creation_input_tokens * p.cache_write
        + usage.cache_read_input_tokens * p.cache_read
        + usage.output_tokens * p.output
    ) / 1_000_000


def worst_case_usd(model: str, max_input_tokens: int, max_output_tokens: int) -> float:
    """Upper bound for a whole diagnosis under its token caps: every input token billed
    as a cache write (the dearest input rate), every output token at the output rate."""
    p = price_for(model, max_input_tokens)
    return (max_input_tokens * max(p.input, p.cache_write) + max_output_tokens * p.output) / 1e6
