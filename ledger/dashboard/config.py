"""Editable economy configuration for the wallet dashboard.

Amounts in this file are HUMAN token amounts. The accounting layer converts
all values to the ledger's 6-decimal base unit before posting transactions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from django.core.exceptions import ImproperlyConfigured


PLATFORM_TOKEN_DECIMALS = 6
DAILY_REWARD_CONFIG_VERSION = 1
DAILY_REWARDS_ENABLED = True

# None means: use django.conf.settings.TIME_ZONE.
DAILY_REWARD_TIME_ZONE = None

# Number of cards shown in the compact dashboard widget.
DAILY_REWARD_WINDOW_SIZE = 5

# Hard guard against an accidental economy-breaking edit in this file.
DAILY_REWARD_MAX_TOKENS_PER_CLAIM = 100_000

# asset accepts: "coins", "chest", "bigchest".
# Keep this sequence ordered. Its length is the reward cycle length.
DAILY_REWARDS = (
    {"amount": 50, "asset": "coins"},
    {"amount": 75, "asset": "coins"},
    {"amount": 100, "asset": "coins"},
    {"amount": 125, "asset": "coins"},
    {"amount": 150, "asset": "chest"},
    {"amount": 200, "asset": "coins"},
    {"amount": 500, "asset": "bigchest"},
    {"amount": 75, "asset": "coins"},
    {"amount": 100, "asset": "coins"},
    {"amount": 100, "asset": "coins"},
    {"amount": 150, "asset": "coins"},
    {"amount": 250, "asset": "chest"},
    {"amount": 400, "asset": "coins"},
    {"amount": 1_000, "asset": "bigchest"},
    {"amount": 100, "asset": "coins"},
    {"amount": 125, "asset": "coins"},
    {"amount": 150, "asset": "coins"},
    {"amount": 200, "asset": "coins"},
    {"amount": 250, "asset": "chest"},
    {"amount": 350, "asset": "coins"},
    {"amount": 1_250, "asset": "bigchest"},
    {"amount": 150, "asset": "coins"},
    {"amount": 175, "asset": "coins"},
    {"amount": 200, "asset": "coins"},
    {"amount": 250, "asset": "chest"},
    {"amount": 300, "asset": "coins"},
    {"amount": 450, "asset": "coins"},
    {"amount": 1_500, "asset": "bigchest"},
    {"amount": 500, "asset": "chest"},
    {"amount": 2_500, "asset": "bigchest"},
)


_ALLOWED_ASSETS = frozenset({"coins", "chest", "bigchest"})


@dataclass(frozen=True)
class DailyRewardDefinition:
    day: int
    amount_tokens: int
    amount_units: int
    asset: str


def tokens_to_units(amount_tokens: int) -> int:
    if isinstance(amount_tokens, bool) or not isinstance(amount_tokens, int):
        raise ImproperlyConfigured("Daily reward amounts must be whole integers")
    if amount_tokens <= 0:
        raise ImproperlyConfigured("Daily reward amounts must be positive")
    if amount_tokens > DAILY_REWARD_MAX_TOKENS_PER_CLAIM:
        raise ImproperlyConfigured(
            "Daily reward amount exceeds DAILY_REWARD_MAX_TOKENS_PER_CLAIM"
        )
    return amount_tokens * (10 ** PLATFORM_TOKEN_DECIMALS)


def _normalize_reward_rows(rows: Iterable[dict]) -> tuple[DailyRewardDefinition, ...]:
    normalized = []
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise ImproperlyConfigured("Every DAILY_REWARDS item must be a dictionary")
        amount_tokens = raw.get("amount")
        asset = str(raw.get("asset") or "").strip().lower()
        if asset not in _ALLOWED_ASSETS:
            raise ImproperlyConfigured(
                f"Invalid daily reward asset on day {index}: {asset or '<empty>'}"
            )
        normalized.append(
            DailyRewardDefinition(
                day=index,
                amount_tokens=amount_tokens,
                amount_units=tokens_to_units(amount_tokens),
                asset=asset,
            )
        )

    if not normalized:
        raise ImproperlyConfigured("DAILY_REWARDS must contain at least one reward")

    window_size = int(DAILY_REWARD_WINDOW_SIZE)
    if window_size <= 0:
        raise ImproperlyConfigured("DAILY_REWARD_WINDOW_SIZE must be positive")
    if window_size > len(normalized):
        raise ImproperlyConfigured(
            "DAILY_REWARD_WINDOW_SIZE cannot exceed the reward cycle length"
        )

    return tuple(normalized)


def get_daily_reward_definitions() -> tuple[DailyRewardDefinition, ...]:
    return _normalize_reward_rows(DAILY_REWARDS)
