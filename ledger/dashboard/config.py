"""Editable economy configuration for the wallet dashboard.

Amounts in this file are HUMAN token amounts. The accounting layer converts
all values to the ledger's 6-decimal base unit before posting transactions.

Reward Chest drop probabilities use basis points: 10_000 bps = 100%.
Every chest's ``chance_bps`` values must add up to exactly 10_000.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Iterable

from django.core.exceptions import ImproperlyConfigured


PLATFORM_TOKEN_DECIMALS = 6

# ---------------------------------------------------------------------------
# Generic Reward Chests
# ---------------------------------------------------------------------------
REWARD_CHEST_CONFIG_VERSION = 4
REWARD_CHEST_TOTAL_CHANCE_BPS = 10_000
REWARD_CHEST_MAX_DROPS = 100
REWARD_CHEST_MAX_TOKENS_PER_DROP = 100_000
REWARD_CHEST_MAX_EXPECTED_VALUE_TOKENS = 20_000
_REWARD_CHEST_ABSOLUTE_MAX_DROPS = 1_000
_LEDGER_MAX_HUMAN_TOKENS = ((2 ** 63) - 1) // (10 ** PLATFORM_TOKEN_DECIMALS)

# Each Reward Chest has a public name and two independent static images.
# Paths are relative to the Django static root. The closed image is shown before
# opening; the opened image is shown once the reward has been claimed.
# ``drops`` are evaluated from top to bottom with a secure server-side roll.
REWARD_CHESTS = {
    "small_chest": {
        "label": "Small Chest",
        "asset": "small_chest",
        "closed_image": "images/wallet/dashboard/chests/small_closed.png",
        "opened_image": "images/wallet/dashboard/chests/small_open.png",
        "drops": (
            {"key": "common_100", "label": "100 tokens", "rarity": "common", "chance_bps": 5_500, "amount": 100},
            {"key": "uncommon_250", "label": "250 tokens", "rarity": "uncommon", "chance_bps": 3_000, "amount": 250},
            {"key": "rare_500", "label": "500 tokens", "rarity": "rare", "chance_bps": 1_200, "amount": 500},
            {"key": "epic_1000", "label": "1,000 tokens", "rarity": "epic", "chance_bps": 280, "amount": 1_000},
            {"key": "jackpot_5000", "label": "5,000 tokens", "rarity": "jackpot", "chance_bps": 20, "amount": 5_000},
        ),
    },
    "medium_chest": {
        "label": "Big Chest",
        "asset": "medium_chest",
        "closed_image": "images/wallet/dashboard/chests/medium_closed.png",
        "opened_image": "images/wallet/dashboard/chests/medium_open.png",
        "drops": (
            {"key": "common_350", "label": "350 tokens", "rarity": "common", "chance_bps": 5_000, "amount": 350},
            {"key": "uncommon_750", "label": "750 tokens", "rarity": "uncommon", "chance_bps": 3_000, "amount": 750},
            {"key": "rare_1500", "label": "1,500 tokens", "rarity": "rare", "chance_bps": 1_500, "amount": 1_500},
            {"key": "epic_3000", "label": "3,000 tokens", "rarity": "epic", "chance_bps": 450, "amount": 3_000},
            {"key": "jackpot_10000", "label": "10,000 tokens", "rarity": "jackpot", "chance_bps": 50, "amount": 10_000},
        ),
    },
    "big_chest": {
        "label": "Huge Chest",
        "asset": "big_chest",
        "closed_image": "images/wallet/dashboard/chests/big_closed.png",
        "opened_image": "images/wallet/dashboard/chests/big_open.png",
        "drops": (
            {"key": "common_350", "label": "350 tokens", "rarity": "common", "chance_bps": 5_000, "amount": 350},
            {"key": "uncommon_750", "label": "750 tokens", "rarity": "uncommon", "chance_bps": 3_000, "amount": 750},
            {"key": "rare_1500", "label": "1,500 tokens", "rarity": "rare", "chance_bps": 1_500, "amount": 1_500},
            {"key": "epic_3000", "label": "3,000 tokens", "rarity": "epic", "chance_bps": 450, "amount": 3_000},
            {"key": "jackpot_10000", "label": "10,000 tokens", "rarity": "jackpot", "chance_bps": 50, "amount": 10_000},
        ),
    },
}

# ---------------------------------------------------------------------------
# Daily Rewards
# ---------------------------------------------------------------------------
DAILY_REWARD_CONFIG_VERSION = 3
DAILY_REWARDS_ENABLED = True

# None means: use django.conf.settings.TIME_ZONE.
DAILY_REWARD_TIME_ZONE = None

# Number of cards shown in the compact dashboard widget.
DAILY_REWARD_WINDOW_SIZE = 5

# Hard guard against an accidental economy-breaking fixed reward.
DAILY_REWARD_MAX_TOKENS_PER_CLAIM = 100_000

# Fixed reward:
#   {"kind": "fixed", "amount": 100, "asset": "coins"}
# Reward Chest:
#   {"kind": "chest", "chest": "small_chest"}
# Rows without ``kind`` remain backward-compatible and are treated as fixed.
DAILY_REWARDS = (
    {"kind": "fixed", "amount": 50, "asset": "coins"},
    {"kind": "fixed", "amount": 75, "asset": "coins"},
    {"kind": "fixed", "amount": 100, "asset": "coins"},
    {"kind": "fixed", "amount": 125, "asset": "coins"},
    {"kind": "chest", "chest": "small_chest"},
    {"kind": "fixed", "amount": 200, "asset": "coins"},
    {"kind": "chest", "chest": "medium_chest"},
    {"kind": "fixed", "amount": 75, "asset": "coins"},
    {"kind": "fixed", "amount": 100, "asset": "coins"},
    {"kind": "fixed", "amount": 100, "asset": "coins"},
    {"kind": "fixed", "amount": 150, "asset": "coins"},
    {"kind": "chest", "chest": "small_chest"},
    {"kind": "fixed", "amount": 400, "asset": "coins"},
    {"kind": "chest", "chest": "medium_chest"},
    {"kind": "fixed", "amount": 100, "asset": "coins"},
    {"kind": "fixed", "amount": 125, "asset": "coins"},
    {"kind": "fixed", "amount": 150, "asset": "coins"},
    {"kind": "fixed", "amount": 200, "asset": "coins"},
    {"kind": "chest", "chest": "small_chest"},
    {"kind": "fixed", "amount": 350, "asset": "coins"},
    {"kind": "chest", "chest": "medium_chest"},
    {"kind": "fixed", "amount": 150, "asset": "coins"},
    {"kind": "fixed", "amount": 175, "asset": "coins"},
    {"kind": "fixed", "amount": 200, "asset": "coins"},
    {"kind": "chest", "chest": "small_chest"},
    {"kind": "fixed", "amount": 300, "asset": "coins"},
    {"kind": "fixed", "amount": 450, "asset": "coins"},
    {"kind": "chest", "chest": "medium_chest"},
    {"kind": "chest", "chest": "small_chest"},
    {"kind": "chest", "chest": "big_chest"},
)


# Current config uses canonical names. Aliases are kept only so already granted
# version 1-3 snapshots remain openable after this cleanup.
_CHEST_ASSET_ALIASES = {
    "chest": "small_chest",
    "smallchest": "small_chest",
    "small_chest": "small_chest",
    "medchest": "medium_chest",
    "mediumchest": "medium_chest",
    "medium_chest": "medium_chest",
    "bigchest": "big_chest",
    "big_chest": "big_chest",
}
_CHEST_IMAGE_PATHS = {
    "small_chest": {
        "closed": "images/wallet/dashboard/chests/small_closed.png",
        "opened": "images/wallet/dashboard/chests/small_open.png",
    },
    "medium_chest": {
        "closed": "images/wallet/dashboard/chests/medium_closed.png",
        "opened": "images/wallet/dashboard/chests/medium_open.png",
    },
    "big_chest": {
        "closed": "images/wallet/dashboard/chests/big_closed.png",
        "opened": "images/wallet/dashboard/chests/big_open.png",
    },
}
_ALLOWED_DAILY_ASSETS = frozenset({"coins"})
_ALLOWED_CHEST_ASSETS = frozenset(_CHEST_IMAGE_PATHS)
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class RewardChestDropDefinition:
    key: str
    label: str
    rarity: str
    chance_bps: int
    amount_tokens: int
    amount_units: int
    cumulative_end_bps: int


@dataclass(frozen=True)
class RewardChestDefinition:
    key: str
    label: str
    asset: str
    closed_image: str
    opened_image: str
    drops: tuple[RewardChestDropDefinition, ...]
    min_amount_tokens: int
    max_amount_tokens: int
    expected_value_numerator: int

    @property
    def expected_value_tokens(self) -> float:
        return self.expected_value_numerator / REWARD_CHEST_TOTAL_CHANCE_BPS


@dataclass(frozen=True)
class DailyRewardDefinition:
    day: int
    kind: str
    asset: str
    chest_closed_image: str
    chest_opened_image: str
    amount_tokens: int | None
    amount_units: int | None
    chest_key: str
    chest_label: str
    min_amount_tokens: int
    max_amount_tokens: int


def _require_whole_positive_int(value, *, field_name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ImproperlyConfigured(f"{field_name} must be a whole integer")
    if value <= 0:
        raise ImproperlyConfigured(f"{field_name} must be positive")
    if value > maximum:
        raise ImproperlyConfigured(f"{field_name} exceeds the configured safety maximum")
    return value


def tokens_to_units(amount_tokens: int) -> int:
    normalized = _require_whole_positive_int(
        amount_tokens,
        field_name="Reward amount",
        maximum=max(
            int(DAILY_REWARD_MAX_TOKENS_PER_CLAIM),
            int(REWARD_CHEST_MAX_TOKENS_PER_DROP),
        ),
    )
    return normalized * (10 ** PLATFORM_TOKEN_DECIMALS)


def _normalize_identifier(value, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ImproperlyConfigured(
            f"{field_name} must use lowercase letters, digits, underscores, or hyphens"
        )
    return normalized


def _normalize_static_image_path(value, *, field_name: str) -> str:
    normalized = str(value or "").strip().replace("\\", "/")
    if (
        not normalized
        or normalized.startswith("/")
        or "://" in normalized
        or any(part in {"", ".", ".."} for part in normalized.split("/"))
        or len(normalized) > 255
    ):
        raise ImproperlyConfigured(f"{field_name} must be a safe static-relative path")
    return normalized


def _normalize_chest_asset(value, *, field_name: str) -> str:
    raw = str(value or "").strip().lower()
    normalized = _CHEST_ASSET_ALIASES.get(raw)
    if normalized is None:
        raise ImproperlyConfigured(
            f"{field_name} has an invalid asset: {raw or '<empty>'}"
        )
    return normalized


def _default_chest_image(asset: str, *, opened: bool) -> str:
    normalized = _normalize_chest_asset(asset, field_name="Reward Chest")
    state = "opened" if opened else "closed"
    return _CHEST_IMAGE_PATHS[normalized][state]


def _normalize_reward_chest_definition(
    key: str,
    raw: dict,
    *,
    enforce_economy_limits: bool = True,
) -> RewardChestDefinition:
    key = _normalize_identifier(key, field_name="Reward Chest key")
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(f"Reward Chest {key} must be a dictionary")

    label = str(raw.get("label") or "").strip()
    if not label or len(label) > 80:
        raise ImproperlyConfigured(f"Reward Chest {key} requires a label of at most 80 characters")

    asset = _normalize_chest_asset(
        raw.get("asset"),
        field_name=f"Reward Chest {key}",
    )

    closed_image = _normalize_static_image_path(
        raw.get("closed_image") or _default_chest_image(asset, opened=False),
        field_name=f"Reward Chest {key} closed_image",
    )
    opened_image = _normalize_static_image_path(
        raw.get("opened_image") or _default_chest_image(asset, opened=True),
        field_name=f"Reward Chest {key} opened_image",
    )

    raw_drops = raw.get("drops")
    if not isinstance(raw_drops, (list, tuple)) or not raw_drops:
        raise ImproperlyConfigured(f"Reward Chest {key} must contain at least one drop")
    maximum_drops = (
        int(REWARD_CHEST_MAX_DROPS)
        if enforce_economy_limits
        else _REWARD_CHEST_ABSOLUTE_MAX_DROPS
    )
    if len(raw_drops) > maximum_drops:
        raise ImproperlyConfigured(f"Reward Chest {key} contains too many drops")

    drops = []
    seen_drop_keys = set()
    cumulative = 0
    expected_value_numerator = 0

    for index, raw_drop in enumerate(raw_drops, start=1):
        if not isinstance(raw_drop, dict):
            raise ImproperlyConfigured(f"Reward Chest {key} drop {index} must be a dictionary")

        drop_key = _normalize_identifier(
            raw_drop.get("key"),
            field_name=f"Reward Chest {key} drop key",
        )
        if drop_key in seen_drop_keys:
            raise ImproperlyConfigured(f"Reward Chest {key} contains duplicate drop key {drop_key}")
        seen_drop_keys.add(drop_key)

        chance_bps = _require_whole_positive_int(
            raw_drop.get("chance_bps"),
            field_name=f"Reward Chest {key} drop {drop_key} chance_bps",
            maximum=REWARD_CHEST_TOTAL_CHANCE_BPS,
        )
        amount_tokens = _require_whole_positive_int(
            raw_drop.get("amount"),
            field_name=f"Reward Chest {key} drop {drop_key} amount",
            maximum=(
                REWARD_CHEST_MAX_TOKENS_PER_DROP
                if enforce_economy_limits
                else _LEDGER_MAX_HUMAN_TOKENS
            ),
        )
        label_value = str(raw_drop.get("label") or f"{amount_tokens:,} tokens").strip()
        if not label_value or len(label_value) > 80:
            raise ImproperlyConfigured(f"Reward Chest {key} drop {drop_key} has an invalid label")
        rarity = _normalize_identifier(
            raw_drop.get("rarity") or "common",
            field_name=f"Reward Chest {key} drop {drop_key} rarity",
        )

        cumulative += chance_bps
        expected_value_numerator += amount_tokens * chance_bps
        drops.append(
            RewardChestDropDefinition(
                key=drop_key,
                label=label_value,
                rarity=rarity,
                chance_bps=chance_bps,
                amount_tokens=amount_tokens,
                amount_units=amount_tokens * (10 ** PLATFORM_TOKEN_DECIMALS),
                cumulative_end_bps=cumulative,
            )
        )

    if cumulative != REWARD_CHEST_TOTAL_CHANCE_BPS:
        raise ImproperlyConfigured(
            f"Reward Chest {key} drop rates must total exactly "
            f"{REWARD_CHEST_TOTAL_CHANCE_BPS} bps, got {cumulative}"
        )

    max_expected_numerator = (
        int(REWARD_CHEST_MAX_EXPECTED_VALUE_TOKENS)
        * REWARD_CHEST_TOTAL_CHANCE_BPS
    )
    if enforce_economy_limits and expected_value_numerator > max_expected_numerator:
        raise ImproperlyConfigured(
            f"Reward Chest {key} expected value exceeds "
            "REWARD_CHEST_MAX_EXPECTED_VALUE_TOKENS"
        )

    amounts = [drop.amount_tokens for drop in drops]
    return RewardChestDefinition(
        key=key,
        label=label,
        asset=asset,
        closed_image=closed_image,
        opened_image=opened_image,
        drops=tuple(drops),
        min_amount_tokens=min(amounts),
        max_amount_tokens=max(amounts),
        expected_value_numerator=expected_value_numerator,
    )


def get_reward_chest_definitions() -> dict[str, RewardChestDefinition]:
    if not isinstance(REWARD_CHESTS, dict):
        raise ImproperlyConfigured("REWARD_CHESTS must be a dictionary")
    normalized = {}
    for raw_key, raw_definition in REWARD_CHESTS.items():
        definition = _normalize_reward_chest_definition(raw_key, raw_definition)
        if definition.key in normalized:
            raise ImproperlyConfigured(f"Duplicate Reward Chest key: {definition.key}")
        normalized[definition.key] = definition
    return normalized


def get_reward_chest_definition(chest_key: str) -> RewardChestDefinition:
    normalized_key = _normalize_identifier(chest_key, field_name="Reward Chest key")
    definition = get_reward_chest_definitions().get(normalized_key)
    if definition is None:
        raise ImproperlyConfigured(f"Unknown Reward Chest: {normalized_key}")
    return definition


def build_reward_chest_snapshot(definition: RewardChestDefinition) -> dict:
    payload = {
        "config_version": int(REWARD_CHEST_CONFIG_VERSION),
        "key": definition.key,
        "label": definition.label,
        "asset": definition.asset,
        "closed_image": definition.closed_image,
        "opened_image": definition.opened_image,
        "drops": [
            {
                "key": drop.key,
                "label": drop.label,
                "rarity": drop.rarity,
                "chance_bps": int(drop.chance_bps),
                "amount": int(drop.amount_tokens),
            }
            for drop in definition.drops
        ],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["fingerprint"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return payload


def reward_chest_definition_from_snapshot(snapshot: dict) -> RewardChestDefinition:
    if not isinstance(snapshot, dict):
        raise ImproperlyConfigured("Reward Chest snapshot must be a dictionary")
    fingerprint = str(snapshot.get("fingerprint") or "").strip().lower()
    if len(fingerprint) != 64:
        raise ImproperlyConfigured("Reward Chest snapshot fingerprint is missing")

    config_version = int(snapshot.get("config_version") or 0)
    if config_version <= 1:
        payload = {
            "config_version": snapshot.get("config_version"),
            "key": snapshot.get("key"),
            "label": snapshot.get("label"),
            "asset": snapshot.get("asset"),
            "drops": snapshot.get("drops"),
        }
    else:
        payload = {
            "config_version": snapshot.get("config_version"),
            "key": snapshot.get("key"),
            "label": snapshot.get("label"),
            "asset": snapshot.get("asset"),
            "closed_image": snapshot.get("closed_image"),
            "opened_image": snapshot.get("opened_image"),
            "drops": snapshot.get("drops"),
        }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if expected != fingerprint:
        raise ImproperlyConfigured("Reward Chest snapshot fingerprint does not match")

    asset = _normalize_chest_asset(
        payload.get("asset"),
        field_name="Reward Chest snapshot",
    )
    return _normalize_reward_chest_definition(
        str(payload.get("key") or ""),
        {
            "label": payload.get("label"),
            "asset": asset,
            "closed_image": payload.get("closed_image")
            or _default_chest_image(asset, opened=False),
            "opened_image": payload.get("opened_image")
            or _default_chest_image(asset, opened=True),
            "drops": payload.get("drops"),
        },
        enforce_economy_limits=False,
    )


def _normalize_reward_rows(rows: Iterable[dict]) -> tuple[DailyRewardDefinition, ...]:
    chest_definitions = get_reward_chest_definitions()
    normalized = []

    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            raise ImproperlyConfigured("Every DAILY_REWARDS item must be a dictionary")

        raw_kind = str(raw.get("kind") or "").strip().lower()
        kind = raw_kind or ("chest" if raw.get("chest") else "fixed")

        if kind == "fixed":
            amount_tokens = _require_whole_positive_int(
                raw.get("amount"),
                field_name=f"Daily reward amount on day {index}",
                maximum=DAILY_REWARD_MAX_TOKENS_PER_CLAIM,
            )
            asset = str(raw.get("asset") or "").strip().lower()
            if asset not in _ALLOWED_DAILY_ASSETS:
                raise ImproperlyConfigured(
                    f"Invalid daily reward asset on day {index}: {asset or '<empty>'}"
                )
            normalized.append(
                DailyRewardDefinition(
                    day=index,
                    kind="fixed",
                    asset=asset,
                    chest_closed_image="",
                    chest_opened_image="",
                    amount_tokens=amount_tokens,
                    amount_units=amount_tokens * (10 ** PLATFORM_TOKEN_DECIMALS),
                    chest_key="",
                    chest_label="",
                    min_amount_tokens=amount_tokens,
                    max_amount_tokens=amount_tokens,
                )
            )
            continue

        if kind == "chest":
            if raw.get("amount") not in (None, ""):
                raise ImproperlyConfigured(
                    f"Daily reward day {index} cannot define both a chest and a fixed amount"
                )
            chest_key = _normalize_identifier(
                raw.get("chest"),
                field_name=f"Daily reward chest on day {index}",
            )
            chest = chest_definitions.get(chest_key)
            if chest is None:
                raise ImproperlyConfigured(
                    f"Daily reward day {index} references unknown Reward Chest {chest_key}"
                )
            normalized.append(
                DailyRewardDefinition(
                    day=index,
                    kind="chest",
                    asset=chest.asset,
                    chest_closed_image=chest.closed_image,
                    chest_opened_image=chest.opened_image,
                    amount_tokens=None,
                    amount_units=None,
                    chest_key=chest.key,
                    chest_label=chest.label,
                    min_amount_tokens=chest.min_amount_tokens,
                    max_amount_tokens=chest.max_amount_tokens,
                )
            )
            continue

        raise ImproperlyConfigured(
            f"Invalid daily reward kind on day {index}: {kind or '<empty>'}"
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
