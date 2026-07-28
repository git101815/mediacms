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

# Public wallet preview. This is display-only and never creates or credits a wallet.
WALLET_GUEST_PREVIEW_BALANCE_TOKENS = 100

# ---------------------------------------------------------------------------
# Wallet visual assets
# ---------------------------------------------------------------------------
# Every path is relative to Django's static root. Templates and services must
# consume these mappings instead of embedding wallet image paths themselves.
WALLET_CHEST_ASSETS = {
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

WALLET_ASSETS = {
    "token_icon": "images/wallet/cf-token.png",
    "hero_art": "images/wallet/dashboard/hero-art.png",
    "vault_chest": WALLET_CHEST_ASSETS["big_chest"]["closed"],
    "daily_reward_coins_few": "images/wallet/dashboard/few_coins.png",
    "daily_reward_coins_pile": "images/wallet/dashboard/coin_pile.png",
    "quest_confirm_email": "images/wallet/dashboard/quest-login.png",
    "quest_daily_login": "images/wallet/dashboard/quest-login.png",
    "quest_watch_previews": "images/wallet/dashboard/quest-watch.png",
    "quest_invite_friend": "images/wallet/dashboard/quest-invite.png",
    "quest_remove_ads": "images/wallet/dashboard/quest-adfree.png",
    "quest_share_x": "images/social-media-icons/x.svg",
    "quest_share_reddit": "images/social-media-icons/reddit.svg",
    "quest_share_telegram": "images/social-media-icons/telegram.svg",
    "quest_share_vk": "images/social-media-icons/vk.svg",
    "quest_share_whatsapp": "images/social-media-icons/whatsapp.svg",
    "referral_art": "images/wallet/dashboard/referral-coins.png",
    "remove_ads_art": "images/wallet/dashboard/remove-ads.png",
}

# Amount tiers are evaluated from the highest matching ``min_amount``.
# Current split:
#   1-29 tokens  -> few_coins.png
#   30+ tokens   -> coin_pile.png
DAILY_REWARD_ASSETS = {
    "coins": {
        "button_asset": "token_icon",
        "tiers": (
            {
                "min_amount": 1,
                "image_asset": "daily_reward_coins_few",
            },
            {
                "min_amount": 30,
                "image_asset": "daily_reward_coins_pile",
            },
        ),
    },
}

WALLET_TOKEN_PACK_ASSETS = {
    "token_pkg_500": "images/wallet/bundles/token_pkg_500.png",
    "token_pkg_1000": "images/wallet/bundles/token_pkg_1000.png",
    "token_pkg_2000": "images/wallet/bundles/token_pkg_2000.png",
    "token_pkg_5000": "images/wallet/bundles/token_pkg_5000.png",
    "token_pkg_10000": "images/wallet/bundles/token_pkg_10000.png",
}
# Reward Chest reveal images are selected explicitly per drop.
# Values reference WALLET_TOKEN_PACK_ASSETS keys. This is visual-only.
#
# The opening animation and the "Possible drops" modal both consume this
# mapping, so each rarity intentionally receives distinct artwork.
REWARD_CHEST_DROP_IMAGE_ASSETS = {
    "small_chest": {
        "common_25": "token_pkg_500",
        "uncommon_50": "token_pkg_1000",
        "rare_100": "token_pkg_2000",
        "epic_250": "token_pkg_5000",
        "jackpot_500": "token_pkg_10000",
    },
    "medium_chest": {
        "common_75": "token_pkg_500",
        "uncommon_150": "token_pkg_1000",
        "rare_450": "token_pkg_2000",
        "epic_750": "token_pkg_5000",
        "jackpot_1500": "token_pkg_10000",
    },
    "big_chest": {
        "common_350": "token_pkg_500",
        "uncommon_750": "token_pkg_1000",
        "rare_1500": "token_pkg_2000",
        "epic_2500": "token_pkg_5000",
        "jackpot_5000": "token_pkg_10000",
    },
}

REWARD_CHEST_RARITY_IMAGE_ASSETS = {
    "common": "images/wallet/dashboard/chests/common.png",
    "uncommon": "images/wallet/dashboard/chests/uncommon.png",
    "rare": "images/wallet/dashboard/chests/rare.png",
    "epic": "images/wallet/dashboard/chests/epic.png",
    "legendary": "images/wallet/dashboard/chests/legendary.png",
}

WALLET_TOKEN_PACK_IMAGE_TEMPLATE = "images/wallet/bundles/{code}.png"

WALLET_NETWORK_DISPLAY_LABELS = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum One",
    "base": "Base",
    "bsc": "BNB Chain",
}

WALLET_ROUTE_ONCHAIN_DECIMALS = {
    ("ethereum", "USDT"): 6,
    ("ethereum", "USDC"): 6,
    ("arbitrum", "USDT"): 6,
    ("arbitrum", "USDC"): 6,
    ("base", "USDT"): 6,
    ("base", "USDC"): 6,
    ("bsc", "USDT"): 18,
    ("bsc", "USDC"): 18,
}

WALLET_PAYMENT_GROUPS = {
    "paypal_us": {"label": "PayPal (US only)", "icon_label": "PayPal", "icon_path": "images/wallet/paypal.svg", "order": 20},
    "revolut_eu": {"label": "Revolut (EU only)", "icon_label": "Revolut", "icon_path": "images/wallet/revolut.svg", "order": 30},
    "transak_card": {"label": "Card / Apple Pay / Google Pay (Transak)", "icon_label": "Transak", "icon_path": "images/wallet/google_apple_card.svg", "order": 35},
    "dfx_bank": {"label": "Bank transfer (DFX)", "icon_label": "SEPA", "icon_path": "images/wallet/sepa.svg", "order": 40},
    "mtpelerin_eur": {"label": "Bank transfer (Mt Pelerin · EUR)", "icon_label": "BANK", "icon_path": "images/wallet/bank.svg", "order": 50},
    "mtpelerin_usd": {"label": "Bank transfer (Mt Pelerin · USD)", "icon_label": "SWIFT", "icon_path": "images/wallet/bank.svg", "order": 51},
    "crypto": {"label": "Crypto", "icon_label": "Crypto", "icon_path": "images/wallet/crypto.svg", "order": 10},
}

WALLET_CRYPTO_ASSET_GROUPS = {
    "USDC": {"label": "USDC", "icon_path": "images/wallet/usdc.svg", "order": 10},
    "USDT": {"label": "USDT", "icon_path": "images/wallet/usdt.svg", "order": 20},
}

WALLET_CRYPTO_NETWORK_GROUPS = {
    "ethereum": {"label": "Ethereum", "icon_path": "images/wallet/eth.svg", "order": 10},
    "arbitrum": {"label": "Arbitrum One", "icon_path": "images/wallet/arb.svg", "order": 20},
    "base": {"label": "Base", "icon_path": "images/wallet/base.svg", "order": 30},
    "bsc": {"label": "BNB Chain", "icon_path": "images/wallet/bnb.svg", "order": 40},
}

WALLET_PAYGATE_PROVIDER_PAYMENT_GROUPS = {
    "paypal": "paypal_us",
    "revolut": "revolut_eu",
    "transak": "transak_card",
}

# ---------------------------------------------------------------------------
# Quest Board
# ---------------------------------------------------------------------------
# The existing one-time quests remain available until claimed. When one is
# still active it occupies one of the four existing cards; no second board or
# alternate layout is rendered.
QUEST_BOARD_CONFIG_VERSION = 6
QUEST_BOARD_ENABLED = True
QUEST_BOARD_SLOT_COUNT = 4
QUEST_BOARD_RESET_LABEL = "One-time"
QUEST_BOARD_MAX_REWARD_TOKENS = 100_000

# One-time starter quests.
QUEST_BOARD_QUESTS = (
    {
        "key": "confirm_email",
        "title": "Confirm Email",
        "description": "Verify your email address",
        "condition": "email_verified",
        "icon_asset": "quest_confirm_email",
        "action_label": "Confirm",
        "action_url_name": "account_email",
        "reward": {
            "kind": "fixed",
            "amount": 50,
            "asset": "coins",
        },
    },
)

# Weekly quest configuration. Every visible string, target and reward belongs
# here. All enabled definitions form the assignment pool. Each user receives a
# stable pseudo-random selection for the current ISO week.
QUEST_BOARD_WEEKLY_ENABLED = True
QUEST_BOARD_WEEKLY_START_AT = "2026-07-28T00:00:00+03:00"
QUEST_BOARD_WEEKLY_SELECTION_SALT = "weekly-quest-selection-v1"
QUEST_BOARD_VISITOR_COOKIE_SECONDS = 180 * 24 * 60 * 60
QUEST_BOARD_ATTRIBUTION_COOKIE_SECONDS = 8 * 24 * 60 * 60
QUEST_BOARD_MIN_SECOND_PAGE_DELAY_SECONDS = 2
QUEST_BOARD_UNFURL_WINDOW_SECONDS = 24 * 60 * 60
QUEST_BOARD_EXCLUDED_PAGE_PREFIXES = (
    "/api/",
    "/static/",
    "/media/",
    "/uploads/",
    "/accounts/",
    "/wallet",
    "/not_the_admin_panel/",
)

# Only posted purchase debits from user wallets count as tokens spent.
QUEST_BOARD_COMMUNITY_SPEND_TRANSACTION_KINDS = ("purchase",)

QUEST_BOARD_WEEKLY_TITLE = "Quests Board"
QUEST_BOARD_WEEKLY_SUBTITLE = "Complete missions to earn CF tokens!"
QUEST_BOARD_WEEKLY_RESET_PREFIX = "Resets in"
QUEST_BOARD_WEEKLY_CLAIMED_LABEL = "Claimed"
QUEST_BOARD_WEEKLY_OPEN_CHEST_LABEL = "Open Chest"
QUEST_BOARD_WEEKLY_CLAIM_LABEL = "Claim"

QUEST_BOARD_WEEKLY_QUESTS = {
    "share_site": {
        "enabled": True,
        "title": "Share the Site to 10 people",
        "description": "",
        "condition": "site_visitors",
        "icon_material": "web_traffic",
        "action_label": "Share Site",
        "action_url": "",
        "landing_path": "/",
        "target": 10,
        "progress_text": "{current} / {target}",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "share_video_x": {
        "enabled": True,
        "title": "Share a Video on X",
        "description": "",
        "condition": "video_share",
        "platform": "tw",
        "icon_asset": "quest_share_x",
        "action_label": "Choose Video",
        "action_url": "/latest",
        "target": 1,
        "progress_pending_text": "waiting",
        "progress_complete_text": "Verified",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "share_video_reddit": {
        "enabled": True,
        "title": "Share a Video on Reddit",
        "description": "",
        "condition": "video_share",
        "platform": "reddit",
        "icon_asset": "quest_share_reddit",
        "action_label": "Choose Video",
        "action_url": "/latest",
        "target": 1,
        "progress_pending_text": "waiting",
        "progress_complete_text": "Verified",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "share_video_telegram": {
        "enabled": True,
        "title": "Share a Video on Telegram",
        "description": "",
        "condition": "video_share",
        "platform": "telegram",
        "icon_asset": "quest_share_telegram",
        "action_label": "Choose Video",
        "action_url": "/latest",
        "target": 1,
        "progress_pending_text": "waiting",
        "progress_complete_text": "Verified",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "share_video_vk": {
        "enabled": True,
        "title": "Share a Video on VK",
        "description": "",
        "condition": "video_share",
        "platform": "vk",
        "icon_asset": "quest_share_vk",
        "action_label": "Choose Video",
        "action_url": "/latest",
        "target": 1,
        "progress_pending_text": "waiting",
        "progress_complete_text": "Verified",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "share_video_whatsapp": {
        "enabled": True,
        "title": "Share a Video on WhatsApp",
        "description": "",
        "condition": "video_share",
        "platform": "whatsapp",
        "icon_asset": "quest_share_whatsapp",
        "action_label": "Choose Video",
        "action_url": "/latest",
        "target": 1,
        "progress_pending_text": "waiting",
        "progress_complete_text": "Verified",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "community_likes": {
        "enabled": False,
        "title": "Community Likes",
        "description": "Help the community reach 100 likes this week",
        "condition": "community_likes",
        "icon_asset": "quest_invite_friend",
        "action_label": "Like a Video",
        "action_url": "/latest",
        "target": 1,
        "personal_target": 1,
        "global_target": 100,
        "progress_text": "You {personal_current:,} · Community {global_current:,} / {global_target:,} likes",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "community_views": {
        "enabled": False,
        "title": "Community Views",
        "description": "Help the community reach 100,000 views this week",
        "condition": "community_views",
        "icon_asset": "quest_watch_previews",
        "action_label": "Watch Videos",
        "action_url": "/latest",
        "target": 1,
        "personal_target": 1,
        "global_target": 100_000,
        "progress_text": "You {personal_current:,} · Community {global_current:,} / {global_target:,} views",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "community_comments": {
        "enabled": False,
        "title": "Community Comments",
        "description": "Help the community post 20 comments this week",
        "condition": "community_comments",
        "icon_asset": "quest_confirm_email",
        "action_label": "Post a Comment",
        "action_url": "/latest",
        "target": 1,
        "personal_target": 1,
        "global_target": 20,
        "progress_text": "You {personal_current:,} · Community {global_current:,} / {global_target:,} comments",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
    "community_spend": {
        "enabled": False,
        "title": "Community Spend",
        "description": "Help the community spend 10,000 tokens this week",
        "condition": "community_spend",
        "icon_asset": "daily_reward_coins_pile",
        "action_label": "Browse Content",
        "action_url": "/latest",
        "target": 1,
        "personal_target": 1,
        "global_target": 10_000,
        "progress_text": "You {personal_current:,} · Community {global_current:,} / {global_target:,} tokens",
        "reward": {"kind": "chest", "chest": "small_chest"},
    },
}

QUEST_BOARD_SOCIAL_HOSTS = {
    "tw": ("x.com", "twitter.com", "t.co"),
    "reddit": ("reddit.com", "redd.it"),
    "vk": ("vk.com",),
    "whatsapp": ("whatsapp.com", "wa.me"),
    "telegram": ("t.me", "telegram.org"),
}

QUEST_BOARD_UNFURL_USER_AGENTS = {
    "tw": ("twitterbot",),
    "reddit": ("redditbot",),
    "vk": ("vkshare", "vkbot"),
    "whatsapp": ("whatsapp",),
    "telegram": ("telegrambot",),
}


# ---------------------------------------------------------------------------
# Referral Program
# ---------------------------------------------------------------------------
# referral_code is generated lazily when the wallet referral module first
# needs it. Attribution is stored directly on users.User.referred_by at signup.
REFERRAL_CONFIG_VERSION = 1
REFERRAL_PROGRAM_ENABLED = True
REFERRAL_REWARD_TOKENS = 200
REFERRAL_GOAL = 10
REFERRAL_MAX_REWARDED_FRIENDS = 10
REFERRAL_MIN_PURCHASE_TOKENS = 500
REFERRAL_CODE_LENGTH = 12
REFERRAL_MAX_REWARD_TOKENS = 2000


# ---------------------------------------------------------------------------
# Bonus Vault
# ---------------------------------------------------------------------------
# Posted token purchase debits fill the vault. Every full threshold unlocks one
# generic Reward Chest. Several completed vaults can be accumulated and opened
# one by one.
BONUS_VAULT_CONFIG_VERSION = 1
BONUS_VAULT_ENABLED = True
BONUS_VAULT_THRESHOLD_TOKENS = 10_000
BONUS_VAULT_CHEST_KEY = "big_chest"
BONUS_VAULT_ELIGIBLE_TRANSACTION_KINDS = ("purchase",)
BONUS_VAULT_SOURCE_TYPE = "bonus_vault"

# Launch cutoff. Purchases before this timestamp do not generate retroactive
# vault rewards. Change this value before deployment when needed.
BONUS_VAULT_START_AT = "2026-07-25T00:00:00+03:00"


# ---------------------------------------------------------------------------
# Generic Reward Chests
# ---------------------------------------------------------------------------
REWARD_CHEST_CONFIG_VERSION = 5
REWARD_CHEST_TOTAL_CHANCE_BPS = 10_000
REWARD_CHEST_MAX_DROPS = 5
REWARD_CHEST_MAX_TOKENS_PER_DROP = 10_000
REWARD_CHEST_MAX_EXPECTED_VALUE_TOKENS = 1_000
_LEDGER_MAX_HUMAN_TOKENS = ((2 ** 63) - 1) // (10 ** PLATFORM_TOKEN_DECIMALS)

# Each Reward Chest has a public name and two independent static images.
# Paths are relative to the Django static root. The closed image is shown before
# opening; the opened image is shown once the reward has been claimed.
# ``drops`` are evaluated from top to bottom with a secure server-side roll.
REWARD_CHESTS = {
    "small_chest": {
        "label": "Small Chest",
        "asset": "small_chest",
        "closed_image": WALLET_CHEST_ASSETS["small_chest"]["closed"],
        "opened_image": WALLET_CHEST_ASSETS["small_chest"]["opened"],
        "drops": (
            {"key": "common_25", "label": "25 tokens", "rarity": "common", "chance_bps": 4_500, "amount": 25},
            {"key": "uncommon_50", "label": "50 tokens", "rarity": "uncommon", "chance_bps": 3_000, "amount": 50},
            {"key": "rare_100", "label": "100 tokens", "rarity": "rare", "chance_bps": 1_500, "amount": 100},
            {"key": "epic_250", "label": "250 tokens", "rarity": "epic", "chance_bps": 650, "amount": 250},
            {"key": "jackpot_500", "label": "500 tokens", "rarity": "jackpot", "chance_bps": 350, "amount": 500},
        ),
    },
    "medium_chest": {
        "label": "Big Chest",
        "asset": "medium_chest",
        "closed_image": WALLET_CHEST_ASSETS["medium_chest"]["closed"],
        "opened_image": WALLET_CHEST_ASSETS["medium_chest"]["opened"],
        "drops": (
            {"key": "common_75", "label": "75 tokens", "rarity": "common", "chance_bps": 4_500, "amount": 75},
            {"key": "uncommon_150", "label": "150 tokens", "rarity": "uncommon", "chance_bps": 3_000, "amount": 150},
            {"key": "rare_450", "label": "450 tokens", "rarity": "rare", "chance_bps": 1_500, "amount": 450},
            {"key": "epic_750", "label": "750 tokens", "rarity": "epic", "chance_bps": 650, "amount": 750},
            {"key": "jackpot_1500", "label": "1,500 tokens", "rarity": "jackpot", "chance_bps": 350, "amount": 1_500},
        ),
    },
    "big_chest": {
        "label": "Huge Chest",
        "asset": "big_chest",
        "closed_image": WALLET_CHEST_ASSETS["big_chest"]["closed"],
        "opened_image": WALLET_CHEST_ASSETS["big_chest"]["opened"],
        "drops": (
            {"key": "common_350", "label": "350 tokens", "rarity": "common", "chance_bps": 4_500, "amount": 350},
            {"key": "uncommon_750", "label": "750 tokens", "rarity": "uncommon", "chance_bps": 3_000, "amount": 750},
            {"key": "rare_1500", "label": "1,500 tokens", "rarity": "rare", "chance_bps": 1_500, "amount": 1_500},
            {"key": "epic_2500", "label": "2,500 tokens", "rarity": "epic", "chance_bps": 650, "amount": 2_500},
            {"key": "jackpot_5000", "label": "5,000 tokens", "rarity": "jackpot", "chance_bps": 350, "amount": 5_000},
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
DAILY_REWARD_MAX_TOKENS_PER_CLAIM = 10_000

# Fixed reward:
#   {"kind": "fixed", "amount": 100, "asset": "coins"}
# Reward Chest:
#   {"kind": "chest", "chest": "small_chest"}
DAILY_REWARDS = (
    # Cycle 1 — 135 expected tokens
    {"kind": "fixed", "amount": 5, "asset": "coins"},       # Day 1
    {"kind": "fixed", "amount": 10, "asset": "coins"},      # Day 2
    {"kind": "fixed", "amount": 30, "asset": "coins"},      # Day 3
    {"kind": "fixed", "amount": 15, "asset": "coins"},      # Day 4
    {"kind": "chest", "chest": "small_chest"},              # Day 5

    # Cycle 2 — 155 expected tokens
    {"kind": "fixed", "amount": 10, "asset": "coins"},      # Day 6
    {"kind": "fixed", "amount": 15, "asset": "coins"},      # Day 7
    {"kind": "fixed", "amount": 35, "asset": "coins"},      # Day 8
    {"kind": "fixed", "amount": 20, "asset": "coins"},      # Day 9
    {"kind": "chest", "chest": "small_chest"},              # Day 10

    # Cycle 3 — 347.5 expected tokens
    {"kind": "fixed", "amount": 15, "asset": "coins"},      # Day 11
    {"kind": "fixed", "amount": 20, "asset": "coins"},      # Day 12
    {"kind": "fixed", "amount": 40, "asset": "coins"},      # Day 13
    {"kind": "fixed", "amount": 25, "asset": "coins"},      # Day 14
    {"kind": "chest", "chest": "medium_chest"},                # Day 15

    # Cycle 4 — 195 expected tokens
    {"kind": "fixed", "amount": 20, "asset": "coins"},      # Day 16
    {"kind": "fixed", "amount": 25, "asset": "coins"},      # Day 17
    {"kind": "fixed", "amount": 45, "asset": "coins"},      # Day 18
    {"kind": "fixed", "amount": 30, "asset": "coins"},      # Day 19
    {"kind": "chest", "chest": "small_chest"},             # Day 20

    # Cycle 5 — 387.5 expected tokens
    {"kind": "fixed", "amount": 25, "asset": "coins"},      # Day 21
    {"kind": "fixed", "amount": 30, "asset": "coins"},      # Day 22
    {"kind": "fixed", "amount": 50, "asset": "coins"},      # Day 23
    {"kind": "fixed", "amount": 35, "asset": "coins"},      # Day 24
    {"kind": "chest", "chest": "medium_chest"},             # Day 25

    # Cycle 6 — 1,125 expected tokens
    {"kind": "fixed", "amount": 30, "asset": "coins"},      # Day 26
    {"kind": "fixed", "amount": 40, "asset": "coins"},      # Day 27
    {"kind": "fixed", "amount": 60, "asset": "coins"},      # Day 28
    {"kind": "fixed", "amount": 50, "asset": "coins"},      # Day 29
    {"kind": "chest", "chest": "big_chest"},                # Day 30
)

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
_ALLOWED_DAILY_ASSETS = frozenset(DAILY_REWARD_ASSETS)
_ALLOWED_CHEST_ASSETS = frozenset(WALLET_CHEST_ASSETS)
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


def get_wallet_asset_paths() -> dict[str, str]:
    if not isinstance(WALLET_ASSETS, dict) or not WALLET_ASSETS:
        raise ImproperlyConfigured("WALLET_ASSETS must be a non-empty dictionary")
    return {
        _normalize_identifier(key, field_name="Wallet asset key"): _normalize_static_image_path(
            path,
            field_name=f"Wallet asset {key}",
        )
        for key, path in WALLET_ASSETS.items()
    }


def get_daily_reward_asset_definition(
    asset_key: str,
    *,
    amount_tokens: int,
) -> dict[str, str | int]:
    key = _normalize_identifier(asset_key, field_name="Daily reward asset")
    raw = DAILY_REWARD_ASSETS.get(key)
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(f"Unknown daily reward asset: {key}")

    normalized_amount = _require_whole_positive_int(
        amount_tokens,
        field_name=f"Daily reward asset {key} amount",
        maximum=DAILY_REWARD_MAX_TOKENS_PER_CLAIM,
    )
    assets = get_wallet_asset_paths()

    button_asset = _normalize_identifier(
        raw.get("button_asset"),
        field_name=f"Daily reward asset {key} button_asset",
    )
    if button_asset not in assets:
        raise ImproperlyConfigured(
            f"Daily reward asset {key} references unknown wallet asset {button_asset}"
        )

    raw_tiers = raw.get("tiers")
    if not isinstance(raw_tiers, (list, tuple)) or not raw_tiers:
        raise ImproperlyConfigured(
            f"Daily reward asset {key} requires at least one amount tier"
        )

    tiers = []
    previous_minimum = 0
    for index, raw_tier in enumerate(raw_tiers, start=1):
        if not isinstance(raw_tier, dict):
            raise ImproperlyConfigured(
                f"Daily reward asset {key} tier {index} must be a dictionary"
            )

        minimum = _require_whole_positive_int(
            raw_tier.get("min_amount"),
            field_name=f"Daily reward asset {key} tier {index} min_amount",
            maximum=DAILY_REWARD_MAX_TOKENS_PER_CLAIM,
        )
        if minimum <= previous_minimum:
            raise ImproperlyConfigured(
                f"Daily reward asset {key} tiers must be ordered by increasing min_amount"
            )

        image_asset = _normalize_identifier(
            raw_tier.get("image_asset"),
            field_name=f"Daily reward asset {key} tier {index} image_asset",
        )
        if image_asset not in assets:
            raise ImproperlyConfigured(
                f"Daily reward asset {key} tier {index} references "
                f"unknown wallet asset {image_asset}"
            )

        tiers.append(
            {
                "min_amount": minimum,
                "image_asset": image_asset,
            }
        )
        previous_minimum = minimum

    if tiers[0]["min_amount"] != 1:
        raise ImproperlyConfigured(
            f"Daily reward asset {key} first tier must start at 1"
        )

    selected = tiers[0]
    for tier in tiers[1:]:
        if normalized_amount < tier["min_amount"]:
            break
        selected = tier

    return {
        "image": assets[selected["image_asset"]],
        "button_image": assets[button_asset],
        "tier_min_amount": selected["min_amount"],
    }


def get_wallet_token_pack_image_path(pack_code: str) -> str:
    code = _normalize_identifier(pack_code, field_name="Token pack code")
    path = WALLET_TOKEN_PACK_ASSETS.get(code)
    if path in (None, ""):
        try:
            path = str(WALLET_TOKEN_PACK_IMAGE_TEMPLATE).format(code=code)
        except (KeyError, ValueError) as exc:
            raise ImproperlyConfigured("Invalid WALLET_TOKEN_PACK_IMAGE_TEMPLATE") from exc
    return _normalize_static_image_path(path, field_name=f"Token pack {code} image")


def get_reward_chest_drop_image_path(
    *,
    chest_key: str,
    drop_key: str,
) -> str:
    chest_key = _normalize_identifier(
        chest_key,
        field_name="Reward Chest drop image chest key",
    )
    drop_key = _normalize_identifier(
        drop_key,
        field_name="Reward Chest drop image drop key",
    )
    chest_mapping = REWARD_CHEST_DROP_IMAGE_ASSETS.get(chest_key)
    if not isinstance(chest_mapping, dict):
        raise ImproperlyConfigured(
            f"No drop image mapping for Reward Chest {chest_key}"
        )
    asset_key = chest_mapping.get(drop_key)
    if not asset_key:
        raise ImproperlyConfigured(
            f"No drop image configured for {chest_key}/{drop_key}"
        )
    asset_key = _normalize_identifier(
        asset_key,
        field_name=f"Reward Chest {chest_key}/{drop_key} image asset",
    )
    image_path = WALLET_TOKEN_PACK_ASSETS.get(asset_key)
    if not image_path:
        raise ImproperlyConfigured(
            f"Unknown token pack asset {asset_key} for "
            f"{chest_key}/{drop_key}"
        )
    return _normalize_static_image_path(
        image_path,
        field_name=f"Reward Chest {chest_key}/{drop_key} image",
    )


def get_reward_chest_rarity_image_path(rarity: str) -> str:
    normalized = _normalize_identifier(
        rarity,
        field_name="Reward Chest rarity image",
    )
    display_rarity = (
        "legendary"
        if normalized == "jackpot"
        else normalized
    )
    image_path = REWARD_CHEST_RARITY_IMAGE_ASSETS.get(
        display_rarity
    )
    if not image_path:
        raise ImproperlyConfigured(
            "No rarity image configured for Reward Chest "
            f"rarity {display_rarity}"
        )
    return _normalize_static_image_path(
        image_path,
        field_name=(
            f"Reward Chest {display_rarity} rarity image"
        ),
    )


def _get_wallet_visual_groups(raw_groups, *, field_name: str, uppercase_keys: bool = False) -> dict[str, dict]:
    if not isinstance(raw_groups, dict):
        raise ImproperlyConfigured(f"{field_name} must be a dictionary")
    normalized = {}
    for raw_key, raw_group in raw_groups.items():
        key = str(raw_key or "").strip()
        key = key.upper() if uppercase_keys else _normalize_identifier(key, field_name=f"{field_name} key")
        if not key or not isinstance(raw_group, dict):
            raise ImproperlyConfigured(f"Invalid {field_name} entry: {raw_key}")
        group = dict(raw_group)
        if group.get("icon_path") not in (None, ""):
            group["icon_path"] = _normalize_static_image_path(group["icon_path"], field_name=f"{field_name} {key} icon_path")
        normalized[key] = group
    return normalized


def get_wallet_payment_groups() -> dict[str, dict]:
    return _get_wallet_visual_groups(WALLET_PAYMENT_GROUPS, field_name="WALLET_PAYMENT_GROUPS")


def get_wallet_crypto_asset_groups() -> dict[str, dict]:
    return _get_wallet_visual_groups(WALLET_CRYPTO_ASSET_GROUPS, field_name="WALLET_CRYPTO_ASSET_GROUPS", uppercase_keys=True)


def get_wallet_crypto_network_groups() -> dict[str, dict]:
    return _get_wallet_visual_groups(WALLET_CRYPTO_NETWORK_GROUPS, field_name="WALLET_CRYPTO_NETWORK_GROUPS")


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
    return WALLET_CHEST_ASSETS[normalized][state]


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
