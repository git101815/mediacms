#!/usr/bin/env python3
"""Prepare disposable accounts for manual Reward Chest click tests.

Run from the MediaCMS repository root with the project environment active:

    ALLOW_MANUAL_CHEST_TESTS=1 python prepare_manual_chest_click_tests.py

This creates:
- one account positioned on the first Small Chest Daily Reward;
- one account positioned on the first medium_chest Daily Reward;
- one account positioned on the first big_chest Daily Reward;
- one account with a pending Bonus Vault chest.

The accounts are intentionally disposable and should only be created in a
local or staging database.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import timedelta
from pathlib import Path


def fail(message: str) -> None:
    raise SystemExit(message)


if os.environ.get("ALLOW_MANUAL_CHEST_TESTS") != "1":
    fail(
        "Refusing to create test accounts. Re-run with "
        "ALLOW_MANUAL_CHEST_TESTS=1."
    )

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "cms.settings")

import django  # noqa: E402

django.setup()

from django.contrib.auth import get_user_model  # noqa: E402
from django.db import transaction  # noqa: E402

from ledger.dashboard import config  # noqa: E402
from ledger.dashboard.daily_rewards import get_daily_reward_date  # noqa: E402
from ledger.dashboard.models import DailyRewardState  # noqa: E402
from ledger.dashboard.reward_chests import grant_reward_chest  # noqa: E402


CHEST_KEYS = (
    "small_chest",
    "medium_chest",
    "big_chest",
)


def find_first_chest_day(chest_key: str) -> int:
    for reward in config.get_daily_reward_definitions():
        if reward.kind == "chest" and reward.chest_key == chest_key:
            return int(reward.day)

    fail(
        f"No Daily Reward uses {chest_key!r}. "
        "Update CHEST_KEYS or the reward configuration."
    )


definitions = {
    chest_key: config.get_reward_chest_definition(chest_key)
    for chest_key in CHEST_KEYS
}
target_days = {
    chest_key: find_first_chest_day(chest_key)
    for chest_key in CHEST_KEYS
}

batch_id = uuid.uuid4().hex[:8]
password = f"ChestTest-{batch_id}!"
reward_date = get_daily_reward_date()
yesterday = reward_date - timedelta(days=1)
user_model = get_user_model()
created_accounts: list[dict] = []


with transaction.atomic():
    for chest_key in CHEST_KEYS:
        day = target_days[chest_key]
        username = f"manual_{chest_key}_{batch_id}"

        user = user_model.objects.create_user(
            username=username,
            password=password,
            is_active=True,
        )
        DailyRewardState.objects.create(
            user=user,
            current_streak=day - 1,
            total_claims=day - 1,
            last_claim_date=yesterday,
        )

        created_accounts.append(
            {
                "kind": "daily",
                "username": username,
                "target": definitions[chest_key].label,
                "day": day,
            }
        )

    vault_username = f"manual_bonus_vault_{batch_id}"
    vault_user = user_model.objects.create_user(
        username=vault_username,
        password=password,
        is_active=True,
    )
    vault_chest = config.get_reward_chest_definition(
        config.BONUS_VAULT_CHEST_KEY
    )
    grant_reward_chest(
        user=vault_user,
        chest_key=config.BONUS_VAULT_CHEST_KEY,
        source_type=config.BONUS_VAULT_SOURCE_TYPE,
        source_ref=f"manual-test:{batch_id}:vault:1",
        metadata={
            "source": "manual_reward_chest_test",
            "bonus_vault_cycle": 1,
            "bonus_vault_threshold_tokens": (
                config.BONUS_VAULT_THRESHOLD_TOKENS
            ),
            "bonus_vault_threshold_units": (
                config.BONUS_VAULT_THRESHOLD_TOKENS
                * (10 ** config.PLATFORM_TOKEN_DECIMALS)
            ),
        },
    )
    created_accounts.append(
        {
            "kind": "vault",
            "username": vault_username,
            "target": vault_chest.label,
            "day": None,
        }
    )


print()
print("Manual Reward Chest test accounts created")
print("=" * 48)
print(f"Password for every account: {password}")
print()
for account in created_accounts:
    if account["kind"] == "daily":
        print(
            f"{account['username']:<42} "
            f"Daily day {account['day']}: {account['target']}"
        )
    else:
        print(
            f"{account['username']:<42} "
            f"Bonus Vault: {account['target']}"
        )

print()
print("Open /accounts/login/, then /wallet.")
print("These accounts are disposable. Do not create them in production.")
