#!/usr/bin/env python3
"""Apply the complete wallet Daily Rewards feature to the MediaCMS fork.

Target: git101815/mediacms, branch advanced-monetisation.
Run from the repository root:
    python apply_daily_rewards_feature.py
"""

from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path.cwd()


def fail(message: str) -> None:
    raise SystemExit(message)


def read(path: str) -> str:
    target = ROOT / path
    if not target.exists():
        fail(f"Missing expected repository file: {path}")
    return target.read_text(encoding="utf-8")


def write(path: str, content: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def write_new(path: str, content: str) -> None:
    target = ROOT / path
    if target.exists():
        current = target.read_text(encoding="utf-8")
        if current == content:
            return
        fail(f"Refusing to overwrite existing file with different content: {path}")
    write(path, content)


def replace_once(path: str, old: str, new: str) -> None:
    content = read(path)
    if new in content:
        return
    count = content.count(old)
    if count != 1:
        fail(f"Expected exactly one patch context in {path}, found {count}")
    write(path, content.replace(old, new, 1))


def append_once(path: str, marker: str, addition: str) -> None:
    content = read(path)
    if marker in content:
        return
    if not content.endswith("\n"):
        content += "\n"
    write(path, content + addition)


# ---------------------------------------------------------------------------
# New dashboard package
# ---------------------------------------------------------------------------
write_new(
    "ledger/dashboard/__init__.py",
    '''"""Backend modules for the gamified wallet dashboard."""\n''',
)

write_new(
    "ledger/dashboard/config.py",
    '''"""Editable economy configuration for the wallet dashboard.

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
''',
)

write_new(
    "ledger/dashboard/models.py",
    '''from django.conf import settings
from django.db import models
from django.db.models import Q


class DailyRewardState(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="daily_reward_state",
    )
    current_streak = models.PositiveIntegerField(default=0)
    total_claims = models.PositiveIntegerField(default=0)
    last_claim_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=Q(current_streak__gte=0),
                name="daily_reward_state_streak_nonnegative",
            ),
            models.CheckConstraint(
                condition=Q(total_claims__gte=0),
                name="daily_reward_state_claims_nonnegative",
            ),
        ]

    def __str__(self):
        return f"Daily rewards for user {self.user_id}"


class DailyRewardClaim(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="daily_reward_claims",
    )
    reward_date = models.DateField()
    streak_day = models.PositiveIntegerField()
    cycle_day = models.PositiveIntegerField()
    amount = models.BigIntegerField()
    ledger_txn = models.OneToOneField(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="daily_reward_claim",
    )
    config_version = models.PositiveIntegerField()
    config_snapshot = models.JSONField(default=dict)
    claimed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-reward_date", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["user", "reward_date"],
                name="unique_daily_reward_claim_per_user_day",
            ),
            models.CheckConstraint(
                condition=Q(streak_day__gt=0),
                name="daily_reward_claim_streak_positive",
            ),
            models.CheckConstraint(
                condition=Q(cycle_day__gt=0),
                name="daily_reward_claim_cycle_positive",
            ),
            models.CheckConstraint(
                condition=Q(amount__gt=0),
                name="daily_reward_claim_amount_positive",
            ),
        ]
        indexes = [
            models.Index(
                fields=["user", "-claimed_at"],
                name="ledger_daily_user_claim_idx",
            ),
        ]

    def __str__(self):
        return f"Daily reward {self.reward_date} for user {self.user_id}"
''',
)

write_new(
    "ledger/dashboard/daily_rewards.py",
    '''from __future__ import annotations

import hashlib
import json
from datetime import datetime, time, timedelta, timezone as datetime_timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.utils import timezone

from ledger.models import (
    LEDGER_METADATA_VERSION,
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
)

from . import config
from .models import DailyRewardClaim, DailyRewardState


DAILY_REWARD_TRANSACTION_KIND = "daily_reward"
DAILY_REWARD_OUTBOX_TOPIC = "ledger.daily_reward.claimed"

_ASSET_IMAGE_PATHS = {
    "coins": "images/wallet/dashboard/reward-coins.png",
    "chest": "images/wallet/dashboard/reward-chest.png",
    "bigchest": "images/wallet/dashboard/reward-bigchest.png",
}


def _require_eligible_user(user):
    if not getattr(user, "is_authenticated", False):
        raise PermissionDenied("Authentication is required")
    if not getattr(user, "pk", None):
        raise PermissionDenied("A persisted user account is required")
    if not getattr(user, "is_active", False):
        raise PermissionDenied("Inactive accounts cannot claim daily rewards")
    return user


def get_daily_reward_timezone() -> ZoneInfo:
    timezone_name = (
        config.DAILY_REWARD_TIME_ZONE
        or getattr(settings, "TIME_ZONE", "UTC")
        or "UTC"
    )
    try:
        return ZoneInfo(str(timezone_name))
    except ZoneInfoNotFoundError as exc:
        raise ImproperlyConfigured(
            f"Unknown DAILY_REWARD_TIME_ZONE: {timezone_name}"
        ) from exc


def _normalize_instant(at=None):
    instant = at or timezone.now()
    if timezone.is_naive(instant):
        instant = instant.replace(tzinfo=datetime_timezone.utc)
    return instant


def get_daily_reward_date(at=None):
    return _normalize_instant(at).astimezone(get_daily_reward_timezone()).date()


def get_next_daily_reward_reset(at=None):
    instant = _normalize_instant(at)
    reward_timezone = get_daily_reward_timezone()
    local_instant = instant.astimezone(reward_timezone)
    next_day = local_instant.date() + timedelta(days=1)
    return datetime.combine(next_day, time.min, tzinfo=reward_timezone)


def _get_cycle_day(streak_day: int, cycle_length: int) -> int:
    return ((int(streak_day) - 1) % int(cycle_length)) + 1


def _calculate_claim_streak(state: DailyRewardState, reward_date) -> int:
    if state.last_claim_date is None:
        return 1
    if state.last_claim_date == reward_date:
        return int(state.current_streak)
    if state.last_claim_date == reward_date - timedelta(days=1):
        return int(state.current_streak) + 1
    if state.last_claim_date < reward_date - timedelta(days=1):
        return 1
    raise ValidationError("Daily reward state contains a future claim date")


def _wallet_claim_block_reason(wallet: TokenWallet | None) -> str:
    if wallet is None:
        return ""
    if wallet.risk_status == LEDGER_RISK_STATUS_BLOCKED:
        return "Wallet is blocked"
    if wallet.review_required or wallet.risk_status == LEDGER_RISK_STATUS_REVIEW:
        return "Wallet is under review"
    return ""


def _lock_wallet_pair(user_wallet: TokenWallet, issuance_wallet: TokenWallet):
    ids = sorted({user_wallet.pk, issuance_wallet.pk})
    locked = {
        wallet.pk: wallet
        for wallet in TokenWallet.objects.select_for_update().filter(pk__in=ids).order_by("pk")
    }
    if len(locked) != 2:
        raise ValidationError("Could not lock daily reward wallets")
    return locked[user_wallet.pk], locked[issuance_wallet.pk]


def _build_transaction_payload(*, user_id, reward_date, streak_day, cycle_day, reward):
    metadata = {
        "source": "wallet_daily_rewards",
        "user_id": int(user_id),
        "reward_date": reward_date.isoformat(),
        "streak_day": int(streak_day),
        "cycle_day": int(cycle_day),
        "cycle_length": len(config.get_daily_reward_definitions()),
        "amount_tokens": int(reward.amount_tokens),
        "amount_units": int(reward.amount_units),
        "asset": reward.asset,
        "config_version": int(config.DAILY_REWARD_CONFIG_VERSION),
    }
    external_id = f"daily-reward:user:{user_id}:date:{reward_date.isoformat()}"
    payload = {
        "external_id": external_id,
        "kind": DAILY_REWARD_TRANSACTION_KIND,
        "metadata": metadata,
    }
    request_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return external_id, request_hash, metadata


def _validate_existing_transaction(*, txn, user_wallet, issuance_wallet, amount, request_hash):
    if txn.kind != DAILY_REWARD_TRANSACTION_KIND:
        raise ValidationError("Daily reward idempotency key belongs to another transaction kind")
    if txn.status != LedgerTransaction.STATUS_POSTED:
        raise ValidationError("Existing daily reward transaction is not posted")
    if txn.request_hash and txn.request_hash != request_hash:
        raise ValidationError("Daily reward idempotency payload does not match")

    existing_entries = list(txn.entries.all())
    if len(existing_entries) != 2:
        raise ValidationError("Existing daily reward transaction must have two entries")
    entries = {entry.wallet_id: int(entry.delta) for entry in existing_entries}
    expected = {
        user_wallet.pk: int(amount),
        issuance_wallet.pk: -int(amount),
    }
    if entries != expected:
        raise ValidationError("Existing daily reward ledger entries do not match")


def _create_daily_reward_ledger_transaction(
    *, user, user_wallet, issuance_wallet, reward_date, streak_day, cycle_day, reward
):
    external_id, request_hash, metadata = _build_transaction_payload(
        user_id=user.pk,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        reward=reward,
    )

    existing = LedgerTransaction.objects.filter(external_id=external_id).first()
    if existing is not None:
        _validate_existing_transaction(
            txn=existing,
            user_wallet=user_wallet,
            issuance_wallet=issuance_wallet,
            amount=reward.amount_units,
            request_hash=request_hash,
        )
        return existing, metadata

    amount = int(reward.amount_units)
    user_wallet.balance = int(user_wallet.balance) + amount
    issuance_wallet.balance = int(issuance_wallet.balance) - amount
    user_wallet.save(update_fields=["balance", "updated_at"])
    issuance_wallet.save(update_fields=["balance", "updated_at"])

    txn = LedgerTransaction.objects.create(
        kind=DAILY_REWARD_TRANSACTION_KIND,
        status=LedgerTransaction.STATUS_POSTED,
        external_id=external_id,
        request_hash=request_hash,
        created_by=user,
        memo=f"Daily reward day {cycle_day}",
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )
    LedgerEntry.objects.create(
        txn=txn,
        wallet=issuance_wallet,
        delta=-amount,
        balance_after=issuance_wallet.balance,
    )
    LedgerEntry.objects.create(
        txn=txn,
        wallet=user_wallet,
        delta=amount,
        balance_after=user_wallet.balance,
    )
    LedgerOutbox.objects.create(
        txn=txn,
        topic=DAILY_REWARD_OUTBOX_TOPIC,
        aggregate_type="ledger_transaction",
        aggregate_id=txn.pk,
        status=LedgerOutbox.STATUS_PENDING,
        payload={
            "txn_id": txn.pk,
            "external_id": external_id,
            **metadata,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )
    return txn, metadata


@transaction.atomic
def claim_daily_reward(*, user, at=None) -> dict:
    user = _require_eligible_user(user)
    if not config.DAILY_REWARDS_ENABLED:
        raise ValidationError("Daily rewards are disabled")

    definitions = config.get_daily_reward_definitions()
    reward_date = get_daily_reward_date(at)

    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)

    state, _created = DailyRewardState.objects.get_or_create(user=locked_user)
    state = DailyRewardState.objects.select_for_update().get(pk=state.pk)

    existing_claim = (
        DailyRewardClaim.objects.select_related("ledger_txn")
        .filter(user=locked_user, reward_date=reward_date)
        .first()
    )
    if existing_claim is not None:
        return {
            "claimed": False,
            "already_claimed": True,
            "claim": existing_claim,
            "txn": existing_claim.ledger_txn,
            "amount_units": int(existing_claim.amount),
            "streak_day": int(existing_claim.streak_day),
            "cycle_day": int(existing_claim.cycle_day),
            "reward_date": reward_date,
        }

    streak_day = _calculate_claim_streak(state, reward_date)
    cycle_day = _get_cycle_day(streak_day, len(definitions))
    reward = definitions[cycle_day - 1]

    user_wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=locked_user,
        defaults={"allow_negative": False},
    )
    issuance_wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=TokenWallet.SYSTEM_ISSUANCE,
        defaults={"allow_negative": True},
    )
    if not issuance_wallet.allow_negative:
        raise ValidationError("System issuance wallet must allow negative balances")

    user_wallet, issuance_wallet = _lock_wallet_pair(user_wallet, issuance_wallet)
    block_reason = _wallet_claim_block_reason(user_wallet)
    issuance_block_reason = _wallet_claim_block_reason(issuance_wallet)
    if block_reason:
        raise ValidationError(block_reason)
    if issuance_block_reason:
        raise ValidationError(f"Daily reward issuance is unavailable: {issuance_block_reason}")

    txn, metadata = _create_daily_reward_ledger_transaction(
        user=locked_user,
        user_wallet=user_wallet,
        issuance_wallet=issuance_wallet,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        reward=reward,
    )

    claim = DailyRewardClaim.objects.create(
        user=locked_user,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        amount=reward.amount_units,
        ledger_txn=txn,
        config_version=config.DAILY_REWARD_CONFIG_VERSION,
        config_snapshot=metadata,
    )

    state.current_streak = streak_day
    state.last_claim_date = reward_date
    state.total_claims = int(state.total_claims) + 1
    state.save(
        update_fields=[
            "current_streak",
            "last_claim_date",
            "total_claims",
            "updated_at",
        ]
    )

    return {
        "claimed": True,
        "already_claimed": False,
        "claim": claim,
        "txn": txn,
        "amount_units": int(reward.amount_units),
        "streak_day": streak_day,
        "cycle_day": cycle_day,
        "reward_date": reward_date,
    }


def _format_token_amount(amount_tokens: int) -> str:
    return f"{int(amount_tokens):,}"


def _get_display_streak(state: DailyRewardState | None, reward_date) -> tuple[int, bool, bool]:
    if state is None or state.last_claim_date is None:
        return 1, False, True
    if state.last_claim_date == reward_date:
        return max(1, int(state.current_streak)), True, False
    if state.last_claim_date == reward_date - timedelta(days=1):
        return max(1, int(state.current_streak) + 1), False, True
    if state.last_claim_date < reward_date - timedelta(days=1):
        return 1, False, True
    return max(1, int(state.current_streak)), False, False


def _build_reward_row(definition, *, status: str) -> dict:
    return {
        "day": definition.day,
        "amount_tokens": definition.amount_tokens,
        "amount_units": definition.amount_units,
        "amount_display": _format_token_amount(definition.amount_tokens),
        "asset": definition.asset,
        "image_path": _ASSET_IMAGE_PATHS[definition.asset],
        "status": status,
    }


def build_daily_rewards_context(*, user, claim_url: str, at=None) -> dict:
    definitions = config.get_daily_reward_definitions()
    reward_date = get_daily_reward_date(at)
    state = DailyRewardState.objects.filter(user=user).first()
    display_streak, claimed_today, date_allows_claim = _get_display_streak(
        state, reward_date
    )
    cycle_day = _get_cycle_day(display_streak, len(definitions))
    current_reward = definitions[cycle_day - 1]

    wallet = TokenWallet.objects.filter(
        wallet_type=TokenWallet.TYPE_USER,
        user=user,
    ).first()
    issuance_wallet = TokenWallet.objects.filter(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=TokenWallet.SYSTEM_ISSUANCE,
    ).first()
    block_reason = _wallet_claim_block_reason(wallet)
    issuance_block_reason = _wallet_claim_block_reason(issuance_wallet)
    if not block_reason and issuance_block_reason:
        block_reason = "Daily reward issuance is unavailable"
    can_claim = bool(
        config.DAILY_REWARDS_ENABLED
        and getattr(user, "is_active", False)
        and date_allows_claim
        and not claimed_today
        and not block_reason
    )

    window_size = int(config.DAILY_REWARD_WINDOW_SIZE)
    start_day = max(1, min(cycle_day - (window_size // 2), len(definitions) - window_size + 1))
    end_day = start_day + window_size - 1

    def status_for(day):
        if day == cycle_day:
            return "current"
        if day < cycle_day:
            return "claimed"
        return "future"

    window = [
        _build_reward_row(definitions[day - 1], status=status_for(day))
        for day in range(start_day, end_day + 1)
    ]
    all_rewards = [
        _build_reward_row(definition, status=status_for(definition.day))
        for definition in definitions
    ]
    current_position = cycle_day - start_day
    timeline_percent = 0
    if window_size > 1:
        timeline_percent = round((current_position / (window_size - 1)) * 90, 2)

    return {
        "enabled": bool(config.DAILY_REWARDS_ENABLED),
        "claim_url": claim_url,
        "reward_date": reward_date,
        "timezone": str(get_daily_reward_timezone()),
        "next_reset_at": get_next_daily_reward_reset(at),
        "streak": display_streak,
        "cycle_day": cycle_day,
        "cycle_length": len(definitions),
        "claimed_today": claimed_today,
        "can_claim": can_claim,
        "block_reason": block_reason,
        "current_reward": _build_reward_row(current_reward, status="current"),
        "window": window,
        "all_rewards": all_rewards,
        "timeline_percent": timeline_percent,
    }
''',
)

write_new(
    "ledger/dashboard/views.py",
    '''from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.shortcuts import redirect
from django.views.decorators.http import require_POST

from .daily_rewards import claim_daily_reward


@login_required
@require_POST
def wallet_claim_daily_reward(request):
    try:
        result = claim_daily_reward(user=request.user)
    except (PermissionDenied, ValidationError) as exc:
        messages.error(request, exc.messages[0] if hasattr(exc, "messages") else str(exc))
        return redirect("wallet")

    if result["claimed"]:
        messages.success(request, "Daily reward claimed.")
    else:
        messages.info(request, "Today's daily reward was already claimed.")
    return redirect("wallet")
''',
)

# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------
write_new(
    "ledger/migrations/0035_daily_rewards.py",
    '''# Generated for wallet dashboard daily rewards.

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ledger", "0034_remove_tokenpack_image"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="DailyRewardState",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("current_streak", models.PositiveIntegerField(default=0)),
                ("total_claims", models.PositiveIntegerField(default=0)),
                ("last_claim_date", models.DateField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("user", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="daily_reward_state", to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name="DailyRewardClaim",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("reward_date", models.DateField()),
                ("streak_day", models.PositiveIntegerField()),
                ("cycle_day", models.PositiveIntegerField()),
                ("amount", models.BigIntegerField()),
                ("config_version", models.PositiveIntegerField()),
                ("config_snapshot", models.JSONField(default=dict)),
                ("claimed_at", models.DateTimeField(auto_now_add=True)),
                ("ledger_txn", models.OneToOneField(on_delete=django.db.models.deletion.PROTECT, related_name="daily_reward_claim", to="ledger.ledgertransaction")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="daily_reward_claims", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-reward_date", "-id"]},
        ),
        migrations.AddConstraint(
            model_name="dailyrewardstate",
            constraint=models.CheckConstraint(condition=models.Q(("current_streak__gte", 0)), name="daily_reward_state_streak_nonnegative"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardstate",
            constraint=models.CheckConstraint(condition=models.Q(("total_claims__gte", 0)), name="daily_reward_state_claims_nonnegative"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.UniqueConstraint(fields=("user", "reward_date"), name="unique_daily_reward_claim_per_user_day"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.CheckConstraint(condition=models.Q(("streak_day__gt", 0)), name="daily_reward_claim_streak_positive"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.CheckConstraint(condition=models.Q(("cycle_day__gt", 0)), name="daily_reward_claim_cycle_positive"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.CheckConstraint(condition=models.Q(("amount__gt", 0)), name="daily_reward_claim_amount_positive"),
        ),
        migrations.AddIndex(
            model_name="dailyrewardclaim",
            index=models.Index(fields=["user", "-claimed_at"], name="ledger_daily_user_claim_idx"),
        ),
    ]
''',
)

# ---------------------------------------------------------------------------
# Register dashboard models in the ledger app and admin
# ---------------------------------------------------------------------------
append_once(
    "ledger/models.py",
    "from .dashboard.models import DailyRewardClaim, DailyRewardState",
    '''\n# Dashboard models are kept in ledger/dashboard while remaining part of the\n# ledger Django app.\nfrom .dashboard.models import DailyRewardClaim, DailyRewardState  # noqa: E402,F401\n''',
)

replace_once(
    "ledger/admin.py",
    '''from .services import (\n''',
    '''from .dashboard.models import DailyRewardClaim, DailyRewardState\nfrom .services import (\n''',
)

append_once(
    "ledger/admin.py",
    "class DailyRewardClaimAdmin",
    '''\n\n@admin.register(DailyRewardState)\nclass DailyRewardStateAdmin(ReadOnlyAdmin):\n    list_display = (\n        "id",\n        "user",\n        "current_streak",\n        "total_claims",\n        "last_claim_date",\n        "updated_at",\n    )\n    search_fields = ("user__username", "user__email")\n    readonly_fields = (\n        "user",\n        "current_streak",\n        "total_claims",\n        "last_claim_date",\n        "created_at",\n        "updated_at",\n    )\n\n\n@admin.register(DailyRewardClaim)\nclass DailyRewardClaimAdmin(ReadOnlyAdmin):\n    list_display = (\n        "id",\n        "user",\n        "reward_date",\n        "streak_day",\n        "cycle_day",\n        "amount",\n        "ledger_txn",\n        "config_version",\n        "claimed_at",\n    )\n    list_filter = ("reward_date", "cycle_day", "config_version")\n    search_fields = (\n        "user__username",\n        "user__email",\n        "ledger_txn__external_id",\n    )\n    readonly_fields = (\n        "user",\n        "reward_date",\n        "streak_day",\n        "cycle_day",\n        "amount",\n        "ledger_txn",\n        "config_version",\n        "config_snapshot",\n        "claimed_at",\n    )\n''',
)

# ---------------------------------------------------------------------------
# URL and wallet view wiring
# ---------------------------------------------------------------------------
replace_once(
    "files/urls.py",
    '''from .feeds import IndexRSSFeed, SearchRSSFeed\n''',
    '''from .feeds import IndexRSSFeed, SearchRSSFeed\nfrom ledger.dashboard import views as wallet_dashboard_views\n''',
)
replace_once(
    "files/urls.py",
    '''    re_path(r"^wallet/ad-free/purchase$", views.wallet_purchase_ad_free, name="wallet_purchase_ad_free"),\n''',
    '''    re_path(r"^wallet/ad-free/purchase$", views.wallet_purchase_ad_free, name="wallet_purchase_ad_free"),\n    path(\n        "wallet/daily-rewards/claim",\n        wallet_dashboard_views.wallet_claim_daily_reward,\n        name="wallet_claim_daily_reward",\n    ),\n''',
)

replace_once(
    "files/views.py",
    '''from ledger.fiat import (\n''',
    '''from ledger.dashboard.daily_rewards import build_daily_rewards_context\nfrom ledger.fiat import (\n''',
)
replace_once(
    "files/views.py",
    '''    context["ad_free"] = {\n        "active": bool(getattr(request.user, "adFreeUser", False)),\n        "price_tokens": ad_free_price_tokens,\n        "price_display": _format_platform_token_amount(ad_free_price_tokens),\n        "purchase_url": reverse("wallet_purchase_ad_free"),\n        "can_purchase": (\n                not getattr(request.user, "adFreeUser", False)\n                and wallet_actions.get("can_deposit", False)\n                and available_balance >= ad_free_price_tokens\n        ),\n    }\n    return render(request, "cms/wallet.html", context)\n''',
    '''    context["ad_free"] = {\n        "active": bool(getattr(request.user, "adFreeUser", False)),\n        "price_tokens": ad_free_price_tokens,\n        "price_display": _format_platform_token_amount(ad_free_price_tokens),\n        "purchase_url": reverse("wallet_purchase_ad_free"),\n        "can_purchase": (\n                not getattr(request.user, "adFreeUser", False)\n                and wallet_actions.get("can_deposit", False)\n                and available_balance >= ad_free_price_tokens\n        ),\n    }\n    context["daily_rewards"] = build_daily_rewards_context(\n        user=request.user,\n        claim_url=reverse("wallet_claim_daily_reward"),\n    )\n    return render(request, "cms/wallet.html", context)\n''',
)

# ---------------------------------------------------------------------------
# Dynamic dashboard UI (same approved layout)
# ---------------------------------------------------------------------------
old_daily = '''          <section class="wallet-game-card wallet-game-panel wallet-game-daily" data-wallet-module="daily-rewards">\n            <div class="wallet-game-panel__head">\n              <div><div class="wallet-game-card__title">Daily Rewards</div><p>Come back every day to earn CF tokens!</p></div>\n              <div class="wallet-game-daily__streak"><i class="material-icons">local_fire_department</i><strong>12</strong><span>/ 30</span></div>\n            </div>\n            <div class="wallet-game-rewards">\n              <article class="wallet-game-reward wallet-game-reward--claimed"><i class="material-icons">done</i><strong>100</strong><span>Day 10</span></article>\n              <article class="wallet-game-reward wallet-game-reward--claimed"><i class="material-icons">done</i><strong>150</strong><span>Day 11</span></article>\n              <article class="wallet-game-reward wallet-game-reward--current"><img src="{% static 'images/wallet/dashboard/reward-chest.png' %}" alt=""><strong>250</strong><span>Day 12</span></article>\n              <article class="wallet-game-reward"><img src="{% static 'images/wallet/dashboard/reward-coins.png' %}" alt=""><strong>400</strong><span>Day 13</span></article>\n              <article class="wallet-game-reward"><img src="{% static 'images/wallet/dashboard/reward-bigchest.png' %}" alt=""><strong>1,000</strong><span>Day 14</span></article>\n            </div>\n            <div class="wallet-game-daily__timeline" aria-hidden="true"><span></span><i></i><i></i><i class="is-current"></i><i></i><i></i></div>\n            <div class="wallet-game-panel__footer">\n              <button type="button" class="wallet-game-button wallet-game-button--primary" data-wallet-action="claim-daily">Claim 250 <img src="{% static 'images/wallet/cf-token.png' %}" alt=""></button>\n              <button type="button" class="wallet-game-link" data-wallet-action="view-daily-rewards">View all rewards <i class="material-icons">arrow_forward</i></button>\n            </div>\n          </section>\n'''
new_daily = '''          <section class="wallet-game-card wallet-game-panel wallet-game-daily" data-wallet-module="daily-rewards">\n            <div class="wallet-game-panel__head">\n              <div><div class="wallet-game-card__title">Daily Rewards</div><p>Come back every day to earn CF tokens!</p></div>\n              <div class="wallet-game-daily__streak"><i class="material-icons">local_fire_department</i><strong>{{ daily_rewards.cycle_day }}</strong><span>/ {{ daily_rewards.cycle_length }}</span></div>\n            </div>\n            <div class="wallet-game-rewards">\n              {% for reward in daily_rewards.window %}\n              <article class="wallet-game-reward{% if reward.status == 'claimed' %} wallet-game-reward--claimed{% elif reward.status == 'current' %} wallet-game-reward--current{% endif %}">\n                {% if reward.status == 'claimed' %}<i class="material-icons">done</i>{% else %}<img src="{% static reward.image_path %}" alt="">{% endif %}\n                <strong>{{ reward.amount_display }}</strong><span>Day {{ reward.day }}</span>\n              </article>\n              {% endfor %}\n            </div>\n            <div class="wallet-game-daily__timeline" aria-hidden="true">\n              <span style="width:{{ daily_rewards.timeline_percent }}%"></span>\n              {% for reward in daily_rewards.window %}<i{% if reward.status == 'claimed' %} class="is-claimed"{% elif reward.status == 'current' %} class="is-current"{% endif %}></i>{% endfor %}\n            </div>\n            <div class="wallet-game-panel__footer">\n              <form method="post" action="{{ daily_rewards.claim_url }}" style="display:contents">\n                {% csrf_token %}\n                <button type="submit" class="wallet-game-button wallet-game-button--primary" data-wallet-action="claim-daily"{% if not daily_rewards.can_claim %} disabled{% endif %}>\n                  {% if daily_rewards.claimed_today %}Claimed{% else %}Claim {{ daily_rewards.current_reward.amount_display }}{% endif %}\n                  <img src="{% static 'images/wallet/cf-token.png' %}" alt="">\n                </button>\n              </form>\n              <button type="button" class="wallet-game-link" data-wallet-open="daily-rewards">View all rewards <i class="material-icons">arrow_forward</i></button>\n            </div>\n          </section>\n'''
replace_once("templates/cms/wallet.html", old_daily, new_daily)
replace_once(
    "templates/cms/wallet.html",
    '''            <div class="wallet-game-streak__badge"><span>Keep your streak!</span><strong>12</strong></div>\n''',
    '''            <div class="wallet-game-streak__badge"><span>Keep your streak!</span><strong>{{ daily_rewards.streak }}</strong></div>\n''',
)

modal = '''\n<div class="wallet-modal" data-wallet-modal="daily-rewards" hidden>\n  <div class="wallet-modal__backdrop" data-wallet-close="daily-rewards"></div>\n  <div class="wallet-modal__dialog wallet-modal__dialog--deposit wallet-daily-rewards-modal" role="dialog" aria-modal="true" aria-labelledby="wallet-daily-rewards-modal-title">\n    <button type="button" class="wallet-modal__close" data-wallet-close="daily-rewards" aria-label="Close">\n      <i class="material-icons" aria-hidden="true">close</i>\n    </button>\n    <h2 id="wallet-daily-rewards-modal-title" class="wallet-modal__title">Daily Rewards</h2>\n    <div class="wallet-daily-rewards-modal__grid">\n      {% for reward in daily_rewards.all_rewards %}\n      <article class="wallet-game-reward{% if reward.status == 'claimed' %} wallet-game-reward--claimed{% elif reward.status == 'current' %} wallet-game-reward--current{% endif %}">\n        {% if reward.status == 'claimed' %}<i class="material-icons">done</i>{% else %}<img src="{% static reward.image_path %}" alt="">{% endif %}\n        <strong>{{ reward.amount_display }}</strong><span>Day {{ reward.day }}</span>\n      </article>\n      {% endfor %}\n    </div>\n  </div>\n</div>\n\n'''
replace_once(
    "templates/cms/wallet.html",
    '''{% if wallet_actions.show_withdraw %}\n<div class="wallet-modal" data-wallet-modal="withdraw" hidden>\n''',
    modal + '''{% if wallet_actions.show_withdraw %}\n<div class="wallet-modal" data-wallet-modal="withdraw" hidden>\n''',
)

# Timeline classes are now data-driven; add only modal-specific styling.
replace_once(
    "frontend/src/static/css/WalletPage.scss",
    '''.wallet-game-daily__timeline i:nth-of-type(-n+2),.wallet-game-daily__timeline i.is-current''',
    '''.wallet-game-daily__timeline i.is-claimed,.wallet-game-daily__timeline i.is-current''',
)
append_once(
    "frontend/src/static/css/WalletPage.scss",
    ".wallet-daily-rewards-modal__grid",
    '''\n.wallet-daily-rewards-modal{max-width:760px}.wallet-daily-rewards-modal__grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:9px;max-height:min(68vh,680px);margin-top:20px;padding-right:4px;overflow:auto}.wallet-daily-rewards-modal__grid .wallet-game-reward{min-height:112px}@media(max-width:760px){.wallet-daily-rewards-modal__grid{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(max-width:430px){.wallet-daily-rewards-modal__grid{grid-template-columns:repeat(2,minmax(0,1fr))}}\n''',
)

# ---------------------------------------------------------------------------
# Functional tests only: accounting, state, permissions, idempotency, routing
# ---------------------------------------------------------------------------
write_new(
    "tests/ledger/test_daily_rewards.py",
    '''from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.test import override_settings
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.daily_rewards import claim_daily_reward, get_daily_reward_date
from ledger.dashboard.models import DailyRewardClaim, DailyRewardState
from ledger.models import (
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
)
from tests.ledger.base import BaseLedgerTestCase


@override_settings(TIME_ZONE="Europe/Moscow")
class TestDailyRewards(BaseLedgerTestCase):
    def instant(self, year=2026, month=7, day=24, hour=12):
        return datetime(year, month, day, hour, tzinfo=datetime_timezone.utc)

    def test_first_claim_posts_balanced_ledger_and_audit_state(self):
        before = int(self.w1.balance)
        result = claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()
        claim = DailyRewardClaim.objects.get(user=self.u1)
        state = DailyRewardState.objects.get(user=self.u1)

        expected = config.DAILY_REWARDS[0]["amount"] * 1_000_000
        self.assertTrue(result["claimed"])
        self.assertEqual(claim.amount, expected)
        self.assertEqual(self.w1.balance, before + expected)
        self.assertEqual(self.issuance.balance, -expected)
        self.assertEqual(state.current_streak, 1)
        self.assertEqual(state.total_claims, 1)
        self.assertEqual(state.last_claim_date, result["reward_date"])

        entries = list(LedgerEntry.objects.filter(txn=claim.ledger_txn))
        self.assertEqual(len(entries), 2)
        self.assertEqual(sum(entry.delta for entry in entries), 0)
        self.assertEqual(
            LedgerOutbox.objects.filter(
                txn=claim.ledger_txn,
                topic="ledger.daily_reward.claimed",
            ).count(),
            1,
        )
        self.assertEqual(claim.config_snapshot["amount_units"], expected)

    def test_second_claim_same_day_is_idempotent(self):
        first = claim_daily_reward(user=self.u1, at=self.instant())
        self.w1.refresh_from_db()
        balance_after_first = int(self.w1.balance)

        second = claim_daily_reward(
            user=self.u1,
            at=self.instant(hour=23),
        )
        self.w1.refresh_from_db()

        self.assertFalse(second["claimed"])
        self.assertTrue(second["already_claimed"])
        self.assertEqual(first["claim"].pk, second["claim"].pk)
        self.assertEqual(self.w1.balance, balance_after_first)
        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 1)
        self.assertEqual(
            LedgerTransaction.objects.filter(kind="daily_reward").count(),
            1,
        )

    def test_consecutive_day_increments_streak_and_uses_next_reward(self):
        first = claim_daily_reward(user=self.u1, at=self.instant(day=24))
        second = claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(first["streak_day"], 1)
        self.assertEqual(second["streak_day"], 2)
        self.assertEqual(second["cycle_day"], 2)
        self.assertEqual(
            second["amount_units"],
            config.DAILY_REWARDS[1]["amount"] * 1_000_000,
        )

    def test_missing_a_day_resets_streak(self):
        claim_daily_reward(user=self.u1, at=self.instant(day=20))
        result = claim_daily_reward(user=self.u1, at=self.instant(day=22))

        self.assertEqual(result["streak_day"], 1)
        self.assertEqual(result["cycle_day"], 1)

    def test_cycle_wraps_after_last_configured_day(self):
        previous_date = get_daily_reward_date(self.instant(day=24))
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=len(config.DAILY_REWARDS),
            total_claims=len(config.DAILY_REWARDS),
            last_claim_date=previous_date,
        )
        result = claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(result["streak_day"], len(config.DAILY_REWARDS) + 1)
        self.assertEqual(result["cycle_day"], 1)

    def test_blocked_and_review_wallets_cannot_claim(self):
        self.w1.risk_status = LEDGER_RISK_STATUS_BLOCKED
        self.w1.save(update_fields=["risk_status"])
        with self.assertRaises(ValidationError):
            claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.risk_status = LEDGER_RISK_STATUS_REVIEW
        self.w1.review_required = True
        self.w1.save(update_fields=["risk_status", "review_required"])
        with self.assertRaises(ValidationError):
            claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 0)

    def test_future_state_is_rejected_without_credit(self):
        reward_date = get_daily_reward_date(self.instant(day=24))
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=5,
            total_claims=5,
            last_claim_date=reward_date + timedelta(days=1),
        )
        with self.assertRaises(ValidationError):
            claim_daily_reward(user=self.u1, at=self.instant(day=24))
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)

    def test_inactive_user_and_disabled_feature_cannot_claim(self):
        self.u1.is_active = False
        self.u1.save(update_fields=["is_active"])
        with self.assertRaises(PermissionDenied):
            claim_daily_reward(user=self.u1, at=self.instant())

        self.u1.is_active = True
        self.u1.save(update_fields=["is_active"])
        with patch.object(config, "DAILY_REWARDS_ENABLED", False):
            with self.assertRaises(ValidationError):
                claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 0)

    def test_config_rejects_invalid_or_dangerous_amounts(self):
        with patch.object(config, "DAILY_REWARDS", ({"amount": 0, "asset": "coins"},)):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_definitions()
        with patch.object(
            config,
            "DAILY_REWARDS",
            ({"amount": config.DAILY_REWARD_MAX_TOKENS_PER_CLAIM + 1, "asset": "coins"},),
        ):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_definitions()

    def test_claim_endpoint_requires_login_and_post(self):
        url = reverse("wallet_claim_daily_reward")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 302)

        self.client.force_login(self.u1)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 405)

    def test_claim_endpoint_changes_balance_once(self):
        self.client.force_login(self.u1)
        url = reverse("wallet_claim_daily_reward")

        first = self.client.post(url)
        second = self.client.post(url)

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 302)
        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 1)
        self.assertEqual(
            LedgerTransaction.objects.filter(kind="daily_reward").count(),
            1,
        )
''',
)

print("Daily Rewards feature applied.")
print("Next commands:")
print("  python manage.py migrate")
print("  pytest tests/ledger/test_daily_rewards.py")
print("  npm/webpack rebuild for frontend static assets")