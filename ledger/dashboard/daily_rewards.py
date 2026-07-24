from __future__ import annotations

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
