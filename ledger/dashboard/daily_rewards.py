from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone as datetime_timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Q
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
from .models import (
    DailyRewardClaim,
    DailyRewardState,
    RewardChestGrant,
)
from .reward_chests import grant_reward_chest, open_reward_chest


DAILY_REWARD_TRANSACTION_KIND = "daily_reward"
DAILY_REWARD_OUTBOX_TOPIC = "ledger.daily_reward.claimed"

_ASSET_IMAGE_PATHS = {
    "coins": "images/wallet/dashboard/reward-coins.png",
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


def get_daily_reward_cycle_reset(
    *,
    at=None,
    cycle_day: int,
    cycle_length: int | None = None,
):
    resolved_cycle_length = (
        len(config.get_daily_reward_definitions())
        if cycle_length is None
        else int(cycle_length)
    )
    resolved_cycle_day = int(cycle_day)
    if resolved_cycle_length < 1:
        raise ValidationError("Daily reward cycle length must be positive")
    if resolved_cycle_day < 1 or resolved_cycle_day > resolved_cycle_length:
        raise ValidationError("Daily reward cycle day is outside the reward cycle")

    instant = _normalize_instant(at)
    reward_timezone = get_daily_reward_timezone()
    reward_date = instant.astimezone(reward_timezone).date()
    days_until_reset = resolved_cycle_length - resolved_cycle_day + 1
    reset_date = reward_date + timedelta(days=days_until_reset)
    return datetime.combine(reset_date, time.min, tzinfo=reward_timezone)


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
        for wallet in TokenWallet.objects.select_for_update()
        .filter(pk__in=ids)
        .order_by("pk")
    }
    if len(locked) != 2:
        raise ValidationError("Could not lock daily reward wallets")
    return locked[user_wallet.pk], locked[issuance_wallet.pk]


def _build_fixed_transaction_payload(
    *, user_id, reward_date, streak_day, cycle_day, reward
):
    metadata = {
        "source": "wallet_daily_rewards",
        "reward_kind": "fixed",
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


def _validate_existing_fixed_transaction(
    *, txn, user_wallet, issuance_wallet, amount, request_hash
):
    if txn.kind != DAILY_REWARD_TRANSACTION_KIND:
        raise ValidationError(
            "Daily reward idempotency key belongs to another transaction kind"
        )
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


def _create_fixed_daily_reward_ledger_transaction(
    *, user, user_wallet, issuance_wallet, reward_date, streak_day, cycle_day, reward
):
    external_id, request_hash, metadata = _build_fixed_transaction_payload(
        user_id=user.pk,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        reward=reward,
    )

    existing = LedgerTransaction.objects.filter(external_id=external_id).first()
    if existing is not None:
        _validate_existing_fixed_transaction(
            txn=existing,
            user_wallet=user_wallet,
            issuance_wallet=issuance_wallet,
            amount=reward.amount_units,
            request_hash=request_hash,
        )
        return existing, metadata

    amount = int(reward.amount_units)
    user_wallet.balance = int(user_wallet.balance) + amount
    user_wallet.promotional_balance = (
        int(user_wallet.promotional_balance) + amount
    )
    user_wallet.restricted_promotional_balance = (
        int(user_wallet.restricted_promotional_balance) + amount
    )
    issuance_wallet.balance = int(issuance_wallet.balance) - amount
    user_wallet.save(
        update_fields=[
            "balance",
            "promotional_balance",
            "restricted_promotional_balance",
            "updated_at",
        ]
    )
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
        promotional_delta=amount,
        restricted_promotional_delta=amount,
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


def _claim_fixed_reward(
    *, user, reward_date, streak_day, cycle_day, reward
) -> tuple[LedgerTransaction, dict, None, dict]:
    user_wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=user,
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
        raise ValidationError(
            f"Daily reward issuance is unavailable: {issuance_block_reason}"
        )

    txn, metadata = _create_fixed_daily_reward_ledger_transaction(
        user=user,
        user_wallet=user_wallet,
        issuance_wallet=issuance_wallet,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        reward=reward,
    )
    result = {
        "reward_kind": "fixed",
        "amount_units": int(reward.amount_units),
        "amount_tokens": int(reward.amount_tokens),
        "drop_key": "",
        "drop_label": "",
        "rarity": "",
    }
    return txn, metadata, None, result


def _daily_chest_source_ref(*, user_id: int, reward_date) -> str:
    return f"user:{int(user_id)}:date:{reward_date.isoformat()}"


def _daily_chest_grant_metadata(grant: RewardChestGrant) -> dict:
    metadata = grant.metadata if isinstance(grant.metadata, dict) else {}
    try:
        reward_date = date.fromisoformat(str(metadata.get("reward_date") or ""))
        streak_day = int(metadata.get("streak_day"))
        cycle_day = int(metadata.get("cycle_day"))
        cycle_length = int(
            metadata.get("cycle_length")
            or len(config.get_daily_reward_definitions())
        )
    except (TypeError, ValueError) as exc:
        raise ValidationError("The prepared daily chest metadata is invalid") from exc
    if streak_day < 1 or cycle_day < 1 or cycle_length < cycle_day:
        raise ValidationError("The prepared daily chest metadata is invalid")
    return {
        "reward_date": reward_date,
        "streak_day": streak_day,
        "cycle_day": cycle_day,
        "cycle_length": cycle_length,
    }


@transaction.atomic
def prepare_daily_reward_chest(
    *,
    user,
    at=None,
    grant_public_id=None,
) -> dict:
    user = _require_eligible_user(user)
    if not config.DAILY_REWARDS_ENABLED:
        raise ValidationError("Daily rewards are disabled")

    now = _normalize_instant(at)
    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)

    if grant_public_id not in (None, ""):
        grant = (
            RewardChestGrant.objects.select_for_update()
            .filter(
                public_id=grant_public_id,
                user=locked_user,
                source_type="daily_reward",
                status=RewardChestGrant.STATUS_PENDING,
            )
            .first()
        )
        if grant is None:
            raise ValidationError("The pending daily Reward Chest was not found")
        if grant.expires_at is not None and grant.expires_at <= now:
            raise ValidationError("Reward Chest has expired")
        definition = config.reward_chest_definition_from_snapshot(
            grant.config_snapshot
        )
        grant_metadata = _daily_chest_grant_metadata(grant)
        return {
            "prepared": True,
            "grant": grant,
            "reward_kind": "chest",
            "chest_label": definition.label,
            "closed_image_path": definition.closed_image,
            "opened_image_path": definition.opened_image,
            "box_state": "closed",
            "streak_day": grant_metadata["streak_day"],
            "cycle_day": grant_metadata["cycle_day"],
            "reward_date": grant_metadata["reward_date"],
        }

    definitions = config.get_daily_reward_definitions()
    reward_date = get_daily_reward_date(at)
    source_ref = _daily_chest_source_ref(
        user_id=locked_user.pk,
        reward_date=reward_date,
    )

    existing_claim = DailyRewardClaim.objects.filter(
        user=locked_user,
        reward_date=reward_date,
    ).first()
    if existing_claim is not None:
        raise ValidationError("Today's daily reward was already claimed")

    existing_grant = (
        RewardChestGrant.objects.select_for_update()
        .filter(
            user=locked_user,
            source_type="daily_reward",
            source_ref=source_ref,
        )
        .first()
    )
    if existing_grant is not None:
        if existing_grant.status != RewardChestGrant.STATUS_PENDING:
            raise ValidationError("Today's daily reward was already claimed")
        if (
            existing_grant.expires_at is not None
            and existing_grant.expires_at <= now
        ):
            raise ValidationError("Reward Chest has expired")
        definition = config.reward_chest_definition_from_snapshot(
            existing_grant.config_snapshot
        )
        grant_metadata = _daily_chest_grant_metadata(existing_grant)
        return {
            "prepared": True,
            "grant": existing_grant,
            "reward_kind": "chest",
            "chest_label": definition.label,
            "closed_image_path": definition.closed_image,
            "opened_image_path": definition.opened_image,
            "box_state": "closed",
            "streak_day": grant_metadata["streak_day"],
            "cycle_day": grant_metadata["cycle_day"],
            "reward_date": grant_metadata["reward_date"],
        }

    state, _created = DailyRewardState.objects.get_or_create(user=locked_user)
    state = DailyRewardState.objects.select_for_update().get(pk=state.pk)
    if state.last_claim_date == reward_date:
        raise ValidationError("Today's daily reward was already claimed")

    streak_day = _calculate_claim_streak(state, reward_date)
    cycle_day = _get_cycle_day(streak_day, len(definitions))
    reward = definitions[cycle_day - 1]
    if reward.kind != "chest":
        raise ValidationError("Today's daily reward is not a chest")

    user_wallet = TokenWallet.objects.filter(
        wallet_type=TokenWallet.TYPE_USER,
        user=locked_user,
    ).first()
    issuance_wallet = TokenWallet.objects.filter(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=TokenWallet.SYSTEM_ISSUANCE,
    ).first()
    block_reason = _wallet_claim_block_reason(user_wallet)
    issuance_block_reason = _wallet_claim_block_reason(issuance_wallet)
    if block_reason:
        raise ValidationError(block_reason)
    if issuance_block_reason:
        raise ValidationError("Daily reward issuance is unavailable")

    cycle_reset_at = get_daily_reward_cycle_reset(
        at=at,
        cycle_day=cycle_day,
        cycle_length=len(definitions),
    )
    grant = grant_reward_chest(
        user=locked_user,
        chest_key=reward.chest_key,
        source_type="daily_reward",
        source_ref=source_ref,
        metadata={
            "reward_date": reward_date.isoformat(),
            "streak_day": int(streak_day),
            "cycle_day": int(cycle_day),
            "cycle_length": len(definitions),
            "cycle_reset_at": cycle_reset_at.isoformat(),
            "daily_reward_config_version": int(
                config.DAILY_REWARD_CONFIG_VERSION
            ),
        },
        expires_at=cycle_reset_at,
    )

    # Claiming the daily slot and opening the chest are separate operations.
    # The daily cycle can continue while the immutable chest grant remains
    # pending until the end of this reward cycle.
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
        "prepared": True,
        "grant": grant,
        "reward_kind": "chest",
        "chest_label": reward.chest_label,
        "closed_image_path": reward.chest_closed_image,
        "opened_image_path": reward.chest_opened_image,
        "box_state": "closed",
        "streak_day": streak_day,
        "cycle_day": cycle_day,
        "reward_date": reward_date,
    }


def _claim_chest_reward(
    *,
    user,
    reward_date,
    streak_day,
    cycle_day,
    reward,
    prepared_grant=None,
    at=None,
):
    source_ref = _daily_chest_source_ref(
        user_id=user.pk,
        reward_date=reward_date,
    )
    if prepared_grant is None:
        cycle_length = len(config.get_daily_reward_definitions())
        cycle_reset_at = get_daily_reward_cycle_reset(
            at=at,
            cycle_day=cycle_day,
            cycle_length=cycle_length,
        )
        grant = grant_reward_chest(
            user=user,
            chest_key=reward.chest_key,
            source_type="daily_reward",
            source_ref=source_ref,
            metadata={
                "reward_date": reward_date.isoformat(),
                "streak_day": int(streak_day),
                "cycle_day": int(cycle_day),
                "cycle_length": cycle_length,
                "cycle_reset_at": cycle_reset_at.isoformat(),
                "daily_reward_config_version": int(
                    config.DAILY_REWARD_CONFIG_VERSION
                ),
            },
            expires_at=cycle_reset_at,
        )
    else:
        grant = prepared_grant
        if grant.user_id != user.pk:
            raise PermissionDenied(
                "Cannot open another user's daily chest"
            )
        if (
            grant.source_type != "daily_reward"
            or grant.source_ref != source_ref
            or grant.chest_key != reward.chest_key
        ):
            raise ValidationError("The prepared daily chest does not match")

    opened = open_reward_chest(
        user=user,
        grant=grant,
        at=_normalize_instant(at),
    )
    snapshot = {
        "source": "wallet_daily_rewards",
        "reward_kind": "chest",
        "user_id": int(user.pk),
        "reward_date": reward_date.isoformat(),
        "streak_day": int(streak_day),
        "cycle_day": int(cycle_day),
        "cycle_length": len(config.get_daily_reward_definitions()),
        "asset": reward.asset,
        "chest_key": reward.chest_key,
        "chest_label": reward.chest_label,
        "chest_closed_image": reward.chest_closed_image,
        "chest_opened_image": reward.chest_opened_image,
        "reward_chest_grant_id": grant.pk,
        "reward_chest_public_id": str(grant.public_id),
        "amount_tokens": int(opened["amount_tokens"]),
        "amount_units": int(opened["amount_units"]),
        "drop_key": opened["drop_key"],
        "drop_label": opened["drop_label"],
        "rarity": opened["rarity"],
        "chance_bps": int(opened["chance_bps"]),
        "roll": int(opened["roll"]),
        "daily_reward_config_version": int(config.DAILY_REWARD_CONFIG_VERSION),
        "reward_chest_config_version": int(grant.config_version),
        "reward_chest_config_fingerprint": str(
            grant.config_snapshot.get("fingerprint") or ""
        ),
    }
    LedgerOutbox.objects.create(
        txn=opened["txn"],
        topic=DAILY_REWARD_OUTBOX_TOPIC,
        aggregate_type="ledger_transaction",
        aggregate_id=opened["txn"].pk,
        status=LedgerOutbox.STATUS_PENDING,
        payload={
            "txn_id": opened["txn"].pk,
            "external_id": opened["txn"].external_id,
            **snapshot,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )
    result = {
        "reward_kind": "chest",
        "amount_units": int(opened["amount_units"]),
        "amount_tokens": int(opened["amount_tokens"]),
        "drop_key": opened["drop_key"],
        "drop_label": opened["drop_label"],
        "rarity": opened["rarity"],
        "chest_label": reward.chest_label,
        "closed_image_path": reward.chest_closed_image,
        "opened_image_path": reward.chest_opened_image,
        "box_state": "opened",
    }
    return opened["txn"], snapshot, grant, result


@transaction.atomic
def open_prepared_daily_reward_chest(
    *,
    user,
    grant_public_id,
    at=None,
) -> dict:
    user = _require_eligible_user(user)
    if not config.DAILY_REWARDS_ENABLED:
        raise ValidationError("Daily rewards are disabled")

    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)
    grant = (
        RewardChestGrant.objects.select_for_update()
        .filter(
            public_id=grant_public_id,
            user=locked_user,
            source_type="daily_reward",
        )
        .first()
    )
    if grant is None:
        raise ValidationError("The prepared daily chest was not found")

    metadata = grant.metadata if isinstance(grant.metadata, dict) else {}
    grant_metadata = _daily_chest_grant_metadata(grant)
    reward_date = grant_metadata["reward_date"]
    streak_day = grant_metadata["streak_day"]
    cycle_day = grant_metadata["cycle_day"]
    cycle_length = grant_metadata["cycle_length"]
    if grant.source_ref != _daily_chest_source_ref(
        user_id=locked_user.pk,
        reward_date=reward_date,
    ):
        raise ValidationError("The prepared daily chest source does not match")

    existing_claim = (
        DailyRewardClaim.objects.select_related(
            "ledger_txn",
            "reward_chest_grant",
        )
        .filter(user=locked_user, reward_date=reward_date)
        .first()
    )
    if existing_claim is not None:
        if existing_claim.reward_chest_grant_id != grant.pk:
            raise ValidationError(
                "The prepared daily chest does not match the existing claim"
            )
        return _existing_claim_result(existing_claim, reward_date)

    opened = open_reward_chest(
        user=locked_user,
        grant=grant,
        at=_normalize_instant(at),
    )
    definition = config.reward_chest_definition_from_snapshot(
        grant.config_snapshot
    )
    snapshot = {
        "source": "wallet_daily_rewards",
        "reward_kind": "chest",
        "user_id": int(locked_user.pk),
        "reward_date": reward_date.isoformat(),
        "streak_day": streak_day,
        "cycle_day": cycle_day,
        "cycle_length": cycle_length,
        "asset": definition.asset,
        "chest_key": grant.chest_key,
        "chest_label": definition.label,
        "chest_closed_image": definition.closed_image,
        "chest_opened_image": definition.opened_image,
        "reward_chest_grant_id": grant.pk,
        "reward_chest_public_id": str(grant.public_id),
        "amount_tokens": int(opened["amount_tokens"]),
        "amount_units": int(opened["amount_units"]),
        "drop_key": opened["drop_key"],
        "drop_label": opened["drop_label"],
        "rarity": opened["rarity"],
        "chance_bps": int(opened["chance_bps"]),
        "roll": int(opened["roll"]),
        "daily_reward_config_version": int(
            metadata.get("daily_reward_config_version")
            or config.DAILY_REWARD_CONFIG_VERSION
        ),
        "reward_chest_config_version": int(grant.config_version),
        "reward_chest_config_fingerprint": str(
            grant.config_snapshot.get("fingerprint") or ""
        ),
    }
    LedgerOutbox.objects.create(
        txn=opened["txn"],
        topic=DAILY_REWARD_OUTBOX_TOPIC,
        aggregate_type="ledger_transaction",
        aggregate_id=opened["txn"].pk,
        status=LedgerOutbox.STATUS_PENDING,
        payload={
            "txn_id": opened["txn"].pk,
            "external_id": opened["txn"].external_id,
            **snapshot,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )
    claim = DailyRewardClaim.objects.create(
        user=locked_user,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        amount=int(opened["amount_units"]),
        ledger_txn=opened["txn"],
        reward_chest_grant=grant,
        config_version=int(
            metadata.get("daily_reward_config_version")
            or config.DAILY_REWARD_CONFIG_VERSION
        ),
        config_snapshot=snapshot,
    )
    return {
        "claimed": True,
        "already_claimed": False,
        "claim": claim,
        "txn": opened["txn"],
        "reward_chest_grant": grant,
        "reward_kind": "chest",
        "amount_units": int(opened["amount_units"]),
        "amount_tokens": int(opened["amount_tokens"]),
        "drop_key": opened["drop_key"],
        "drop_label": opened["drop_label"],
        "rarity": opened["rarity"],
        "chest_label": definition.label,
        "closed_image_path": definition.closed_image,
        "opened_image_path": definition.opened_image,
        "box_state": "opened",
        "streak_day": streak_day,
        "cycle_day": cycle_day,
        "reward_date": reward_date,
    }


def _existing_claim_result(existing_claim: DailyRewardClaim, reward_date) -> dict:
    snapshot = existing_claim.config_snapshot or {}
    reward_kind = str(snapshot.get("reward_kind") or "fixed")
    amount_units = int(existing_claim.amount)
    return {
        "claimed": False,
        "already_claimed": True,
        "claim": existing_claim,
        "txn": existing_claim.ledger_txn,
        "reward_chest_grant": existing_claim.reward_chest_grant,
        "reward_kind": reward_kind,
        "amount_units": amount_units,
        "amount_tokens": amount_units // (10 ** config.PLATFORM_TOKEN_DECIMALS),
        "drop_key": str(snapshot.get("drop_key") or ""),
        "drop_label": str(snapshot.get("drop_label") or ""),
        "rarity": str(snapshot.get("rarity") or ""),
        "chest_label": str(snapshot.get("chest_label") or ""),
        "closed_image_path": str(snapshot.get("chest_closed_image") or ""),
        "opened_image_path": str(snapshot.get("chest_opened_image") or ""),
        "box_state": "opened" if reward_kind == "chest" else "",
        "streak_day": int(existing_claim.streak_day),
        "cycle_day": int(existing_claim.cycle_day),
        "reward_date": reward_date,
    }


@transaction.atomic
def claim_daily_reward(
    *,
    user,
    at=None,
    grant_public_id=None,
) -> dict:
    user = _require_eligible_user(user)
    if not config.DAILY_REWARDS_ENABLED:
        raise ValidationError("Daily rewards are disabled")
    if grant_public_id not in (None, ""):
        return open_prepared_daily_reward_chest(
            user=user,
            grant_public_id=grant_public_id,
            at=at,
        )

    definitions = config.get_daily_reward_definitions()
    reward_date = get_daily_reward_date(at)

    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)

    state, _created = DailyRewardState.objects.get_or_create(user=locked_user)
    state = DailyRewardState.objects.select_for_update().get(pk=state.pk)

    existing_claim = (
        DailyRewardClaim.objects.select_related(
            "ledger_txn", "reward_chest_grant"
        )
        .filter(user=locked_user, reward_date=reward_date)
        .first()
    )
    if existing_claim is not None:
        return _existing_claim_result(existing_claim, reward_date)

    if state.last_claim_date == reward_date:
        pending_grant = RewardChestGrant.objects.select_for_update().filter(
            user=locked_user,
            source_type="daily_reward",
            source_ref=_daily_chest_source_ref(
                user_id=locked_user.pk,
                reward_date=reward_date,
            ),
            status=RewardChestGrant.STATUS_PENDING,
        ).first()
        if pending_grant is not None:
            return open_prepared_daily_reward_chest(
                user=locked_user,
                grant_public_id=str(pending_grant.public_id),
                at=at,
            )
        raise ValidationError("Today's daily reward was already claimed")

    streak_day = _calculate_claim_streak(state, reward_date)
    cycle_day = _get_cycle_day(streak_day, len(definitions))
    reward = definitions[cycle_day - 1]

    if reward.kind == "fixed":
        txn, claim_snapshot, chest_grant, reward_result = _claim_fixed_reward(
            user=locked_user,
            reward_date=reward_date,
            streak_day=streak_day,
            cycle_day=cycle_day,
            reward=reward,
        )
    elif reward.kind == "chest":
        txn, claim_snapshot, chest_grant, reward_result = _claim_chest_reward(
            user=locked_user,
            reward_date=reward_date,
            streak_day=streak_day,
            cycle_day=cycle_day,
            reward=reward,
            at=at,
        )
    else:
        raise ValidationError("Unsupported daily reward kind")

    claim = DailyRewardClaim.objects.create(
        user=locked_user,
        reward_date=reward_date,
        streak_day=streak_day,
        cycle_day=cycle_day,
        amount=reward_result["amount_units"],
        ledger_txn=txn,
        reward_chest_grant=chest_grant,
        config_version=config.DAILY_REWARD_CONFIG_VERSION,
        config_snapshot=claim_snapshot,
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
        "reward_chest_grant": chest_grant,
        **reward_result,
        "streak_day": streak_day,
        "cycle_day": cycle_day,
        "reward_date": reward_date,
    }


def _active_pending_daily_grants(*, user, at=None):
    instant = _normalize_instant(at)
    return list(
        RewardChestGrant.objects.filter(
            user=user,
            source_type="daily_reward",
            status=RewardChestGrant.STATUS_PENDING,
        )
        .filter(Q(expires_at__isnull=True) | Q(expires_at__gt=instant))
        .order_by("granted_at", "id")
    )


def _pending_daily_chest_context(grant: RewardChestGrant) -> dict:
    definition = config.reward_chest_definition_from_snapshot(
        grant.config_snapshot
    )
    metadata = grant.metadata if isinstance(grant.metadata, dict) else {}
    return {
        "grant_public_id": str(grant.public_id),
        "chest_key": grant.chest_key,
        "chest_label": definition.label,
        "image_path": definition.closed_image,
        "button_image_path": definition.closed_image,
        "cycle_day": int(metadata.get("cycle_day") or 0),
        "reward_date": str(metadata.get("reward_date") or ""),
        "expires_at": grant.expires_at,
    }


def _format_token_amount(amount_tokens: int) -> str:
    return f"{int(amount_tokens):,}"


def _get_display_streak(
    state: DailyRewardState | None, reward_date
) -> tuple[int, bool, bool]:
    if state is None or state.last_claim_date is None:
        return 1, False, True
    if state.last_claim_date == reward_date:
        return max(1, int(state.current_streak)), True, False
    if state.last_claim_date == reward_date - timedelta(days=1):
        return max(1, int(state.current_streak) + 1), False, True
    if state.last_claim_date < reward_date - timedelta(days=1):
        return 1, False, True
    return max(1, int(state.current_streak)), False, False


def _build_reward_row(definition, *, status: str, opened: bool = False) -> dict:
    if definition.kind == "fixed":
        amount_display = _format_token_amount(definition.amount_tokens)
        claim_label = f"Claim {amount_display}"
        asset_definition = config.get_daily_reward_asset_definition(
            definition.asset,
            amount_tokens=definition.amount_tokens,
        )
        image_path = asset_definition["image"]
        button_image_path = asset_definition["button_image"]
        box_state = ""
        odds = []
    else:
        amount_display = definition.chest_label
        claim_label = "Open chest"
        box_state = "opened" if opened else "closed"
        image_path = definition.chest_opened_image if opened else definition.chest_closed_image
        button_image_path = image_path
        chest = config.get_reward_chest_definition(definition.chest_key)
        odds = [
            {
                "key": drop.key,
                "label": drop.label,
                "rarity": drop.rarity,
                "chance_bps": drop.chance_bps,
                "chance_percent": drop.chance_bps / 100,
                "amount_tokens": drop.amount_tokens,
                "amount_display": _format_token_amount(drop.amount_tokens),
            }
            for drop in chest.drops
        ]

    return {
        "day": definition.day,
        "kind": definition.kind,
        "amount_tokens": definition.amount_tokens,
        "amount_units": definition.amount_units,
        "amount_display": amount_display,
        "asset": definition.asset,
        "asset_tier_min_amount": (
            asset_definition["tier_min_amount"]
            if definition.kind == "fixed"
            else None
        ),
        "image_path": image_path,
        "closed_image_path": definition.chest_closed_image,
        "opened_image_path": definition.chest_opened_image,
        "box_state": box_state,
        "button_image_path": button_image_path,
        "claim_label": claim_label,
        "chest_key": definition.chest_key,
        "chest_label": definition.chest_label,
        "min_amount_tokens": definition.min_amount_tokens,
        "max_amount_tokens": definition.max_amount_tokens,
        "odds": odds,
        "status": status,
    }



def _build_next_chest_row(
    definitions,
    *,
    cycle_day: int,
    claimed_today: bool,
) -> dict | None:
    """Return the next unopened chest in the configured streak rotation."""
    first_offset = 1 if claimed_today else 0
    cycle_length = len(definitions)

    for offset in range(first_offset, first_offset + cycle_length):
        definition = definitions[(cycle_day - 1 + offset) % cycle_length]
        if definition.kind == "chest":
            row = _build_reward_row(definition, status="future", opened=False)
            row["days_until_unlock"] = offset
            return row
    return None


def build_daily_rewards_context(
    *,
    user,
    claim_url: str,
    at=None,
    preview: bool = False,
) -> dict:
    definitions = config.get_daily_reward_definitions()
    reward_date = get_daily_reward_date(at)
    preview = bool(preview)
    state = (
        None
        if preview
        else DailyRewardState.objects.filter(user=user).first()
    )
    display_streak, claimed_today, date_allows_claim = _get_display_streak(
        state, reward_date
    )
    cycle_day = _get_cycle_day(display_streak, len(definitions))
    current_reward = definitions[cycle_day - 1]
    next_chest = _build_next_chest_row(
        definitions,
        cycle_day=cycle_day,
        claimed_today=claimed_today,
    )
    pending_grants = (
        []
        if preview
        else _active_pending_daily_grants(user=user, at=at)
    )
    pending_cycle_days = {
        int(grant.metadata.get("cycle_day") or 0)
        for grant in pending_grants
        if isinstance(grant.metadata, dict)
    }
    pending_chest = (
        _pending_daily_chest_context(pending_grants[0])
        if pending_grants
        else None
    )

    if preview:
        wallet = None
        issuance_wallet = None
        block_reason = ""
        eligible_user = True
    else:
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
        eligible_user = bool(getattr(user, "is_active", False))

    can_claim = bool(
        config.DAILY_REWARDS_ENABLED
        and eligible_user
        and date_allows_claim
        and not claimed_today
        and not block_reason
    )

    window_size = int(config.DAILY_REWARD_WINDOW_SIZE)
    start_day = max(
        1,
        min(
            cycle_day - (window_size // 2),
            len(definitions) - window_size + 1,
        ),
    )
    end_day = start_day + window_size - 1

    def status_for(day):
        if day in pending_cycle_days:
            return "pending"
        if day == cycle_day:
            return "claimed" if claimed_today else "current"
        if day < cycle_day:
            return "claimed"
        return "future"

    def is_opened(day, status):
        return status == "claimed" or (
            claimed_today
            and day == cycle_day
            and day not in pending_cycle_days
        )

    window = []
    for day in range(start_day, end_day + 1):
        status = status_for(day)
        window.append(_build_reward_row(
            definitions[day - 1], status=status, opened=is_opened(day, status)
        ))

    all_rewards = []
    for definition in definitions:
        status = status_for(definition.day)
        all_rewards.append(_build_reward_row(
            definition, status=status, opened=is_opened(definition.day, status)
        ))
    current_position = cycle_day - start_day
    timeline_percent = 0
    if window_size > 1:
        timeline_percent = round(
            (current_position / (window_size - 1)) * 90,
            2,
        )

    return {
        "enabled": bool(config.DAILY_REWARDS_ENABLED),
        "preview": preview,
        "assets": config.get_wallet_asset_paths(),
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
        "current_reward": _build_reward_row(
            current_reward,
            status=status_for(cycle_day),
            opened=(claimed_today and cycle_day not in pending_cycle_days),
        ),
        "pending_chest": pending_chest,
        "pending_chest_count": len(pending_grants),
        "next_chest": next_chest,
        "window": window,
        "all_rewards": all_rewards,
        "timeline_percent": timeline_percent,
    }
