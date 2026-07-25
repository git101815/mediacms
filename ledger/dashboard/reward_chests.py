from __future__ import annotations

import hashlib
import json
import re
import secrets

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
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
from .models import RewardChestGrant


REWARD_CHEST_TRANSACTION_KIND = "reward_chest"
REWARD_CHEST_OUTBOX_TOPIC = "ledger.reward_chest.opened"
_SOURCE_TYPE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def _require_eligible_user(user):
    if not getattr(user, "is_authenticated", False):
        raise PermissionDenied("Authentication is required")
    if not getattr(user, "pk", None):
        raise PermissionDenied("A persisted user account is required")
    if not getattr(user, "is_active", False):
        raise PermissionDenied("Inactive accounts cannot receive Reward Chests")
    return user


def _normalize_source_type(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SOURCE_TYPE_RE.fullmatch(normalized):
        raise ValidationError(
            "Reward Chest source type must use lowercase letters, digits, underscores, or hyphens"
        )
    return normalized


def _normalize_source_ref(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValidationError("Reward Chest source reference is required")
    if len(normalized) > 160:
        raise ValidationError("Reward Chest source reference is too long")
    return normalized


def _normalize_metadata(metadata) -> dict:
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        raise ValidationError("Reward Chest metadata must be a dictionary")
    return dict(metadata)


def _wallet_open_block_reason(wallet: TokenWallet | None) -> str:
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
        raise ValidationError("Could not lock Reward Chest wallets")
    return locked[user_wallet.pk], locked[issuance_wallet.pk]


def _validate_existing_grant(
    *, grant: RewardChestGrant, user_id: int, chest_key: str
) -> RewardChestGrant:
    if grant.user_id != int(user_id):
        raise ValidationError("Reward Chest source reference already belongs to another user")
    if grant.chest_key != chest_key:
        raise ValidationError("Reward Chest source reference was reused for another chest")
    return grant


@transaction.atomic
def grant_reward_chest(
    *,
    user,
    chest_key: str,
    source_type: str,
    source_ref: str,
    metadata=None,
    expires_at=None,
) -> RewardChestGrant:
    """Create one immutable, idempotent Reward Chest entitlement.

    ``source_type`` + ``source_ref`` is the business idempotency key. The
    snapshot is captured when the grant is created, so later config edits do
    not change an already granted chest.
    """

    user = _require_eligible_user(user)
    definition = config.get_reward_chest_definition(chest_key)
    source_type = _normalize_source_type(source_type)
    source_ref = _normalize_source_ref(source_ref)
    metadata = _normalize_metadata(metadata)

    now = timezone.now()
    if expires_at is not None:
        if timezone.is_naive(expires_at):
            raise ValidationError("Reward Chest expiry must be timezone-aware")
        if expires_at <= now:
            raise ValidationError("Reward Chest expiry must be in the future")

    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)

    existing = (
        RewardChestGrant.objects.select_for_update()
        .filter(source_type=source_type, source_ref=source_ref)
        .first()
    )
    if existing is not None:
        return _validate_existing_grant(
            grant=existing,
            user_id=locked_user.pk,
            chest_key=definition.key,
        )

    snapshot = config.build_reward_chest_snapshot(definition)
    try:
        with transaction.atomic():
            return RewardChestGrant.objects.create(
                user=locked_user,
                chest_key=definition.key,
                source_type=source_type,
                source_ref=source_ref,
                status=RewardChestGrant.STATUS_PENDING,
                config_version=config.REWARD_CHEST_CONFIG_VERSION,
                config_snapshot=snapshot,
                metadata=metadata,
                expires_at=expires_at,
            )
    except IntegrityError:
        existing = RewardChestGrant.objects.select_for_update().get(
            source_type=source_type,
            source_ref=source_ref,
        )
        return _validate_existing_grant(
            grant=existing,
            user_id=locked_user.pk,
            chest_key=definition.key,
        )


def _select_drop(definition: config.RewardChestDefinition):
    roll = secrets.randbelow(config.REWARD_CHEST_TOTAL_CHANCE_BPS)
    for drop in definition.drops:
        if roll < drop.cumulative_end_bps:
            return roll, drop
    raise ValidationError("Reward Chest drop table did not resolve the roll")


def _build_transaction_payload(
    *,
    grant: RewardChestGrant,
    definition: config.RewardChestDefinition,
    roll: int,
    drop: config.RewardChestDropDefinition,
) -> tuple[str, str, dict]:
    external_id = f"reward-chest:grant:{grant.public_id}"
    metadata = {
        "source": "reward_chest",
        "reward_chest_grant_id": grant.pk,
        "reward_chest_public_id": str(grant.public_id),
        "user_id": grant.user_id,
        "chest_key": definition.key,
        "chest_label": definition.label,
        "chest_closed_image": definition.closed_image,
        "chest_opened_image": definition.opened_image,
        "source_type": grant.source_type,
        "source_ref": grant.source_ref,
        "roll": int(roll),
        "drop_key": drop.key,
        "drop_label": drop.label,
        "rarity": drop.rarity,
        "chance_bps": int(drop.chance_bps),
        "amount_tokens": int(drop.amount_tokens),
        "amount_units": int(drop.amount_units),
        "config_version": int(grant.config_version),
        "config_fingerprint": str(grant.config_snapshot.get("fingerprint") or ""),
    }
    payload = {
        "external_id": external_id,
        "kind": REWARD_CHEST_TRANSACTION_KIND,
        "metadata": metadata,
    }
    request_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
    ).hexdigest()
    return external_id, request_hash, metadata


def _opened_result(grant: RewardChestGrant) -> dict:
    if (
        grant.status != RewardChestGrant.STATUS_OPENED
        or grant.ledger_txn_id is None
        or grant.amount is None
        or grant.roll is None
        or not grant.drop_key
    ):
        raise ValidationError("Reward Chest opened state is incomplete")
    return {
        "opened": False,
        "already_opened": True,
        "grant": grant,
        "txn": grant.ledger_txn,
        "roll": int(grant.roll),
        "drop_key": grant.drop_key,
        "drop_label": grant.drop_label,
        "rarity": grant.rarity,
        "chance_bps": int(grant.chance_bps or 0),
        "amount_units": int(grant.amount),
        "amount_tokens": int(grant.amount) // (10 ** config.PLATFORM_TOKEN_DECIMALS),
    }


@transaction.atomic
def open_reward_chest(*, user, grant, at=None) -> dict:
    """Open a granted chest exactly once and credit the selected token drop."""

    user = _require_eligible_user(user)
    grant_id = grant.pk if isinstance(grant, RewardChestGrant) else int(grant)
    locked_grant = RewardChestGrant.objects.select_for_update().get(pk=grant_id)

    if locked_grant.user_id != user.pk:
        raise PermissionDenied("Cannot open another user's Reward Chest")
    if locked_grant.status == RewardChestGrant.STATUS_OPENED:
        return _opened_result(locked_grant)
    if locked_grant.status == RewardChestGrant.STATUS_REVOKED:
        raise ValidationError("Reward Chest has been revoked")
    if locked_grant.status != RewardChestGrant.STATUS_PENDING:
        raise ValidationError("Reward Chest is not openable")

    now = at or timezone.now()
    if timezone.is_naive(now):
        raise ValidationError("Reward Chest open time must be timezone-aware")
    if locked_grant.expires_at is not None and locked_grant.expires_at <= now:
        raise ValidationError("Reward Chest has expired")

    definition = config.reward_chest_definition_from_snapshot(
        locked_grant.config_snapshot
    )
    if definition.key != locked_grant.chest_key:
        raise ValidationError("Reward Chest snapshot key does not match the grant")

    user_wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user_id=locked_grant.user_id,
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
    user_block_reason = _wallet_open_block_reason(user_wallet)
    issuance_block_reason = _wallet_open_block_reason(issuance_wallet)
    if user_block_reason:
        raise ValidationError(user_block_reason)
    if issuance_block_reason:
        raise ValidationError(
            f"Reward Chest issuance is unavailable: {issuance_block_reason}"
        )

    roll, drop = _select_drop(definition)
    amount = int(drop.amount_units)
    external_id, request_hash, txn_metadata = _build_transaction_payload(
        grant=locked_grant,
        definition=definition,
        roll=roll,
        drop=drop,
    )

    if LedgerTransaction.objects.filter(external_id=external_id).exists():
        raise ValidationError("Reward Chest ledger transaction exists but grant is not opened")

    user_wallet.balance = int(user_wallet.balance) + amount
    issuance_wallet.balance = int(issuance_wallet.balance) - amount
    user_wallet.save(update_fields=["balance", "updated_at"])
    issuance_wallet.save(update_fields=["balance", "updated_at"])

    txn = LedgerTransaction.objects.create(
        kind=REWARD_CHEST_TRANSACTION_KIND,
        status=LedgerTransaction.STATUS_POSTED,
        external_id=external_id,
        request_hash=request_hash,
        created_by_id=locked_grant.user_id,
        memo=f"{definition.label} opened",
        metadata=txn_metadata,
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
        topic=REWARD_CHEST_OUTBOX_TOPIC,
        aggregate_type="reward_chest_grant",
        aggregate_id=locked_grant.pk,
        status=LedgerOutbox.STATUS_PENDING,
        payload={
            "txn_id": txn.pk,
            "external_id": external_id,
            **txn_metadata,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    locked_grant.status = RewardChestGrant.STATUS_OPENED
    locked_grant.roll = roll
    locked_grant.drop_key = drop.key
    locked_grant.drop_label = drop.label
    locked_grant.rarity = drop.rarity
    locked_grant.chance_bps = drop.chance_bps
    locked_grant.amount = amount
    locked_grant.ledger_txn = txn
    locked_grant.opened_at = now
    locked_grant.save(
        update_fields=[
            "status",
            "roll",
            "drop_key",
            "drop_label",
            "rarity",
            "chance_bps",
            "amount",
            "ledger_txn",
            "opened_at",
        ]
    )

    return {
        "opened": True,
        "already_opened": False,
        "grant": locked_grant,
        "txn": txn,
        "roll": roll,
        "drop_key": drop.key,
        "drop_label": drop.label,
        "rarity": drop.rarity,
        "chance_bps": drop.chance_bps,
        "amount_units": amount,
        "amount_tokens": drop.amount_tokens,
    }
