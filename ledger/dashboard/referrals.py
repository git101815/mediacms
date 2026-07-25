from __future__ import annotations

import hashlib
import json
import logging
import secrets

from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Sum
from django.urls import reverse

from ledger.models import (
    LEDGER_ACTION_PURCHASE,
    LEDGER_METADATA_VERSION,
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
)

from . import config


logger = logging.getLogger(__name__)

REFERRAL_REWARD_TRANSACTION_KIND = "referral_reward"
REFERRAL_REWARD_OUTBOX_TOPIC = "ledger.referral_reward.earned"
REFERRAL_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def _require_user(user):
    if not getattr(user, "is_authenticated", False):
        raise PermissionDenied("Authentication is required")
    if not getattr(user, "pk", None):
        raise PermissionDenied("A persisted user is required")
    if not getattr(user, "is_active", False):
        raise PermissionDenied("An active user is required")
    return user


def _positive_int(name: str, value, *, minimum: int = 1, maximum: int = 1_000_000) -> int:
    if isinstance(value, bool):
        raise ImproperlyConfigured(f"{name} must be an integer")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(f"{name} must be an integer") from exc
    if normalized < minimum or normalized > maximum:
        raise ImproperlyConfigured(
            f"{name} must be between {minimum} and {maximum}"
        )
    return normalized


def get_referral_reward_tokens() -> int:
    return _positive_int(
        "REFERRAL_REWARD_TOKENS",
        config.REFERRAL_REWARD_TOKENS,
        maximum=config.REFERRAL_MAX_REWARD_TOKENS,
    )


def get_referral_reward_units() -> int:
    return get_referral_reward_tokens() * (10 ** config.PLATFORM_TOKEN_DECIMALS)


def get_referral_goal() -> int:
    return _positive_int("REFERRAL_GOAL", config.REFERRAL_GOAL, maximum=10_000)


def get_referral_reward_cap() -> int:
    return _positive_int(
        "REFERRAL_MAX_REWARDED_FRIENDS",
        config.REFERRAL_MAX_REWARDED_FRIENDS,
        maximum=10_000,
    )


def get_referral_min_purchase_units() -> int:
    tokens = _positive_int(
        "REFERRAL_MIN_PURCHASE_TOKENS",
        config.REFERRAL_MIN_PURCHASE_TOKENS,
        maximum=config.REFERRAL_MAX_REWARD_TOKENS,
    )
    return tokens * (10 ** config.PLATFORM_TOKEN_DECIMALS)


def normalize_referral_code(value: str) -> str:
    return str(value or "").strip().upper()


def get_referrer_by_code(code: str):
    normalized = normalize_referral_code(code)
    if not normalized:
        return None
    user_model = get_user_model()
    return (
        user_model.objects.filter(
            referral_code=normalized,
            is_active=True,
        )
        .only("id", "username", "referral_code")
        .first()
    )


@transaction.atomic
def ensure_referral_code(*, user) -> str:
    user = _require_user(user)
    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)

    existing = normalize_referral_code(locked_user.referral_code)
    if existing:
        return existing

    length = _positive_int(
        "REFERRAL_CODE_LENGTH",
        config.REFERRAL_CODE_LENGTH,
        minimum=8,
        maximum=32,
    )
    for _attempt in range(64):
        code = "".join(
            secrets.choice(REFERRAL_CODE_ALPHABET)
            for _ in range(length)
        )
        locked_user.referral_code = code
        try:
            with transaction.atomic():
                locked_user.save(update_fields=["referral_code"])
        except IntegrityError:
            locked_user.referral_code = None
            continue
        user.referral_code = code
        return code

    raise ValidationError("Could not allocate a referral code")


def assign_referrer_from_signup(*, request, user):
    if not bool(config.REFERRAL_PROGRAM_ENABLED):
        return None
    if not getattr(user, "pk", None):
        return None
    if getattr(user, "referred_by_id", None):
        return user.referred_by

    resolver_match = getattr(request, "resolver_match", None)
    kwargs = getattr(resolver_match, "kwargs", {}) if resolver_match else {}
    code = normalize_referral_code(kwargs.get("referral_code"))
    if not code:
        return None

    referrer = get_referrer_by_code(code)
    if referrer is None or referrer.pk == user.pk:
        return None

    user_model = get_user_model()
    updated = (
        user_model.objects.filter(
            pk=user.pk,
            referred_by__isnull=True,
        )
        .exclude(pk=referrer.pk)
        .update(referred_by=referrer)
    )
    if updated:
        user.referred_by_id = referrer.pk
        return referrer
    return user_model.objects.select_related("referred_by").get(
        pk=user.pk
    ).referred_by


def _purchase_amount_units(purchase: LedgerTransaction, invitee_id: int) -> int:
    metadata = purchase.metadata or {}
    for key in ("price_tokens", "amount_units"):
        value = metadata.get(key)
        if value not in (None, ""):
            try:
                normalized = int(value)
            except (TypeError, ValueError):
                continue
            if normalized > 0:
                return normalized

    entry = (
        purchase.entries.filter(
            wallet__user_id=invitee_id,
            delta__lt=0,
        )
        .order_by("id")
        .first()
    )
    return abs(int(entry.delta)) if entry is not None else 0


def _wallet_block_reason(wallet: TokenWallet) -> str:
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
        raise ValidationError("Could not lock referral reward wallets")
    return locked[user_wallet.pk], locked[issuance_wallet.pk]


@transaction.atomic
def award_referral_for_purchase(*, purchase_txn_id: int) -> dict:
    if not bool(config.REFERRAL_PROGRAM_ENABLED):
        return {"awarded": False, "reason": "disabled"}

    purchase = (
        LedgerTransaction.objects.select_for_update()
        .filter(pk=purchase_txn_id)
        .first()
    )
    if purchase is None:
        return {"awarded": False, "reason": "missing_purchase"}
    if (
        purchase.kind != LEDGER_ACTION_PURCHASE
        or purchase.status != LedgerTransaction.STATUS_POSTED
        or purchase.created_by_id is None
    ):
        return {"awarded": False, "reason": "ineligible_transaction"}

    user_model = get_user_model()
    invitee_snapshot = (
        user_model.objects.filter(pk=purchase.created_by_id)
        .values("id", "referred_by_id")
        .first()
    )
    if not invitee_snapshot or not invitee_snapshot["referred_by_id"]:
        return {"awarded": False, "reason": "not_referred"}

    user_ids = sorted(
        {
            invitee_snapshot["id"],
            invitee_snapshot["referred_by_id"],
        }
    )
    locked_users = {
        candidate.pk: candidate
        for candidate in user_model.objects.select_for_update()
        .filter(pk__in=user_ids)
        .order_by("pk")
    }
    invitee = locked_users.get(invitee_snapshot["id"])
    if invitee is None or not invitee.referred_by_id:
        return {"awarded": False, "reason": "not_referred"}
    referrer = locked_users.get(invitee.referred_by_id)
    if referrer is None or referrer.pk == invitee.pk:
        return {"awarded": False, "reason": "invalid_referrer"}
    if purchase.created_at < invitee.date_joined:
        return {"awarded": False, "reason": "purchase_before_signup"}

    external_id = f"referral-reward:invitee:{invitee.pk}"
    existing_reward = LedgerTransaction.objects.filter(
        external_id=external_id,
    ).first()
    if existing_reward is not None:
        return {
            "awarded": False,
            "reason": "already_rewarded",
            "reward_txn_id": existing_reward.pk,
        }

    purchase_amount = _purchase_amount_units(
        purchase,
        invitee_id=invitee.pk,
    )
    if purchase_amount < get_referral_min_purchase_units():
        return {"awarded": False, "reason": "purchase_below_minimum"}

    rewarded_count = LedgerTransaction.objects.filter(
        kind=REFERRAL_REWARD_TRANSACTION_KIND,
        status=LedgerTransaction.STATUS_POSTED,
        created_by=referrer,
    ).count()
    if rewarded_count >= get_referral_reward_cap():
        return {"awarded": False, "reason": "cap_reached"}

    user_wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=referrer,
        defaults={"allow_negative": False},
    )
    issuance_wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=TokenWallet.SYSTEM_ISSUANCE,
        defaults={"allow_negative": True},
    )
    if not issuance_wallet.allow_negative:
        raise ValidationError("System issuance wallet must allow negative balances")

    user_wallet, issuance_wallet = _lock_wallet_pair(
        user_wallet,
        issuance_wallet,
    )
    user_block_reason = _wallet_block_reason(user_wallet)
    issuance_block_reason = _wallet_block_reason(issuance_wallet)
    if user_block_reason:
        raise ValidationError(user_block_reason)
    if issuance_block_reason:
        raise ValidationError(
            f"Referral issuance is unavailable: {issuance_block_reason}"
        )

    reward_units = get_referral_reward_units()
    reward_tokens = get_referral_reward_tokens()
    request_hash = hashlib.sha256(
        json.dumps(
            {
                "external_id": external_id,
                "invitee_user_id": invitee.pk,
                "referrer_user_id": referrer.pk,
                "qualifying_purchase_id": purchase.pk,
                "amount_units": reward_units,
                "config_version": int(config.REFERRAL_CONFIG_VERSION),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    user_wallet.balance = int(user_wallet.balance) + reward_units
    issuance_wallet.balance = int(issuance_wallet.balance) - reward_units
    user_wallet.save(update_fields=["balance", "updated_at"])
    issuance_wallet.save(update_fields=["balance", "updated_at"])

    reward_txn = LedgerTransaction.objects.create(
        kind=REFERRAL_REWARD_TRANSACTION_KIND,
        external_id=external_id,
        request_hash=request_hash,
        created_by=referrer,
        memo=f"Referral reward for {invitee.username}",
        metadata={
            "source": "wallet_referral_program",
            "referral_code": referrer.referral_code or "",
            "referrer_user_id": referrer.pk,
            "invitee_user_id": invitee.pk,
            "qualifying_purchase_id": purchase.pk,
            "amount_tokens": reward_tokens,
            "amount_units": reward_units,
            "config_version": int(config.REFERRAL_CONFIG_VERSION),
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )
    LedgerEntry.objects.create(
        txn=reward_txn,
        wallet=user_wallet,
        delta=reward_units,
        balance_after=user_wallet.balance,
    )
    LedgerEntry.objects.create(
        txn=reward_txn,
        wallet=issuance_wallet,
        delta=-reward_units,
        balance_after=issuance_wallet.balance,
    )
    LedgerOutbox.objects.create(
        txn=reward_txn,
        topic=REFERRAL_REWARD_OUTBOX_TOPIC,
        aggregate_type="ledger_transaction",
        aggregate_id=reward_txn.pk,
        payload={
            "referral_code": referrer.referral_code or "",
            "referrer_user_id": referrer.pk,
            "invitee_user_id": invitee.pk,
            "qualifying_purchase_id": purchase.pk,
            "amount_tokens": reward_tokens,
            "amount_units": reward_units,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    return {
        "awarded": True,
        "reason": "rewarded",
        "reward_txn_id": reward_txn.pk,
        "amount_tokens": reward_tokens,
        "amount_units": reward_units,
    }


def safely_award_referral_for_purchase(*, purchase_txn_id: int) -> None:
    try:
        award_referral_for_purchase(
            purchase_txn_id=purchase_txn_id,
        )
    except Exception:
        logger.exception(
            "Referral reward processing failed for purchase transaction %s",
            purchase_txn_id,
        )


def sync_referral_rewards_for_referrer(*, referrer, limit: int = 50) -> int:
    if not bool(config.REFERRAL_PROGRAM_ENABLED):
        return 0

    user_model = get_user_model()
    rewarded_invitee_ids = set()
    reward_metadata = LedgerTransaction.objects.filter(
        kind=REFERRAL_REWARD_TRANSACTION_KIND,
        status=LedgerTransaction.STATUS_POSTED,
        created_by=referrer,
    ).values_list("metadata", flat=True)
    for metadata in reward_metadata:
        try:
            invitee_id = int((metadata or {}).get("invitee_user_id"))
        except (TypeError, ValueError):
            continue
        if invitee_id > 0:
            rewarded_invitee_ids.add(invitee_id)

    pending_invitees = list(
        user_model.objects.filter(referred_by=referrer)
        .exclude(pk__in=rewarded_invitee_ids)
        .order_by("date_joined", "id")
        .values_list("id", flat=True)[:max(1, int(limit))]
    )

    awarded = 0
    for invitee_id in pending_invitees:
        purchase_ids = list(
            LedgerTransaction.objects.filter(
                kind=LEDGER_ACTION_PURCHASE,
                status=LedgerTransaction.STATUS_POSTED,
                created_by_id=invitee_id,
            )
            .order_by("created_at", "id")
            .values_list("id", flat=True)[:20]
        )
        for purchase_id in purchase_ids:
            try:
                result = award_referral_for_purchase(
                    purchase_txn_id=purchase_id,
                )
            except (PermissionDenied, ValidationError):
                break
            if result.get("awarded"):
                awarded += 1
                break
            if result.get("reason") in {
                "already_rewarded",
                "cap_reached",
            }:
                break

    return awarded


def _format_units(units: int) -> str:
    scale = 10 ** config.PLATFORM_TOKEN_DECIMALS
    whole, remainder = divmod(abs(int(units)), scale)
    if remainder:
        fraction = f"{remainder:0{config.PLATFORM_TOKEN_DECIMALS}d}".rstrip("0")
        text = f"{whole:,}.{fraction}"
    else:
        text = f"{whole:,}"
    return f"-{text}" if int(units) < 0 else text


def build_referral_context(*, user, request) -> dict:
    user = _require_user(user)
    goal = get_referral_goal()
    reward_tokens = get_referral_reward_tokens()

    if not bool(config.REFERRAL_PROGRAM_ENABLED):
        return {
            "enabled": False,
            "share_url": "",
            "joined_count": 0,
            "rewarded_count": 0,
            "pending_count": 0,
            "goal": goal,
            "progress_percent": 0,
            "earned_display": "0",
            "reward_per_friend_display": f"{reward_tokens:,}",
        }

    code = ensure_referral_code(user=user)
    sync_referral_rewards_for_referrer(referrer=user)

    user_model = get_user_model()
    joined_count = user_model.objects.filter(referred_by=user).count()
    reward_txns = LedgerTransaction.objects.filter(
        kind=REFERRAL_REWARD_TRANSACTION_KIND,
        status=LedgerTransaction.STATUS_POSTED,
        created_by=user,
    )
    rewarded_count = reward_txns.count()
    pending_count = max(joined_count - rewarded_count, 0)
    earned_units = int(
        LedgerEntry.objects.filter(
            txn__in=reward_txns,
            wallet__user=user,
            delta__gt=0,
        ).aggregate(total=Sum("delta"))["total"] or 0
    )
    relative_url = reverse(
        "account_referral_signup",
        kwargs={"referral_code": code},
    )

    return {
        "enabled": True,
        "code": code,
        "share_url": request.build_absolute_uri(relative_url),
        "joined_count": joined_count,
        "rewarded_count": rewarded_count,
        "pending_count": pending_count,
        "goal": goal,
        "progress_percent": min(100, (rewarded_count * 100) // goal),
        "earned_units": earned_units,
        "earned_display": _format_units(earned_units),
        "reward_per_friend_tokens": reward_tokens,
        "reward_per_friend_display": f"{reward_tokens:,}",
        "max_rewarded_friends": get_referral_reward_cap(),
    }
