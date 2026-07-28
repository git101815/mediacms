from __future__ import annotations

from datetime import datetime
import re

from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from ledger.models import (
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    LedgerEntry,
    LedgerTransaction,
    TokenWallet,
)

from . import config
from .models import RewardChestGrant
from .reward_chests import grant_reward_chest, open_reward_chest


_BONUS_VAULT_KIND_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def _require_eligible_user(user):
    if not getattr(user, "is_authenticated", False):
        raise PermissionDenied("Authentication is required")
    if not getattr(user, "pk", None):
        raise PermissionDenied("A persisted user account is required")
    if not getattr(user, "is_active", False):
        raise PermissionDenied("Inactive accounts cannot use the Bonus Vault")
    return user


def _get_threshold_tokens() -> int:
    value = config.BONUS_VAULT_THRESHOLD_TOKENS
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ImproperlyConfigured(
            "BONUS_VAULT_THRESHOLD_TOKENS must be a positive whole integer"
        )

    maximum = ((2 ** 63) - 1) // (10 ** config.PLATFORM_TOKEN_DECIMALS)
    if value > maximum:
        raise ImproperlyConfigured(
            "BONUS_VAULT_THRESHOLD_TOKENS exceeds the ledger maximum"
        )
    return value


def _get_threshold_units() -> int:
    return _get_threshold_tokens() * (10 ** config.PLATFORM_TOKEN_DECIMALS)


def _get_eligible_transaction_kinds() -> tuple[str, ...]:
    raw = config.BONUS_VAULT_ELIGIBLE_TRANSACTION_KINDS
    if isinstance(raw, str) or not isinstance(raw, (list, tuple, set, frozenset)):
        raise ImproperlyConfigured(
            "BONUS_VAULT_ELIGIBLE_TRANSACTION_KINDS must be a collection"
        )

    normalized = []
    for value in raw:
        kind = str(value or "").strip().lower()
        if not _BONUS_VAULT_KIND_RE.fullmatch(kind):
            raise ImproperlyConfigured(
                "BONUS_VAULT_ELIGIBLE_TRANSACTION_KINDS contains an invalid kind"
            )
        if kind not in normalized:
            normalized.append(kind)

    if not normalized:
        raise ImproperlyConfigured(
            "BONUS_VAULT_ELIGIBLE_TRANSACTION_KINDS cannot be empty"
        )
    return tuple(normalized)


def _get_source_type() -> str:
    source_type = str(config.BONUS_VAULT_SOURCE_TYPE or "").strip().lower()
    if not _BONUS_VAULT_KIND_RE.fullmatch(source_type):
        raise ImproperlyConfigured(
            "BONUS_VAULT_SOURCE_TYPE must be a valid lowercase identifier"
        )
    return source_type


def _get_start_at():
    value = config.BONUS_VAULT_START_AT
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                "BONUS_VAULT_START_AT must be an aware datetime or ISO-8601 timestamp"
            ) from exc

    if timezone.is_naive(parsed):
        raise ImproperlyConfigured("BONUS_VAULT_START_AT must include a timezone")
    return parsed


def _get_chest_definition():
    return config.get_reward_chest_definition(config.BONUS_VAULT_CHEST_KEY)


def _get_total_eligible_spend_units(*, wallet: TokenWallet) -> int:
    filters = {
        "wallet": wallet,
        "txn__kind__in": _get_eligible_transaction_kinds(),
        "txn__status": LedgerTransaction.STATUS_POSTED,
        "delta__lt": 0,
    }
    start_at = _get_start_at()
    if start_at is not None:
        filters["created_at__gte"] = start_at

    signed_total = (
        LedgerEntry.objects.filter(**filters)
        .aggregate(total=Sum("delta"))
        .get("total")
    )
    return max(0, -int(signed_total or 0))


def _grant_threshold_units(grant: RewardChestGrant, current_threshold: int) -> int:
    metadata = grant.metadata if isinstance(grant.metadata, dict) else {}
    value = metadata.get("bonus_vault_threshold_units")
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = current_threshold
    if normalized <= 0:
        normalized = current_threshold
    return normalized


def _build_numbers(*, user, wallet: TokenWallet) -> dict:
    threshold_units = _get_threshold_units()
    source_type = _get_source_type()
    grants = list(
        RewardChestGrant.objects.filter(
            user=user,
            source_type=source_type,
        )
        .only("id", "status", "metadata")
        .order_by("id")
    )

    total_spend_units = _get_total_eligible_spend_units(wallet=wallet)
    consumed_units = sum(
        _grant_threshold_units(grant, threshold_units)
        for grant in grants
    )
    unallocated_units = max(0, total_spend_units - consumed_units)
    ungranted_count = unallocated_units // threshold_units
    remainder_units = unallocated_units % threshold_units
    pending_count = sum(
        grant.status == RewardChestGrant.STATUS_PENDING
        for grant in grants
    )

    return {
        "threshold_units": threshold_units,
        "total_spend_units": total_spend_units,
        "consumed_units": consumed_units,
        "unallocated_units": unallocated_units,
        "ungranted_count": int(ungranted_count),
        "remainder_units": int(remainder_units),
        "pending_count": int(pending_count),
        "ready_count": int(pending_count + ungranted_count),
        "grant_count": len(grants),
    }


def build_bonus_vault_context(*, user, wallet: TokenWallet, open_url: str) -> dict:
    user = _require_eligible_user(user)
    chest = _get_chest_definition()
    numbers = _build_numbers(user=user, wallet=wallet)
    ready_count = numbers["ready_count"]

    progress_units = (
        numbers["threshold_units"]
        if ready_count > 0
        else numbers["remainder_units"]
    )
    progress_percent = min(
        100,
        int((progress_units * 100) // numbers["threshold_units"]),
    )
    remaining_units = (
        0
        if ready_count > 0
        else numbers["threshold_units"] - numbers["remainder_units"]
    )

    block_reason = ""
    if wallet.risk_status == LEDGER_RISK_STATUS_BLOCKED:
        block_reason = "Wallet is blocked"
    elif wallet.review_required or wallet.risk_status == LEDGER_RISK_STATUS_REVIEW:
        block_reason = "Wallet is under review"

    enabled = bool(config.BONUS_VAULT_ENABLED)
    can_open = bool(enabled and ready_count > 0 and not block_reason)

    if ready_count > 1:
        button_label = f"Open Vault ({ready_count})"
    elif ready_count == 1:
        button_label = "Open Vault"
    else:
        button_label = "Vault Locked"

    return {
        "enabled": enabled,
        "config_version": int(config.BONUS_VAULT_CONFIG_VERSION),
        "chest_key": chest.key,
        "chest_label": chest.label,
        "image_path": chest.closed_image,
        "opened_image_path": chest.opened_image,
        "reward_min_tokens": chest.min_amount_tokens,
        "reward_max_tokens": chest.max_amount_tokens,
        "reward_range_display": (
            f"{chest.min_amount_tokens:,}–{chest.max_amount_tokens:,}"
        ),
        "threshold_tokens": _get_threshold_tokens(),
        "threshold_units": numbers["threshold_units"],
        "threshold_display": f"{_get_threshold_tokens():,}",
        "total_eligible_spend_units": numbers["total_spend_units"],
        "total_eligible_spend_tokens": (
            numbers["total_spend_units"] // (10 ** config.PLATFORM_TOKEN_DECIMALS)
        ),
        "progress_units": progress_units,
        "progress_tokens": (
            progress_units // (10 ** config.PLATFORM_TOKEN_DECIMALS)
        ),
        "progress_percent": progress_percent,
        "remaining_units": remaining_units,
        "remaining_tokens": (
            remaining_units // (10 ** config.PLATFORM_TOKEN_DECIMALS)
        ),
        "remaining_display": (
            f"{remaining_units // (10 ** config.PLATFORM_TOKEN_DECIMALS):,}"
        ),
        "pending_count": numbers["pending_count"],
        "ungranted_count": numbers["ungranted_count"],
        "ready_count": ready_count,
        "can_open": can_open,
        "block_reason": block_reason,
        "button_label": button_label,
        "open_url": open_url,
    }


def _pending_bonus_vault_result(grant: RewardChestGrant) -> dict:
    definition = config.reward_chest_definition_from_snapshot(
        grant.config_snapshot
    )
    return {
        "prepared": True,
        "grant": grant,
        "chest_name": definition.label,
        "closed_image_path": definition.closed_image,
        "opened_image_path": definition.opened_image,
        "box_state": "closed",
    }


@transaction.atomic
def prepare_bonus_vault(*, user) -> dict:
    user = _require_eligible_user(user)
    if not config.BONUS_VAULT_ENABLED:
        raise ValidationError("The Bonus Vault is disabled")

    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)
    wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=locked_user,
        defaults={"allow_negative": False},
    )

    source_type = _get_source_type()
    pending_grant = (
        RewardChestGrant.objects.select_for_update()
        .filter(
            user=locked_user,
            source_type=source_type,
            status=RewardChestGrant.STATUS_PENDING,
        )
        .order_by("id")
        .first()
    )

    if pending_grant is None:
        numbers = _build_numbers(user=locked_user, wallet=wallet)
        if numbers["ungranted_count"] <= 0:
            raise ValidationError("The Bonus Vault is not full yet")

        cycle_number = numbers["grant_count"] + 1
        threshold_units = numbers["threshold_units"]
        source_ref = f"user:{locked_user.pk}:cycle:{cycle_number}"
        pending_grant = grant_reward_chest(
            user=locked_user,
            chest_key=config.BONUS_VAULT_CHEST_KEY,
            source_type=source_type,
            source_ref=source_ref,
            metadata={
                "source": "bonus_vault",
                "bonus_vault_config_version": int(
                    config.BONUS_VAULT_CONFIG_VERSION
                ),
                "bonus_vault_cycle": cycle_number,
                "bonus_vault_threshold_tokens": _get_threshold_tokens(),
                "bonus_vault_threshold_units": threshold_units,
                "eligible_transaction_kinds": list(
                    _get_eligible_transaction_kinds()
                ),
                "eligible_spend_units_at_grant": numbers[
                    "total_spend_units"
                ],
            },
        )

    return _pending_bonus_vault_result(pending_grant)


@transaction.atomic
def open_bonus_vault(*, user, grant_public_id=None) -> dict:
    user = _require_eligible_user(user)
    if not config.BONUS_VAULT_ENABLED:
        raise ValidationError("The Bonus Vault is disabled")

    if grant_public_id not in (None, ""):
        user_model = get_user_model()
        locked_user = user_model.objects.select_for_update().get(pk=user.pk)
        pending_grant = (
            RewardChestGrant.objects.select_for_update()
            .filter(
                public_id=grant_public_id,
                user=locked_user,
                source_type=_get_source_type(),
            )
            .first()
        )
        if pending_grant is None:
            raise ValidationError(
                "The prepared Bonus Vault chest was not found"
            )
    else:
        prepared = prepare_bonus_vault(user=user)
        locked_user = user
        pending_grant = prepared["grant"]

    result = open_reward_chest(user=locked_user, grant=pending_grant)
    metadata = (
        pending_grant.metadata
        if isinstance(pending_grant.metadata, dict)
        else {}
    )
    result["bonus_vault_cycle"] = int(
        metadata.get("bonus_vault_cycle") or 0
    )
    return result
