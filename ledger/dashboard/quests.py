from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Callable

from allauth.account.models import EmailAddress
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.urls import NoReverseMatch, reverse

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


QUEST_REWARD_TRANSACTION_KIND = "quest_reward"
QUEST_REWARD_OUTBOX_TOPIC = "ledger.quest_reward.claimed"

_QUEST_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True)
class QuestDefinition:
    key: str
    title: str
    description: str
    condition: str
    icon_asset: str
    icon_path: str
    action_label: str
    action_url_name: str
    reward_kind: str
    reward_asset: str
    reward_tokens: int
    reward_units: int


def _require_eligible_user(user):
    if not getattr(user, "is_authenticated", False):
        raise PermissionDenied("Authentication is required")
    if not getattr(user, "pk", None):
        raise PermissionDenied("A persisted user account is required")
    if not getattr(user, "is_active", False):
        raise PermissionDenied("Inactive accounts cannot claim quest rewards")
    return user


def _normalize_identifier(value, *, field_name: str, pattern=_IDENTIFIER_RE) -> str:
    normalized = str(value or "").strip().lower()
    if not pattern.fullmatch(normalized):
        raise ImproperlyConfigured(f"{field_name} is invalid")
    return normalized


def _require_text(value, *, field_name: str, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ImproperlyConfigured(f"{field_name} is required")
    if len(normalized) > maximum:
        raise ImproperlyConfigured(f"{field_name} is too long")
    return normalized


def _require_positive_int(value, *, field_name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ImproperlyConfigured(f"{field_name} must be a whole integer")
    if value <= 0 or value > maximum:
        raise ImproperlyConfigured(
            f"{field_name} must be between 1 and {maximum}"
        )
    return int(value)


def _email_verified_progress(user) -> tuple[int, int]:
    verified = EmailAddress.objects.filter(
        user=user,
        verified=True,
    ).exists()
    return (1 if verified else 0), 1


_CONDITION_HANDLERS: dict[str, Callable] = {
    "email_verified": _email_verified_progress,
}


def get_quest_slot_count() -> int:
    value = config.QUEST_BOARD_SLOT_COUNT
    if isinstance(value, bool) or not isinstance(value, int):
        raise ImproperlyConfigured("QUEST_BOARD_SLOT_COUNT must be an integer")
    if value < 1 or value > 12:
        raise ImproperlyConfigured(
            "QUEST_BOARD_SLOT_COUNT must be between 1 and 12"
        )
    return int(value)


def get_quest_definitions() -> tuple[QuestDefinition, ...]:
    raw_definitions = config.QUEST_BOARD_QUESTS
    if not isinstance(raw_definitions, (list, tuple)):
        raise ImproperlyConfigured(
            "QUEST_BOARD_QUESTS must be a list or tuple"
        )

    slot_count = get_quest_slot_count()
    if len(raw_definitions) > slot_count:
        raise ImproperlyConfigured(
            "QUEST_BOARD_QUESTS contains more quests than available slots"
        )

    assets = config.get_wallet_asset_paths()
    definitions = []
    seen_keys = set()

    maximum_reward = _require_positive_int(
        config.QUEST_BOARD_MAX_REWARD_TOKENS,
        field_name="QUEST_BOARD_MAX_REWARD_TOKENS",
        maximum=((2 ** 63) - 1) // (10 ** config.PLATFORM_TOKEN_DECIMALS),
    )

    for index, raw in enumerate(raw_definitions, start=1):
        if not isinstance(raw, dict):
            raise ImproperlyConfigured(
                f"Quest definition {index} must be a dictionary"
            )

        key = _normalize_identifier(
            raw.get("key"),
            field_name=f"Quest definition {index} key",
            pattern=_QUEST_KEY_RE,
        )
        if key in seen_keys:
            raise ImproperlyConfigured(f"Duplicate quest key: {key}")
        seen_keys.add(key)

        condition = _normalize_identifier(
            raw.get("condition"),
            field_name=f"Quest {key} condition",
        )
        if condition not in _CONDITION_HANDLERS:
            raise ImproperlyConfigured(
                f"Quest {key} uses unsupported condition: {condition}"
            )

        icon_asset = _normalize_identifier(
            raw.get("icon_asset"),
            field_name=f"Quest {key} icon_asset",
        )
        if icon_asset not in assets:
            raise ImproperlyConfigured(
                f"Quest {key} references unknown wallet asset: {icon_asset}"
            )

        reward = raw.get("reward")
        if not isinstance(reward, dict):
            raise ImproperlyConfigured(
                f"Quest {key} reward must be a dictionary"
            )

        reward_kind = _normalize_identifier(
            reward.get("kind"),
            field_name=f"Quest {key} reward kind",
        )
        if reward_kind != "fixed":
            raise ImproperlyConfigured(
                f"Quest {key} currently supports only fixed rewards"
            )

        reward_asset = _normalize_identifier(
            reward.get("asset"),
            field_name=f"Quest {key} reward asset",
        )
        if reward_asset not in config.DAILY_REWARD_ASSETS:
            raise ImproperlyConfigured(
                f"Quest {key} references unknown reward asset: {reward_asset}"
            )

        reward_tokens = _require_positive_int(
            reward.get("amount"),
            field_name=f"Quest {key} reward amount",
            maximum=maximum_reward,
        )

        definitions.append(
            QuestDefinition(
                key=key,
                title=_require_text(
                    raw.get("title"),
                    field_name=f"Quest {key} title",
                    maximum=80,
                ),
                description=_require_text(
                    raw.get("description"),
                    field_name=f"Quest {key} description",
                    maximum=160,
                ),
                condition=condition,
                icon_asset=icon_asset,
                icon_path=assets[icon_asset],
                action_label=_require_text(
                    raw.get("action_label"),
                    field_name=f"Quest {key} action_label",
                    maximum=40,
                ),
                action_url_name=_require_text(
                    raw.get("action_url_name"),
                    field_name=f"Quest {key} action_url_name",
                    maximum=120,
                ),
                reward_kind=reward_kind,
                reward_asset=reward_asset,
                reward_tokens=reward_tokens,
                reward_units=reward_tokens
                * (10 ** config.PLATFORM_TOKEN_DECIMALS),
            )
        )

    return tuple(definitions)


def get_quest_definition(quest_key: str) -> QuestDefinition:
    normalized_key = _normalize_identifier(
        quest_key,
        field_name="Quest key",
        pattern=_QUEST_KEY_RE,
    )
    for definition in get_quest_definitions():
        if definition.key == normalized_key:
            return definition
    raise ValidationError("Unknown quest")


def _quest_external_id(*, user_id: int, quest_key: str) -> str:
    return f"quest-reward:user:{int(user_id)}:quest:{quest_key}"


def _evaluate_progress(*, user, definition: QuestDefinition) -> tuple[int, int]:
    handler = _CONDITION_HANDLERS[definition.condition]
    current, target = handler(user)
    current = max(0, int(current))
    target = max(1, int(target))
    return min(current, target), target


def _resolve_action_url(definition: QuestDefinition) -> str:
    try:
        return reverse(definition.action_url_name)
    except NoReverseMatch as exc:
        raise ImproperlyConfigured(
            f"Quest {definition.key} action URL does not exist: "
            f"{definition.action_url_name}"
        ) from exc


def _claimed_external_ids(*, user, definitions) -> set[str]:
    external_ids = [
        _quest_external_id(user_id=user.pk, quest_key=definition.key)
        for definition in definitions
    ]
    if not external_ids:
        return set()

    return set(
        LedgerTransaction.objects.filter(
            external_id__in=external_ids,
            kind=QUEST_REWARD_TRANSACTION_KIND,
            status=LedgerTransaction.STATUS_POSTED,
        ).values_list("external_id", flat=True)
    )


def build_quest_board_context(*, user) -> dict:
    user = _require_eligible_user(user)
    enabled = bool(config.QUEST_BOARD_ENABLED)
    definitions = get_quest_definitions() if enabled else ()
    claimed_external_ids = _claimed_external_ids(
        user=user,
        definitions=definitions,
    )

    rows = []
    completed_count = 0
    claimed_count = 0

    for definition in definitions:
        current, target = _evaluate_progress(
            user=user,
            definition=definition,
        )
        complete = current >= target
        external_id = _quest_external_id(
            user_id=user.pk,
            quest_key=definition.key,
        )
        claimed = external_id in claimed_external_ids

        if complete:
            completed_count += 1
        if claimed:
            claimed_count += 1

        if claimed:
            status = "claimed"
            button_label = "Claimed"
        elif complete:
            status = "complete"
            button_label = "Claim"
        else:
            status = "in_progress"
            button_label = definition.action_label

        rows.append(
            {
                "empty": False,
                "key": definition.key,
                "title": definition.title,
                "description": definition.description,
                "condition": definition.condition,
                "icon_path": definition.icon_path,
                "reward_kind": definition.reward_kind,
                "reward_asset": definition.reward_asset,
                "reward_tokens": definition.reward_tokens,
                "reward_display": f"{definition.reward_tokens:,}",
                "current": current,
                "target": target,
                "progress_percent": min(
                    100,
                    int((current * 100) // target),
                ),
                "complete": complete,
                "claimed": claimed,
                "status": status,
                "button_label": button_label,
                "can_claim": bool(complete and not claimed),
                "claim_url": reverse(
                    "wallet_claim_quest",
                    kwargs={"quest_key": definition.key},
                ),
                "action_url": (
                    ""
                    if complete or claimed
                    else _resolve_action_url(definition)
                ),
            }
        )

    slot_count = get_quest_slot_count()
    while len(rows) < slot_count:
        rows.append(
            {
                "empty": True,
                "slot": len(rows) + 1,
            }
        )

    reset_label = str(config.QUEST_BOARD_RESET_LABEL or "").strip()
    normalized_schedule = re.sub(r"[\\s_-]+", " ", reset_label).strip().lower()
    show_schedule = bool(
        reset_label
        and normalized_schedule not in {"one time", "once"}
    )

    return {
        "enabled": enabled,
        "config_version": int(config.QUEST_BOARD_CONFIG_VERSION),
        "slot_count": slot_count,
        "active_count": len(definitions),
        "completed_count": completed_count,
        "claimed_count": claimed_count,
        "reset_label": reset_label,
        "show_schedule": show_schedule,
        "slots": rows,
    }


def _wallet_claim_block_reason(wallet: TokenWallet | None) -> str:
    if wallet is None:
        return ""
    if wallet.risk_status == LEDGER_RISK_STATUS_BLOCKED:
        return "Wallet is blocked"
    if wallet.review_required or wallet.risk_status == LEDGER_RISK_STATUS_REVIEW:
        return "Wallet is under review"
    return ""


def _lock_wallet_pair(
    user_wallet: TokenWallet,
    issuance_wallet: TokenWallet,
):
    ids = sorted({user_wallet.pk, issuance_wallet.pk})
    locked = {
        wallet.pk: wallet
        for wallet in TokenWallet.objects.select_for_update()
        .filter(pk__in=ids)
        .order_by("pk")
    }
    if len(locked) != 2:
        raise ValidationError("Could not lock quest reward wallets")
    return locked[user_wallet.pk], locked[issuance_wallet.pk]


def _validate_existing_transaction(
    *,
    txn: LedgerTransaction,
    user_id: int,
    external_id: str,
) -> dict:
    if txn.external_id != external_id:
        raise ValidationError("Quest reward transaction key does not match")
    if txn.kind != QUEST_REWARD_TRANSACTION_KIND:
        raise ValidationError(
            "Quest reward idempotency key belongs to another transaction kind"
        )
    if txn.status != LedgerTransaction.STATUS_POSTED:
        raise ValidationError(
            "Existing quest reward transaction is not posted"
        )
    if txn.created_by_id != int(user_id):
        raise ValidationError(
            "Quest reward transaction belongs to another user"
        )

    amount_units = int((txn.metadata or {}).get("amount_units") or 0)
    if amount_units <= 0:
        raise ValidationError(
            "Existing quest reward transaction has invalid metadata"
        )

    return {
        "claimed": False,
        "already_claimed": True,
        "txn": txn,
        "amount_units": amount_units,
        "amount_tokens": amount_units
        // (10 ** config.PLATFORM_TOKEN_DECIMALS),
        "quest_key": str((txn.metadata or {}).get("quest_key") or ""),
        "quest_title": str((txn.metadata or {}).get("quest_title") or ""),
    }


@transaction.atomic
def claim_quest_reward(*, user, quest_key: str) -> dict:
    user = _require_eligible_user(user)
    if not config.QUEST_BOARD_ENABLED:
        raise ValidationError("The Quest Board is disabled")

    definition = get_quest_definition(quest_key)
    user_model = get_user_model()
    locked_user = user_model.objects.select_for_update().get(pk=user.pk)

    external_id = _quest_external_id(
        user_id=locked_user.pk,
        quest_key=definition.key,
    )
    existing = (
        LedgerTransaction.objects.select_for_update()
        .filter(external_id=external_id)
        .first()
    )
    if existing is not None:
        return _validate_existing_transaction(
            txn=existing,
            user_id=locked_user.pk,
            external_id=external_id,
        )

    current, target = _evaluate_progress(
        user=locked_user,
        definition=definition,
    )
    if current < target:
        raise ValidationError("Quest requirements are not complete")

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
        raise ValidationError(
            "System issuance wallet must allow negative balances"
        )

    user_wallet, issuance_wallet = _lock_wallet_pair(
        user_wallet,
        issuance_wallet,
    )
    user_block_reason = _wallet_claim_block_reason(user_wallet)
    issuance_block_reason = _wallet_claim_block_reason(issuance_wallet)
    if user_block_reason:
        raise ValidationError(user_block_reason)
    if issuance_block_reason:
        raise ValidationError(
            f"Quest reward issuance is unavailable: "
            f"{issuance_block_reason}"
        )

    metadata = {
        "source": "wallet_quest_board",
        "user_id": int(locked_user.pk),
        "quest_key": definition.key,
        "quest_title": definition.title,
        "condition": definition.condition,
        "progress_current": current,
        "progress_target": target,
        "reward_kind": definition.reward_kind,
        "reward_asset": definition.reward_asset,
        "amount_tokens": int(definition.reward_tokens),
        "amount_units": int(definition.reward_units),
        "quest_board_config_version": int(
            config.QUEST_BOARD_CONFIG_VERSION
        ),
    }
    payload = {
        "external_id": external_id,
        "kind": QUEST_REWARD_TRANSACTION_KIND,
        "metadata": metadata,
    }
    request_hash = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()

    amount = int(definition.reward_units)
    user_wallet.balance = int(user_wallet.balance) + amount
    issuance_wallet.balance = int(issuance_wallet.balance) - amount
    user_wallet.save(update_fields=["balance", "updated_at"])
    issuance_wallet.save(update_fields=["balance", "updated_at"])

    txn = LedgerTransaction.objects.create(
        kind=QUEST_REWARD_TRANSACTION_KIND,
        status=LedgerTransaction.STATUS_POSTED,
        external_id=external_id,
        request_hash=request_hash,
        created_by=locked_user,
        memo=f"Quest reward: {definition.title}",
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
        topic=QUEST_REWARD_OUTBOX_TOPIC,
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

    return {
        "claimed": True,
        "already_claimed": False,
        "txn": txn,
        "amount_units": amount,
        "amount_tokens": definition.reward_tokens,
        "quest_key": definition.key,
        "quest_title": definition.title,
    }
