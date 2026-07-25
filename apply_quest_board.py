#!/usr/bin/env python3
'''Implement a configurable Quest Board with only the Confirm Email quest active.

Run from the MediaCMS repository root:
    python apply_quest_board.py

This implementation:
- keeps four board slots;
- reads quests exclusively from ledger/dashboard/config.py;
- configures only the one-time Confirm Email quest;
- leaves the other three slots empty;
- credits quest rewards through the immutable ledger;
- uses LedgerTransaction.external_id as the idempotent claim record;
- requires no database migration.
'''

from __future__ import annotations

from pathlib import Path

ROOT = Path.cwd()


def fail(message: str) -> None:
    raise SystemExit(message)


def read(relative_path: str) -> str:
    path = ROOT / relative_path
    if not path.is_file():
        fail(f"Missing expected file: {relative_path}")
    return path.read_text(encoding="utf-8")


def write(relative_path: str, content: str) -> None:
    path = ROOT / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def replace_once(relative_path: str, old: str, new: str) -> None:
    content = read(relative_path)

    if new in content:
        print(f"{relative_path}: already updated")
        return

    count = content.count(old)
    if count != 1:
        fail(
            f"{relative_path}: expected exactly one patch anchor, found {count}. "
            "The local branch differs from the expected advanced-monetisation state."
        )

    write(relative_path, content.replace(old, new, 1))
    print(f"{relative_path}: updated")


def create_once(relative_path: str, content: str) -> None:
    path = ROOT / relative_path
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing == content:
            print(f"{relative_path}: already created")
            return
        fail(f"{relative_path}: already exists with different content")

    write(relative_path, content)
    print(f"{relative_path}: created")


# ---------------------------------------------------------------------------
# Central configuration
# ---------------------------------------------------------------------------

replace_once(
    "ledger/dashboard/config.py",
    '''    "quest_daily_login": "images/wallet/dashboard/quest-login.png",
''',
    '''    "quest_confirm_email": "images/wallet/dashboard/quest-login.png",
    "quest_daily_login": "images/wallet/dashboard/quest-login.png",
''',
)

replace_once(
    "ledger/dashboard/config.py",
    '''# ---------------------------------------------------------------------------
# Bonus Vault
# ---------------------------------------------------------------------------
''',
    '''# ---------------------------------------------------------------------------
# Quest Board
# ---------------------------------------------------------------------------
# The board always keeps this many visual slots. Only definitions present in
# QUEST_BOARD_QUESTS are active; remaining slots are intentionally empty.
QUEST_BOARD_CONFIG_VERSION = 1
QUEST_BOARD_ENABLED = True
QUEST_BOARD_SLOT_COUNT = 4
QUEST_BOARD_RESET_LABEL = "One-time"
QUEST_BOARD_MAX_REWARD_TOKENS = 100_000

# Supported condition types are implemented in ledger/dashboard/quests.py.
# Reward amounts are HUMAN token amounts and are converted to ledger units.
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


# ---------------------------------------------------------------------------
# Bonus Vault
# ---------------------------------------------------------------------------
''',
)


# ---------------------------------------------------------------------------
# Configurable Quest Board service
# ---------------------------------------------------------------------------

quests_module = '''from __future__ import annotations

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

    return {
        "enabled": enabled,
        "config_version": int(config.QUEST_BOARD_CONFIG_VERSION),
        "slot_count": slot_count,
        "active_count": len(definitions),
        "completed_count": completed_count,
        "claimed_count": claimed_count,
        "reset_label": str(config.QUEST_BOARD_RESET_LABEL or "").strip(),
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
'''

create_once("ledger/dashboard/quests.py", quests_module)


# ---------------------------------------------------------------------------
# Claim endpoint
# ---------------------------------------------------------------------------

replace_once(
    "ledger/dashboard/views.py",
    '''from .bonus_vault import open_bonus_vault
from .daily_rewards import claim_daily_reward
''',
    '''from .bonus_vault import open_bonus_vault
from .daily_rewards import claim_daily_reward
from .quests import claim_quest_reward
''',
)

replace_once(
    "ledger/dashboard/views.py",
    '''@login_required
@require_POST
def wallet_open_bonus_vault(request):
''',
    '''@login_required
@require_POST
def wallet_claim_quest(request, quest_key):
    try:
        result = claim_quest_reward(
            user=request.user,
            quest_key=quest_key,
        )
    except (PermissionDenied, ValidationError) as exc:
        messages.error(
            request,
            exc.messages[0] if hasattr(exc, "messages") else str(exc),
        )
        return redirect("wallet")

    if result["claimed"]:
        messages.success(
            request,
            (
                f"Quest completed: {result['quest_title']} "
                f"(+{result['amount_tokens']:,} tokens)."
            ),
        )
    else:
        messages.info(request, "This quest reward was already claimed.")
    return redirect("wallet")


@login_required
@require_POST
def wallet_open_bonus_vault(request):
''',
)

replace_once(
    "files/urls.py",
    '''    path(
        "wallet/bonus-vault/open",
        wallet_dashboard_views.wallet_open_bonus_vault,
        name="wallet_open_bonus_vault",
    ),
''',
    '''    path(
        "wallet/bonus-vault/open",
        wallet_dashboard_views.wallet_open_bonus_vault,
        name="wallet_open_bonus_vault",
    ),
    path(
        "wallet/quests/<slug:quest_key>/claim",
        wallet_dashboard_views.wallet_claim_quest,
        name="wallet_claim_quest",
    ),
''',
)


# ---------------------------------------------------------------------------
# Wallet context
# ---------------------------------------------------------------------------

replace_once(
    "files/views.py",
    '''from ledger.dashboard.bonus_vault import build_bonus_vault_context
from ledger.dashboard.daily_rewards import build_daily_rewards_context
''',
    '''from ledger.dashboard.bonus_vault import build_bonus_vault_context
from ledger.dashboard.daily_rewards import build_daily_rewards_context
from ledger.dashboard.quests import build_quest_board_context
''',
)

replace_once(
    "files/views.py",
    '''    context["bonus_vault"] = build_bonus_vault_context(
        user=request.user,
        wallet=wallet_obj,
        open_url=reverse("wallet_open_bonus_vault"),
    )
    return render(request, "cms/wallet.html", context)
''',
    '''    context["bonus_vault"] = build_bonus_vault_context(
        user=request.user,
        wallet=wallet_obj,
        open_url=reverse("wallet_open_bonus_vault"),
    )
    context["quest_board"] = build_quest_board_context(
        user=request.user,
    )
    return render(request, "cms/wallet.html", context)
''',
)


# ---------------------------------------------------------------------------
# Replace the four hardcoded fake quests with configured rows + empty slots
# ---------------------------------------------------------------------------

replace_once(
    "templates/cms/wallet.html",
    '''          <section class="wallet-game-card wallet-game-panel wallet-game-quests" data-wallet-module="quests">
            <div class="wallet-game-panel__head">
              <div><div class="wallet-game-card__title"><i class="material-icons wallet-game-card__title-accent">track_changes</i><span>Quests Board</span></div><p>Complete missions to earn CF tokens!</p></div>
              <div class="wallet-game-quests__reset"><span>Resets in</span><strong>09h 45m</strong></div>
            </div>
            <div class="wallet-game-quests__grid">
              <article class="wallet-game-quest" data-wallet-quest="daily-login">
                <img src="{% static daily_rewards.assets.quest_daily_login %}" alt=""><strong>Daily Login</strong><span>Login to the platform</span>
                <div class="wallet-game-quest__meter"><i style="width:100%"></i></div><div class="wallet-game-quest__progress">1 / 1</div>
                <div class="wallet-game-quest__reward">+50 <img src="{% static daily_rewards.assets.token_icon %}" alt=""></div><button type="button" data-wallet-action="claim-quest">Claim</button>
              </article>
              <article class="wallet-game-quest" data-wallet-quest="watch-previews">
                <img src="{% static daily_rewards.assets.quest_watch_previews %}" alt=""><strong>Watch Previews</strong><span>Watch 3 previews</span>
                <div class="wallet-game-quest__meter wallet-game-quest__meter--amber"><i style="width:66%"></i></div><div class="wallet-game-quest__progress">2 / 3</div>
                <div class="wallet-game-quest__reward">+75 <img src="{% static daily_rewards.assets.token_icon %}" alt=""></div><button type="button" data-wallet-action="go-quest">Go</button>
              </article>
              <article class="wallet-game-quest" data-wallet-quest="invite-friend">
                <img src="{% static daily_rewards.assets.quest_invite_friend %}" alt=""><strong>Invite a Friend</strong><span>Invite 1 friend</span>
                <div class="wallet-game-quest__meter"><i style="width:0%"></i></div><div class="wallet-game-quest__progress">0 / 1</div>
                <div class="wallet-game-quest__reward">+200 <img src="{% static daily_rewards.assets.token_icon %}" alt=""></div><button type="button" data-wallet-action="invite-friend">Invite</button>
              </article>
              <article class="wallet-game-quest" data-wallet-quest="remove-ads">
                <img src="{% static daily_rewards.assets.quest_remove_ads %}" alt=""><strong>Unlock Ad-Free</strong><span>Remove ads forever</span>
                <div class="wallet-game-quest__meter"><i style="width:{% if ad_free.active %}100{% else %}0{% endif %}%"></i></div><div class="wallet-game-quest__progress">{% if ad_free.active %}1 / 1{% else %}0 / 1{% endif %}</div>
                <div class="wallet-game-quest__reward">+500 <img src="{% static daily_rewards.assets.token_icon %}" alt=""></div><button type="button" data-wallet-scroll-to="wallet-ad-free-offer">{% if ad_free.active %}Done{% else %}View Offer{% endif %}</button>
              </article>
            </div>
          </section>
''',
    '''          <section class="wallet-game-card wallet-game-panel wallet-game-quests" data-wallet-module="quests">
            <div class="wallet-game-panel__head">
              <div><div class="wallet-game-card__title"><i class="material-icons wallet-game-card__title-accent">track_changes</i><span>Quests Board</span></div><p>Complete missions to earn CF tokens!</p></div>
              {% if quest_board.reset_label %}
              <div class="wallet-game-quests__reset"><span>Schedule</span><strong>{{ quest_board.reset_label }}</strong></div>
              {% endif %}
            </div>
            <div class="wallet-game-quests__grid">
              {% for quest in quest_board.slots %}
                {% if quest.empty %}
              <article class="wallet-game-quest wallet-game-quest--empty" aria-hidden="true" style="visibility:hidden"></article>
                {% else %}
              <article class="wallet-game-quest{% if quest.claimed %} wallet-game-quest--claimed{% elif quest.complete %} wallet-game-quest--complete{% endif %}" data-wallet-quest="{{ quest.key }}">
                <img src="{% static quest.icon_path %}" alt="">
                <strong>{{ quest.title }}</strong>
                <span>{{ quest.description }}</span>
                <div class="wallet-game-quest__meter{% if not quest.complete %} wallet-game-quest__meter--amber{% endif %}"><i style="width:{{ quest.progress_percent }}%"></i></div>
                <div class="wallet-game-quest__progress">{{ quest.current }} / {{ quest.target }}</div>
                <div class="wallet-game-quest__reward">+{{ quest.reward_display }} <img src="{% static daily_rewards.assets.token_icon %}" alt=""></div>
                {% if quest.claimed %}
                <button type="button" disabled>Claimed</button>
                {% elif quest.can_claim %}
                <form method="post" action="{{ quest.claim_url }}" style="display:contents">
                  {% csrf_token %}
                  <button type="submit" data-wallet-action="claim-quest">Claim</button>
                </form>
                {% elif quest.action_url %}
                <form method="get" action="{{ quest.action_url }}" style="display:contents">
                  <button type="submit" data-wallet-action="go-quest">{{ quest.button_label }}</button>
                </form>
                {% else %}
                <button type="button" disabled>{{ quest.button_label }}</button>
                {% endif %}
              </article>
                {% endif %}
              {% endfor %}
            </div>
          </section>
''',
)


# ---------------------------------------------------------------------------
# Functional tests only
# ---------------------------------------------------------------------------

quest_tests = '''from unittest.mock import patch

from allauth.account.models import EmailAddress
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.quests import (
    QUEST_REWARD_OUTBOX_TOPIC,
    QUEST_REWARD_TRANSACTION_KIND,
    build_quest_board_context,
    claim_quest_reward,
    get_quest_definitions,
)
from ledger.models import LedgerEntry, LedgerOutbox, LedgerTransaction
from tests.ledger.base import BaseLedgerTestCase


class TestQuestBoard(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        if not self.u1.email:
            self.u1.email = f"quest-{self.u1.pk}@example.test"
            self.u1.save(update_fields=["email"])

        self.email_address, _created = EmailAddress.objects.update_or_create(
            user=self.u1,
            email=self.u1.email,
            defaults={
                "primary": True,
                "verified": False,
            },
        )

    def _verify_email(self):
        self.email_address.verified = True
        self.email_address.save(update_fields=["verified"])

    def _context(self):
        return build_quest_board_context(user=self.u1)

    def test_config_contains_only_confirm_email_and_three_empty_slots(self):
        definitions = get_quest_definitions()
        context = self._context()

        self.assertEqual(len(definitions), 1)
        self.assertEqual(definitions[0].key, "confirm_email")
        self.assertEqual(context["active_count"], 1)
        self.assertEqual(context["slot_count"], 4)
        self.assertEqual(
            sum(1 for row in context["slots"] if row["empty"]),
            3,
        )

    def test_unverified_email_cannot_claim(self):
        context = self._context()
        quest = context["slots"][0]

        self.assertEqual(quest["key"], "confirm_email")
        self.assertEqual(quest["current"], 0)
        self.assertEqual(quest["target"], 1)
        self.assertFalse(quest["complete"])
        self.assertFalse(quest["can_claim"])
        self.assertEqual(quest["action_url"], reverse("account_email"))

        with self.assertRaises(ValidationError):
            claim_quest_reward(
                user=self.u1,
                quest_key="confirm_email",
            )

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=QUEST_REWARD_TRANSACTION_KIND,
            ).count(),
            0,
        )

    def test_verified_email_can_claim_configured_reward_once(self):
        self._verify_email()
        definition = get_quest_definitions()[0]
        context = self._context()
        quest = context["slots"][0]

        self.assertTrue(quest["complete"])
        self.assertTrue(quest["can_claim"])
        self.assertFalse(quest["claimed"])

        before_user = int(self.w1.balance)
        before_issuance = int(self.issuance.balance)

        first = claim_quest_reward(
            user=self.u1,
            quest_key=definition.key,
        )
        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()

        self.assertTrue(first["claimed"])
        self.assertFalse(first["already_claimed"])
        self.assertEqual(
            self.w1.balance,
            before_user + definition.reward_units,
        )
        self.assertEqual(
            self.issuance.balance,
            before_issuance - definition.reward_units,
        )

        second = claim_quest_reward(
            user=self.u1,
            quest_key=definition.key,
        )
        self.w1.refresh_from_db()

        self.assertFalse(second["claimed"])
        self.assertTrue(second["already_claimed"])
        self.assertEqual(
            self.w1.balance,
            before_user + definition.reward_units,
        )
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=QUEST_REWARD_TRANSACTION_KIND,
            ).count(),
            1,
        )
        self.assertEqual(
            LedgerEntry.objects.filter(txn=first["txn"]).count(),
            2,
        )
        self.assertEqual(
            LedgerOutbox.objects.filter(
                txn=first["txn"],
                topic=QUEST_REWARD_OUTBOX_TOPIC,
            ).count(),
            1,
        )

        claimed_context = self._context()
        claimed_quest = claimed_context["slots"][0]
        self.assertTrue(claimed_quest["claimed"])
        self.assertFalse(claimed_quest["can_claim"])
        self.assertEqual(claimed_quest["status"], "claimed")

    def test_claim_endpoint_requires_post(self):
        self._verify_email()
        self.client.force_login(self.u1)
        url = reverse(
            "wallet_claim_quest",
            kwargs={"quest_key": "confirm_email"},
        )

        self.assertEqual(self.client.get(url).status_code, 405)

        response = self.client.post(url)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=QUEST_REWARD_TRANSACTION_KIND,
            ).count(),
            1,
        )

    def test_invalid_or_oversized_config_is_rejected(self):
        invalid = (
            {
                "key": "confirm_email",
                "title": "Confirm Email",
                "description": "Verify your email address",
                "condition": "unknown_condition",
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
        with patch.object(config, "QUEST_BOARD_QUESTS", invalid):
            with self.assertRaises(ImproperlyConfigured):
                get_quest_definitions()

        with patch.object(config, "QUEST_BOARD_SLOT_COUNT", 0):
            with self.assertRaises(ImproperlyConfigured):
                get_quest_definitions()
'''

create_once("tests/ledger/test_quest_board.py", quest_tests)


print()
print("Configurable Quest Board applied.")
print("Active config: Confirm Email only; three slots remain empty.")
print("No migration is required.")
print()
print("Run:")
print("  git diff --check")
print("  python manage.py check")
print(
    "  pytest -q tests/ledger/test_quest_board.py "
    "tests/ledger/test_bonus_vault.py "
    "tests/ledger/test_daily_rewards.py"
)
