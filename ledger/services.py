from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Max, Q
from .models import (
    LEDGER_METADATA_VERSION,
    LEDGER_OUTBOX_MAX_RETRIES,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
    LedgerSaga,
    LedgerSagaStep,
    LEDGER_OUTBOX_RETRY_DELAY_SECONDS,
    LedgerHold,
    LedgerVelocityWindow,
    LEDGER_ACTION_DEPOSIT,
    LEDGER_ACTION_PURCHASE,
    LEDGER_ACTION_TRANSFER,
    LEDGER_ACTION_WITHDRAWAL,
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    WalletRequest,
    DepositSession,
    ObservedOnchainTransfer,
    SYSTEM_WALLET_EXTERNAL_ASSET_CLEARING,
    DepositAddress,
)
import uuid
import hashlib
import json
from django.utils import timezone
from datetime import timedelta
from django.db.models import Q

ACTIVE_DEPOSIT_SESSION_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_SEEN_ONCHAIN,
    DepositSession.STATUS_CONFIRMING,
}
ACTIVE_DEPOSIT_WATCH_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_SEEN_ONCHAIN,
    DepositSession.STATUS_CONFIRMING,
}

def _compute_next_retry_at() -> timezone.datetime:
    return timezone.now() + timedelta(seconds=LEDGER_OUTBOX_RETRY_DELAY_SECONDS)

def _require_authenticated_actor(actor):
    if actor is None:
        raise PermissionDenied("Actor is required")
    if not getattr(actor, "is_authenticated", False):
        raise PermissionDenied("Authenticated actor required")
    if not getattr(actor, "is_active", False):
        raise PermissionDenied("Active actor required")
    return actor


def _require_perm(actor, perm_codename: str):
    actor = _require_authenticated_actor(actor)
    if getattr(actor, "is_superuser", False):
        return actor
    if not actor.has_perm(perm_codename):
        raise PermissionDenied(f"Missing permission: {perm_codename}")
    return actor


def _resolve_created_by(*, actor, created_by):
    actor = _require_authenticated_actor(actor)

    if created_by is None:
        return actor

    if created_by.id == actor.id:
        return created_by

    if getattr(actor, "is_superuser", False):
        return created_by

    if actor.has_perm("ledger.can_impersonate_ledger_creator"):
        return created_by

    raise PermissionDenied("Cannot set created_by for another user")

def _create_outbox_event(*, txn: LedgerTransaction, topic: str, payload: dict, metadata_version: int) -> LedgerOutbox:
    return LedgerOutbox.objects.create(
        txn=txn,
        topic=topic,
        aggregate_type="ledger_transaction",
        aggregate_id=txn.id,
        payload=payload,
        metadata_version=metadata_version,
    )

def get_failed_outbox_events(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")
    return LedgerOutbox.objects.filter(
        status=LedgerOutbox.STATUS_FAILED
    ).order_by("created_at")[:limit]


def get_dead_lettered_outbox_events(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")
    return LedgerOutbox.objects.filter(
        status=LedgerOutbox.STATUS_DEAD_LETTERED
    ).order_by("dead_lettered_at", "created_at")[:limit]


def get_stale_pending_outbox_events(*, actor, older_than_seconds: int = 900, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    threshold = timezone.now() - timedelta(seconds=older_than_seconds)
    return LedgerOutbox.objects.filter(
        status=LedgerOutbox.STATUS_PENDING,
        created_at__lte=threshold,
    ).order_by("created_at")[:limit]

def get_failed_sagas(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    return LedgerSaga.objects.filter(
        status=LedgerSaga.STATUS_FAILED
    ).order_by("failed_at", "created_at")[:limit]

def get_wallet_available_balance(wallet: TokenWallet) -> int:
    return int(wallet.balance) - int(wallet.held_balance)

def _require_wallet_not_blocked(wallet: TokenWallet):
    if wallet.risk_status == LEDGER_RISK_STATUS_BLOCKED:
        raise ValidationError("Wallet is blocked")
    if wallet.review_required or wallet.risk_status == LEDGER_RISK_STATUS_REVIEW:
        raise ValidationError("Wallet requires review")

def _require_wallet_owner(actor, wallet: TokenWallet):
    actor = _require_authenticated_actor(actor)
    if getattr(actor, "is_superuser", False):
        return actor
    if wallet.user_id != actor.id:
        raise PermissionDenied("Cannot manage another user's wallet")
    return actor


def _normalize_wallet_request_amount(amount) -> int:
    try:
        normalized = int(amount)
    except (TypeError, ValueError):
        raise ValidationError("Amount must be a whole number")
    if normalized <= 0:
        raise ValidationError("Amount must be greater than zero")
    return normalized


def _build_wallet_request_reference(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"

def _floor_window_start(now, window_seconds: int):
    ts = int(now.timestamp())
    floored = ts - (ts % window_seconds)
    return timezone.datetime.fromtimestamp(floored, tz=now.tzinfo)

def get_stale_compensating_sagas(*, actor, older_than_seconds: int = 900, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_sagas")

    threshold = timezone.now() - timedelta(seconds=older_than_seconds)
    return LedgerSaga.objects.filter(
        status=LedgerSaga.STATUS_COMPENSATING,
        created_at__lte=threshold,
    ).order_by("created_at")[:limit]

def get_system_wallet(system_key: str, *, allow_negative: bool) -> TokenWallet:
    wallet, created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=system_key,
        defaults={
            "allow_negative": allow_negative,
        },
    )
    if not created and wallet.allow_negative != allow_negative:
        raise ValidationError(
            f"System wallet '{system_key}' has allow_negative={wallet.allow_negative}, "
            f"expected {allow_negative}"
        )
    return wallet

def _infer_action_from_kind(kind: str) -> str:
    lowered = kind.lower()
    if "withdraw" in lowered:
        return LEDGER_ACTION_WITHDRAWAL
    if "purchase" in lowered or "buy" in lowered:
        return LEDGER_ACTION_PURCHASE
    if "deposit" in lowered or "mint" in lowered:
        return LEDGER_ACTION_DEPOSIT
    return LEDGER_ACTION_TRANSFER

def _normalize_chain(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_evm_address(value: str) -> str:
    return (value or "").strip().lower()

def _parse_derivation_index_from_ref(address_derivation_ref: str):
    raw_value = (address_derivation_ref or "").strip()
    if not raw_value:
        return None

    tail = raw_value.rsplit(":", 1)[-1]
    try:
        parsed = int(tail)
    except (TypeError, ValueError):
        return None

    if parsed < 0:
        return None

    return parsed

def build_evm_event_key(*, chain: str, txid: str, log_index: int) -> str:
    normalized_chain = _normalize_chain(chain)
    normalized_txid = (txid or "").strip().lower()
    return f"{normalized_chain}:{normalized_txid}:{int(log_index)}"


def get_external_asset_clearing_wallet() -> TokenWallet:
    return get_system_wallet(
        TokenWallet.SYSTEM_EXTERNAL_ASSET_CLEARING,
        allow_negative=True,
    )

@transaction.atomic
def create_wallet_deposit_request(
    *,
    actor,
    wallet: TokenWallet,
    amount: int,
    notes: str = "",
    metadata=None,
) -> WalletRequest:
    actor = _require_wallet_owner(actor, wallet)

    if metadata is None:
        metadata = {}

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    _require_wallet_not_blocked(wallet)

    normalized_amount = _normalize_wallet_request_amount(amount)
    reference = _build_wallet_request_reference("dep")

    wallet_request = WalletRequest.objects.create(
        wallet=wallet,
        request_type=WalletRequest.REQUEST_TYPE_DEPOSIT,
        status=WalletRequest.STATUS_PENDING,
        amount=normalized_amount,
        asset_code="TOKENS",
        reference=reference,
        notes=notes,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
        created_by=actor,
    )
    return wallet_request


@transaction.atomic
def create_wallet_withdrawal_request(
    *,
    actor,
    wallet: TokenWallet,
    amount: int,
    destination_address: str,
    notes: str = "",
    metadata=None,
) -> WalletRequest:
    actor = _require_wallet_owner(actor, wallet)

    if metadata is None:
        metadata = {}

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    _require_wallet_not_blocked(wallet)

    normalized_amount = _normalize_wallet_request_amount(amount)
    destination_address = (destination_address or "").strip()
    if not destination_address:
        raise ValidationError("Destination address is required")

    available_balance = get_wallet_available_balance(wallet)
    if not wallet.allow_negative and normalized_amount > available_balance:
        raise ValidationError("Insufficient available balance")

    reference = _build_wallet_request_reference("wdr")

    wallet.held_balance += normalized_amount
    wallet.save(update_fields=["held_balance", "updated_at"])

    hold = LedgerHold.objects.create(
        wallet=wallet,
        amount=normalized_amount,
        reason=f"Reserved for withdrawal request {reference}",
        created_by=actor,
        metadata={
            **metadata,
            "reference": reference,
            "destination_address": destination_address,
            "source": "wallet_ui",
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    wallet_request = WalletRequest.objects.create(
        wallet=wallet,
        request_type=WalletRequest.REQUEST_TYPE_WITHDRAWAL,
        status=WalletRequest.STATUS_PENDING,
        amount=normalized_amount,
        asset_code="TOKENS",
        destination_address=destination_address,
        reference=reference,
        notes=notes,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
        hold=hold,
        created_by=actor,
    )
    return wallet_request

@transaction.atomic
def create_pending_ledger_transaction(*, actor, kind: str, created_by=None, external_id=None, memo="", metadata=None):
    _require_perm(actor, "ledger.can_create_pending_ledger_transaction")
    created_by = _resolve_created_by(actor=actor, created_by=created_by)

    if metadata is None:
        metadata = {}

    request_hash = None
    if external_id:
        payload = {
            "kind": kind,
            "memo": memo,
            "entries": [],
            "metadata": metadata,
            "status": LedgerTransaction.STATUS_PENDING,
        }
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        request_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    if external_id:
        try:
            with transaction.atomic():
                txn = LedgerTransaction.objects.create(
                    kind=kind,
                    status=LedgerTransaction.STATUS_PENDING,
                    external_id=external_id,
                    request_hash=request_hash,
                    created_by=created_by,
                    memo=memo,
                    metadata=metadata,
                    metadata_version=LEDGER_METADATA_VERSION,
                )
        except IntegrityError:
            existing = LedgerTransaction.objects.get(external_id=external_id)
            if existing.request_hash and existing.request_hash != request_hash:
                raise ValidationError("Idempotency key reused with different payload")
            return existing
    else:
        txn = LedgerTransaction.objects.create(
            kind=kind,
            status=LedgerTransaction.STATUS_PENDING,
            external_id=None,
            created_by=created_by,
            memo=memo,
            request_hash=None,
            metadata=metadata,
            metadata_version=LEDGER_METADATA_VERSION,
        )

    _create_outbox_event(
        txn=txn,
        topic="ledger.transaction.pending",
        payload={
            "txn_id": txn.id,
            "kind": txn.kind,
            "status": txn.status,
            "external_id": txn.external_id,
            "created_by_id": txn.created_by_id,
            "metadata": txn.metadata,
        },
        metadata_version=txn.metadata_version,
    )
    return txn

@transaction.atomic
def apply_ledger_transaction(*, actor, kind: str, entries: list, created_by=None, external_id=None, memo="", metadata=None):
    """
    entries: list[tuple[TokenWallet, int]] signed delta.
    """

    _require_perm(actor, "ledger.can_apply_raw_ledger_transaction")
    created_by = _resolve_created_by(actor=actor, created_by=created_by)

    if metadata is None:
        metadata = {}

    if not entries:
        raise ValidationError("No entries")

    # agrégation par wallet
    aggregated = {}
    for wallet, delta in entries:
        if wallet.id is None:
            raise ValidationError("Unsaved wallet in entries")
        aggregated[wallet.id] = aggregated.get(wallet.id, 0) + int(delta)

    normalized_entries = [[wallet_id, delta] for wallet_id, delta in aggregated.items() if delta != 0]
    normalized_entries.sort(key=lambda x: (x[0], x[1]))

    if len(normalized_entries) < 2:
        raise ValidationError("A ledger transaction must have at least two balanced entries")

    if sum(delta for _, delta in normalized_entries) != 0:
        raise ValidationError("Ledger transaction is not balanced")

    payload = {
        "kind": kind,
        "memo": memo,
        "entries": normalized_entries,
        "metadata": metadata,
        "status": LedgerTransaction.STATUS_POSTED,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    request_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    # Idempotence (exactly-once)
    if external_id:
        try:
            with transaction.atomic():
                txn = LedgerTransaction.objects.create(
                    kind=kind,
                    external_id=external_id,
                    request_hash=request_hash,
                    created_by=created_by,
                    memo=memo,
                    metadata=metadata,
                    status=LedgerTransaction.STATUS_POSTED,
                    metadata_version=LEDGER_METADATA_VERSION,
                )
        except IntegrityError:
            existing = LedgerTransaction.objects.get(external_id=external_id)
            if existing.request_hash and existing.request_hash != request_hash:
                raise ValidationError("Idempotency key reused with different payload")
            return existing
    else:
        txn = LedgerTransaction.objects.create(
            kind=kind,
            external_id=None,
            created_by=created_by,
            memo=memo,
            request_hash=None,
            metadata=metadata,
            status=LedgerTransaction.STATUS_POSTED,
            metadata_version=LEDGER_METADATA_VERSION,
        )
    # Lock wallets (stable order, unique request)
    wallet_ids = [wallet_id for wallet_id, _ in normalized_entries]
    if any(wid is None for wid in wallet_ids):
        raise ValidationError("Unsaved wallet in entries")

    locked_wallets = (
        TokenWallet.objects.select_for_update()
        .filter(id__in=wallet_ids)
        .order_by("id")
    )
    locked = {w.id: w for w in locked_wallets}
    if len(locked) != len(wallet_ids):
        raise ValidationError("Unknown wallet in entries")
    action = _infer_action_from_kind(kind)

    for wallet_id, delta in normalized_entries:
        w = locked[wallet_id]

        _require_wallet_not_blocked(w)

        if delta < 0:
            enforce_wallet_velocity_limits(
                wallet=w,
                action=action,
                amount=abs(delta),
            )

        new_balance = w.balance + delta
        available_after = new_balance - int(w.held_balance)

        if not w.allow_negative and available_after < 0:
            raise ValidationError("Insufficient available funds")

        w.balance = new_balance
        w.save(update_fields=["balance", "updated_at"])

        LedgerEntry.objects.create(
            txn=txn,
            wallet=w,
            delta=delta,
            balance_after=new_balance,
        )

        if delta < 0:
            record_wallet_velocity(
                wallet=w,
                action=action,
                amount=abs(delta),
            )
        elif delta > 0 and action == LEDGER_ACTION_DEPOSIT:
            record_wallet_velocity(
                wallet=w,
                action=LEDGER_ACTION_DEPOSIT,
                amount=abs(delta),
            )
    _create_outbox_event(
        txn=txn,
        topic="ledger.transaction.posted",
        payload={
            "txn_id": txn.id,
            "kind": txn.kind,
            "status": txn.status,
            "external_id": txn.external_id,
            "created_by_id": txn.created_by_id,
            "entry_count": len(normalized_entries),
            "metadata": txn.metadata,
        },
        metadata_version=txn.metadata_version,
    )
    return txn

@transaction.atomic
def reverse_ledger_transaction(*, actor, original_txn: LedgerTransaction, created_by=None, external_id=None, memo="", metadata=None):
    _require_perm(actor, "ledger.can_reverse_ledger_transaction")
    created_by = _resolve_created_by(actor=actor, created_by=created_by)

    if metadata is None:
        metadata = {}

    if original_txn.status != LedgerTransaction.STATUS_POSTED:
        raise ValidationError("Only posted transactions can be reversed")

    if hasattr(original_txn, "reversal_txn"):
        return original_txn.reversal_txn

    original_entries = list(original_txn.entries.select_related("wallet").order_by("id"))
    if not original_entries:
        raise ValidationError("Cannot reverse a transaction without ledger entries")

    payload_metadata = {
        **metadata,
        "reversal_of_txn_id": original_txn.id,
        "reversal_of_external_id": original_txn.external_id,
    }

    request_hash = None
    if external_id:
        payload = {
            "kind": f"{original_txn.kind}_reversal",
            "memo": memo,
            "entries": [[entry.wallet_id, -entry.delta] for entry in original_entries],
            "metadata": payload_metadata,
            "status": LedgerTransaction.STATUS_REVERSED,
            "reversal_of": original_txn.id,
        }
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        request_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    if external_id:
        try:
            with transaction.atomic():
                txn = LedgerTransaction.objects.create(
                    kind=f"{original_txn.kind}_reversal",
                    status=LedgerTransaction.STATUS_REVERSED,
                    reversal_of=original_txn,
                    external_id=external_id,
                    request_hash=request_hash,
                    created_by=created_by,
                    memo=memo,
                    metadata=payload_metadata,
                    metadata_version=LEDGER_METADATA_VERSION
                )
        except IntegrityError:
            existing = LedgerTransaction.objects.get(external_id=external_id)
            if existing.request_hash and existing.request_hash != request_hash:
                raise ValidationError("Idempotency key reused with different payload")
            return existing
    else:
        txn = LedgerTransaction.objects.create(
            kind=f"{original_txn.kind}_reversal",
            status=LedgerTransaction.STATUS_REVERSED,
            reversal_of=original_txn,
            external_id=None,
            created_by=created_by,
            memo=memo,
            request_hash=None,
            metadata=payload_metadata,
            metadata_version=LEDGER_METADATA_VERSION
        )

    wallet_ids = [entry.wallet_id for entry in original_entries]
    locked_wallets = (
        TokenWallet.objects.select_for_update()
        .filter(id__in=wallet_ids)
        .order_by("id")
    )
    locked = {w.id: w for w in locked_wallets}
    if len(locked) != len(wallet_ids):
        raise ValidationError("Unknown wallet in reversal entries")

    for entry in original_entries:
        w = locked[entry.wallet_id]
        delta = -entry.delta
        new_balance = w.balance + delta

        if not w.allow_negative and new_balance < 0:
            raise ValidationError("Insufficient funds")

        w.balance = new_balance
        w.save(update_fields=["balance", "updated_at"])

        LedgerEntry.objects.create(
            txn=txn,
            wallet=w,
            delta=delta,
            balance_after=new_balance,
        )
    _create_outbox_event(
        txn=txn,
        topic="ledger.transaction.reversed",
        payload={
            "txn_id": txn.id,
            "kind": txn.kind,
            "status": txn.status,
            "external_id": txn.external_id,
            "created_by_id": txn.created_by_id,
            "reversal_of_id": txn.reversal_of_id,
            "metadata": txn.metadata,
        },
        metadata_version=txn.metadata_version,
    )
    return txn

def mark_outbox_event_dispatched(*, actor, event: LedgerOutbox) -> LedgerOutbox:
    _require_perm(actor, "ledger.can_manage_ledger_outbox")
    now = timezone.now()
    event.status = LedgerOutbox.STATUS_DISPATCHED
    event.dispatched_at = now
    event.last_attempt_at = now
    event.next_retry_at = None
    event.save(update_fields=["status", "dispatched_at", "last_attempt_at", "next_retry_at"])
    return event


def mark_outbox_event_failed(*, actor, event: LedgerOutbox, error_message: str) -> LedgerOutbox:
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    now = timezone.now()
    event.fail_count += 1
    event.last_error = error_message[:2000]
    event.last_attempt_at = now

    if event.fail_count >= LEDGER_OUTBOX_MAX_RETRIES:
        event.status = LedgerOutbox.STATUS_DEAD_LETTERED
        event.dead_lettered_at = now
        event.dead_letter_reason = error_message[:2000]
        event.next_retry_at = None
        event.save(
            update_fields=[
                "status",
                "fail_count",
                "last_error",
                "last_attempt_at",
                "dead_lettered_at",
                "dead_letter_reason",
                "next_retry_at",
            ]
        )
        return event

    event.status = LedgerOutbox.STATUS_FAILED
    event.next_retry_at = _compute_next_retry_at()
    event.save(
        update_fields=[
            "status",
            "fail_count",
            "last_error",
            "last_attempt_at",
            "next_retry_at",
        ]
    )
    return event

def move_outbox_event_to_dlq(*, actor, event: LedgerOutbox, reason: str) -> LedgerOutbox:
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    now = timezone.now()
    event.status = LedgerOutbox.STATUS_DEAD_LETTERED
    event.dead_lettered_at = now
    event.dead_letter_reason = reason[:2000]
    event.last_error = reason[:2000]
    event.next_retry_at = None
    event.save(
        update_fields=[
            "status",
            "dead_lettered_at",
            "dead_letter_reason",
            "last_error",
            "next_retry_at",
        ]
    )
    return event

def get_dispatchable_outbox_events(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    now = timezone.now()
    return LedgerOutbox.objects.filter(
        Q(status=LedgerOutbox.STATUS_PENDING)
        | Q(
            status=LedgerOutbox.STATUS_FAILED,
            next_retry_at__isnull=False,
            next_retry_at__lte=now,
        )
    ).order_by("created_at")[:limit]

@transaction.atomic
def create_ledger_saga(*, actor, saga_type: str, created_by=None, external_id=None, metadata=None) -> LedgerSaga:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    created_by = _resolve_created_by(actor=actor, created_by=created_by)
    if metadata is None:
        metadata = {}

    if external_id:
        try:
            with transaction.atomic():
                return LedgerSaga.objects.create(
                    saga_type=saga_type,
                    external_id=external_id,
                    status=LedgerSaga.STATUS_PENDING,
                    created_by=created_by,
                    metadata=metadata,
                    metadata_version=LEDGER_METADATA_VERSION,
                )
        except IntegrityError:
            return LedgerSaga.objects.get(external_id=external_id)

    return LedgerSaga.objects.create(
        saga_type=saga_type,
        external_id=None,
        status=LedgerSaga.STATUS_PENDING,
        created_by=created_by,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )

@transaction.atomic
def add_saga_step(*, actor, saga: LedgerSaga, step_key: str, step_order: int, payload=None) -> LedgerSagaStep:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    if payload is None:
        payload = {}

    return LedgerSagaStep.objects.create(
        saga=saga,
        step_key=step_key,
        step_order=step_order,
        status=LedgerSagaStep.STATUS_PENDING,
        payload=payload,
        metadata_version=LEDGER_METADATA_VERSION,
    )

@transaction.atomic
def start_ledger_saga(*, actor, saga: LedgerSaga) -> LedgerSaga:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    if saga.status != LedgerSaga.STATUS_PENDING:
        return saga

    saga.status = LedgerSaga.STATUS_RUNNING
    saga.started_at = timezone.now()
    saga.save(update_fields=["status", "started_at"])
    return saga

@transaction.atomic
def start_saga_step(*, actor, step: LedgerSagaStep) -> LedgerSagaStep:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    if step.status != LedgerSagaStep.STATUS_PENDING:
        return step

    step.status = LedgerSagaStep.STATUS_RUNNING
    step.started_at = timezone.now()
    step.save(update_fields=["status", "started_at"])
    return step

@transaction.atomic
def complete_saga_step(*, actor, step: LedgerSagaStep, txn: LedgerTransaction = None) -> LedgerSagaStep:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    step.status = LedgerSagaStep.STATUS_COMPLETED
    step.completed_at = timezone.now()
    if txn is not None:
        step.txn = txn
        step.save(update_fields=["status", "completed_at", "txn"])
    else:
        step.save(update_fields=["status", "completed_at"])
    return step

@transaction.atomic
def fail_saga_step(*, actor, step: LedgerSagaStep, error_message: str) -> LedgerSagaStep:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    step.status = LedgerSagaStep.STATUS_FAILED
    step.failed_at = timezone.now()
    step.last_error = error_message[:2000]
    step.save(update_fields=["status", "failed_at", "last_error"])

    saga = step.saga
    saga.status = LedgerSaga.STATUS_FAILED
    saga.failed_at = timezone.now()
    saga.last_error = error_message[:2000]
    saga.save(update_fields=["status", "failed_at", "last_error"])

    return step

@transaction.atomic
def complete_ledger_saga(*, actor, saga: LedgerSaga) -> LedgerSaga:
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    if saga.steps.filter(
        status__in=[
            LedgerSagaStep.STATUS_PENDING,
            LedgerSagaStep.STATUS_RUNNING,
            LedgerSagaStep.STATUS_FAILED,
        ]
    ).exists():
        raise ValidationError("Cannot complete saga with unfinished or failed steps")

    saga.status = LedgerSaga.STATUS_COMPLETED
    saga.completed_at = timezone.now()
    saga.save(update_fields=["status", "completed_at"])
    return saga

@transaction.atomic
def compensate_ledger_saga(*, actor, saga: LedgerSaga, created_by=None, reason: str = "") -> LedgerSaga:
    _require_perm(actor, "ledger.can_compensate_ledger_sagas")
    created_by = _resolve_created_by(actor=actor, created_by=created_by)
    if saga.status not in [LedgerSaga.STATUS_FAILED, LedgerSaga.STATUS_COMPENSATING]:
        raise ValidationError("Only failed or compensating sagas can be compensated")

    saga.status = LedgerSaga.STATUS_COMPENSATING
    saga.save(update_fields=["status"])

    steps = list(
        saga.steps.select_related("txn").order_by("-step_order", "-id")
    )

    for step in steps:
        if step.status not in [
            LedgerSagaStep.STATUS_COMPLETED,
            LedgerSagaStep.STATUS_FAILED,
        ]:
            continue

        if step.txn and step.txn.status == LedgerTransaction.STATUS_POSTED:
            compensation_txn = reverse_ledger_transaction(
                actor=actor,
                original_txn=step.txn,
                created_by=created_by,
                external_id=f"saga-comp-{saga.id}-{step.id}",
                memo=reason or f"Compensation for saga {saga.id} step {step.step_key}",
            )
            step.compensation_txn = compensation_txn
            step.status = LedgerSagaStep.STATUS_COMPENSATED
            step.compensated_at = timezone.now()
            step.save(update_fields=["status", "compensated_at", "compensation_txn"])
        elif step.status == LedgerSagaStep.STATUS_COMPLETED:
            step.status = LedgerSagaStep.STATUS_COMPENSATED
            step.compensated_at = timezone.now()
            step.save(update_fields=["status", "compensated_at"])

        step.status = LedgerSagaStep.STATUS_COMPENSATED
        step.compensated_at = timezone.now()
        if step.compensation_txn_id:
            step.save(update_fields=["status", "compensated_at", "compensation_txn"])
        else:
            step.save(update_fields=["status", "compensated_at"])

    saga.status = LedgerSaga.STATUS_COMPENSATED
    saga.compensated_at = timezone.now()
    saga.save(update_fields=["status", "compensated_at"])
    return saga

def replay_failed_outbox_event(*, actor, event: LedgerOutbox) -> LedgerOutbox:
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    if event.status != LedgerOutbox.STATUS_FAILED:
        raise ValidationError("Only failed outbox events can be replayed")

    event.status = LedgerOutbox.STATUS_PENDING
    event.next_retry_at = None
    event.save(update_fields=["status", "next_retry_at"])
    return event

def redrive_dead_lettered_outbox_event(*, actor, event: LedgerOutbox) -> LedgerOutbox:
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    if event.status != LedgerOutbox.STATUS_DEAD_LETTERED:
        raise ValidationError("Only dead-lettered outbox events can be redriven")

    now = timezone.now()
    event.status = LedgerOutbox.STATUS_PENDING
    event.redrive_count += 1
    event.last_redriven_at = now
    event.dead_lettered_at = None
    event.dead_letter_reason = ""
    event.next_retry_at = None
    event.save(
        update_fields=[
            "status",
            "redrive_count",
            "last_redriven_at",
            "dead_lettered_at",
            "dead_letter_reason",
            "next_retry_at",
        ]
    )
    return event

@transaction.atomic
def create_wallet_hold(*, actor, wallet: TokenWallet, amount: int, reason: str = "", metadata=None) -> LedgerHold:
    _require_perm(actor, "ledger.can_manage_wallet_holds")

    if metadata is None:
        metadata = {}

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    if amount <= 0:
        raise ValidationError("Hold amount must be > 0")

    available_balance = get_wallet_available_balance(wallet)
    if not wallet.allow_negative and amount > available_balance:
        raise ValidationError("Insufficient available balance for hold")

    wallet.held_balance += int(amount)
    wallet.save(update_fields=["held_balance", "updated_at"])

    return LedgerHold.objects.create(
        wallet=wallet,
        amount=int(amount),
        reason=reason,
        created_by=actor,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )

@transaction.atomic
def release_wallet_hold(*, actor, hold: LedgerHold, reason: str = "") -> LedgerHold:
    _require_perm(actor, "ledger.can_manage_wallet_holds")

    hold = LedgerHold.objects.select_for_update().select_related("wallet").get(id=hold.id)
    if hold.released:
        return hold

    wallet = TokenWallet.objects.select_for_update().get(id=hold.wallet_id)
    wallet.held_balance -= int(hold.amount)
    if wallet.held_balance < 0:
        raise ValidationError("Wallet held balance cannot be negative")

    wallet.save(update_fields=["held_balance", "updated_at"])

    hold.released = True
    hold.released_by = actor
    hold.released_at = timezone.now()
    if reason:
        hold.reason = reason
    hold.save(update_fields=["released", "released_by", "released_at", "reason"])
    return hold

@transaction.atomic
def record_wallet_velocity(*, wallet: TokenWallet, action: str, amount: int):
    now = timezone.now()

    for window_seconds in [3600, 86400]:
        window_start = _floor_window_start(now, window_seconds)
        obj, _ = LedgerVelocityWindow.objects.select_for_update().get_or_create(
            wallet=wallet,
            action=action,
            window_seconds=window_seconds,
            window_start=window_start,
            defaults={"amount": 0, "count": 0},
        )
        obj.amount += int(amount)
        obj.count += 1
        obj.save(update_fields=["amount", "count", "updated_at"])

def get_wallet_velocity_amount(*, wallet: TokenWallet, action: str, window_seconds: int) -> int:
    now = timezone.now()
    window_start = _floor_window_start(now, window_seconds)
    obj = LedgerVelocityWindow.objects.filter(
        wallet=wallet,
        action=action,
        window_seconds=window_seconds,
        window_start=window_start,
    ).first()
    return int(obj.amount) if obj else 0

def enforce_wallet_velocity_limits(*, wallet: TokenWallet, action: str, amount: int):
    if amount <= 0:
        return

    hourly_current = get_wallet_velocity_amount(wallet=wallet, action=action, window_seconds=3600)
    daily_current = get_wallet_velocity_amount(wallet=wallet, action=action, window_seconds=86400)

    if action in [LEDGER_ACTION_WITHDRAWAL, LEDGER_ACTION_TRANSFER, LEDGER_ACTION_PURCHASE]:
        if wallet.hourly_outflow_limit is not None and hourly_current + amount > wallet.hourly_outflow_limit:
            raise ValidationError("Hourly outflow limit exceeded")
        if wallet.daily_outflow_limit is not None and daily_current + amount > wallet.daily_outflow_limit:
            raise ValidationError("Daily outflow limit exceeded")

    if action == LEDGER_ACTION_DEPOSIT:
        if wallet.hourly_inflow_limit is not None and hourly_current + amount > wallet.hourly_inflow_limit:
            raise ValidationError("Hourly inflow limit exceeded")
        if wallet.daily_inflow_limit is not None and daily_current + amount > wallet.daily_inflow_limit:
            raise ValidationError("Daily inflow limit exceeded")

@transaction.atomic
def set_wallet_risk_status(*, actor, wallet: TokenWallet, risk_status: str, reason: str = "", review_required: bool = False) -> TokenWallet:
    _require_perm(actor, "ledger.can_manage_wallet_risk")

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    wallet.risk_status = risk_status
    wallet.risk_reason = reason
    wallet.review_required = review_required
    wallet.save(update_fields=["risk_status", "risk_reason", "review_required", "updated_at"])
    return wallet

@transaction.atomic
def set_wallet_velocity_limits(
    *,
    actor,
    wallet: TokenWallet,
    hourly_outflow_limit=None,
    daily_outflow_limit=None,
    hourly_inflow_limit=None,
    daily_inflow_limit=None,
) -> TokenWallet:
    _require_perm(actor, "ledger.can_manage_wallet_risk")

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    wallet.hourly_outflow_limit = hourly_outflow_limit
    wallet.daily_outflow_limit = daily_outflow_limit
    wallet.hourly_inflow_limit = hourly_inflow_limit
    wallet.daily_inflow_limit = daily_inflow_limit
    wallet.save(
        update_fields=[
            "hourly_outflow_limit",
            "daily_outflow_limit",
            "hourly_inflow_limit",
            "daily_inflow_limit",
            "updated_at",
        ]
    )
    return wallet

@transaction.atomic
def create_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    chain: str,
    asset_code: str,
    token_contract_address: str,
    deposit_address: str,
    address_derivation_ref: str,
    expires_at,
    required_confirmations: int = 1,
    min_amount: int = 1,
    metadata=None,
) -> DepositSession:
    actor = _require_authenticated_actor(actor)

    if metadata is None:
        metadata = {}

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)

    if wallet.wallet_type != TokenWallet.TYPE_USER:
        raise ValidationError("Deposit sessions can only target user wallets")

    if wallet.user_id != actor.id and not actor.has_perm("ledger.can_manage_deposit_sessions"):
        raise PermissionDenied("Cannot create a deposit session for another user's wallet")

    if expires_at <= timezone.now():
        raise ValidationError("Deposit session expiry must be in the future")

    chain = _normalize_chain(chain)
    token_contract_address = _normalize_evm_address(token_contract_address)
    deposit_address = _normalize_evm_address(deposit_address)

    if min_amount <= 0:
        raise ValidationError("Minimum amount must be positive")

    if required_confirmations < 1:
        raise ValidationError("Required confirmations must be at least 1")

    return DepositSession.objects.create(
        user=wallet.user,
        wallet=wallet,
        chain=chain,
        asset_code=(asset_code or "").strip().upper(),
        token_contract_address=token_contract_address,
        deposit_address=deposit_address,
        address_derivation_ref=(address_derivation_ref or "").strip(),
        status=DepositSession.STATUS_AWAITING_PAYMENT,
        min_amount=int(min_amount),
        required_confirmations=int(required_confirmations),
        expires_at=expires_at,
        created_by=actor,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )

@transaction.atomic
def record_onchain_observation(
    *,
    actor,
    deposit_session: DepositSession,
    chain: str,
    txid: str,
    log_index: int,
    block_number: int | None,
    from_address: str,
    to_address: str,
    token_contract_address: str,
    asset_code: str,
    amount: int,
    confirmations: int,
    raw_payload=None,
) -> ObservedOnchainTransfer:
    _require_perm(actor, "ledger.can_record_onchain_observations")

    if raw_payload is None:
        raw_payload = {}

    deposit_session = (
        DepositSession.objects.select_for_update()
        .select_related("wallet")
        .get(id=deposit_session.id)
    )

    chain = _normalize_chain(chain)
    txid = (txid or "").strip().lower()
    log_index = int(log_index)
    block_number = None if block_number is None else int(block_number)
    token_contract_address = _normalize_evm_address(token_contract_address)
    from_address = _normalize_evm_address(from_address)
    to_address = _normalize_evm_address(to_address)
    asset_code = (asset_code or "").strip().upper()
    amount = int(amount)
    confirmations = int(confirmations)

    if log_index < 0:
        raise ValidationError("Log index cannot be negative")

    if block_number is not None and block_number < 0:
        raise ValidationError("Block number cannot be negative")

    if amount <= 0:
        raise ValidationError("Observed amount must be positive")

    if confirmations < 0:
        raise ValidationError("Confirmations cannot be negative")

    if deposit_session.status in {
        DepositSession.STATUS_EXPIRED,
        DepositSession.STATUS_FAILED,
    }:
        raise ValidationError("Cannot record an observation for a closed deposit session")

    if chain != deposit_session.chain:
        raise ValidationError("Observed chain does not match deposit session chain")

    if asset_code != deposit_session.asset_code:
        raise ValidationError("Observed asset does not match deposit session asset")

    if token_contract_address != deposit_session.token_contract_address:
        raise ValidationError("Observed token contract does not match deposit session token contract")

    if to_address != deposit_session.deposit_address:
        raise ValidationError("Observed destination address does not match deposit session address")

    event_key = build_evm_event_key(
        chain=chain,
        txid=txid,
        log_index=log_index,
    )

    defaults = {
        "chain": chain,
        "txid": txid,
        "log_index": log_index,
        "block_number": block_number,
        "from_address": from_address,
        "to_address": to_address,
        "token_contract_address": token_contract_address,
        "asset_code": asset_code,
        "amount": amount,
        "confirmations": confirmations,
        "deposit_session": deposit_session,
        "raw_payload": raw_payload,
        "metadata_version": LEDGER_METADATA_VERSION,
    }

    observed, created = ObservedOnchainTransfer.objects.get_or_create(
        event_key=event_key,
        defaults=defaults,
    )

    if not created:
        immutable_mismatch = (
            observed.chain != chain
            or observed.txid != txid
            or observed.log_index != log_index
            or observed.to_address != to_address
            or observed.token_contract_address != token_contract_address
            or observed.asset_code != asset_code
            or int(observed.amount) != amount
        )
        if immutable_mismatch:
            raise ValidationError("On-chain event key reused with different payload")

        if observed.deposit_session_id != deposit_session.id:
            raise ValidationError("Observed event already linked to another deposit session")

        update_fields = []

        if block_number is not None and observed.block_number != block_number:
            observed.block_number = block_number
            update_fields.append("block_number")

        if confirmations > observed.confirmations:
            observed.confirmations = confirmations
            update_fields.append("confirmations")

        if raw_payload and observed.raw_payload != raw_payload:
            observed.raw_payload = raw_payload
            update_fields.append("raw_payload")

        if update_fields:
            update_fields.append("updated_at")
            observed.save(update_fields=update_fields)

    if deposit_session.status == DepositSession.STATUS_CREDITED:
        if observed.status != ObservedOnchainTransfer.STATUS_CREDITED:
            raise ValidationError(
                "Credited deposit session is linked to a non-credited observed transfer"
            )

    if observed.status == ObservedOnchainTransfer.STATUS_CREDITED:
        if deposit_session.status != DepositSession.STATUS_CREDITED:
            raise ValidationError(
                "Credited observed transfer is linked to a non-credited deposit session"
            )

        session_update_fields = []

        if deposit_session.observed_txid != observed.txid:
            deposit_session.observed_txid = observed.txid
            session_update_fields.append("observed_txid")

        if deposit_session.observed_amount != observed.amount:
            deposit_session.observed_amount = observed.amount
            session_update_fields.append("observed_amount")

        max_confirmations = max(deposit_session.confirmations, observed.confirmations)
        if deposit_session.confirmations != max_confirmations:
            deposit_session.confirmations = max_confirmations
            session_update_fields.append("confirmations")

        if session_update_fields:
            session_update_fields.append("updated_at")
            deposit_session.save(update_fields=session_update_fields)

        return observed

    if observed.confirmations >= deposit_session.required_confirmations:
        desired_observed_status = ObservedOnchainTransfer.STATUS_CONFIRMED
        desired_session_status = DepositSession.STATUS_CONFIRMING
        desired_confirmed_at = observed.confirmed_at or timezone.now()
    else:
        desired_observed_status = (
            ObservedOnchainTransfer.STATUS_CONFIRMING
            if observed.confirmations > 0
            else ObservedOnchainTransfer.STATUS_OBSERVED
        )
        desired_session_status = (
            DepositSession.STATUS_CONFIRMING
            if observed.confirmations > 0
            else DepositSession.STATUS_SEEN_ONCHAIN
        )
        desired_confirmed_at = observed.confirmed_at

    observed_update_fields = []

    if observed.status != desired_observed_status:
        observed.status = desired_observed_status
        observed_update_fields.append("status")

    if observed.confirmed_at != desired_confirmed_at:
        observed.confirmed_at = desired_confirmed_at
        observed_update_fields.append("confirmed_at")

    if observed_update_fields:
        observed_update_fields.append("updated_at")
        observed.save(update_fields=observed_update_fields)

    session_update_fields = []

    if deposit_session.observed_txid != observed.txid:
        deposit_session.observed_txid = observed.txid
        session_update_fields.append("observed_txid")

    if deposit_session.observed_amount != observed.amount:
        deposit_session.observed_amount = observed.amount
        session_update_fields.append("observed_amount")

    if deposit_session.confirmations != observed.confirmations:
        deposit_session.confirmations = observed.confirmations
        session_update_fields.append("confirmations")

    if deposit_session.status != DepositSession.STATUS_CREDITED:
        if deposit_session.status != desired_session_status:
            deposit_session.status = desired_session_status
            session_update_fields.append("status")

    if session_update_fields:
        session_update_fields.append("updated_at")
        deposit_session.save(update_fields=session_update_fields)

    return observed

@transaction.atomic
def credit_confirmed_deposit_session(
    *,
    actor,
    deposit_session: DepositSession,
    observed_transfer: ObservedOnchainTransfer,
    created_by=None,
) -> LedgerTransaction:
    _require_perm(actor, "ledger.can_credit_confirmed_deposits")
    created_by = _resolve_created_by(actor=actor, created_by=created_by)

    deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
    observed_transfer = ObservedOnchainTransfer.objects.select_for_update().get(id=observed_transfer.id)

    wallet = TokenWallet.objects.select_for_update().get(id=deposit_session.wallet_id)
    deposit_session.wallet = wallet

    if observed_transfer.credited_ledger_txn_id and deposit_session.status != DepositSession.STATUS_CREDITED:
        raise ValidationError("Observed transfer already linked to a credited ledger transaction")

    if observed_transfer.deposit_session_id != deposit_session.id:
        raise ValidationError("Observed transfer does not belong to this deposit session")

    if deposit_session.status == DepositSession.STATUS_CREDITED:
        if not deposit_session.credited_ledger_txn_id:
            raise ValidationError("Credited session missing linked ledger transaction")
        return deposit_session.credited_ledger_txn

    if deposit_session.expires_at <= timezone.now():
        raise ValidationError("Deposit session has expired")

    if observed_transfer.status not in {
        ObservedOnchainTransfer.STATUS_CONFIRMED,
        ObservedOnchainTransfer.STATUS_CREDITED,
    }:
        raise ValidationError("Observed transfer is not confirmed")

    if observed_transfer.chain != deposit_session.chain:
        raise ValidationError("Observed chain does not match deposit session chain")

    if observed_transfer.asset_code != deposit_session.asset_code:
        raise ValidationError("Observed asset does not match deposit session asset")

    if observed_transfer.token_contract_address != deposit_session.token_contract_address:
        raise ValidationError("Observed token contract does not match deposit session token contract")

    if observed_transfer.to_address != deposit_session.deposit_address:
        raise ValidationError("Observed destination address does not match deposit session address")

    if int(observed_transfer.amount) < int(deposit_session.min_amount):
        raise ValidationError("Observed amount is below deposit minimum")

    if int(observed_transfer.confirmations) < int(deposit_session.required_confirmations):
        raise ValidationError("Observed transfer does not have enough confirmations")

    clearing_wallet = get_external_asset_clearing_wallet()
    external_id = f"deposit-credit:{observed_transfer.event_key}"

    txn = apply_ledger_transaction(
        actor=actor,
        kind="deposit",
        entries=[
            (clearing_wallet, -int(observed_transfer.amount)),
            (deposit_session.wallet, int(observed_transfer.amount)),
        ],
        created_by=created_by,
        external_id=external_id,
        memo=f"Confirmed on-chain deposit {observed_transfer.txid}",
        metadata={
            "source": "onchain_deposit",
            "deposit_session_id": deposit_session.id,
            "deposit_session_public_id": str(deposit_session.public_id),
            "observed_transfer_id": observed_transfer.id,
            "event_key": observed_transfer.event_key,
            "chain": deposit_session.chain,
            "asset_code": deposit_session.asset_code,
            "token_contract_address": deposit_session.token_contract_address,
            "deposit_address": deposit_session.deposit_address,
            "txid": observed_transfer.txid,
            "log_index": observed_transfer.log_index,
            "block_number": observed_transfer.block_number,
            "confirmations": observed_transfer.confirmations,
            "amount": observed_transfer.amount,
        },
    )

    deposit_session.observed_txid = observed_transfer.txid
    deposit_session.observed_amount = observed_transfer.amount
    deposit_session.confirmations = observed_transfer.confirmations
    deposit_session.status = DepositSession.STATUS_CREDITED
    deposit_session.credited_ledger_txn = txn
    deposit_session.save(
        update_fields=[
            "observed_txid",
            "observed_amount",
            "confirmations",
            "status",
            "credited_ledger_txn",
            "updated_at",
        ]
    )

    observed_transfer.status = ObservedOnchainTransfer.STATUS_CREDITED
    observed_transfer.credited_ledger_txn = txn
    if observed_transfer.confirmed_at is None:
        observed_transfer.confirmed_at = timezone.now()
    observed_transfer.save(
        update_fields=[
            "status",
            "credited_ledger_txn",
            "confirmed_at",
            "updated_at",
        ]
    )

    return txn

def build_deposit_option_key(*, chain: str, asset_code: str, token_contract_address: str) -> str:
    normalized_chain = _normalize_chain(chain)
    normalized_asset = (asset_code or "").strip().upper()
    normalized_contract = _normalize_evm_address(token_contract_address)
    contract_part = normalized_contract or "native"
    return f"{normalized_chain}:{normalized_asset}:{contract_part}"


def parse_deposit_option_key(option_key: str) -> tuple[str, str, str]:
    raw_value = (option_key or "").strip()
    parts = raw_value.split(":", 2)
    if len(parts) != 3:
        raise ValidationError("Invalid deposit option")
    chain, asset_code, contract_part = parts
    token_contract_address = "" if contract_part == "native" else contract_part
    return _normalize_chain(chain), asset_code.strip().upper(), _normalize_evm_address(token_contract_address)

def list_available_deposit_options() -> list[dict]:
    rows = (
        DepositAddress.objects.filter(status=DepositAddress.STATUS_AVAILABLE)
        .values(
            "display_label",
            "chain",
            "asset_code",
            "token_contract_address",
            "required_confirmations",
            "min_amount",
            "session_ttl_seconds",
        )
        .order_by("display_label", "chain", "asset_code", "token_contract_address")
        .distinct()
    )

    options = []
    for row in rows:
        option_key = build_deposit_option_key(
            chain=row["chain"],
            asset_code=row["asset_code"],
            token_contract_address=row["token_contract_address"],
        )
        options.append(
            {
                "key": option_key,
                "label": row["display_label"],
                "chain": row["chain"],
                "asset_code": row["asset_code"],
                "token_contract_address": row["token_contract_address"],
                "required_confirmations": row["required_confirmations"],
                "min_amount": row["min_amount"],
                "session_ttl_seconds": row["session_ttl_seconds"],
            }
        )
    return options

@transaction.atomic
def open_user_deposit_session(*, actor, wallet: TokenWallet, option_key: str) -> DepositSession:
    actor = _require_authenticated_actor(actor)

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    if wallet.wallet_type != TokenWallet.TYPE_USER:
        raise ValidationError("Deposit sessions can only target user wallets")
    if wallet.user_id != actor.id:
        raise PermissionDenied("Cannot open a deposit session for another user's wallet")

    chain, asset_code, token_contract_address = parse_deposit_option_key(option_key)

    existing_session = (
        DepositSession.objects.select_for_update()
        .filter(
            wallet=wallet,
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract_address,
            status__in=ACTIVE_DEPOSIT_SESSION_STATUSES,
            expires_at__gt=timezone.now(),
        )
        .order_by("-created_at")
        .first()
    )
    if existing_session:
        return existing_session

    address_row = (
        DepositAddress.objects.select_for_update()
        .filter(
            status=DepositAddress.STATUS_AVAILABLE,
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract_address,
        )
        .order_by("id")
        .first()
    )
    if address_row is None:
        raise ValidationError("No deposit address is currently available for this asset")

    expires_at = timezone.now() + timedelta(seconds=int(address_row.session_ttl_seconds))

    session = create_deposit_session(
        actor=actor,
        wallet=wallet,
        chain=address_row.chain,
        asset_code=address_row.asset_code,
        token_contract_address=address_row.token_contract_address,
        deposit_address=address_row.address,
        address_derivation_ref=address_row.address_derivation_ref,
        expires_at=expires_at,
        required_confirmations=address_row.required_confirmations,
        min_amount=address_row.min_amount,
        metadata={
            "display_label": address_row.display_label,
            "address_pool_id": address_row.id,
            "allocation_source": "app_pool",
        },
    )

    address_row.status = DepositAddress.STATUS_ALLOCATED
    address_row.allocated_deposit_session = session
    address_row.save(update_fields=["status", "allocated_deposit_session", "updated_at"])

    return session

@transaction.atomic
def ingest_deposit_observation_event(
    *,
    actor,
    session_public_id,
    chain: str,
    txid: str,
    log_index: int,
    block_number: int | None,
    from_address: str,
    to_address: str,
    token_contract_address: str,
    asset_code: str,
    amount: int,
    confirmations: int,
    raw_payload=None,
):
    if raw_payload is None:
        raw_payload = {}

    deposit_session = (
        DepositSession.objects.select_for_update()
        .select_related("wallet")
        .get(public_id=session_public_id)
    )

    observed_transfer = record_onchain_observation(
        actor=actor,
        deposit_session=deposit_session,
        chain=chain,
        txid=txid,
        log_index=log_index,
        block_number=block_number,
        from_address=from_address,
        to_address=to_address,
        token_contract_address=token_contract_address,
        asset_code=asset_code,
        amount=amount,
        confirmations=confirmations,
        raw_payload=raw_payload,
    )

    deposit_session.refresh_from_db()
    observed_transfer.refresh_from_db()

    ledger_txn = None
    if observed_transfer.confirmations >= deposit_session.required_confirmations:
        ledger_txn = credit_confirmed_deposit_session(
            actor=actor,
            deposit_session=deposit_session,
            observed_transfer=observed_transfer,
        )
        deposit_session.refresh_from_db()
        observed_transfer.refresh_from_db()

    return {
        "deposit_session": deposit_session,
        "observed_transfer": observed_transfer,
        "ledger_txn": ledger_txn,
    }

@transaction.atomic
def provision_deposit_address(
    *,
    actor,
    chain: str,
    asset_code: str,
    token_contract_address: str,
    display_label: str,
    address: str,
    address_derivation_ref: str,
    derivation_index,
    required_confirmations: int,
    min_amount: int,
    session_ttl_seconds: int,
    metadata=None,
):
    _require_perm(actor, "ledger.can_manage_deposit_addresses")

    if metadata is None:
        metadata = {}

    chain = _normalize_chain(chain)
    asset_code = (asset_code or "").strip().upper()
    token_contract_address = _normalize_evm_address(token_contract_address)
    address = _normalize_evm_address(address)
    address_derivation_ref = (address_derivation_ref or "").strip()
    display_label = (display_label or "").strip()

    required_confirmations = int(required_confirmations)
    min_amount = int(min_amount)
    session_ttl_seconds = int(session_ttl_seconds)

    if derivation_index is None:
        derivation_index = _parse_derivation_index_from_ref(address_derivation_ref)
    else:
        derivation_index = int(derivation_index)

    if not chain:
        raise ValidationError("Chain is required")

    if not asset_code:
        raise ValidationError("Asset code is required")

    if not display_label:
        raise ValidationError("Display label is required")

    if not address:
        raise ValidationError("Address is required")

    if not address_derivation_ref:
        raise ValidationError("Address derivation reference is required")

    if derivation_index is None:
        raise ValidationError("Derivation index is required")

    if derivation_index < 0:
        raise ValidationError("Derivation index cannot be negative")

    if required_confirmations < 1:
        raise ValidationError("Required confirmations must be at least 1")

    if min_amount <= 0:
        raise ValidationError("Minimum amount must be positive")

    if session_ttl_seconds <= 0:
        raise ValidationError("Session TTL must be positive")

    existing_by_address = (
        DepositAddress.objects.select_for_update()
        .filter(address=address)
        .first()
    )
    existing_by_ref = (
        DepositAddress.objects.select_for_update()
        .filter(address_derivation_ref=address_derivation_ref)
        .first()
    )
    existing_by_index = (
        DepositAddress.objects.select_for_update()
        .filter(
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract_address,
            derivation_index=derivation_index,
        )
        .first()
    )

    candidate_ids = {
        obj.id
        for obj in (existing_by_address, existing_by_ref, existing_by_index)
        if obj is not None
    }

    if len(candidate_ids) > 1:
        raise ValidationError(
            "Address, derivation reference, and derivation index already belong to different rows"
        )

    existing = None
    if existing_by_address is not None:
        existing = existing_by_address
    elif existing_by_ref is not None:
        existing = existing_by_ref
    else:
        existing = existing_by_index

    if existing is not None:
        immutable_mismatch = (
            existing.chain != chain
            or existing.asset_code != asset_code
            or existing.token_contract_address != token_contract_address
            or existing.display_label != display_label
            or existing.address != address
            or existing.address_derivation_ref != address_derivation_ref
            or existing.derivation_index != derivation_index
            or int(existing.required_confirmations) != required_confirmations
            or int(existing.min_amount) != min_amount
            or int(existing.session_ttl_seconds) != session_ttl_seconds
        )
        if immutable_mismatch:
            raise ValidationError("Deposit address already exists with different immutable fields")

        if existing.status != DepositAddress.STATUS_AVAILABLE:
            raise ValidationError("Deposit address already exists in a non-available state")

        return existing, False

    created = DepositAddress.objects.create(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
        display_label=display_label,
        address=address,
        address_derivation_ref=address_derivation_ref,
        derivation_index=derivation_index,
        required_confirmations=required_confirmations,
        min_amount=min_amount,
        session_ttl_seconds=session_ttl_seconds,
        status=DepositAddress.STATUS_AVAILABLE,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )
    return created, True

@transaction.atomic
def provision_deposit_addresses_batch(*, actor, address_rows):
    _require_perm(actor, "ledger.can_manage_deposit_addresses")

    if not isinstance(address_rows, list) or not address_rows:
        raise ValidationError("Addresses payload must be a non-empty list")

    created_count = 0
    existing_count = 0
    rows = []

    for row in address_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each address row must be an object")

        address_obj, created = provision_deposit_address(
            actor=actor,
            chain=row.get("chain", ""),
            asset_code=row.get("asset_code", ""),
            token_contract_address=row.get("token_contract_address", ""),
            display_label=row.get("display_label", ""),
            address=row.get("address", ""),
            address_derivation_ref=row.get("address_derivation_ref", ""),
            required_confirmations=row.get("required_confirmations"),
            min_amount=row.get("min_amount"),
            session_ttl_seconds=row.get("session_ttl_seconds"),
            metadata=row.get("metadata") or {},
            derivation_index=row.get("derivation_index"),
        )

        if created:
            created_count += 1
        else:
            existing_count += 1

        rows.append(
            {
                "id": address_obj.id,
                "chain": address_obj.chain,
                "asset_code": address_obj.asset_code,
                "token_contract_address": address_obj.token_contract_address,
                "address": address_obj.address,
                "address_derivation_ref": address_obj.address_derivation_ref,
                "status": address_obj.status,
                "created": created,
                "derivation_index": address_obj.derivation_index,
            }
        )

    return {
        "created_count": created_count,
        "existing_count": existing_count,
        "rows": rows,
    }

def get_deposit_address_pool_stats(*, actor, option_rows):
    _require_perm(actor, "ledger.can_manage_deposit_addresses")

    if not isinstance(option_rows, list) or not option_rows:
        raise ValidationError("Options payload must be a non-empty list")

    results = []
    for row in option_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each option row must be an object")

        chain = _normalize_chain(row.get("chain", ""))
        asset_code = (row.get("asset_code", "") or "").strip().upper()
        token_contract_address = _normalize_evm_address(row.get("token_contract_address", ""))
        start_index = int(row.get("start_index", 0))
        if start_index < 0:
            raise ValidationError("start_index cannot be negative")
        if not chain or not asset_code:
            raise ValidationError("Each option requires chain and asset_code")

        qs = DepositAddress.objects.filter(
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract_address,
        )
        known_indexes = []

        for derivation_index, address_derivation_ref in qs.values_list(
            "derivation_index",
            "address_derivation_ref",
        ):
            if derivation_index is not None:
                known_indexes.append(int(derivation_index))
                continue

            parsed = _parse_derivation_index_from_ref(address_derivation_ref)
            if parsed is not None:
                known_indexes.append(parsed)

        max_derivation_index = max(known_indexes) if known_indexes else None
        next_derivation_index = (
            max(start_index, max_derivation_index + 1)
            if max_derivation_index is not None
            else start_index
        )
        results.append(
            {
                "chain": chain,
                "asset_code": asset_code,
                "token_contract_address": token_contract_address,
                "available_count": qs.filter(status=DepositAddress.STATUS_AVAILABLE).count(),
                "allocated_count": qs.filter(status=DepositAddress.STATUS_ALLOCATED).count(),
                "retired_count": qs.filter(status=DepositAddress.STATUS_RETIRED).count(),
                "max_derivation_index": max_derivation_index,
                "next_derivation_index": next_derivation_index,
            }
        )

    return results

@transaction.atomic
def list_active_deposit_watch_targets(*, actor, option_rows):
    _require_perm(actor, "ledger.can_view_deposit_sessions")

    if not isinstance(option_rows, list) or not option_rows:
        raise ValidationError("Options payload must be a non-empty list")

    results = []

    for row in option_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each option row must be an object")

        chain = _normalize_chain(row.get("chain", ""))
        asset_code = (row.get("asset_code", "") or "").strip().upper()
        token_contract_address = _normalize_evm_address(row.get("token_contract_address", ""))

        if not chain or not asset_code:
            raise ValidationError("Each option requires chain and asset_code")

        sessions = (
            DepositSession.objects.filter(
                chain=chain,
                asset_code=asset_code,
                token_contract_address=token_contract_address,
                status__in=ACTIVE_DEPOSIT_WATCH_STATUSES,
            )
            .order_by("id")
            .values(
                "public_id",
                "deposit_address",
                "required_confirmations",
                "min_amount",
                "status",
                "expires_at",
            )
        )

        targets = []
        for session in sessions:
            targets.append(
                {
                    "session_public_id": str(session["public_id"]),
                    "deposit_address": session["deposit_address"],
                    "required_confirmations": session["required_confirmations"],
                    "min_amount": session["min_amount"],
                    "status": session["status"],
                    "expires_at": session["expires_at"].isoformat(),
                }
            )

        results.append(
            {
                "chain": chain,
                "asset_code": asset_code,
                "token_contract_address": token_contract_address,
                "targets": targets,
            }
        )

    return results