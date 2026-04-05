from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
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
)
import uuid
import hashlib
import json
from django.utils import timezone
from datetime import timedelta
from django.db.models import Q

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