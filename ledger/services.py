from django.core.exceptions import ValidationError
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
)
import hashlib
import json
from django.utils import timezone

def _create_outbox_event(*, txn: LedgerTransaction, topic: str, payload: dict, metadata_version: int) -> LedgerOutbox:
    return LedgerOutbox.objects.create(
        txn=txn,
        topic=topic,
        aggregate_type="ledger_transaction",
        aggregate_id=txn.id,
        payload=payload,
        metadata_version=metadata_version,
    )

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

@transaction.atomic
def create_pending_ledger_transaction(*, kind: str, created_by=None, external_id=None, memo="", metadata=None):
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
def apply_ledger_transaction(*, kind: str, entries: list, created_by=None, external_id=None, memo="", metadata=None):
    """
    entries: list[tuple[TokenWallet, int]] signed delta.
    """
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

    for wallet_id, delta in normalized_entries:
        w = locked[wallet_id]
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
def reverse_ledger_transaction(*, original_txn: LedgerTransaction, created_by=None, external_id=None, memo="", metadata=None):
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
def mark_outbox_event_dispatched(event: LedgerOutbox) -> LedgerOutbox:
    event.status = LedgerOutbox.STATUS_DISPATCHED
    event.dispatched_at = timezone.now()
    event.save(update_fields=["status", "dispatched_at"])
    return event


def mark_outbox_event_failed(event: LedgerOutbox, error_message: str) -> LedgerOutbox:
    event.fail_count += 1
    event.last_error = error_message[:2000]

    if event.fail_count >= LEDGER_OUTBOX_MAX_RETRIES:
        event.status = LedgerOutbox.STATUS_DEAD_LETTERED
        event.dead_lettered_at = timezone.now()
        event.dead_letter_reason = error_message[:2000]
        event.save(
            update_fields=[
                "status",
                "fail_count",
                "last_error",
                "dead_lettered_at",
                "dead_letter_reason",
            ]
        )
        return event

    event.status = LedgerOutbox.STATUS_FAILED
    event.save(update_fields=["status", "fail_count", "last_error"])
    return event

def move_outbox_event_to_dlq(event: LedgerOutbox, reason: str) -> LedgerOutbox:
    event.status = LedgerOutbox.STATUS_DEAD_LETTERED
    event.dead_lettered_at = timezone.now()
    event.dead_letter_reason = reason[:2000]
    event.last_error = reason[:2000]
    event.save(
        update_fields=[
            "status",
            "dead_lettered_at",
            "dead_letter_reason",
            "last_error",
        ]
    )
    return event

def get_dispatchable_outbox_events(limit: int = 100):
    return LedgerOutbox.objects.filter(
        status__in=[
            LedgerOutbox.STATUS_PENDING,
            LedgerOutbox.STATUS_FAILED,
        ]
    ).order_by("created_at")[:limit]

@transaction.atomic
def create_ledger_saga(*, saga_type: str, created_by=None, external_id=None, metadata=None) -> LedgerSaga:
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
def add_saga_step(*, saga: LedgerSaga, step_key: str, step_order: int, payload=None) -> LedgerSagaStep:
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
def start_ledger_saga(saga: LedgerSaga) -> LedgerSaga:
    if saga.status != LedgerSaga.STATUS_PENDING:
        return saga

    saga.status = LedgerSaga.STATUS_RUNNING
    saga.started_at = timezone.now()
    saga.save(update_fields=["status", "started_at"])
    return saga

@transaction.atomic
def start_saga_step(step: LedgerSagaStep) -> LedgerSagaStep:
    if step.status != LedgerSagaStep.STATUS_PENDING:
        return step

    step.status = LedgerSagaStep.STATUS_RUNNING
    step.started_at = timezone.now()
    step.save(update_fields=["status", "started_at"])
    return step

@transaction.atomic
def complete_saga_step(step: LedgerSagaStep, *, txn: LedgerTransaction = None) -> LedgerSagaStep:
    step.status = LedgerSagaStep.STATUS_COMPLETED
    step.completed_at = timezone.now()
    if txn is not None:
        step.txn = txn
        step.save(update_fields=["status", "completed_at", "txn"])
    else:
        step.save(update_fields=["status", "completed_at"])
    return step

@transaction.atomic
def fail_saga_step(step: LedgerSagaStep, *, error_message: str) -> LedgerSagaStep:
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
def complete_ledger_saga(saga: LedgerSaga) -> LedgerSaga:
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
def compensate_ledger_saga(*, saga: LedgerSaga, created_by=None, reason: str = "") -> LedgerSaga:
    if saga.status not in [LedgerSaga.STATUS_FAILED, LedgerSaga.STATUS_COMPENSATING]:
        raise ValidationError("Only failed or compensating sagas can be compensated")

    saga.status = LedgerSaga.STATUS_COMPENSATING
    saga.save(update_fields=["status"])

    steps = list(
        saga.steps.select_related("txn").order_by("-step_order", "-id")
    )

    for step in steps:
        if step.status != LedgerSagaStep.STATUS_COMPLETED:
            continue

        if step.txn and step.txn.status == LedgerTransaction.STATUS_POSTED:
            compensation_txn = reverse_ledger_transaction(
                original_txn=step.txn,
                created_by=created_by,
                external_id=f"saga-comp-{saga.id}-{step.id}",
                memo=reason or f"Compensation for saga {saga.id} step {step.step_key}",
            )
            step.compensation_txn = compensation_txn

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