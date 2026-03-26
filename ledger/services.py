from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from .models import LedgerEntry, LedgerTransaction, TokenWallet
import hashlib
import json


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
                return LedgerTransaction.objects.create(
                    kind=kind,
                    status=LedgerTransaction.STATUS_PENDING,
                    external_id=external_id,
                    request_hash=request_hash,
                    created_by=created_by,
                    memo=memo,
                    metadata=metadata,
                )
        except IntegrityError:
            existing = LedgerTransaction.objects.get(external_id=external_id)
            if existing.request_hash and existing.request_hash != request_hash:
                raise ValidationError("Idempotency key reused with different payload")
            return existing

    return LedgerTransaction.objects.create(
        kind=kind,
        status=LedgerTransaction.STATUS_PENDING,
        external_id=None,
        created_by=created_by,
        memo=memo,
        request_hash=None,
        metadata=metadata,
    )

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

    return txn