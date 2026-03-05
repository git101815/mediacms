from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from .models import LedgerEntry, LedgerTransaction, TokenWallet
import hashlib
import json

@transaction.atomic
def apply_ledger_transaction(*, kind: str, entries: list, created_by=None, external_id=None, memo="", metadata=None):
    """
    entries: list[tuple[TokenWallet, int]] signed delta.
    """
    if metadata is None:
        metadata = {}

    if not entries:
        raise ValidationError("No entries")

    # Idempotency fingerprint (stable)
    normalized_entries = []
    for (wallet, delta) in entries:
        if wallet.id is None:
            raise ValidationError("Unsaved wallet in entries")
        normalized_entries.append([int(wallet.id), int(delta)])
    normalized_entries.sort(key=lambda x: (x[0], x[1]))

    payload = {
        "kind": kind,
        "memo": memo,
        "entries": normalized_entries,
        "metadata": metadata,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    request_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    # Idempotence (exactly-once)
    if external_id:
        try:
            txn = LedgerTransaction.objects.create(
                kind=kind,
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
    else:
        txn = LedgerTransaction.objects.create(
            kind=kind,
            external_id=None,
            created_by=created_by,
            memo=memo,
            request_hash=None,
            metadata=metadata,
        )
    # Lock wallets (stable order, unique request)
    wallet_ids = sorted({wallet.id for (wallet, _) in entries})
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

    for (wallet, delta) in entries:
        w = locked[wallet.id]
        delta = int(delta)
        new_balance = w.balance + delta
        if new_balance < 0:
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