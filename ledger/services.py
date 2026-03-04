from django.core.exceptions import ValidationError
from django.db import transaction

from .models import TokenWallet, LedgerTransaction, LedgerEntry

@transaction.atomic
def apply_ledger_transaction(*, kind: str, entries: list, created_by=None, external_id=None, memo="", metadata=None):
    """
    entries: list[tuple[TokenWallet, int]]  delta signé.
    """
    if metadata is None:
        metadata = {}

    # Idempotence (si external_id est utilisé)
    if external_id:
        existing = LedgerTransaction.objects.filter(external_id=external_id).first()
        if existing:
            return existing

    txn = LedgerTransaction.objects.create(
        kind=kind,
        external_id=external_id,
        created_by=created_by,
        memo=memo,
        metadata=metadata,
    )

    # Lock wallets (ordre stable)
    wallet_ids = sorted({w.id for (w, _) in entries})
    locked = {
        w.id: TokenWallet.objects.select_for_update().get(id=w.id)
        for w in TokenWallet.objects.filter(id__in=wallet_ids)
    }

    for (wallet, delta) in entries:
        w = locked[wallet.id]
        new_balance = w.balance + int(delta)
        if new_balance < 0:
            raise ValidationError("Insufficient funds")
        w.balance = new_balance
        w.save(update_fields=["balance", "updated_at"])

        LedgerEntry.objects.create(
            txn=txn,
            wallet=w,
            delta=int(delta),
            balance_after=new_balance,
        )

    return txn