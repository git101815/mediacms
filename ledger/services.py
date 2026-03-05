from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from .models import LedgerEntry, LedgerTransaction, TokenWallet
@transaction.atomic
def apply_ledger_transaction(*, kind: str, entries: list, created_by=None, external_id=None, memo="", metadata=None):
    """
    entries: list[tuple[TokenWallet, int]] signed delta.
    """
    if metadata is None:
        metadata = {}

    if not entries:
        raise ValidationError("No entries")

    # Idempotence (exactly-once)
    if external_id:
        try:
            txn = LedgerTransaction.objects.create(
                kind=kind,
                external_id=external_id,
                created_by=created_by,
                memo=memo,
                metadata=metadata,
            )
        except IntegrityError:
            return LedgerTransaction.objects.get(external_id=external_id)
    else:
        txn = LedgerTransaction.objects.create(
            kind=kind,
            external_id=None,
            created_by=created_by,
            memo=memo,
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