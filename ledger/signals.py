from django.contrib.auth import get_user_model
from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db.utils import ProgrammingError, OperationalError

from .models import LEDGER_ACTION_PURCHASE, LedgerTransaction, TokenWallet

User = get_user_model()

@receiver(post_save, sender=User)
def ensure_token_wallet(sender, instance, created, **kwargs):
    try:
        TokenWallet.objects.get_or_create(
            user=instance,
            defaults={
                "wallet_type": TokenWallet.TYPE_USER,
                "allow_negative": False,
            },
        )
    except (ProgrammingError, OperationalError):
        return


@receiver(post_save, sender=LedgerTransaction)
def queue_referral_reward_for_purchase(sender, instance, created, **kwargs):
    if not created:
        return
    if (
        instance.kind != LEDGER_ACTION_PURCHASE
        or instance.status != LedgerTransaction.STATUS_POSTED
    ):
        return

    purchase_txn_id = instance.pk

    def process_after_commit():
        from ledger.dashboard.referrals import (
            safely_award_referral_for_purchase,
        )

        safely_award_referral_for_purchase(
            purchase_txn_id=purchase_txn_id,
        )

    transaction.on_commit(process_after_commit)
