from django.conf import settings
from django.db import models
from django.utils import timezone

class TokenWallet(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="token_wallet",
        db_index=True,
    )
    balance = models.BigIntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                check=models.Q(balance__gte=0),
                name="tokenwallet_balance_non_negative",
            ),
        ]

    def __str__(self):
        return f"{self.user.username} ({self.balance})"


class LedgerTransaction(models.Model):
    # Exemples: mint, burn, transfer, purchase, refund, adjustment
    kind = models.CharField(max_length=32, db_index=True)
    external_id = models.CharField(max_length=64, null=True, blank=True, unique=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="token_transactions_created",
    )
    memo = models.TextField(blank=True, default="")
    metadata = models.JSONField(blank=True, default=dict)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    def __str__(self):
        return f"{self.kind} #{self.id}"


class LedgerEntry(models.Model):
    txn = models.ForeignKey(LedgerTransaction, on_delete=models.CASCADE, related_name="entries")
    wallet = models.ForeignKey(TokenWallet, on_delete=models.CASCADE, related_name="entries", db_index=True)
    delta = models.BigIntegerField()
    balance_after = models.BigIntegerField()
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["wallet", "-created_at"]),
            models.Index(fields=["txn"]),
        ]
        constraints = [
            models.CheckConstraint(check=~models.Q(delta=0), name="ledgerentry_delta_non_zero"),
        ]