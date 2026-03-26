from __future__ import annotations
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone

USER_WALLET_TYPE = "user"
SYSTEM_WALLET_TYPE = "system"
SYSTEM_WALLET_ISSUANCE = "issuance"
SYSTEM_WALLET_PLATFORM_FEES = "platform_fees"
LEDGER_TXN_STATUS_PENDING = "pending"
LEDGER_TXN_STATUS_POSTED = "posted"
LEDGER_TXN_STATUS_REVERSED = "reversed"
LEDGER_TXN_STATUS_CHOICES = (
    (LEDGER_TXN_STATUS_PENDING, "Pending"),
    (LEDGER_TXN_STATUS_POSTED, "Posted"),
    (LEDGER_TXN_STATUS_REVERSED, "Reversed"),
)
LEDGER_OUTBOX_STATUS_PENDING = "pending"
LEDGER_OUTBOX_STATUS_DISPATCHED = "dispatched"
LEDGER_OUTBOX_STATUS_FAILED = "failed"

LEDGER_OUTBOX_STATUS_CHOICES = (
    (LEDGER_OUTBOX_STATUS_PENDING, "Pending"),
    (LEDGER_OUTBOX_STATUS_DISPATCHED, "Dispatched"),
    (LEDGER_OUTBOX_STATUS_FAILED, "Failed"),
)
class ImmutableLedgerRow(models.Model):
    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if self.pk:
            raise ValidationError("Ledger rows are immutable (use reversal/compensation).")
        return super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        raise ValidationError("Ledger rows are immutable (no delete).")

class LedgerImmutableQuerySet(models.QuerySet):
    def update(self, **kwargs):
        raise ValidationError("Ledger rows are immutable (no update).")

    def delete(self):
        raise ValidationError("Ledger rows are immutable (no delete).")

class LedgerImmutableManager(models.Manager.from_queryset(LedgerImmutableQuerySet)):
    def bulk_update(self, objs, fields, batch_size=None):
        raise ValidationError("Ledger rows are immutable (no bulk_update).")

class TokenWallet(models.Model):
    TYPE_USER = USER_WALLET_TYPE
    TYPE_SYSTEM = SYSTEM_WALLET_TYPE
    TYPE_CHOICES = (
        (TYPE_USER, "User"),
        (TYPE_SYSTEM, "System"),
    )

    SYSTEM_ISSUANCE = SYSTEM_WALLET_ISSUANCE
    SYSTEM_PLATFORM_FEES = SYSTEM_WALLET_PLATFORM_FEES
    SYSTEM_CHOICES = (
        (SYSTEM_ISSUANCE, "Issuance"),
        (SYSTEM_PLATFORM_FEES, "Platform fees"),
    )

    wallet_type = models.CharField(max_length=16, choices=TYPE_CHOICES, default=TYPE_USER, db_index=True)

    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="token_wallet",
        null=True,
        blank=True,
        db_index=True,
    )

    system_key = models.CharField(max_length=32, choices=SYSTEM_CHOICES, null=True, blank=True, unique=True)

    balance = models.BigIntegerField(default=0)
    allow_negative = models.BooleanField(default=False)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=(
                    (
                        models.Q(wallet_type=USER_WALLET_TYPE)
                        & models.Q(user__isnull=False)
                        & models.Q(system_key__isnull=True)
                    )
                    | (
                        models.Q(wallet_type=SYSTEM_WALLET_TYPE)
                        & models.Q(user__isnull=True)
                        & models.Q(system_key__isnull=False)
                    )
                ),
                name="tokenwallet_valid_owner_shape",
            ),
            models.CheckConstraint(
                condition=(models.Q(allow_negative=True) | models.Q(balance__gte=0)),
                name="tokenwallet_balance_non_negative_unless_allowed",
            ),
        ]

    def __str__(self):
        if self.wallet_type == self.TYPE_SYSTEM:
            return f"[system:{self.system_key}] ({self.balance})"
        return f"{self.user.username} ({self.balance})"


class LedgerTransaction(models.Model):
    objects = LedgerImmutableManager()

    STATUS_PENDING = LEDGER_TXN_STATUS_PENDING
    STATUS_POSTED = LEDGER_TXN_STATUS_POSTED
    STATUS_REVERSED = LEDGER_TXN_STATUS_REVERSED
    STATUS_CHOICES = LEDGER_TXN_STATUS_CHOICES

    kind = models.CharField(max_length=32, db_index=True)
    external_id = models.CharField(max_length=64, null=True, blank=True, unique=True)
    request_hash = models.CharField(max_length=64, null=True, blank=True, db_index=True)
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

    status = models.CharField(
        max_length=16,
        choices=LEDGER_TXN_STATUS_CHOICES,
        default=LEDGER_TXN_STATUS_POSTED,
        db_index=True,
    )

    reversal_of = models.OneToOneField(
        "self",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="reversal_txn",
    )

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=(models.Q(external_id__isnull=True) | models.Q(request_hash__isnull=False)),
                name="ledgertransaction_request_hash_if_external_id",
            ),
            models.CheckConstraint(
                condition=(
                    models.Q(status=LEDGER_TXN_STATUS_PENDING, reversal_of__isnull=True)
                    | models.Q(status=LEDGER_TXN_STATUS_POSTED, reversal_of__isnull=True)
                    | models.Q(status=LEDGER_TXN_STATUS_REVERSED, reversal_of__isnull=False)
                ),
                name="ledgertransaction_reversal_requires_reversal_of",
            ),
        ]

    def __str__(self):
        return f"{self.kind}/{self.status} #{self.id}"

class LedgerEntry(ImmutableLedgerRow):
    objects = LedgerImmutableManager()
    txn = models.ForeignKey(
        LedgerTransaction,
        on_delete=models.PROTECT,
        related_name="entries",
    )
    wallet = models.ForeignKey(
        TokenWallet,
        on_delete=models.PROTECT,
        related_name="entries",
        db_index=True,
    )
    delta = models.BigIntegerField()
    balance_after = models.BigIntegerField()
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["wallet", "-created_at"]),
            models.Index(fields=["txn"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=~models.Q(delta=0),
                name="ledgerentry_delta_non_zero",
            ),
        ]
    def __str__(self):
        return f"Entry #{self.id} txn={self.txn_id} wallet={self.wallet_id} delta={self.delta}"

class LedgerOutbox(models.Model):
    STATUS_PENDING = LEDGER_OUTBOX_STATUS_PENDING
    STATUS_DISPATCHED = LEDGER_OUTBOX_STATUS_DISPATCHED
    STATUS_FAILED = LEDGER_OUTBOX_STATUS_FAILED
    STATUS_CHOICES = LEDGER_OUTBOX_STATUS_CHOICES

    txn = models.ForeignKey(
        LedgerTransaction,
        on_delete=models.PROTECT,
        related_name="outbox_events",
        db_index=True,
    )
    topic = models.CharField(max_length=64, db_index=True)
    aggregate_type = models.CharField(max_length=32, default="ledger_transaction", db_index=True)
    aggregate_id = models.BigIntegerField(db_index=True)
    status = models.CharField(
        max_length=16,
        choices=LEDGER_OUTBOX_STATUS_CHOICES,
        default=LEDGER_OUTBOX_STATUS_PENDING,
        db_index=True,
    )
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    dispatched_at = models.DateTimeField(null=True, blank=True, db_index=True)
    fail_count = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["topic", "status", "created_at"]),
            models.Index(fields=["aggregate_type", "aggregate_id"]),
        ]

    def __str__(self):
        return f"Outbox #{self.id} {self.topic} {self.status} txn={self.txn_id}"