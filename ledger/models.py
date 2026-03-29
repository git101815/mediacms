from __future__ import annotations
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from datetime import timedelta

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
LEDGER_OUTBOX_STATUS_DEAD_LETTERED = "dead_lettered"
LEDGER_OUTBOX_RETRY_DELAY_SECONDS = 300

LEDGER_OUTBOX_STATUS_CHOICES = (
    (LEDGER_OUTBOX_STATUS_PENDING, "Pending"),
    (LEDGER_OUTBOX_STATUS_DISPATCHED, "Dispatched"),
    (LEDGER_OUTBOX_STATUS_FAILED, "Failed"),
    (LEDGER_OUTBOX_STATUS_DEAD_LETTERED, "Dead lettered"),
)
LEDGER_METADATA_VERSION = 1
LEDGER_OUTBOX_MAX_RETRIES = 5

LEDGER_SAGA_STATUS_PENDING = "pending"
LEDGER_SAGA_STATUS_RUNNING = "running"
LEDGER_SAGA_STATUS_COMPLETED = "completed"
LEDGER_SAGA_STATUS_FAILED = "failed"
LEDGER_SAGA_STATUS_COMPENSATING = "compensating"
LEDGER_SAGA_STATUS_COMPENSATED = "compensated"

LEDGER_SAGA_STATUS_CHOICES = (
    (LEDGER_SAGA_STATUS_PENDING, "Pending"),
    (LEDGER_SAGA_STATUS_RUNNING, "Running"),
    (LEDGER_SAGA_STATUS_COMPLETED, "Completed"),
    (LEDGER_SAGA_STATUS_FAILED, "Failed"),
    (LEDGER_SAGA_STATUS_COMPENSATING, "Compensating"),
    (LEDGER_SAGA_STATUS_COMPENSATED, "Compensated"),
)

LEDGER_SAGA_STEP_STATUS_PENDING = "pending"
LEDGER_SAGA_STEP_STATUS_RUNNING = "running"
LEDGER_SAGA_STEP_STATUS_COMPLETED = "completed"
LEDGER_SAGA_STEP_STATUS_FAILED = "failed"
LEDGER_SAGA_STEP_STATUS_COMPENSATED = "compensated"
LEDGER_SAGA_STEP_STATUS_SKIPPED = "skipped"

LEDGER_SAGA_STEP_STATUS_CHOICES = (
    (LEDGER_SAGA_STEP_STATUS_PENDING, "Pending"),
    (LEDGER_SAGA_STEP_STATUS_RUNNING, "Running"),
    (LEDGER_SAGA_STEP_STATUS_COMPLETED, "Completed"),
    (LEDGER_SAGA_STEP_STATUS_FAILED, "Failed"),
    (LEDGER_SAGA_STEP_STATUS_COMPENSATED, "Compensated"),
    (LEDGER_SAGA_STEP_STATUS_SKIPPED, "Skipped"),
)

LEDGER_RISK_STATUS_CLEAR = "clear"
LEDGER_RISK_STATUS_REVIEW = "review"
LEDGER_RISK_STATUS_BLOCKED = "blocked"

LEDGER_RISK_STATUS_CHOICES = (
    (LEDGER_RISK_STATUS_CLEAR, "Clear"),
    (LEDGER_RISK_STATUS_REVIEW, "Review"),
    (LEDGER_RISK_STATUS_BLOCKED, "Blocked"),
)

LEDGER_ACTION_DEPOSIT = "deposit"
LEDGER_ACTION_WITHDRAWAL = "withdrawal"
LEDGER_ACTION_TRANSFER = "transfer"
LEDGER_ACTION_PURCHASE = "purchase"

LEDGER_ACTION_CHOICES = (
    (LEDGER_ACTION_DEPOSIT, "Deposit"),
    (LEDGER_ACTION_WITHDRAWAL, "Withdrawal"),
    (LEDGER_ACTION_TRANSFER, "Transfer"),
    (LEDGER_ACTION_PURCHASE, "Purchase"),
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

    held_balance = models.BigIntegerField(default=0)
    risk_status = models.CharField(
        max_length=16,
        choices=LEDGER_RISK_STATUS_CHOICES,
        default=LEDGER_RISK_STATUS_CLEAR,
        db_index=True,
    )
    risk_reason = models.TextField(blank=True, default="")
    review_required = models.BooleanField(default=False, db_index=True)
    daily_outflow_limit = models.BigIntegerField(null=True, blank=True)
    hourly_outflow_limit = models.BigIntegerField(null=True, blank=True)
    daily_inflow_limit = models.BigIntegerField(null=True, blank=True)
    hourly_inflow_limit = models.BigIntegerField(null=True, blank=True)

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
            models.CheckConstraint(
                condition=models.Q(held_balance__gte=0),
                name="tokenwallet_held_balance_gte_0",
            ),
            models.CheckConstraint(
                condition=models.Q(balance__gte=models.F("held_balance")) | models.Q(allow_negative=True),
                name="tokenwallet_balance_gte_held_balance_if_not_negative",
            ),
        ]

    permissions = [
        ("can_manage_wallet_risk", "Can manage wallet risk flags and limits"),
        ("can_manage_wallet_holds", "Can manage wallet holds"),
        ("can_view_wallet_risk", "Can view wallet risk state"),
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
    metadata_version = models.PositiveSmallIntegerField(
        default=LEDGER_METADATA_VERSION,
    )
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
        permissions = [
            ("can_apply_raw_ledger_transaction", "Can apply raw ledger transactions"),
            ("can_create_pending_ledger_transaction", "Can create pending ledger transactions"),
            ("can_reverse_ledger_transaction", "Can reverse ledger transactions"),
            ("can_impersonate_ledger_creator", "Can set created_by to another user"),
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
        permissions = [
            ("can_view_ledger_entries", "Can view ledger entries"),
        ]
    def __str__(self):
        return f"Entry #{self.id} txn={self.txn_id} wallet={self.wallet_id} delta={self.delta}"

class LedgerOutbox(models.Model):
    STATUS_PENDING = LEDGER_OUTBOX_STATUS_PENDING
    STATUS_DISPATCHED = LEDGER_OUTBOX_STATUS_DISPATCHED
    STATUS_FAILED = LEDGER_OUTBOX_STATUS_FAILED
    STATUS_CHOICES = LEDGER_OUTBOX_STATUS_CHOICES
    STATUS_DEAD_LETTERED = LEDGER_OUTBOX_STATUS_DEAD_LETTERED

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
    metadata_version = models.PositiveSmallIntegerField(
        default=LEDGER_METADATA_VERSION,
    )
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    dispatched_at = models.DateTimeField(null=True, blank=True, db_index=True)
    fail_count = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")
    dead_lettered_at = models.DateTimeField(null=True, blank=True, db_index=True)
    dead_letter_reason = models.TextField(blank=True, default="")
    last_attempt_at = models.DateTimeField(null=True, blank=True, db_index=True)
    next_retry_at = models.DateTimeField(null=True, blank=True, db_index=True)
    redrive_count = models.PositiveIntegerField(default=0)
    last_redriven_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "created_at"]),
            models.Index(fields=["topic", "status", "created_at"]),
            models.Index(fields=["aggregate_type", "aggregate_id"]),
            models.Index(fields=["status", "next_retry_at"]),
            models.Index(fields=["status", "dead_lettered_at"]),
            models.Index(fields=["status", "last_attempt_at"]),
        ]
        permissions = [
            ("can_manage_ledger_outbox", "Can manage ledger outbox"),
        ]

    def __str__(self):
        return f"Outbox #{self.id} {self.topic} {self.status} txn={self.txn_id}"

class LedgerSaga(models.Model):
    STATUS_PENDING = LEDGER_SAGA_STATUS_PENDING
    STATUS_RUNNING = LEDGER_SAGA_STATUS_RUNNING
    STATUS_COMPLETED = LEDGER_SAGA_STATUS_COMPLETED
    STATUS_FAILED = LEDGER_SAGA_STATUS_FAILED
    STATUS_COMPENSATING = LEDGER_SAGA_STATUS_COMPENSATING
    STATUS_COMPENSATED = LEDGER_SAGA_STATUS_COMPENSATED
    STATUS_CHOICES = LEDGER_SAGA_STATUS_CHOICES

    saga_type = models.CharField(max_length=64, db_index=True)
    external_id = models.CharField(max_length=64, null=True, blank=True, unique=True)
    status = models.CharField(
        max_length=16,
        choices=LEDGER_SAGA_STATUS_CHOICES,
        default=LEDGER_SAGA_STATUS_PENDING,
        db_index=True,
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ledger_sagas_created",
    )
    metadata = models.JSONField(blank=True, default=dict)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)
    started_at = models.DateTimeField(null=True, blank=True, db_index=True)
    completed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    failed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    compensated_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        permissions = [
            ("can_manage_ledger_sagas", "Can manage ledger sagas"),
            ("can_compensate_ledger_sagas", "Can compensate ledger sagas"),
            ("can_view_ledger_sagas", "Can view ledger sagas"),
        ]

    def __str__(self):
        return f"Saga #{self.id} {self.saga_type} {self.status}"

class LedgerSagaStep(models.Model):
    STATUS_PENDING = LEDGER_SAGA_STEP_STATUS_PENDING
    STATUS_RUNNING = LEDGER_SAGA_STEP_STATUS_RUNNING
    STATUS_COMPLETED = LEDGER_SAGA_STEP_STATUS_COMPLETED
    STATUS_FAILED = LEDGER_SAGA_STEP_STATUS_FAILED
    STATUS_COMPENSATED = LEDGER_SAGA_STEP_STATUS_COMPENSATED
    STATUS_SKIPPED = LEDGER_SAGA_STEP_STATUS_SKIPPED
    STATUS_CHOICES = LEDGER_SAGA_STEP_STATUS_CHOICES

    saga = models.ForeignKey(
        LedgerSaga,
        on_delete=models.CASCADE,
        related_name="steps",
    )
    step_key = models.CharField(max_length=64)
    step_order = models.PositiveIntegerField()
    status = models.CharField(
        max_length=16,
        choices=LEDGER_SAGA_STEP_STATUS_CHOICES,
        default=LEDGER_SAGA_STEP_STATUS_PENDING,
        db_index=True,
    )
    txn = models.ForeignKey(
        LedgerTransaction,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="saga_steps",
    )
    compensation_txn = models.ForeignKey(
        LedgerTransaction,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="compensated_saga_steps",
    )
    payload = models.JSONField(blank=True, default=dict)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)
    started_at = models.DateTimeField(null=True, blank=True, db_index=True)
    completed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    failed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    compensated_at = models.DateTimeField(null=True, blank=True, db_index=True)
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["saga", "step_key"],
                name="ledgersagastep_unique_step_key_per_saga",
            ),
            models.UniqueConstraint(
                fields=["saga", "step_order"],
                name="ledgersagastep_unique_step_order_per_saga",
            ),
        ]
        indexes = [
            models.Index(fields=["saga", "step_order"]),
            models.Index(fields=["saga", "status"]),
        ]
        permissions = [
            ("can_view_ledger_saga_steps", "Can view ledger saga steps"),
        ]

    def __str__(self):
        return f"SagaStep #{self.id} saga={self.saga_id} {self.step_key} {self.status}"

class LedgerVelocityWindow(models.Model):
    wallet = models.ForeignKey(
        TokenWallet,
        on_delete=models.CASCADE,
        related_name="velocity_windows",
    )
    action = models.CharField(max_length=32, choices=LEDGER_ACTION_CHOICES, db_index=True)
    window_seconds = models.PositiveIntegerField(db_index=True)
    amount = models.BigIntegerField(default=0)
    count = models.PositiveIntegerField(default=0)
    window_start = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["wallet", "action", "window_seconds", "window_start"],
                name="ledgervelocitywindow_unique_wallet_action_window",
            ),
            models.CheckConstraint(
                condition=models.Q(amount__gte=0),
                name="ledgervelocitywindow_amount_gte_0",
            ),
        ]
        indexes = [
            models.Index(fields=["wallet", "action", "window_seconds", "window_start"]),
        ]
        permissions = [
            ("can_view_wallet_velocity", "Can view wallet velocity windows"),
        ]

    def __str__(self):
        return f"Velocity wallet={self.wallet_id} action={self.action} {self.window_seconds}s amount={self.amount}"

class LedgerHold(models.Model):
    wallet = models.ForeignKey(
        TokenWallet,
        on_delete=models.PROTECT,
        related_name="holds",
    )
    amount = models.BigIntegerField()
    reason = models.CharField(max_length=128, blank=True, default="")
    released = models.BooleanField(default=False, db_index=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ledger_holds_created",
    )
    released_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ledger_holds_released",
    )
    metadata = models.JSONField(blank=True, default=dict)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    released_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=models.Q(amount__gt=0),
                name="ledgerhold_amount_gt_0",
            ),
        ]
        indexes = [
            models.Index(fields=["wallet", "released", "created_at"]),
        ]
        permissions = [
            ("can_manage_wallet_holds", "Can manage wallet holds"),
            ("can_view_wallet_holds", "Can view wallet holds"),
        ]

    def __str__(self):
        return f"Hold #{self.id} wallet={self.wallet_id} amount={self.amount} released={self.released}"