from __future__ import annotations
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone
from datetime import timedelta
import uuid

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

WALLET_REQUEST_TYPE_DEPOSIT = "deposit"
WALLET_REQUEST_TYPE_WITHDRAWAL = "withdrawal"
WALLET_REQUEST_TYPE_CHOICES = (
    (WALLET_REQUEST_TYPE_DEPOSIT, "Deposit"),
    (WALLET_REQUEST_TYPE_WITHDRAWAL, "Withdrawal"),
)

WALLET_REQUEST_STATUS_PENDING = "pending"
WALLET_REQUEST_STATUS_APPROVED = "approved"
WALLET_REQUEST_STATUS_REJECTED = "rejected"
WALLET_REQUEST_STATUS_CANCELED = "canceled"
WALLET_REQUEST_STATUS_COMPLETED = "completed"
WALLET_REQUEST_STATUS_CHOICES = (
    (WALLET_REQUEST_STATUS_PENDING, "Pending"),
    (WALLET_REQUEST_STATUS_APPROVED, "Approved"),
    (WALLET_REQUEST_STATUS_REJECTED, "Rejected"),
    (WALLET_REQUEST_STATUS_CANCELED, "Canceled"),
    (WALLET_REQUEST_STATUS_COMPLETED, "Completed"),
)
SYSTEM_WALLET_EXTERNAL_ASSET_CLEARING = "external_asset_clearing"

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
    SYSTEM_EXTERNAL_ASSET_CLEARING = SYSTEM_WALLET_EXTERNAL_ASSET_CLEARING
    SYSTEM_CHOICES = (
        (SYSTEM_ISSUANCE, "Issuance"),
        (SYSTEM_PLATFORM_FEES, "Platform fees"),
        (SYSTEM_EXTERNAL_ASSET_CLEARING, "External asset clearing"),
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

class WalletRequest(models.Model):
    REQUEST_TYPE_DEPOSIT = WALLET_REQUEST_TYPE_DEPOSIT
    REQUEST_TYPE_WITHDRAWAL = WALLET_REQUEST_TYPE_WITHDRAWAL
    REQUEST_TYPE_CHOICES = WALLET_REQUEST_TYPE_CHOICES

    STATUS_PENDING = WALLET_REQUEST_STATUS_PENDING
    STATUS_APPROVED = WALLET_REQUEST_STATUS_APPROVED
    STATUS_REJECTED = WALLET_REQUEST_STATUS_REJECTED
    STATUS_CANCELED = WALLET_REQUEST_STATUS_CANCELED
    STATUS_COMPLETED = WALLET_REQUEST_STATUS_COMPLETED
    STATUS_CHOICES = WALLET_REQUEST_STATUS_CHOICES

    wallet = models.ForeignKey(
        TokenWallet,
        on_delete=models.PROTECT,
        related_name="wallet_requests",
    )
    request_type = models.CharField(max_length=16, choices=REQUEST_TYPE_CHOICES, db_index=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    amount = models.BigIntegerField()
    asset_code = models.CharField(max_length=16, default="TOKENS")
    destination_address = models.CharField(max_length=255, blank=True)
    reference = models.CharField(max_length=64, unique=True, db_index=True)
    notes = models.TextField(blank=True)
    rejection_reason = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    metadata_version = models.PositiveIntegerField(default=LEDGER_METADATA_VERSION)

    hold = models.OneToOneField(
        "ledger.LedgerHold",
        on_delete=models.SET_NULL,
        related_name="wallet_request",
        null=True,
        blank=True,
    )
    linked_ledger_txn = models.ForeignKey(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="wallet_requests",
        null=True,
        blank=True,
    )

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="wallet_requests_created",
    )
    reviewed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="wallet_requests_reviewed",
    )

    reviewed_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at", "-id")
        indexes = [
            models.Index(fields=["wallet", "status", "created_at"]),
            models.Index(fields=["wallet", "request_type", "created_at"]),
        ]
        permissions = [
            ("can_manage_wallet_requests", "Can manage wallet requests"),
            ("can_review_wallet_requests", "Can review wallet requests"),
        ]

    def clean(self):
        if int(self.amount) <= 0:
            raise ValidationError("Request amount must be greater than zero")
        if self.request_type == self.REQUEST_TYPE_WITHDRAWAL and not self.destination_address.strip():
            raise ValidationError("Destination address is required for withdrawal requests")

    def __str__(self):
        return f"{self.get_request_type_display()} {self.amount} [{self.status}] #{self.reference}"

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

class DepositSession(models.Model):
    STATUS_AWAITING_PAYMENT = "awaiting_payment"
    STATUS_SEEN_ONCHAIN = "seen_onchain"
    STATUS_CONFIRMING = "confirming"
    STATUS_CREDITED = "credited"
    STATUS_SWEPT = "swept"
    STATUS_EXPIRED = "expired"
    STATUS_FAILED = "failed"
    STATUS_CANCELED = "canceled"

    STATUS_CHOICES = (
        (STATUS_AWAITING_PAYMENT, "Awaiting payment"),
        (STATUS_SEEN_ONCHAIN, "Seen on-chain"),
        (STATUS_CONFIRMING, "Confirming"),
        (STATUS_CREDITED, "Credited"),
        (STATUS_SWEPT, "Swept"),
        (STATUS_EXPIRED, "Expired"),
        (STATUS_FAILED, "Failed"),
        (STATUS_CANCELED, "Canceled"),
    )

    public_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False, db_index=True)

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="deposit_sessions",
        db_index=True,
    )
    wallet = models.ForeignKey(
        TokenWallet,
        on_delete=models.PROTECT,
        related_name="deposit_sessions",
        db_index=True,
    )

    chain = models.CharField(max_length=32, db_index=True)
    asset_code = models.CharField(max_length=32, db_index=True)
    token_contract_address = models.CharField(max_length=64, blank=True, default="", db_index=True)

    deposit_address = models.CharField(max_length=128, unique=True)
    address_derivation_ref = models.CharField(max_length=128, unique=True)

    status = models.CharField(
        max_length=32,
        choices=STATUS_CHOICES,
        default=STATUS_AWAITING_PAYMENT,
        db_index=True,
    )

    min_amount = models.BigIntegerField(default=1)
    required_confirmations = models.PositiveIntegerField(default=1)
    expires_at = models.DateTimeField(db_index=True)

    observed_txid = models.CharField(max_length=128, blank=True, default="", db_index=True)
    observed_amount = models.BigIntegerField(null=True, blank=True)
    confirmations = models.PositiveIntegerField(default=0)

    credited_ledger_txn = models.OneToOneField(
        LedgerTransaction,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="credited_deposit_session",
    )

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="deposit_sessions_created",
    )

    metadata = models.JSONField(default=dict, blank=True)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    swept_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["user", "status", "-created_at"]),
            models.Index(fields=["wallet", "status", "-created_at"]),
            models.Index(fields=["chain", "asset_code", "status"]),
            models.Index(fields=["expires_at", "status"]),
            models.Index(fields=["status", "swept_at"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(min_amount__gt=0),
                name="depositsession_min_amount_gt_0",
            ),
            models.CheckConstraint(
                condition=models.Q(required_confirmations__gte=1),
                name="depositsession_required_confirmations_gte_1",
            ),
            models.CheckConstraint(
                condition=models.Q(confirmations__gte=0),
                name="depositsession_confirmations_gte_0",
            ),
            models.CheckConstraint(
                condition=models.Q(observed_amount__isnull=True) | models.Q(observed_amount__gt=0),
                name="depositsession_observed_amount_gt_0_if_present",
            ),
        ]
        permissions = [
            ("can_manage_deposit_sessions", "Can manage deposit sessions"),
            ("can_view_deposit_sessions", "Can view deposit sessions"),
            ("can_credit_confirmed_deposits", "Can credit confirmed deposits"),
        ]

    def __str__(self):
        return f"DepositSession #{self.id} user={self.user_id} {self.chain}/{self.asset_code} {self.status}"

class ObservedOnchainTransfer(models.Model):
    STATUS_OBSERVED = "observed"
    STATUS_CONFIRMING = "confirming"
    STATUS_CONFIRMED = "confirmed"
    STATUS_CREDITED = "credited"
    STATUS_IGNORED = "ignored"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = (
        (STATUS_OBSERVED, "Observed"),
        (STATUS_CONFIRMING, "Confirming"),
        (STATUS_CONFIRMED, "Confirmed"),
        (STATUS_CREDITED, "Credited"),
        (STATUS_IGNORED, "Ignored"),
        (STATUS_FAILED, "Failed"),
    )

    event_key = models.CharField(max_length=160, unique=True)

    chain = models.CharField(max_length=32, db_index=True)
    txid = models.CharField(max_length=128, db_index=True)
    log_index = models.BigIntegerField(null=True, blank=True)
    block_number = models.BigIntegerField(null=True, blank=True, db_index=True)

    from_address = models.CharField(max_length=128, blank=True, default="")
    to_address = models.CharField(max_length=128, db_index=True)
    token_contract_address = models.CharField(max_length=64, blank=True, default="", db_index=True)
    asset_code = models.CharField(max_length=32, db_index=True)

    amount = models.BigIntegerField()
    confirmations = models.PositiveIntegerField(default=0)

    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_OBSERVED,
        db_index=True,
    )

    deposit_session = models.ForeignKey(
        DepositSession,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="observed_transfers",
    )

    credited_ledger_txn = models.OneToOneField(
        LedgerTransaction,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="credited_onchain_transfer",
    )

    raw_payload = models.JSONField(default=dict, blank=True)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)

    first_seen_at = models.DateTimeField(default=timezone.now, db_index=True)
    confirmed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["chain", "txid"]),
            models.Index(fields=["chain", "to_address", "status"]),
            models.Index(fields=["deposit_session", "status"]),
            models.Index(fields=["status", "first_seen_at"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(amount__gt=0),
                name="observedonchaintransfer_amount_gt_0",
            ),
            models.CheckConstraint(
                condition=models.Q(confirmations__gte=0),
                name="observedonchaintransfer_confirmations_gte_0",
            ),
        ]
        permissions = [
            ("can_record_onchain_observations", "Can record on-chain observations"),
            ("can_view_onchain_transfers", "Can view on-chain transfers"),
        ]

    def __str__(self):
        return f"ObservedOnchainTransfer #{self.id} {self.event_key} {self.status}"

class DepositAddress(models.Model):
    STATUS_AVAILABLE = "available"
    STATUS_ALLOCATED = "allocated"
    STATUS_RETIRED = "retired"

    STATUS_CHOICES = (
        (STATUS_AVAILABLE, "Available"),
        (STATUS_ALLOCATED, "Allocated"),
        (STATUS_RETIRED, "Retired"),
    )

    chain = models.CharField(max_length=32, db_index=True)
    asset_code = models.CharField(max_length=32, db_index=True)
    token_contract_address = models.CharField(max_length=64, blank=True, default="", db_index=True)

    display_label = models.CharField(max_length=64)
    address = models.CharField(max_length=128, unique=True)
    address_derivation_ref = models.CharField(max_length=128, unique=True)

    required_confirmations = models.PositiveIntegerField(default=1)
    min_amount = models.BigIntegerField(default=1)
    session_ttl_seconds = models.PositiveIntegerField(default=3600)

    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_AVAILABLE,
        db_index=True,
    )

    allocated_deposit_session = models.OneToOneField(
        "ledger.DepositSession",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="allocated_address",
    )

    derivation_index = models.PositiveBigIntegerField(
        null=True,
        blank=True,
        db_index=True,
    )

    metadata = models.JSONField(default=dict, blank=True)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "chain", "asset_code"]),
            models.Index(fields=["status", "chain", "asset_code", "token_contract_address"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(required_confirmations__gte=1),
                name="depositaddress_required_confirmations_gte_1",
            ),
            models.CheckConstraint(
                condition=models.Q(min_amount__gt=0),
                name="depositaddress_min_amount_gt_0",
            ),
            models.CheckConstraint(
                condition=models.Q(session_ttl_seconds__gt=0),
                name="depositaddress_session_ttl_seconds_gt_0",
            ),
            models.UniqueConstraint(
                fields=["chain", "asset_code", "token_contract_address", "derivation_index"],
                condition=models.Q(derivation_index__isnull=False),
                name="uniq_depositaddress_option_derivation_index",
            ),
        ]
        permissions = [
            ("can_manage_deposit_addresses", "Can manage deposit addresses"),
            ("can_view_deposit_addresses", "Can view deposit addresses"),
        ]

    def __str__(self):
        return f"DepositAddress #{self.id} {self.display_label} {self.address} {self.status}"

class InternalAPIRequestNonce(models.Model):
    service_name = models.CharField(max_length=64, db_index=True)
    nonce = models.CharField(max_length=128)
    request_sha256 = models.CharField(max_length=64)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    expires_at = models.DateTimeField(db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["service_name", "created_at"]),
            models.Index(fields=["expires_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["service_name", "nonce"],
                name="uniq_internalapirequestnonce_service_nonce",
            ),
        ]

    def __str__(self):
        return f"InternalAPIRequestNonce #{self.id} {self.service_name}:{self.nonce}"

class DepositSweepJob(models.Model):
    STATUS_PENDING = "pending"
    STATUS_FUNDING_BROADCASTED = "funding_broadcasted"
    STATUS_READY_TO_SWEEP = "ready_to_sweep"
    STATUS_SWEEP_BROADCASTED = "sweep_broadcasted"
    STATUS_CONFIRMED = "confirmed"
    STATUS_FAILED = "failed"
    STATUS_ABANDONED = "abandoned"

    STATUS_CHOICES = (
        (STATUS_PENDING, "Pending"),
        (STATUS_FUNDING_BROADCASTED, "Funding broadcasted"),
        (STATUS_READY_TO_SWEEP, "Ready to sweep"),
        (STATUS_SWEEP_BROADCASTED, "Sweep broadcasted"),
        (STATUS_CONFIRMED, "Confirmed"),
        (STATUS_FAILED, "Failed"),
        (STATUS_ABANDONED, "Abandoned"),
    )

    public_id = models.UUIDField(default=uuid.uuid4, unique=True, editable=False, db_index=True)

    deposit_session = models.ForeignKey(
        "ledger.DepositSession",
        on_delete=models.PROTECT,
        related_name="sweep_jobs",
    )
    observed_transfer = models.OneToOneField(
        "ledger.ObservedOnchainTransfer",
        on_delete=models.PROTECT,
        related_name="sweep_job",
    )

    chain = models.CharField(max_length=32, db_index=True)
    asset_code = models.CharField(max_length=32, db_index=True)
    token_contract_address = models.CharField(max_length=64, blank=True, default="", db_index=True)

    source_address = models.CharField(max_length=128, db_index=True)
    address_derivation_ref = models.CharField(max_length=128)
    derivation_index = models.PositiveBigIntegerField(null=True, blank=True, db_index=True)

    amount = models.BigIntegerField()

    destination_address = models.CharField(max_length=128, blank=True, default="")
    gas_funding_txid = models.CharField(max_length=128, blank=True, default="", db_index=True)
    sweep_txid = models.CharField(max_length=128, blank=True, default="", db_index=True)

    status = models.CharField(
        max_length=32,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING,
        db_index=True,
    )

    claimed_by_service = models.CharField(max_length=64, blank=True, default="")
    claim_expires_at = models.DateTimeField(null=True, blank=True, db_index=True)

    attempt_count = models.PositiveIntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    metadata = models.JSONField(default=dict, blank=True)
    metadata_version = models.PositiveSmallIntegerField(default=LEDGER_METADATA_VERSION)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    confirmed_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "chain", "asset_code"]),
            models.Index(fields=["status", "claim_expires_at"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(amount__gt=0),
                name="depositsweepjob_amount_gt_0",
            ),
        ]
        permissions = [
            ("can_manage_deposit_sweep_jobs", "Can manage deposit sweep jobs"),
            ("can_view_deposit_sweep_jobs", "Can view deposit sweep jobs"),
        ]

    def __str__(self):
        return f"DepositSweepJob #{self.id} {self.chain}/{self.asset_code} {self.status}"