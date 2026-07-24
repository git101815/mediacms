import uuid

from django.conf import settings
from django.db import models
from django.db.models import Q


class RewardChestGrant(models.Model):
    STATUS_PENDING = "pending"
    STATUS_OPENED = "opened"
    STATUS_REVOKED = "revoked"
    STATUS_CHOICES = (
        (STATUS_PENDING, "Pending"),
        (STATUS_OPENED, "Opened"),
        (STATUS_REVOKED, "Revoked"),
    )

    public_id = models.UUIDField(
        default=uuid.uuid4,
        unique=True,
        editable=False,
        db_index=True,
    )
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="reward_chest_grants",
    )
    chest_key = models.CharField(max_length=64, db_index=True)
    source_type = models.CharField(max_length=32, db_index=True)
    source_ref = models.CharField(max_length=160)
    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING,
        db_index=True,
    )

    config_version = models.PositiveIntegerField()
    config_snapshot = models.JSONField(default=dict)
    metadata = models.JSONField(default=dict, blank=True)

    roll = models.PositiveSmallIntegerField(null=True, blank=True)
    drop_key = models.CharField(max_length=64, blank=True, default="")
    drop_label = models.CharField(max_length=80, blank=True, default="")
    rarity = models.CharField(max_length=64, blank=True, default="")
    chance_bps = models.PositiveIntegerField(null=True, blank=True)
    amount = models.BigIntegerField(null=True, blank=True)
    ledger_txn = models.OneToOneField(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="reward_chest_grant",
        null=True,
        blank=True,
    )

    expires_at = models.DateTimeField(null=True, blank=True, db_index=True)
    granted_at = models.DateTimeField(auto_now_add=True, db_index=True)
    opened_at = models.DateTimeField(null=True, blank=True, db_index=True)
    revoked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-granted_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["source_type", "source_ref"],
                name="unique_reward_chest_source_ref",
            ),
            models.CheckConstraint(
                condition=Q(amount__isnull=True) | Q(amount__gt=0),
                name="reward_chest_amount_positive_if_set",
            ),
            models.CheckConstraint(
                condition=Q(roll__isnull=True) | (Q(roll__gte=0) & Q(roll__lt=10_000)),
                name="reward_chest_roll_in_range",
            ),
            models.CheckConstraint(
                condition=Q(chance_bps__isnull=True)
                | (Q(chance_bps__gt=0) & Q(chance_bps__lte=10_000)),
                name="reward_chest_chance_in_range",
            ),
        ]
        indexes = [
            models.Index(
                fields=["user", "status", "-granted_at"],
                name="ledger_chest_user_status_idx",
            ),
            models.Index(
                fields=["chest_key", "status"],
                name="ledger_chest_key_status_idx",
            ),
        ]

    def __str__(self):
        return f"Reward Chest {self.chest_key} for user {self.user_id} [{self.status}]"


class DailyRewardState(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="daily_reward_state",
    )
    current_streak = models.PositiveIntegerField(default=0)
    total_claims = models.PositiveIntegerField(default=0)
    last_claim_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                condition=Q(current_streak__gte=0),
                name="daily_reward_state_streak_nonnegative",
            ),
            models.CheckConstraint(
                condition=Q(total_claims__gte=0),
                name="daily_reward_state_claims_nonnegative",
            ),
        ]

    def __str__(self):
        return f"Daily rewards for user {self.user_id}"


class DailyRewardClaim(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="daily_reward_claims",
    )
    reward_date = models.DateField()
    streak_day = models.PositiveIntegerField()
    cycle_day = models.PositiveIntegerField()
    amount = models.BigIntegerField()
    ledger_txn = models.OneToOneField(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="daily_reward_claim",
    )
    reward_chest_grant = models.OneToOneField(
        RewardChestGrant,
        on_delete=models.PROTECT,
        related_name="daily_reward_claim",
        null=True,
        blank=True,
    )
    config_version = models.PositiveIntegerField()
    config_snapshot = models.JSONField(default=dict)
    claimed_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-reward_date", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["user", "reward_date"],
                name="unique_daily_reward_claim_per_user_day",
            ),
            models.CheckConstraint(
                condition=Q(streak_day__gt=0),
                name="daily_reward_claim_streak_positive",
            ),
            models.CheckConstraint(
                condition=Q(cycle_day__gt=0),
                name="daily_reward_claim_cycle_positive",
            ),
            models.CheckConstraint(
                condition=Q(amount__gt=0),
                name="daily_reward_claim_amount_positive",
            ),
        ]
        indexes = [
            models.Index(
                fields=["user", "-claimed_at"],
                name="ledger_daily_user_claim_idx",
            ),
        ]

    def __str__(self):
        return f"Daily reward {self.reward_date} for user {self.user_id}"
