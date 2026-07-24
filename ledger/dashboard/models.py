from django.conf import settings
from django.db import models
from django.db.models import Q


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
