from django.conf import settings
from django.db import models


class CreatorSubscriptionPeriod(models.Model):
    subscription = models.ForeignKey(
        "premium.CreatorSubscription",
        on_delete=models.PROTECT,
        related_name="paid_periods",
    )
    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="creator_subscription_periods",
    )
    plan = models.ForeignKey(
        "premium.CreatorSubscriptionPlan",
        on_delete=models.PROTECT,
        related_name="paid_periods",
    )
    txn = models.OneToOneField(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="creator_subscription_period",
    )
    period_start = models.DateTimeField(db_index=True)
    period_end = models.DateTimeField(db_index=True)
    price_tokens = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["subscription", "period_start"],
                name="uniq_sub_period_start",
            ),
            models.CheckConstraint(
                condition=models.Q(period_end__gt=models.F("period_start")),
                name="sub_period_end_after_start",
            ),
            models.CheckConstraint(
                condition=models.Q(price_tokens__gt=0),
                name="sub_period_price_gt_0",
            ),
        ]
        indexes = [
            models.Index(
                fields=["subscription", "period_start", "period_end"],
                name="prem_sub_period_sub_idx",
            ),
            models.Index(
                fields=["creator", "period_start", "period_end"],
                name="prem_sub_period_creator_idx",
            ),
        ]

    def __str__(self):
        return f"{self.subscription_id}: {self.period_start} - {self.period_end}"


class PremiumMediaRelease(models.Model):
    media = models.OneToOneField(
        "files.Media",
        on_delete=models.PROTECT,
        related_name="premium_release",
    )
    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="premium_media_releases",
    )
    released_at = models.DateTimeField(db_index=True)
    processed_at = models.DateTimeField(null=True, blank=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["creator", "released_at"],
                name="prem_release_creator_idx",
            ),
            models.Index(
                fields=["processed_at", "id"],
                name="prem_release_pending_idx",
            ),
        ]

    def __str__(self):
        return f"Release {self.media_id} at {self.released_at}"
