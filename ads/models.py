from __future__ import annotations

import uuid

from django.conf import settings
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone


class AdCampaign(models.Model):
    PLACEMENT_HOME = "home_leaderboard"
    PLACEMENT_SIDEBAR = "media_sidebar_rectangle"
    PLACEMENT_CHOICES = (
        (PLACEMENT_HOME, "Homepage banner · 728×90"),
        (PLACEMENT_SIDEBAR, "Video page sidebar · 300×250"),
    )

    PRICING_CPM = "cpm"
    PRICING_CPC = "cpc"
    PRICING_CHOICES = (
        (PRICING_CPM, "CPM"),
        (PRICING_CPC, "CPC"),
    )

    REVIEW_PENDING = "pending"
    REVIEW_APPROVED = "approved"
    REVIEW_REJECTED = "rejected"
    REVIEW_CHOICES = (
        (REVIEW_PENDING, "Pending review"),
        (REVIEW_APPROVED, "Approved"),
        (REVIEW_REJECTED, "Rejected"),
    )

    DELIVERY_ACTIVE = "active"
    DELIVERY_PAUSED_USER = "paused_user"
    DELIVERY_PAUSED_FUNDS = "paused_funds"
    DELIVERY_CHOICES = (
        (DELIVERY_ACTIVE, "Active"),
        (DELIVERY_PAUSED_USER, "Paused"),
        (DELIVERY_PAUSED_FUNDS, "Paused · insufficient funds"),
    )

    advertiser = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="ad_campaigns",
        db_index=True,
    )
    name = models.CharField(max_length=120)
    placement = models.CharField(max_length=40, choices=PLACEMENT_CHOICES, db_index=True)
    creative = models.ImageField(upload_to="ads/creatives/%Y/%m/%d")
    target_url = models.URLField(max_length=1000)
    pricing_model = models.CharField(max_length=8, choices=PRICING_CHOICES, db_index=True)
    # Smallest ledger unit: one micro-token (1e-6 token).
    # CPM: micro-tokens per 1,000 impressions.
    # CPC: micro-tokens per click.
    bid_microtokens = models.BigIntegerField(validators=[MinValueValidator(1)])

    review_status = models.CharField(
        max_length=16,
        choices=REVIEW_CHOICES,
        default=REVIEW_PENDING,
        db_index=True,
    )
    review_note = models.TextField(blank=True, default="")
    delivery_status = models.CharField(
        max_length=24,
        choices=DELIVERY_CHOICES,
        default=DELIVERY_ACTIVE,
        db_index=True,
    )

    impressions = models.PositiveBigIntegerField(default=0)
    clicks = models.PositiveBigIntegerField(default=0)
    spend_microtokens = models.PositiveBigIntegerField(default=0)

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at", "-id")
        indexes = [
            models.Index(fields=["placement", "review_status", "delivery_status"]),
            models.Index(fields=["advertiser", "review_status", "delivery_status"]),
        ]
        constraints = [
            models.CheckConstraint(
                condition=models.Q(bid_microtokens__gt=0),
                name="adcampaign_bid_positive",
            ),
        ]

    def __str__(self):
        return f"{self.name} #{self.pk or 'new'}"

    @property
    def placement_dimensions(self):
        if self.placement == self.PLACEMENT_HOME:
            return (728, 90)
        return (300, 250)

    @property
    def visible_status(self):
        if self.review_status == self.REVIEW_PENDING:
            return "pending_review"
        if self.review_status == self.REVIEW_REJECTED:
            return "rejected"
        return self.delivery_status


class AdSettlementBatch(models.Model):
    STATUS_PENDING = "pending"
    STATUS_POSTED = "posted"
    STATUS_CHOICES = (
        (STATUS_PENDING, "Pending"),
        (STATUS_POSTED, "Posted"),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    campaign = models.ForeignKey(
        AdCampaign,
        on_delete=models.PROTECT,
        related_name="settlement_batches",
    )
    advertiser = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="ad_settlement_batches",
    )
    amount_microtokens = models.PositiveBigIntegerField(default=0)
    impressions = models.PositiveBigIntegerField(default=0)
    clicks = models.PositiveBigIntegerField(default=0)
    status = models.CharField(
        max_length=12,
        choices=STATUS_CHOICES,
        default=STATUS_PENDING,
        db_index=True,
    )
    ledger_txn = models.OneToOneField(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="ad_settlement_batch",
    )
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    posted_at = models.DateTimeField(null=True, blank=True, db_index=True)
    redis_acked_at = models.DateTimeField(null=True, blank=True, db_index=True)

    class Meta:
        ordering = ("created_at",)
        indexes = [
            models.Index(fields=["campaign", "redis_acked_at", "created_at"]),
            models.Index(fields=["status", "redis_acked_at", "created_at"]),
        ]

    def __str__(self):
        return f"Ads settlement {self.id} ({self.amount_microtokens})"
