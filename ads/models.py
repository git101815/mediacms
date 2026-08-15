from __future__ import annotations

import uuid

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator
from django.db import models
from django.utils import timezone


AD_PLACEMENT_HOME = "home_leaderboard"
AD_PLACEMENT_SIDEBAR = "media_sidebar_rectangle"
AD_PLACEMENT_PREROLL = "video_preroll"
AD_PLACEMENT_MIDROLL = "video_midroll"
AD_PLACEMENT_POSTROLL = "video_postroll"
AD_PLACEMENT_POPUNDER = "popunder"

AD_CAMPAIGN_PLACEMENT_CHOICES = (
    (AD_PLACEMENT_HOME, "Banner · 728×90"),
    (AD_PLACEMENT_SIDEBAR, "Banner · 300×250"),
    (AD_PLACEMENT_PREROLL, "In-video · Preroll"),
    (AD_PLACEMENT_MIDROLL, "In-video · Midroll"),
    (AD_PLACEMENT_POSTROLL, "In-video · Postroll"),
    (AD_PLACEMENT_POPUNDER, "Popunder"),
)

AD_CREATIVE_IN_VIDEO = "in_video"
AD_CREATIVE_POPUNDER = AD_PLACEMENT_POPUNDER
AD_CREATIVE_PLACEMENT_CHOICES = (
    (AD_PLACEMENT_HOME, "Banner · 728×90"),
    (AD_PLACEMENT_SIDEBAR, "Banner · 300×250"),
    (
        AD_CREATIVE_IN_VIDEO,
        "In-video · VAST (Preroll / Midroll / Postroll)",
    ),
    (AD_CREATIVE_POPUNDER, "Popunder · URL"),
)


def creative_format_for_campaign_placement(placement):
    if placement in {
        AD_PLACEMENT_PREROLL,
        AD_PLACEMENT_MIDROLL,
        AD_PLACEMENT_POSTROLL,
    }:
        return AD_CREATIVE_IN_VIDEO
    return placement

AD_REVIEW_PENDING = "pending"
AD_REVIEW_APPROVED = "approved"
AD_REVIEW_REJECTED = "rejected"
AD_REVIEW_CHOICES = (
    (AD_REVIEW_PENDING, "Pending review"),
    (AD_REVIEW_APPROVED, "Approved"),
    (AD_REVIEW_REJECTED, "Rejected"),
)


class AdCampaign(models.Model):
    PLACEMENT_HOME = AD_PLACEMENT_HOME
    PLACEMENT_SIDEBAR = AD_PLACEMENT_SIDEBAR
    PLACEMENT_PREROLL = AD_PLACEMENT_PREROLL
    PLACEMENT_MIDROLL = AD_PLACEMENT_MIDROLL
    PLACEMENT_POSTROLL = AD_PLACEMENT_POSTROLL
    PLACEMENT_POPUNDER = AD_PLACEMENT_POPUNDER
    PLACEMENT_CHOICES = AD_CAMPAIGN_PLACEMENT_CHOICES

    PRICING_CPM = "cpm"
    PRICING_CPC = "cpc"
    PRICING_CHOICES = (
        (PRICING_CPM, "CPM"),
        (PRICING_CPC, "CPC"),
    )

    REVIEW_PENDING = AD_REVIEW_PENDING
    REVIEW_APPROVED = AD_REVIEW_APPROVED
    REVIEW_REJECTED = AD_REVIEW_REJECTED
    REVIEW_CHOICES = AD_REVIEW_CHOICES

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
    placement = models.CharField(
        max_length=40,
        choices=PLACEMENT_CHOICES,
        db_index=True,
    )
    target_url = models.URLField(max_length=1000, blank=True)
    pricing_model = models.CharField(
        max_length=8,
        choices=PRICING_CHOICES,
        db_index=True,
    )
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

    creatives = models.ManyToManyField(
        "AdCreative",
        through="AdCampaignCreative",
        related_name="campaigns",
        blank=True,
    )

    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-created_at", "-id")
        indexes = [
            models.Index(
                fields=["placement", "review_status", "delivery_status"]
            ),
            models.Index(
                fields=["advertiser", "review_status", "delivery_status"]
            ),
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
        if self.placement == self.PLACEMENT_SIDEBAR:
            return (300, 250)
        return None

    @property
    def creative_format(self):
        return creative_format_for_campaign_placement(self.placement)

    @classmethod
    def delivery_slots(cls):
        return tuple(value for value, _label in cls.PLACEMENT_CHOICES)

    @property
    def visible_status(self):
        if self.review_status == self.REVIEW_PENDING:
            return "pending_review"
        if self.review_status == self.REVIEW_REJECTED:
            return "rejected"
        return self.delivery_status


class AdCreative(models.Model):
    PLACEMENT_HOME = AD_PLACEMENT_HOME
    PLACEMENT_SIDEBAR = AD_PLACEMENT_SIDEBAR
    PLACEMENT_IN_VIDEO = AD_CREATIVE_IN_VIDEO
    PLACEMENT_POPUNDER = AD_CREATIVE_POPUNDER
    PLACEMENT_CHOICES = AD_CREATIVE_PLACEMENT_CHOICES

    REVIEW_PENDING = AD_REVIEW_PENDING
    REVIEW_APPROVED = AD_REVIEW_APPROVED
    REVIEW_REJECTED = AD_REVIEW_REJECTED
    REVIEW_CHOICES = AD_REVIEW_CHOICES

    advertiser = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="ad_creatives",
        db_index=True,
    )
    name = models.CharField(max_length=120)
    placement = models.CharField(
        max_length=40,
        choices=PLACEMENT_CHOICES,
        db_index=True,
    )
    # FileField is intentional: banners may be PNG/JPG/GIF/SVG.
    # The field name is kept for backward compatibility with existing banners.
    image = models.FileField(
        upload_to="ads/creatives/%Y/%m/%d",
        blank=True,
    )
    vast_url = models.URLField(
        max_length=2000,
        blank=True,
        default="",
    )
    destination_url = models.URLField(
        max_length=1000,
        blank=True,
        default="",
    )
    review_status = models.CharField(
        max_length=16,
        choices=REVIEW_CHOICES,
        default=REVIEW_PENDING,
        db_index=True,
    )
    review_note = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-updated_at", "-id")

    def __str__(self):
        return f"{self.name} #{self.pk or 'new'}"

    @property
    def placement_dimensions(self):
        if self.placement == self.PLACEMENT_HOME:
            return (728, 90)
        if self.placement == self.PLACEMENT_SIDEBAR:
            return (300, 250)
        return None

    @property
    def is_banner(self):
        return self.placement in {
            self.PLACEMENT_HOME,
            self.PLACEMENT_SIDEBAR,
        }

    @property
    def is_in_video(self):
        return self.placement == self.PLACEMENT_IN_VIDEO

    @property
    def is_popunder(self):
        return self.placement == self.PLACEMENT_POPUNDER

    @property
    def source_kind(self):
        if self.is_banner:
            return "banner"
        if self.is_in_video:
            return "vast"
        if self.is_popunder:
            return "url"
        return "unknown"


class AdCampaignCreative(models.Model):
    campaign = models.ForeignKey(
        AdCampaign,
        on_delete=models.CASCADE,
        related_name="creative_links",
    )
    creative = models.ForeignKey(
        AdCreative,
        on_delete=models.CASCADE,
        related_name="campaign_links",
    )
    # Equal weights (1/1) give a true A/B rotation. The field is kept in the
    # data model so weighted rotation can be exposed later without a migration.
    weight = models.PositiveIntegerField(
        default=1,
        validators=[MinValueValidator(1)],
    )
    enabled = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ("id",)
        constraints = [
            models.UniqueConstraint(
                fields=["campaign", "creative"],
                name="adcampaigncreative_unique_pair",
            ),
            models.CheckConstraint(
                condition=models.Q(weight__gt=0),
                name="adcampaigncreative_weight_positive",
            ),
        ]

    def clean(self):
        super().clean()
        if not self.campaign_id or not self.creative_id:
            return
        if self.campaign.advertiser_id != self.creative.advertiser_id:
            raise ValidationError(
                "Campaign and creative must belong to the same advertiser."
            )
        if self.campaign.creative_format != self.creative.placement:
            raise ValidationError(
                "Creative format must match the campaign placement."
            )

    def save(self, *args, **kwargs):
        self.full_clean()
        return super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.campaign_id} → {self.creative_id}"


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
            models.Index(
                fields=["campaign", "redis_acked_at", "created_at"]
            ),
            models.Index(
                fields=["status", "redis_acked_at", "created_at"]
            ),
        ]

    def __str__(self):
        return f"Ads settlement {self.id} ({self.amount_microtokens})"
