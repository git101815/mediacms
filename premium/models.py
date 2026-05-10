from django.conf import settings
from django.db import models


class PremiumMediaAsset(models.Model):
    STATUS_DRAFT = "draft"
    STATUS_READY = "ready"
    STATUS_DISABLED = "disabled"

    STORAGE_DIRECT_URL = "direct_url"
    STORAGE_S3 = "s3"

    PLAYBACK_MP4 = "mp4"
    PLAYBACK_HLS = "hls"

    media = models.OneToOneField(
        "files.Media",
        on_delete=models.PROTECT,
        related_name="premium_asset",
    )

    price_tokens = models.BigIntegerField(default=0)

    status = models.CharField(
        max_length=32,
        choices=[
            (STATUS_DRAFT, "Draft"),
            (STATUS_READY, "Ready"),
            (STATUS_DISABLED, "Disabled"),
        ],
        default=STATUS_DRAFT,
        db_index=True,
    )

    storage_backend = models.CharField(
        max_length=32,
        choices=[
            (STORAGE_DIRECT_URL, "Direct URL"),
            (STORAGE_S3, "S3"),
        ],
        default=STORAGE_DIRECT_URL,
    )

    playback_format = models.CharField(
        max_length=16,
        choices=[
            (PLAYBACK_MP4, "MP4"),
            (PLAYBACK_HLS, "HLS"),
        ],
        default=PLAYBACK_MP4,
    )

    direct_url = models.URLField(blank=True, default="")
    storage_bucket = models.CharField(max_length=255, blank=True, default="")
    storage_key = models.CharField(max_length=1024, blank=True, default="")

    file_name = models.CharField(max_length=255, blank=True, default="")
    content_type = models.CharField(max_length=128, blank=True, default="video/mp4")
    codec = models.CharField(max_length=32, blank=True, default="h264")
    video_height = models.PositiveIntegerField(default=1080)
    size_bytes = models.BigIntegerField(null=True, blank=True)

    premium_published_at = models.DateTimeField(null=True, blank=True, db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["status", "premium_published_at"]),
        ]

    def __str__(self):
        return f"Premium asset for {self.media_id}"


class PremiumMediaUnlock(models.Model):
    SOURCE_PURCHASE = "purchase"
    SOURCE_SUBSCRIPTION = "subscription"
    SOURCE_ADMIN = "admin"
    SOURCE_PROMO = "promo"

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="premium_unlocks",
    )
    media = models.ForeignKey(
        "files.Media",
        on_delete=models.PROTECT,
        related_name="premium_unlocks",
    )

    source_type = models.CharField(
        max_length=32,
        choices=[
            (SOURCE_PURCHASE, "Purchase"),
            (SOURCE_SUBSCRIPTION, "Subscription"),
            (SOURCE_ADMIN, "Admin"),
            (SOURCE_PROMO, "Promo"),
        ],
        db_index=True,
    )

    source_txn = models.ForeignKey(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
    )

    source_subscription = models.ForeignKey(
        "premium.CreatorSubscription",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
    )

    unlocked_at = models.DateTimeField(auto_now_add=True, db_index=True)
    revoked_at = models.DateTimeField(null=True, blank=True, db_index=True)
    metadata = models.JSONField(default=dict, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["user", "media"],
                name="unique_premium_unlock_per_user_media",
            )
        ]
        indexes = [
            models.Index(fields=["user", "revoked_at", "-unlocked_at"]),
            models.Index(fields=["media", "revoked_at"]),
        ]

    def __str__(self):
        return f"{self.user_id} unlocked {self.media_id}"


class MediaPurchase(models.Model):
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="premium_media_purchases",
    )
    media = models.ForeignKey(
        "files.Media",
        on_delete=models.PROTECT,
        related_name="premium_purchases",
    )
    txn = models.OneToOneField(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        related_name="premium_media_purchase",
    )
    price_tokens = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["user", "media"],
                name="unique_media_purchase_per_user",
            )
        ]

    def __str__(self):
        return f"{self.user_id} purchased {self.media_id}"


class PremiumCollection(models.Model):
    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="premium_collections",
    )
    name = models.CharField(max_length=100)
    slug = models.SlugField(max_length=100)
    description = models.TextField(blank=True, default="")
    is_active = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["creator", "slug"],
                name="unique_premium_collection_slug_per_creator",
            )
        ]

    def __str__(self):
        return f"{self.creator_id} / {self.slug}"


class PremiumCollectionMedia(models.Model):
    collection = models.ForeignKey(
        "premium.PremiumCollection",
        on_delete=models.CASCADE,
        related_name="collection_media",
    )
    media = models.ForeignKey(
        "files.Media",
        on_delete=models.PROTECT,
        related_name="premium_collection_links",
    )
    added_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["collection", "media"],
                name="unique_media_per_premium_collection",
            )
        ]


class CreatorSubscriptionPlan(models.Model):
    POLICY_FUTURE_RELEASES = "future_releases"
    POLICY_ACTIVE_FULL_CATALOG = "active_full_catalog"
    POLICY_SELECTED_COLLECTIONS = "selected_collections"

    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="creator_subscription_plans",
    )
    code = models.SlugField(max_length=64)
    name = models.CharField(max_length=100)
    price_tokens = models.BigIntegerField()

    access_policy = models.CharField(
        max_length=32,
        choices=[
            (POLICY_FUTURE_RELEASES, "Future releases"),
            (POLICY_ACTIVE_FULL_CATALOG, "Active full catalog"),
            (POLICY_SELECTED_COLLECTIONS, "Selected collections"),
        ],
        default=POLICY_FUTURE_RELEASES,
        db_index=True,
    )

    included_collections = models.ManyToManyField(
        "premium.PremiumCollection",
        related_name="subscription_plans",
        blank=True,
    )

    is_active = models.BooleanField(default=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["creator", "code"],
                name="unique_creator_subscription_plan_code",
            )
        ]

    def __str__(self):
        return f"{self.creator_id} / {self.code}"


class CreatorSubscription(models.Model):
    STATUS_ACTIVE = "active"
    STATUS_PAST_DUE = "past_due"
    STATUS_CANCELED = "canceled"
    STATUS_EXPIRED = "expired"

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="creator_subscriptions",
    )
    creator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="creator_subscribers",
    )
    plan = models.ForeignKey(
        "premium.CreatorSubscriptionPlan",
        on_delete=models.PROTECT,
        related_name="subscriptions",
    )

    status = models.CharField(max_length=16, default=STATUS_ACTIVE, db_index=True)
    current_period_start = models.DateTimeField(db_index=True)
    current_period_end = models.DateTimeField(db_index=True)

    cancel_at_period_end = models.BooleanField(default=False)

    last_txn = models.ForeignKey(
        "ledger.LedgerTransaction",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["user", "creator", "status", "current_period_end"]),
        ]

    def __str__(self):
        return f"{self.user_id} -> {self.creator_id}"