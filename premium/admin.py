from django.contrib import admin

from .models import (
    CreatorSubscription,
    CreatorSubscriptionPlan,
    MediaPurchase,
    PremiumCollection,
    PremiumCollectionMedia,
    PremiumMediaAsset,
    PremiumMediaUnlock,
)


@admin.register(PremiumMediaAsset)
class PremiumMediaAssetAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "media",
        "status",
        "price_tokens",
        "storage_backend",
        "playback_format",
        "premium_published_at",
    ]
    list_filter = ["status", "storage_backend", "playback_format"]
    search_fields = ["media__title", "media__friendly_token", "storage_key", "direct_url"]


@admin.register(PremiumMediaUnlock)
class PremiumMediaUnlockAdmin(admin.ModelAdmin):
    list_display = ["id", "user", "media", "source_type", "unlocked_at", "revoked_at"]
    list_filter = ["source_type", "revoked_at"]
    search_fields = ["user__username", "media__title", "media__friendly_token"]


@admin.register(MediaPurchase)
class MediaPurchaseAdmin(admin.ModelAdmin):
    list_display = ["id", "user", "media", "price_tokens", "created_at"]
    search_fields = ["user__username", "media__title", "media__friendly_token"]


class PremiumCollectionMediaInline(admin.TabularInline):
    model = PremiumCollectionMedia
    extra = 0


@admin.register(PremiumCollection)
class PremiumCollectionAdmin(admin.ModelAdmin):
    list_display = ["id", "creator", "name", "slug", "is_active", "created_at"]
    list_filter = ["is_active"]
    search_fields = ["creator__username", "name", "slug"]
    inlines = [PremiumCollectionMediaInline]


@admin.register(CreatorSubscriptionPlan)
class CreatorSubscriptionPlanAdmin(admin.ModelAdmin):
    list_display = ["id", "creator", "code", "name", "price_tokens", "access_policy", "is_active"]
    list_filter = ["access_policy", "is_active"]
    search_fields = ["creator__username", "code", "name"]
    filter_horizontal = ["included_collections"]


@admin.register(CreatorSubscription)
class CreatorSubscriptionAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "user",
        "creator",
        "plan",
        "status",
        "current_period_start",
        "current_period_end",
        "cancel_at_period_end",
    ]
    list_filter = ["status", "cancel_at_period_end"]
    search_fields = ["user__username", "creator__username", "plan__code"]