from django.contrib import admin
from django.db.models import Count

from .forms import AdCreativeForm
from .models import (
    AdCampaign,
    AdCampaignCreative,
    AdCreative,
    AdSettlementBatch,
)


@admin.action(description="Approve selected campaigns")
def approve_campaigns(modeladmin, request, queryset):
    for campaign in queryset:
        campaign.review_status = AdCampaign.REVIEW_APPROVED
        campaign.review_note = ""
        campaign.save(
            update_fields=[
                "review_status",
                "review_note",
                "updated_at",
            ]
        )


@admin.action(description="Reject selected campaigns")
def reject_campaigns(modeladmin, request, queryset):
    for campaign in queryset:
        campaign.review_status = AdCampaign.REVIEW_REJECTED
        campaign.save(
            update_fields=["review_status", "updated_at"]
        )


@admin.action(description="Approve selected creatives")
def approve_creatives(modeladmin, request, queryset):
    for creative in queryset:
        creative.review_status = AdCreative.REVIEW_APPROVED
        creative.review_note = ""
        creative.save(
            update_fields=[
                "review_status",
                "review_note",
                "updated_at",
            ]
        )


@admin.action(description="Reject selected creatives")
def reject_creatives(modeladmin, request, queryset):
    for creative in queryset:
        creative.review_status = AdCreative.REVIEW_REJECTED
        creative.save(
            update_fields=["review_status", "updated_at"]
        )


class AdCampaignCreativeInline(admin.TabularInline):
    model = AdCampaignCreative
    extra = 0
    fields = ("creative", "enabled", "weight")


@admin.register(AdCampaign)
class AdCampaignAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "advertiser",
        "placement",
        "pricing_model",
        "bid_microtokens",
        "creative_count",
        "review_status",
        "delivery_status",
        "impressions",
        "clicks",
        "spend_microtokens",
        "updated_at",
    )
    list_filter = (
        "placement",
        "pricing_model",
        "review_status",
        "delivery_status",
    )
    search_fields = (
        "name",
        "advertiser__username",
        "advertiser__email",
        "target_url",
        "creatives__name",
    )
    readonly_fields = (
        "impressions",
        "clicks",
        "spend_microtokens",
        "created_at",
        "updated_at",
    )
    actions = (approve_campaigns, reject_campaigns)
    inlines = (AdCampaignCreativeInline,)

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .annotate(_creative_count=Count("creatives", distinct=True))
        )

    @admin.display(description="Creatives")
    def creative_count(self, obj):
        return int(getattr(obj, "_creative_count", 0))

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(AdCreative)
class AdCreativeAdmin(admin.ModelAdmin):
    form = AdCreativeForm
    list_display = (
        "id",
        "name",
        "advertiser",
        "placement",
        "review_status",
        "campaign_count",
        "updated_at",
    )
    list_filter = ("placement", "review_status")
    search_fields = (
        "name",
        "advertiser__username",
        "advertiser__email",
    )
    readonly_fields = ("created_at", "updated_at")
    actions = (approve_creatives, reject_creatives)

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .annotate(_campaign_count=Count("campaigns", distinct=True))
        )

    @admin.display(description="Campaigns")
    def campaign_count(self, obj):
        return int(getattr(obj, "_campaign_count", 0))


@admin.register(AdSettlementBatch)
class AdSettlementBatchAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "campaign",
        "advertiser",
        "amount_microtokens",
        "impressions",
        "clicks",
        "status",
        "posted_at",
        "redis_acked_at",
    )
    list_filter = ("status", "redis_acked_at")
    search_fields = (
        "id",
        "campaign__name",
        "advertiser__username",
        "ledger_txn__external_id",
    )
    readonly_fields = [
        field.name for field in AdSettlementBatch._meta.fields
    ]
