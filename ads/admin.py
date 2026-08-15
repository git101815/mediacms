from django.contrib import admin

from .models import AdCampaign, AdSettlementBatch


@admin.action(description="Approve selected campaigns")
def approve_campaigns(modeladmin, request, queryset):
    for campaign in queryset:
        campaign.review_status = AdCampaign.REVIEW_APPROVED
        campaign.review_note = ""
        campaign.save(update_fields=["review_status", "review_note", "updated_at"])


@admin.action(description="Reject selected campaigns")
def reject_campaigns(modeladmin, request, queryset):
    for campaign in queryset:
        campaign.review_status = AdCampaign.REVIEW_REJECTED
        campaign.save(update_fields=["review_status", "updated_at"])


@admin.register(AdCampaign)
class AdCampaignAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "advertiser",
        "placement",
        "pricing_model",
        "bid_microtokens",
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
    )
    readonly_fields = (
        "impressions",
        "clicks",
        "spend_microtokens",
        "created_at",
        "updated_at",
    )
    actions = (approve_campaigns, reject_campaigns)

    def has_delete_permission(self, request, obj=None):
        return False


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
    readonly_fields = [field.name for field in AdSettlementBatch._meta.fields]
