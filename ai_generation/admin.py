
from django.contrib import admin

from .models import AIGenerationRequest, AIGenerationRuntimeState


@admin.register(AIGenerationRequest)
class AIGenerationRequestAdmin(admin.ModelAdmin):
    list_display = (
        "public_id",
        "user",
        "status",
        "price_tokens",
        "provider",
        "created_at",
        "completed_at",
    )
    list_filter = ("status", "provider", "created_at")
    search_fields = (
        "public_id",
        "user__username",
        "provider_request_id",
        "prompt",
    )
    readonly_fields = (
        "public_id",
        "charge_txn",
        "charged_at",
        "completed_at",
        "created_at",
        "updated_at",
    )


@admin.register(AIGenerationRuntimeState)
class AIGenerationRuntimeStateAdmin(admin.ModelAdmin):
    list_display = ("key", "current_generation", "updated_at")
    readonly_fields = ("updated_at",)
