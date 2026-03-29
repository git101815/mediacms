from django.contrib import admin
from .models import TokenWallet, LedgerTransaction, LedgerEntry,LedgerOutbox, LedgerSaga, LedgerSagaStep

class ReadOnlyAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_view_permission(self, request, obj=None):
        return True

@admin.register(TokenWallet)
class TokenWalletAdmin(ReadOnlyAdmin):
    list_display = ("user", "balance", "updated_at")
    search_fields = ("user__username", "user__email")
    readonly_fields = ("user", "balance", "created_at", "updated_at")

@admin.register(LedgerTransaction)
class LedgerTransactionAdmin(ReadOnlyAdmin):
    list_display = ("id", "kind", "status", "external_id", "reversal_of", "created_by", "created_at", "metadata_version",)
    search_fields = ("external_id", "kind", "created_by__username")
    readonly_fields = ("kind", "external_id", "created_by", "memo", "metadata", "created_at", "status", "reversal_of", "metadata_version")

@admin.register(LedgerEntry)
class LedgerEntryAdmin(ReadOnlyAdmin):
    list_display = ("id", "txn", "wallet", "delta", "balance_after", "created_at")
    search_fields = ("wallet__user__username", "txn__id")
    readonly_fields = ("txn", "wallet", "delta", "balance_after", "created_at")

@admin.register(LedgerOutbox)
class LedgerOutboxAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "topic",
        "status",
        "txn",
        "aggregate_id",
        "created_at",
        "dispatched_at",
        "fail_count",
        "dead_lettered_at",
        "metadata_version",
    )
    search_fields = ("topic", "txn__id", "txn__external_id")
    readonly_fields = (
        "txn",
        "topic",
        "aggregate_type",
        "aggregate_id",
        "status",
        "payload",
        "created_at",
        "dispatched_at",
        "fail_count",
        "last_error",
        "metadata_version",
        "dead_lettered_at",
        "dead_letter_reason",
    )

@admin.register(LedgerSaga)
class LedgerSagaAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "saga_type",
        "status",
        "external_id",
        "created_by",
        "created_at",
        "started_at",
        "completed_at",
        "failed_at",
        "compensated_at",
    )
    search_fields = ("external_id", "saga_type", "created_by__username")
    readonly_fields = (
        "saga_type",
        "external_id",
        "status",
        "created_by",
        "metadata",
        "metadata_version",
        "started_at",
        "completed_at",
        "failed_at",
        "compensated_at",
        "last_error",
        "created_at",
    )

    @admin.register(LedgerSagaStep)
    class LedgerSagaStepAdmin(ReadOnlyAdmin):
        list_display = (
            "id",
            "saga",
            "step_key",
            "step_order",
            "status",
            "txn",
            "compensation_txn",
            "created_at",
        )
        search_fields = ("step_key", "saga__external_id", "txn__external_id")
        readonly_fields = (
            "saga",
            "step_key",
            "step_order",
            "status",
            "txn",
            "compensation_txn",
            "payload",
            "metadata_version",
            "started_at",
            "completed_at",
            "failed_at",
            "compensated_at",
            "last_error",
            "created_at",
        )
