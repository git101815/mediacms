from django.contrib import admin
from .models import TokenWallet, LedgerTransaction, LedgerEntry

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
    list_display = ("id", "kind", "external_id", "created_by", "created_at")
    search_fields = ("external_id", "kind", "created_by__username")
    readonly_fields = ("kind", "external_id", "created_by", "memo", "metadata", "created_at")

@admin.register(LedgerEntry)
class LedgerEntryAdmin(ReadOnlyAdmin):
    list_display = ("id", "txn", "wallet", "delta", "balance_after", "created_at")
    search_fields = ("wallet__user__username", "txn__id")
    readonly_fields = ("txn", "wallet", "delta", "balance_after", "created_at")