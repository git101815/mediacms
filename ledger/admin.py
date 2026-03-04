from django.contrib import admin
from .models import TokenWallet, LedgerTransaction, LedgerEntry

@admin.register(TokenWallet)
class TokenWalletAdmin(admin.ModelAdmin):
    list_display = ("user", "balance", "updated_at")
    search_fields = ("user__username", "user__email")
    readonly_fields = ("created_at", "updated_at")

@admin.register(LedgerTransaction)
class LedgerTransactionAdmin(admin.ModelAdmin):
    list_display = ("id", "kind", "external_id", "created_by", "created_at")
    search_fields = ("external_id", "kind", "created_by__username")
    readonly_fields = ("created_at",)

@admin.register(LedgerEntry)
class LedgerEntryAdmin(admin.ModelAdmin):
    list_display = ("id", "txn", "wallet", "delta", "balance_after", "created_at")
    search_fields = ("wallet__user__username", "txn__id")
    readonly_fields = ("created_at",)