from django import forms
from django.contrib import admin, messages

from .models import (
    TokenWallet,
    LedgerTransaction,
    LedgerEntry,
    LedgerOutbox,
    LedgerSaga,
    LedgerSagaStep,
    LedgerHold,
    LedgerVelocityWindow,
    WalletRequest,
    DepositSession,
    ObservedOnchainTransfer,
    DepositAddress,
    InternalAPIRequestNonce,
    TokenPack,
)
from .services import complete_wallet_withdrawal_request, reject_wallet_request

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
    list_display = (
        "id",
        "user",
        "wallet_type",
        "system_key",
        "balance",
        "held_balance",
        "risk_status",
        "review_required",
        "hourly_outflow_limit",
        "daily_outflow_limit",
        "updated_at",
    )
    list_filter = ("wallet_type", "risk_status", "review_required", "allow_negative")
    search_fields = ("user__username", "user__email")
    readonly_fields = ("user", "balance", "created_at", "updated_at")

class WalletRequestReviewAdminForm(forms.ModelForm):
    ACTION_NONE = ""
    ACTION_REJECT = "reject"
    ACTION_COMPLETE = "complete"

    review_action = forms.ChoiceField(
        required=False,
        choices=(
            (ACTION_NONE, "No review action"),
            (ACTION_REJECT, "Reject request"),
            (ACTION_COMPLETE, "Mark paid / completed"),
        ),
        help_text="Choose one review action and click Save.",
    )
    review_payout_txid = forms.CharField(
        required=False,
        max_length=128,
        help_text="Required when marking a withdrawal request as paid.",
    )
    review_rejection_reason = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 3}),
        help_text="Optional reason stored on rejected requests.",
    )

    class Meta:
        model = WalletRequest
        fields = "__all__"

    def clean(self):
        cleaned_data = super().clean()
        action = (cleaned_data.get("review_action") or "").strip()
        payout_txid = (cleaned_data.get("review_payout_txid") or "").strip()

        if action == self.ACTION_COMPLETE and not payout_txid:
            raise forms.ValidationError("Payout txid is required when marking a request as paid.")

        return cleaned_data

@admin.register(WalletRequest)
class WalletRequestAdmin(admin.ModelAdmin):
    form = WalletRequestReviewAdminForm

    list_display = (
        "id",
        "wallet",
        "request_type",
        "status",
        "amount",
        "asset_code",
        "reference",
        "destination_address",
        "payout_txid",
        "created_by",
        "reviewed_by",
        "created_at",
        "updated_at",
    )
    list_filter = ("request_type", "status", "asset_code")
    search_fields = (
        "reference",
        "wallet__user__username",
        "wallet__user__email",
        "destination_address",
        "notes",
        "rejection_reason",
        "payout_txid",
    )
    readonly_fields = (
        "wallet",
        "request_type",
        "status",
        "amount",
        "asset_code",
        "destination_address",
        "reference",
        "notes",
        "rejection_reason",
        "payout_txid",
        "metadata",
        "metadata_version",
        "hold",
        "linked_ledger_txn",
        "created_by",
        "reviewed_by",
        "reviewed_at",
        "completed_at",
        "created_at",
        "updated_at",
    )
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "wallet",
                    "request_type",
                    "status",
                    "amount",
                    "asset_code",
                    "destination_address",
                    "reference",
                    "notes",
                )
            },
        ),
        (
            "Processing",
            {
                "fields": (
                    "hold",
                    "linked_ledger_txn",
                    "payout_txid",
                    "rejection_reason",
                    "created_by",
                    "reviewed_by",
                    "reviewed_at",
                    "completed_at",
                )
            },
        ),
        (
            "Metadata",
            {
                "classes": ("collapse",),
                "fields": (
                    "metadata",
                    "metadata_version",
                    "created_at",
                    "updated_at",
                ),
            },
        ),
        (
            "Review action",
            {
                "fields": (
                    "review_action",
                    "review_payout_txid",
                    "review_rejection_reason",
                ),
                "description": "Use one review action per save. Mark paid requires a payout txid.",
            },
        ),
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        if not change:
            return

        review_action = (form.cleaned_data.get("review_action") or "").strip()
        if not review_action:
            super().save_model(request, obj, form, change)
            return

        if review_action == WalletRequestReviewAdminForm.ACTION_REJECT:
            updated_request = reject_wallet_request(
                actor=request.user,
                wallet_request=obj,
                rejection_reason=form.cleaned_data.get("review_rejection_reason", ""),
            )
            self.message_user(
                request,
                f"Wallet request {updated_request.reference} rejected.",
                level=messages.SUCCESS,
            )
            return

        if review_action == WalletRequestReviewAdminForm.ACTION_COMPLETE:
            updated_request = complete_wallet_withdrawal_request(
                actor=request.user,
                wallet_request=obj,
                payout_txid=form.cleaned_data.get("review_payout_txid", ""),
            )
            self.message_user(
                request,
                f"Wallet request {updated_request.reference} marked as paid.",
                level=messages.SUCCESS,
            )
            return

        super().save_model(request, obj, form, change)

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
        "last_attempt_at",
        "next_retry_at",
        "dispatched_at",
        "fail_count",
        "redrive_count",
        "dead_lettered_at",
        "metadata_version",
    )
    list_filter = ("status", "topic", "metadata_version")
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
        "last_attempt_at",
        "next_retry_at",
        "redrive_count",
        "last_redriven_at",
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
    list_filter = ("status", "saga_type", "metadata_version")
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

@admin.register(LedgerHold)
class LedgerHoldAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "wallet",
        "amount",
        "released",
        "created_by",
        "released_by",
        "created_at",
        "released_at",
    )
    list_filter = ("released", "metadata_version")
    search_fields = ("wallet__user__username", "reason")
    readonly_fields = (
        "wallet",
        "amount",
        "reason",
        "released",
        "created_by",
        "released_by",
        "metadata",
        "metadata_version",
        "created_at",
        "released_at",
    )

@admin.register(LedgerVelocityWindow)
class LedgerVelocityWindowAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "wallet",
        "action",
        "window_seconds",
        "amount",
        "count",
        "window_start",
        "updated_at",
    )
    list_filter = ("action", "window_seconds")
    search_fields = ("wallet__user__username",)
    readonly_fields = (
        "wallet",
        "action",
        "window_seconds",
        "amount",
        "count",
        "window_start",
        "created_at",
        "updated_at",
    )

@admin.register(DepositSession)
class DepositSessionAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "public_id",
        "user",
        "wallet",
        "chain",
        "asset_code",
        "deposit_address",
        "status",
        "required_confirmations",
        "confirmations",
        "observed_amount",
        "credited_ledger_txn",
        "expires_at",
        "created_at",
    )
    list_filter = ("chain", "asset_code", "status", "required_confirmations")
    search_fields = (
        "public_id",
        "user__username",
        "user__email",
        "deposit_address",
        "observed_txid",
        "address_derivation_ref",
    )
    readonly_fields = (
        "public_id",
        "user",
        "wallet",
        "chain",
        "asset_code",
        "token_contract_address",
        "deposit_address",
        "address_derivation_ref",
        "status",
        "min_amount",
        "required_confirmations",
        "expires_at",
        "observed_txid",
        "observed_amount",
        "confirmations",
        "credited_ledger_txn",
        "created_by",
        "metadata",
        "metadata_version",
        "created_at",
        "updated_at",
    )


@admin.register(ObservedOnchainTransfer)
class ObservedOnchainTransferAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "event_key",
        "chain",
        "asset_code",
        "txid",
        "log_index",
        "block_number",
        "to_address",
        "amount",
        "confirmations",
    )
    list_filter = ("chain", "asset_code")
    search_fields = (
        "event_key",
        "txid",
        "to_address",
        "from_address",
        "token_contract_address",
    )
    readonly_fields = (
        "event_key",
        "chain",
        "txid",
        "log_index",
        "block_number",
        "from_address",
        "to_address",
        "token_contract_address",
        "asset_code",
        "amount",
        "confirmations",
    )

@admin.register(DepositAddress)
class DepositAddressAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "display_label",
        "chain",
        "asset_code",
        "token_contract_address",
        "address",
        "status",
        "required_confirmations",
        "min_amount",
        "allocated_deposit_session",
        "created_at",
    )
    list_filter = ("status", "chain", "asset_code")
    search_fields = ("display_label", "address", "address_derivation_ref", "token_contract_address")
    readonly_fields = (
        "display_label",
        "chain",
        "asset_code",
        "token_contract_address",
        "address",
        "address_derivation_ref",
        "required_confirmations",
        "min_amount",
        "session_ttl_seconds",
        "status",
        "allocated_deposit_session",
        "metadata",
        "metadata_version",
        "created_at",
        "updated_at",
    )

@admin.register(InternalAPIRequestNonce)
class InternalAPIRequestNonceAdmin(ReadOnlyAdmin):
    list_display = (
        "id",
        "service_name",
        "nonce",
        "request_sha256",
        "created_at",
        "expires_at",
    )
    list_filter = ("service_name",)
    search_fields = ("service_name", "nonce", "request_sha256")
    readonly_fields = (
        "service_name",
        "nonce",
        "request_sha256",
        "created_at",
        "expires_at",
    )

@admin.register(TokenPack)
class TokenPackAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "code",
        "name",
        "token_amount",
        "gross_stable_amount",
        "badge_text",
        "is_active",
        "sort_order",
        "updated_at",
    )
    list_filter = ("is_active",)
    search_fields = ("code", "name", "description", "badge_text")
    readonly_fields = ("created_at", "updated_at", "metadata_version")