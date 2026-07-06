from datetime import timedelta
from unittest.mock import patch
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from ledger.models import (
    LEDGER_RISK_STATUS_REVIEW,
    DepositAddress,
    DepositSession,
    WalletRequest,
)
from ledger.services import (
    apply_ledger_transaction,
    create_pending_ledger_transaction,
    create_wallet_withdrawal_request,
    reverse_ledger_transaction,
    set_wallet_risk_status,
)
from files.views import (
    _build_wallet_deposit_options,
    _build_wallet_token_pack_rows,
)
from ledger.providers.paygate import PAYGATE_PROVIDER_KEY
from .base import BaseLedgerTestCase


class TestWalletView(BaseLedgerTestCase):
    def _enable_creator_withdrawals(self, user):
        user.advancedUser = True
        user.save(update_fields=["advancedUser"])

    def _status_option_keys(self, response):
        return [item["key"] for item in response.context["status_select_options"]]

    def test_wallet_page_requires_login(self):
        response = self.client.get(reverse("wallet"))
        self.assertEqual(response.status_code, 302)

    def test_wallet_page_shows_balances_holds_and_entries(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -500_000_000),
                (self.w1, 500_000_000),
            ],
            created_by=self.u1,
            memo="Initial top-up",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Wallet")
        self.assertContains(response, "500.00")
        self.assertContains(response, "500.00")
        self.assertContains(response, "0.00")
        self.assertContains(response, "Initial top-up")

    def test_wallet_page_hides_internal_risk_reason_without_permission(self):
        self.grant_perm(self.operator, "can_manage_wallet_risk")
        set_wallet_risk_status(
            actor=self.operator,
            wallet=self.w1,
            risk_status=LEDGER_RISK_STATUS_REVIEW,
            reason="Manual review required",
            review_required=True,
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertContains(response, "Wallet under review")
        self.assertNotContains(response, "Manual review required")

    def test_wallet_page_shows_review_banner_with_permission(self):
        self.grant_perm(self.operator, "can_manage_wallet_risk")
        self.grant_perm(self.u1, "can_view_wallet_risk")
        set_wallet_risk_status(
            actor=self.operator,
            wallet=self.w1,
            risk_status=LEDGER_RISK_STATUS_REVIEW,
            reason="Manual review required",
            review_required=True,
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Wallet under review")

    def test_wallet_page_filters_transaction_tabs(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -300_000_000),
                (self.w1, 300_000_000),
            ],
            created_by=self.u1,
            memo="Funding top-up",
        )
        apply_ledger_transaction(
            actor=self.operator,
            kind="purchase",
            entries=[
                (self.w1, -150_000_000),
                (self.issuance, 150_000_000),
            ],
            created_by=self.u1,
            memo="Purchase row",
        )
        apply_ledger_transaction(
            actor=self.operator,
            kind="transfer",
            entries=[
                (self.w1, -75_000_000),
                (self.w2, 75_000_000),
            ],
            created_by=self.u1,
            memo="Transfer row",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "purchases"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Purchase row")
        self.assertNotContains(response, "Transfer row")

    def test_wallet_page_filters_reversed_transactions_on_all_tab(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -500_000_000),
                (self.w1, 500_000_000),
            ],
            created_by=self.u1,
            memo="Funding top-up",
        )
        purchase_txn = apply_ledger_transaction(
            actor=self.operator,
            kind="purchase",
            entries=[
                (self.w1, -400_000_000),
                (self.issuance, 400_000_000),
            ],
            created_by=self.u1,
            memo="Posted purchase",
        )
        reverse_ledger_transaction(
            actor=self.operator,
            original_txn=purchase_txn,
            created_by=self.u1,
            memo="Reversed purchase",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "all", "status": "reversed"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Reversed purchase")
        self.assertNotContains(response, "Posted purchase")
        self.assertIn("reversed", self._status_option_keys(response))

    def test_wallet_page_pending_filter_shows_empty_state_when_no_matching_entry_exists(self):
        create_pending_ledger_transaction(
            actor=self.operator,
            kind="withdrawal",
            created_by=self.u1,
            memo="Pending withdrawal",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "all", "status": "pending"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("pending", self._status_option_keys(response))
        self.assertContains(response, "No activity yet")
        self.assertContains(response, "Your wallet activity will appear here.")
        self.assertNotContains(response, "Pending withdrawal")

    def test_wallet_page_invalid_filters_fallback_to_all(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -250_000_000),
                (self.w1, 250_000_000),
            ],
            created_by=self.u1,
            memo="Fallback row",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "nope", "status": "???"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Fallback row")
        self.assertIn("all", self._status_option_keys(response))

    def test_wallet_page_all_tab_exposes_cross_type_status_options(self):
        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "all"})

        self.assertEqual(response.status_code, 200)
        status_keys = self._status_option_keys(response)
        self.assertIn("all", status_keys)
        self.assertIn("pending", status_keys)
        self.assertIn("posted", status_keys)
        self.assertIn("payment_detected", status_keys)
        self.assertIn("completed", status_keys)

    def test_wallet_page_paginates_activity(self):
        total_required = sum((index + 1) * 1_000_000 for index in range(25))

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -total_required),
                (self.w1, total_required),
            ],
            created_by=self.u1,
            memo="Funding top-up",
        )
        for index in range(25):
            apply_ledger_transaction(
                actor=self.operator,
                kind="purchase",
                entries=[
                    (self.w1, -(index + 1) * 1_000_000),
                    (self.issuance, (index + 1) * 1_000_000),
                ],
                created_by=self.u1,
                memo=f"Purchase {index}",
            )

        self.client.force_login(self.u1)
        page_one = self.client.get(reverse("wallet"), {"tab": "purchases"})
        page_two = self.client.get(reverse("wallet"), {"tab": "purchases", "page": 2})

        self.assertContains(page_one, "Purchase 24")
        self.assertContains(page_one, "Purchase 5")
        self.assertNotContains(page_one, "Purchase 4")
        self.assertContains(page_two, "Purchase 4")
        self.assertContains(page_two, "Purchase 0")

    def test_wallet_page_hides_cash_out_for_non_creator(self):
        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Buy tokens")
        self.assertNotContains(response, "Cash out")

    def test_wallet_page_shows_cash_out_for_creator(self):
        self._enable_creator_withdrawals(self.u1)

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Buy tokens")
        self.assertContains(response, "Cash out")

    def test_user_can_open_deposit_session_from_wallet(self):
        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/10",
            required_confirmations=12,
            min_amount=1_000_000,
            session_ttl_seconds=3600,
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                return_tab="all",
                return_status="all",
            ),
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            WalletRequest.objects.filter(request_type=WalletRequest.REQUEST_TYPE_DEPOSIT).count(),
            0,
        )

        session = DepositSession.objects.get(wallet=self.w1)
        self.assertRedirects(
            response,
            reverse("wallet_deposit_session", kwargs={"public_id": session.public_id}),
        )

        wallet_response = self.client.get(reverse("wallet"))
        self.assertContains(wallet_response, "Ethereum · USDT")

    def test_user_can_create_withdrawal_request_from_wallet(self):
        self._enable_creator_withdrawals(self.u1)

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -500_000_000),
                (self.w1, 500_000_000),
            ],
            created_by=self.u1,
            memo="Initial top-up",
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "120",
                "destination_address": "0xabc123",
                "notes": "First withdrawal",
                "return_tab": "all",
                "return_status": "all",
            },
        )

        self.assertEqual(response.status_code, 302)

        wallet_request = WalletRequest.objects.get(
            wallet=self.w1,
            request_type=WalletRequest.REQUEST_TYPE_WITHDRAWAL,
        )
        self.assertEqual(wallet_request.status, WalletRequest.STATUS_PENDING)
        self.assertEqual(wallet_request.amount, 120_000_000)
        self.assertEqual(wallet_request.destination_address, "0xabc123")
        self.assertEqual(wallet_request.notes, "First withdrawal")
        self.assertIsNotNone(wallet_request.hold)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 500_000_000)
        self.assertEqual(self.w1.held_balance, 120_000_000)

        wallet_response = self.client.get(reverse("wallet"), {"tab": "withdrawals"})
        self.assertContains(wallet_response, "First withdrawal")
        self.assertContains(wallet_response, "0xabc123")
        self.assertContains(wallet_response, "120.00")

    def test_withdrawal_request_rejected_for_non_creator(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -100_000_000),
                (self.w1, 100_000_000),
            ],
            created_by=self.u1,
            memo="Small top-up",
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "10",
                "destination_address": "0xoverflow",
                "notes": "No creator rights",
                "return_tab": "all",
                "return_status": "all",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(WalletRequest.objects.count(), 0)

    def test_withdrawal_request_rejected_when_amount_exceeds_available_balance(self):
        self._enable_creator_withdrawals(self.u1)

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -100_000_000),
                (self.w1, 100_000_000),
            ],
            created_by=self.u1,
            memo="Small top-up",
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "150",
                "destination_address": "0xoverflow",
                "notes": "Too much",
                "return_tab": "all",
                "return_status": "all",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(WalletRequest.objects.count(), 0)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.held_balance, 0)

    @patch("ledger.services._derive_session_deposit_address")
    def test_wallet_deposit_request_redirects_to_existing_active_session(self, mocked_derive):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "m/44'/60'/0'/0/1",
        )

        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/10",
            derivation_index=10,
            required_confirmations=12,
            min_amount=1_000_000,
            session_ttl_seconds=3600,
        )

        self.client.force_login(self.u1)

        first = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                return_tab="all",
                return_status="all",
            ),
        )
        second = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                return_tab="all",
                return_status="all",
            ),
        )

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 302)
        self.assertEqual(DepositSession.objects.filter(wallet=self.w1).count(), 1)

    def test_wallet_deposit_session_status_json_contains_min_amount(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x1212121212121212121212121212121212121212",
            address_derivation_ref="m/44'/60'/0'/0/15",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=1_000_000,
        )

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse("wallet_deposit_session_status", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["min_amount"], 1_000_000)
        self.assertEqual(payload["min_amount_display"], "1.00")

    def test_wallet_deposit_session_cancel_marks_session_canceled(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x1313131313131313131313131313131313131313",
            address_derivation_ref="m/44'/60'/0'/0/16",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=1_000_000,
        )

        self.client.force_login(self.u1)
        response = self.client.post(
            reverse("wallet_deposit_session_cancel", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(response.status_code, 302)
        session.refresh_from_db()
        self.assertEqual(session.status, getattr(DepositSession, "STATUS_CANCELED", "canceled"))

    def test_wallet_all_tab_can_filter_deposit_public_status(self):
        awaiting = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            address_derivation_ref="m/44'/60'/0'/0/1",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=1_000_000,
        )
        credited = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/2",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_CREDITED,
            required_confirmations=12,
            min_amount=1_000_000,
        )

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse("wallet"),
            {"tab": "all", "status": "payment_detected"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, credited.deposit_address)
        self.assertNotContains(response, awaiting.deposit_address)

    def test_wallet_withdrawals_tab_can_filter_wallet_request_status(self):
        self._enable_creator_withdrawals(self.u1)

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -500_000_000),
                (self.w1, 500_000_000),
            ],
            created_by=self.u1,
            memo="Initial top-up",
        )

        pending_request = create_wallet_withdrawal_request(
            actor=self.u1,
            wallet=self.w1,
            amount="10",
            destination_address="0x111",
            notes="Pending request",
        )

        completed_request = create_wallet_withdrawal_request(
            actor=self.u1,
            wallet=self.w1,
            amount="5",
            destination_address="0x222",
            notes="Completed request",
        )
        completed_request.status = WalletRequest.STATUS_COMPLETED
        completed_request.save(update_fields=["status", "updated_at"])

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse("wallet"),
            {"tab": "withdrawals", "status": WalletRequest.STATUS_COMPLETED},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, completed_request.reference)
        self.assertNotContains(response, pending_request.reference)

    def test_wallet_token_pack_rows_expose_admin_token_pack_image(self):
        self.default_token_pack.image = "wallet/token_packs/starter.png"
        self.default_token_pack.save(update_fields=["image"])

        rows = _build_wallet_token_pack_rows()
        row = next(item for item in rows if item["code"] == self.default_token_pack.code)

        self.assertIn("wallet/token_packs/starter.png", row["image_url"])

    @override_settings(
        WALLET_PAYMENT_METHOD_PRICE_BPS={"crypto": 1000},
        WALLET_PAYMENT_METHOD_PRICE_FIXED_CANONICAL={"crypto": 0.3},
    )
    @patch("ledger.services._derive_session_deposit_address")
    def test_wallet_deposit_request_applies_payment_group_fixed_and_bps_fees(self, mocked_derive):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa77",
            "m/44'/60'/0'/0/77",
        )

        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/10",
            derivation_index=10,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                return_tab="all",
                return_status="all",
            ),
        )

        self.assertEqual(response.status_code, 302)

        session = DepositSession.objects.get(wallet=self.w1)
        token_pack = (session.metadata or {}).get("token_pack") or {}

        self.assertEqual(token_pack["net_stable_amount"], 1_000_000)
        self.assertEqual(token_pack["fixed_fee_stable_amount"], 300_000)
        self.assertEqual(token_pack["percentage_fee_stable_amount"], 100_000)
        self.assertEqual(token_pack["fee_stable_amount"], 400_000)
        self.assertEqual(token_pack["gross_stable_amount"], 1_400_000)

        self.assertEqual(session.min_amount, 1_400_000)
        self.assertEqual(session.expected_onchain_raw_amount, 1_400_000)

    @override_settings(
        WALLET_PAYMENT_METHOD_PRICE_BPS={
            "credit_card_link": 1000,
            "paypal_us": 800,
            "revolut_eu": 500,
            "crypto": 0,
        },
        WALLET_PAYMENT_METHOD_PRICE_FIXED_CANONICAL={
            "credit_card_link": 0.3,
            "paypal_us": 0.3,
            "revolut_eu": 0.3,
            "crypto": 0,
        },
    )
    @patch("files.views.list_available_deposit_options", return_value=[])
    @patch("files.views.get_malum_deposit_option", return_value=None)
    @patch("files.views.get_paygate_deposit_options")
    def test_wallet_deposit_options_group_paygate_providers_with_icons_and_fees(
        self,
        mocked_paygate_options,
        mocked_malum_option,
        mocked_crypto_options,
    ):
        mocked_paygate_options.return_value = [
            {
                "key": "paygate:usd:stripe:hosted_checkout",
                "label": "Stripe",
                "route_label": "Stripe",
                "network_label": "PayGate",
                "network_display": "PayGate",
                "chain": "paygate",
                "asset_code": "USD",
                "token_contract_address": "",
                "required_confirmations": 1,
                "min_amount": 1_000_000,
                "payment_method_key": "paygate:stripe",
                "payment_method_label": "Stripe",
                "payment_method_type": "provider",
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": "stripe",
            },
            {
                "key": "paygate:usd:paypal:hosted_checkout",
                "label": "PayPal",
                "route_label": "PayPal",
                "network_label": "PayGate",
                "network_display": "PayGate",
                "chain": "paygate",
                "asset_code": "USD",
                "token_contract_address": "",
                "required_confirmations": 1,
                "min_amount": 1_000_000,
                "payment_method_key": "paygate:paypal",
                "payment_method_label": "PayPal",
                "payment_method_type": "provider",
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": "paypal",
            },
            {
                "key": "paygate:usd:revolut:hosted_checkout",
                "label": "Revolut",
                "route_label": "Revolut",
                "network_label": "PayGate",
                "network_display": "PayGate",
                "chain": "paygate",
                "asset_code": "USD",
                "token_contract_address": "",
                "required_confirmations": 1,
                "min_amount": 1_000_000,
                "payment_method_key": "paygate:revolut",
                "payment_method_label": "Revolut",
                "payment_method_type": "provider",
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": "revolut",
            },
        ]

        options = _build_wallet_deposit_options()
        by_group = {item["payment_group_key"]: item for item in options}

        self.assertEqual(
            by_group["credit_card_link"]["payment_group_label"],
            "Credit Card (via Link by Stripe)",
        )
        self.assertEqual(by_group["credit_card_link"]["payment_group_icon_path"], "images/wallet/card.svg")
        self.assertEqual(by_group["credit_card_link"]["payment_price_bps"], 1000)
        self.assertEqual(by_group["credit_card_link"]["payment_price_fixed_canonical"], 300_000)

        self.assertEqual(by_group["paypal_us"]["payment_group_label"], "PayPal (US only)")
        self.assertEqual(by_group["paypal_us"]["payment_group_icon_path"], "images/wallet/paypal.svg")
        self.assertEqual(by_group["paypal_us"]["payment_price_bps"], 800)
        self.assertEqual(by_group["paypal_us"]["payment_price_fixed_canonical"], 300_000)

        self.assertEqual(by_group["revolut_eu"]["payment_group_label"], "Revolut (EU only)")
        self.assertEqual(by_group["revolut_eu"]["payment_group_icon_path"], "images/wallet/revolut.svg")
        self.assertEqual(by_group["revolut_eu"]["payment_price_bps"], 500)
        self.assertEqual(by_group["revolut_eu"]["payment_price_fixed_canonical"], 300_000)