from datetime import timedelta
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from files.views import _build_wallet_deposit_options
from ledger.models import (
    LEDGER_RISK_STATUS_REVIEW,
    DepositAddress,
    DepositSession,
    WalletRequest,
)
from ledger.providers.paygate import PAYGATE_PROVIDER_KEY
from ledger.services import (
    apply_ledger_transaction,
    reverse_ledger_transaction,
    set_wallet_risk_status,
)

from .base import BaseLedgerTestCase


class TestWalletViewFunctional(BaseLedgerTestCase):
    def _enable_creator_withdrawals(self, user):
        user.advancedUser = True
        user.save(update_fields=["advancedUser"])

    def _create_crypto_route(self):
        return DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address=(
                "0xdac17f958d2ee523a2206206994597c13d831ec7"
            ),
            display_label="Ethereum · USDT",
            address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/10",
            derivation_index=10,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

    def _credit_wallet(self, amount_units):
        return apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -amount_units),
                (self.w1, amount_units),
            ],
            created_by=self.u1,
            memo="Funding top-up",
        )

    def test_authenticated_wallet_context_contains_real_balance_and_activity(self):
        self._credit_wallet(500_000_000)
        self.client.force_login(self.u1)

        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["wallet_auth_required"])
        self.assertEqual(response.context["wallet"].pk, self.w1.pk)
        self.assertEqual(
            response.context["available_balance_units"],
            500_000_000,
        )
        self.assertEqual(
            [row["memo"] for row in response.context["transaction_rows"]],
            ["Funding top-up"],
        )
        self.assertEqual(
            response.context["transaction_rows"][0]["delta"],
            500_000_000,
        )

    def test_wallet_risk_permission_controls_reason_visibility(self):
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

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["can_view_risk_reason"])
        self.assertEqual(
            response.context["wallet_banner"]["tone"],
            "warning",
        )
        self.assertFalse(
            response.context["wallet_actions"]["can_deposit"]
        )
        self.assertFalse(
            response.context["wallet_actions"]["can_withdraw"]
        )

        self.grant_perm(self.u1, "can_view_wallet_risk")
        response = self.client.get(reverse("wallet"))

        self.assertTrue(response.context["can_view_risk_reason"])

    def test_wallet_filters_transaction_tabs_and_invalid_filters(self):
        self._credit_wallet(300_000_000)
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

        response = self.client.get(
            reverse("wallet"),
            {"tab": "purchases"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["active_tab"], "purchases")
        self.assertEqual(
            [row["memo"] for row in response.context["transaction_rows"]],
            ["Purchase row"],
        )

        invalid = self.client.get(
            reverse("wallet"),
            {"tab": "invalid", "status": "invalid"},
        )
        self.assertEqual(invalid.context["active_tab"], "all")
        self.assertEqual(invalid.context["active_status"], "all")

    def test_wallet_filters_reversed_transactions(self):
        self._credit_wallet(500_000_000)
        purchase = apply_ledger_transaction(
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
            original_txn=purchase,
            created_by=self.u1,
            memo="Reversed purchase",
        )
        self.client.force_login(self.u1)

        response = self.client.get(
            reverse("wallet"),
            {"tab": "all", "status": "reversed"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["active_status"], "reversed")
        self.assertEqual(
            [row["memo"] for row in response.context["transaction_rows"]],
            ["Reversed purchase"],
        )

    def test_wallet_paginates_purchase_activity(self):
        total_required = sum(
            (index + 1) * 1_000_000
            for index in range(25)
        )
        self._credit_wallet(total_required)
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
        page_one = self.client.get(
            reverse("wallet"),
            {"tab": "purchases"},
        )
        page_two = self.client.get(
            reverse("wallet"),
            {"tab": "purchases", "page": 2},
        )

        self.assertEqual(page_one.context["page_obj"].number, 1)
        self.assertEqual(page_two.context["page_obj"].number, 2)
        self.assertEqual(
            page_one.context["page_obj"].paginator.per_page,
            20,
        )
        self.assertEqual(
            [row["memo"] for row in page_one.context["transaction_rows"]],
            [f"Purchase {index}" for index in range(24, 4, -1)],
        )
        self.assertEqual(
            [row["memo"] for row in page_two.context["transaction_rows"]],
            [f"Purchase {index}" for index in range(4, -1, -1)],
        )

    def test_wallet_action_state_uses_creator_flag_and_balance(self):
        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertFalse(
            response.context["wallet_actions"]["show_withdraw"]
        )
        self.assertFalse(
            response.context["wallet_actions"]["can_withdraw"]
        )

        self._enable_creator_withdrawals(self.u1)
        self._credit_wallet(100_000_000)
        response = self.client.get(reverse("wallet"))

        self.assertTrue(
            response.context["wallet_actions"]["show_withdraw"]
        )
        self.assertTrue(
            response.context["wallet_actions"]["can_withdraw"]
        )

    @patch("ledger.services._derive_session_deposit_address")
    def test_wallet_deposit_request_creates_session_not_wallet_request(
        self,
        mocked_derive,
    ):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "m/44'/60'/0'/0/11",
        )
        self._create_crypto_route()
        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            WalletRequest.objects.filter(
                request_type=WalletRequest.REQUEST_TYPE_DEPOSIT
            ).count(),
            0,
        )
        session = DepositSession.objects.get(wallet=self.w1)
        self.assertEqual(
            response.url,
            reverse(
                "wallet_deposit_session",
                kwargs={"public_id": session.public_id},
            ),
        )
        self.assertEqual(
            (session.metadata or {})["token_pack"]["code"],
            self.default_token_pack.code,
        )

    @patch("ledger.services._derive_session_deposit_address")
    def test_wallet_deposit_request_reuses_existing_active_session(
        self,
        mocked_derive,
    ):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "m/44'/60'/0'/0/11",
        )
        self._create_crypto_route()
        self.client.force_login(self.u1)

        first = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )
        second = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 302)
        self.assertEqual(
            DepositSession.objects.filter(wallet=self.w1).count(),
            1,
        )
        self.assertEqual(first.url, second.url)

    def test_wallet_withdrawal_request_creates_hold(self):
        self._enable_creator_withdrawals(self.u1)
        self._credit_wallet(500_000_000)
        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "120",
                "payout_asset_code": "USDT",
                "payout_chain": "ethereum",
                "destination_address": "0xabc123",
                "notes": "First withdrawal",
            },
        )

        self.assertEqual(response.status_code, 302)
        request_row = WalletRequest.objects.get(
            wallet=self.w1,
            request_type=WalletRequest.REQUEST_TYPE_WITHDRAWAL,
        )
        self.assertEqual(
            request_row.status,
            WalletRequest.STATUS_PENDING,
        )
        self.assertEqual(request_row.amount, 120_000_000)
        self.assertEqual(request_row.payout_asset_code, "USDT")
        self.assertEqual(request_row.payout_chain, "ethereum")
        self.assertEqual(
            request_row.destination_address,
            "0xabc123",
        )
        self.assertEqual(request_row.metadata["payout_asset_code"], "USDT")
        self.assertEqual(request_row.metadata["payout_chain"], "ethereum")
        self.assertEqual(
            request_row.hold.metadata["payout_asset_code"],
            "USDT",
        )
        self.assertEqual(
            request_row.hold.metadata["payout_chain"],
            "ethereum",
        )
        self.assertIsNotNone(request_row.hold)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 500_000_000)
        self.assertEqual(self.w1.held_balance, 120_000_000)

    def test_wallet_rejects_withdrawal_for_non_creator(self):
        self._credit_wallet(100_000_000)
        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "10",
                "payout_asset_code": "USDT",
                "payout_chain": "ethereum",
                "destination_address": "0xabc",
                "notes": "",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(WalletRequest.objects.exists())
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.held_balance, 0)

    def test_wallet_rejects_unsupported_withdrawal_crypto_network_pair(self):
        self._enable_creator_withdrawals(self.u1)
        self._credit_wallet(100_000_000)
        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "10",
                "payout_asset_code": "BNB",
                "payout_chain": "ethereum",
                "destination_address": "0xabc",
                "notes": "",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("open_modal=withdraw", response.url)
        self.assertFalse(WalletRequest.objects.exists())
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.held_balance, 0)

    def test_wallet_rejects_withdrawal_above_available_balance(self):
        self._enable_creator_withdrawals(self.u1)
        self._credit_wallet(100_000_000)
        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "150",
                "payout_asset_code": "USDT",
                "payout_chain": "ethereum",
                "destination_address": "0xabc",
                "notes": "",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertFalse(WalletRequest.objects.exists())
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.held_balance, 0)

    def test_deposit_session_status_and_cancel_are_owner_scoped(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address=(
                "0xdac17f958d2ee523a2206206994597c13d831ec7"
            ),
            deposit_address=(
                "0x1212121212121212121212121212121212121212"
            ),
            address_derivation_ref="m/44'/60'/0'/0/15",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=1_000_000,
        )

        self.client.force_login(self.u2)
        forbidden = self.client.get(
            reverse(
                "wallet_deposit_session_status",
                kwargs={"public_id": session.public_id},
            )
        )
        self.assertEqual(forbidden.status_code, 404)

        self.client.force_login(self.u1)
        status_response = self.client.get(
            reverse(
                "wallet_deposit_session_status",
                kwargs={"public_id": session.public_id},
            )
        )
        self.assertEqual(status_response.status_code, 200)
        payload = status_response.json()
        self.assertEqual(payload["public_id"], str(session.public_id))
        self.assertEqual(payload["min_amount"], 1_000_000)
        self.assertEqual(
            payload["status"],
            "awaiting_payment",
        )

        cancel_response = self.client.post(
            reverse(
                "wallet_deposit_session_cancel",
                kwargs={"public_id": session.public_id},
            )
        )
        self.assertEqual(cancel_response.status_code, 302)
        session.refresh_from_db()
        self.assertEqual(
            session.status,
            DepositSession.STATUS_CANCELED,
        )

    @override_settings(
        WALLET_PAYMENT_METHOD_PRICE_BPS={"crypto": 1000},
        WALLET_PAYMENT_METHOD_PRICE_FIXED_CANONICAL={
            "crypto": 0.3
        },
    )
    @patch("ledger.services._derive_session_deposit_address")
    def test_wallet_deposit_request_applies_payment_group_fees(
        self,
        mocked_derive,
    ):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa77",
            "m/44'/60'/0'/0/77",
        )
        self._create_crypto_route()
        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )

        self.assertEqual(response.status_code, 302)
        session = DepositSession.objects.get(wallet=self.w1)
        token_pack = (session.metadata or {})["token_pack"]

        self.assertEqual(
            token_pack["net_stable_amount"],
            1_000_000,
        )
        self.assertEqual(
            token_pack["fixed_fee_stable_amount"],
            300_000,
        )
        self.assertEqual(
            token_pack["percentage_fee_stable_amount"],
            100_000,
        )
        self.assertEqual(
            token_pack["fee_stable_amount"],
            400_000,
        )
        self.assertEqual(
            token_pack["gross_stable_amount"],
            1_400_000,
        )
        self.assertEqual(session.min_amount, 1_400_000)
        self.assertEqual(
            session.expected_onchain_raw_amount,
            1_400_000,
        )

    @override_settings(
        WALLET_PAYMENT_METHOD_PRICE_BPS={
            "paypal_us": 800,
            "revolut_eu": 500,
            "crypto": 0,
        },
        WALLET_PAYMENT_METHOD_PRICE_FIXED_CANONICAL={
            "paypal_us": 0.3,
            "revolut_eu": 0.3,
            "crypto": 0,
        },
    )
    @patch(
        "files.views.get_mtpelerin_deposit_options",
        return_value=[],
    )
    @patch(
        "files.views.get_dfx_deposit_options",
        return_value=[],
    )
    @patch(
        "files.views.list_available_deposit_options",
        return_value=[],
    )
    @patch(
        "files.views.get_malum_deposit_option",
        return_value=None,
    )
    @patch("files.views.get_paygate_deposit_options")
    def test_wallet_deposit_options_group_paygate_providers_with_fees(
        self,
        mocked_paygate_options,
        _mocked_malum_option,
        _mocked_crypto_options,
        _mocked_dfx_options,
        _mocked_mtpelerin_options,
    ):
        mocked_paygate_options.return_value = [
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
        by_group = {
            item["payment_group_key"]: item
            for item in options
        }

        self.assertEqual(
            by_group["paypal_us"]["provider_key"],
            PAYGATE_PROVIDER_KEY,
        )
        self.assertEqual(
            by_group["paypal_us"]["paygate_provider_id"],
            "paypal",
        )
        self.assertEqual(
            by_group["revolut_eu"]["paygate_provider_id"],
            "revolut",
        )
        self.assertEqual(
            by_group["paypal_us"]["payment_price_bps"],
            800,
        )
        self.assertEqual(
            by_group["paypal_us"][
                "payment_price_fixed_canonical"
            ],
            300_000,
        )
        self.assertEqual(
            by_group["revolut_eu"]["payment_price_bps"],
            500,
        )
        self.assertEqual(
            by_group["revolut_eu"][
                "payment_price_fixed_canonical"
            ],
            300_000,
        )
