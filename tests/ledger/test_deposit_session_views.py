from datetime import timedelta
from unittest.mock import patch

from django.urls import reverse
from django.utils import timezone

from ledger.models import DepositAddress, DepositSession, TokenPack
from ledger.services import (
    _convert_platform_token_units_to_canonical_stable_units,
    credit_confirmed_deposit_session,
    record_onchain_observation,
)

from .base import BaseLedgerTestCase


class TestDepositSessionViews(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            address_derivation_ref="m/44'/60'/0'/0/11",
            derivation_index=11,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

    def _default_session_metadata(self):
        return {
            "display_label": "Ethereum · USDT",
            "allocation_source": "session_derivation",
            "route_template_id": 1,
            "chain_family": "evm",
            "token_pack": self.default_token_pack_snapshot(),
            "payment_method": {
                "key": "crypto:usdt",
                "type": "crypto",
                "label": "USDT",
                "show_network_step": True,
            },
            "expected_canonical_stable_amount": int(self.default_token_pack.gross_stable_amount),
            "expected_route_raw_amount": int(self.default_token_pack.gross_stable_amount),
        }

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_deposit_session_reuses_existing_active_session(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "m/44'/60'/0'/0/12",
        )

        self.client.force_login(self.u1)

        first = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )
        second = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )

        self.assertEqual(DepositSession.objects.count(), 1)
        session = DepositSession.objects.get(wallet=self.w1)

        expected_url = reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        self.assertRedirects(first, expected_url)
        self.assertRedirects(second, expected_url)
        self.assertEqual(session.derivation_index, 12)
        self.assertEqual(session.derivation_path, "m/44'/60'/0'/0/12")
        self.assertEqual(session.route_key, self.default_deposit_option_key())
        self.assertEqual(session.min_amount, self.default_token_pack.gross_stable_amount)
        self.assertEqual((session.metadata or {}).get("token_pack", {}).get("code"), self.default_token_pack.code)

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_deposit_session_does_not_reuse_expired_session(self, mocked_derive):
        mocked_derive.side_effect = [
            ("0xcccccccccccccccccccccccccccccccccccccccc", "m/44'/60'/0'/0/13"),
            ("0xdddddddddddddddddddddddddddddddddddddddd", "m/44'/60'/0'/0/14"),
        ]

        self.client.force_login(self.u1)

        first = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )
        session = DepositSession.objects.get(wallet=self.w1)
        DepositSession.objects.filter(id=session.id).update(
            expires_at=timezone.now() - timedelta(seconds=1)
        )

        second = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 302)
        self.assertEqual(DepositSession.objects.count(), 2)

    def test_deposit_session_page_is_owner_only(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            route_key=self.default_deposit_option_key(),
            display_label="Ethereum · USDT",
            deposit_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/12",
            derivation_index=12,
            derivation_path="m/44'/60'/0'/0/12",
            expires_at=timezone.now() + timedelta(hours=1),
            metadata=self._default_session_metadata(),
        )

        self.client.force_login(self.u2)
        response = self.client.get(
            reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        )
        self.assertEqual(response.status_code, 404)

    def test_deposit_session_page_renders_min_amount_payload(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            route_key=self.default_deposit_option_key(),
            display_label="Ethereum · USDT",
            deposit_address="0xcccccccccccccccccccccccccccccccccccccccc",
            address_derivation_ref="m/44'/60'/0'/0/13",
            derivation_index=13,
            derivation_path="m/44'/60'/0'/0/13",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=self.default_token_pack.gross_stable_amount,
            metadata=self._default_session_metadata(),
        )

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "0xcccccccccccccccccccccccccccccccccccccccc")
        self.assertContains(response, "Expected amount")
        self.assertContains(response, "1.00 USDT")
        self.assertContains(response, "Starter")

    def test_deposit_session_status_endpoint_returns_payload(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            route_key=self.default_deposit_option_key(),
            display_label="Ethereum · USDT",
            deposit_address="0xdddddddddddddddddddddddddddddddddddddddd",
            address_derivation_ref="m/44'/60'/0'/0/14",
            derivation_index=14,
            derivation_path="m/44'/60'/0'/0/14",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=self.default_token_pack.gross_stable_amount,
            metadata=self._default_session_metadata(),
        )

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse("wallet_deposit_session_status", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], DepositSession.STATUS_AWAITING_PAYMENT)
        self.assertEqual(payload["confirmations"], 0)
        self.assertEqual(payload["deposit_address"], "0xdddddddddddddddddddddddddddddddddddddddd")
        self.assertEqual(payload["required_confirmations"], 12)
        self.assertEqual(payload["min_amount"], self.default_token_pack.gross_stable_amount)
        self.assertEqual(payload["token_pack_name"], self.default_token_pack.name)
        self.assertIn("expires_at", payload)
        self.assertIn("expires_at_iso", payload)

    @patch("ledger.services._derive_session_deposit_address")
    def test_wallet_deposit_status_json_maps_credited_session_to_payment_detected(self, mocked_derive):
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")

        mocked_derive.return_value = (
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "m/44'/60'/0'/0/0",
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(),
        )
        self.assertEqual(response.status_code, 302)

        session = DepositSession.objects.get(wallet=self.w1)

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xviewflow0001",
            log_index=1,
            block_number=987654,
            from_address="0xffffffffffffffffffffffffffffffffffffffff",
            to_address=session.deposit_address,
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=session.min_amount,
            confirmations=session.required_confirmations,
            raw_payload={"source": "view-integration-test", "txid": "0xviewflow0001"},
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        session.refresh_from_db()
        self.assertEqual(session.status, DepositSession.STATUS_CREDITED)

        status_response = self.client.get(
            reverse("wallet_deposit_session_status", kwargs={"public_id": session.public_id}),
            HTTP_X_REQUESTED_WITH="XMLHttpRequest",
        )

        self.assertEqual(status_response.status_code, 200)
        payload = status_response.json()

        self.assertEqual(payload["status"], "payment_detected")
        self.assertEqual(payload["observed_txid"], "0xviewflow0001")
        self.assertEqual(payload["confirmations"], session.required_confirmations)
        self.assertFalse(payload["is_terminal"])

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_deposit_session_converts_canonical_pack_amount_to_bsc_raw_amount(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbfebace943c8adc81341f4e5f34f9f89dbdbee64",
            "m/44'/60'/0'/0/99",
        )

        DepositAddress.objects.create(
            chain="bsc",
            asset_code="USDT",
            token_contract_address="0x55d398326f99059ff775485246999027b3197955",
            display_label="BNB Chain · USDT",
            address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/99",
            derivation_index=99,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        bsc_pack = TokenPack.objects.create(
            code="500_tokens",
            name="500_tokens",
            description="500 tokens",
            badge_text="",
            token_amount=500_000_000,
            gross_stable_amount=7_000_000,
            is_active=True,
            sort_order=10,
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                deposit_option_key="bsc:USDT:0x55d398326f99059ff775485246999027b3197955",
                token_pack_key=bsc_pack.code,
            ),
        )

        self.assertEqual(response.status_code, 302)

        session = DepositSession.objects.get(wallet=self.w1)

        expected_canonical_amount = _convert_platform_token_units_to_canonical_stable_units(
            bsc_pack.token_amount
        )
        expected_onchain_amount = expected_canonical_amount * 10 ** 12

        self.assertEqual(session.chain, "bsc")
        self.assertEqual(session.asset_code, "USDT")
        self.assertEqual(session.min_amount, expected_canonical_amount)
        self.assertEqual(session.expected_onchain_raw_amount, expected_onchain_amount)
        self.assertEqual(
            int((session.metadata or {}).get("expected_canonical_stable_amount")),
            expected_canonical_amount,
        )
        self.assertEqual(
            int((session.metadata or {}).get("expected_route_raw_amount")),
            expected_onchain_amount,
        )

        page_response = self.client.get(
            reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(page_response.status_code, 200)
        self.assertContains(page_response, "500_tokens")
        self.assertContains(page_response, "500 tokens")
        self.assertContains(page_response, "$5")
        self.assertContains(page_response, "5.00 USDT")
        self.assertNotContains(page_response, "0.00 USDT")

    @patch("ledger.services._derive_session_deposit_address")
    def test_bsc_deposit_underpayment_does_not_credit_canonical_amount_as_raw(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbfebace943c8adc81341f4e5f34f9f89dbdbee64",
            "m/44'/60'/0'/0/99",
        )

        DepositAddress.objects.create(
            chain="bsc",
            asset_code="USDT",
            token_contract_address="0x55d398326f99059ff775485246999027b3197955",
            display_label="BNB Chain · USDT",
            address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/99",
            derivation_index=99,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        bsc_pack = TokenPack.objects.create(
            code="500_tokens_underpay",
            name="500_tokens",
            description="500 tokens",
            badge_text="",
            token_amount=500_000_000,
            gross_stable_amount=7_000_000,
            is_active=True,
            sort_order=11,
        )

        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                deposit_option_key="bsc:USDT:0x55d398326f99059ff775485246999027b3197955",
                token_pack_key=bsc_pack.code,
            ),
        )

        self.assertEqual(response.status_code, 302)

        session = DepositSession.objects.get(wallet=self.w1)

        expected_canonical_amount = _convert_platform_token_units_to_canonical_stable_units(
            bsc_pack.token_amount
        )
        expected_onchain_amount = expected_canonical_amount * 10 ** 12

        self.assertEqual(session.chain, "bsc")
        self.assertEqual(session.asset_code, "USDT")
        self.assertEqual(session.min_amount, expected_canonical_amount)
        self.assertEqual(session.expected_onchain_raw_amount, expected_onchain_amount)

        with self.assertRaisesMessage(Exception, "Observed canonical stable amount must be positive"):
            record_onchain_observation(
                actor=self.operator,
                deposit_session=session,
                chain="bsc",
                txid="0xbscunderpay0001",
                log_index=1,
                block_number=987654,
                from_address="0xffffffffffffffffffffffffffffffffffffffff",
                to_address=session.deposit_address,
                token_contract_address="0x55d398326f99059ff775485246999027b3197955",
                asset_code="USDT",
                amount=expected_canonical_amount,
                confirmations=session.required_confirmations,
                raw_payload={"source": "bsc-underpayment-test"},
            )

        session.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertNotEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.w1.balance, 0)

    @patch("ledger.services._derive_session_deposit_address")
    def test_bsc_partial_payment_displays_received_canonical_amount(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbfebace943c8adc81341f4e5f34f9f89dbdbee64",
            "m/44'/60'/0'/0/99",
        )

        DepositAddress.objects.create(
            chain="bsc",
            asset_code="USDT",
            token_contract_address="0x55d398326f99059ff775485246999027b3197955",
            display_label="BNB Chain · USDT",
            address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/99",
            derivation_index=99,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        bsc_pack = TokenPack.objects.create(
            code="500_tokens_partial_display",
            name="500_tokens",
            description="500 tokens",
            badge_text="",
            token_amount=500_000_000,
            gross_stable_amount=7_000_000,
            is_active=True,
            sort_order=12,
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                deposit_option_key="bsc:USDT:0x55d398326f99059ff775485246999027b3197955",
                token_pack_key=bsc_pack.code,
            ),
        )

        self.assertEqual(response.status_code, 302)

        session = DepositSession.objects.get(wallet=self.w1)
        session.status = DepositSession.STATUS_CONFIRMING
        session.confirmations = 201
        session.observed_amount = 4_000_000
        session.observed_txid = ""
        session.save(
            update_fields=[
                "status",
                "confirmations",
                "observed_amount",
                "observed_txid",
                "updated_at",
            ]
        )

        page_response = self.client.get(
            reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(page_response.status_code, 200)
        self.assertContains(page_response, "5.00 USDT")
        self.assertContains(page_response, 'data-deposit-observed-amount>4</span>')
        self.assertContains(page_response, "<span>USDT</span>")
        self.assertNotContains(page_response, "0.00 USDT")

        status_response = self.client.get(
            reverse("wallet_deposit_session_status", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_response.json()["observed_amount_display"], "4")

    @patch("ledger.services._derive_session_deposit_address")
    def test_bsc_partial_payment_is_seen_but_not_credited_until_expected_amount(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbfebace943c8adc81341f4e5f34f9f89dbdbee64",
            "m/44'/60'/0'/0/99",
        )

        DepositAddress.objects.create(
            chain="bsc",
            asset_code="USDT",
            token_contract_address="0x55d398326f99059ff775485246999027b3197955",
            display_label="BNB Chain · USDT",
            address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/99",
            derivation_index=99,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        bsc_pack = TokenPack.objects.create(
            code="500_tokens_partial_credit",
            name="500_tokens",
            description="500 tokens",
            badge_text="",
            token_amount=500_000_000,
            gross_stable_amount=7_000_000,
            is_active=True,
            sort_order=13,
        )

        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            self.default_deposit_request_payload(
                deposit_option_key="bsc:USDT:0x55d398326f99059ff775485246999027b3197955",
                token_pack_key=bsc_pack.code,
            ),
        )

        self.assertEqual(response.status_code, 302)

        session = DepositSession.objects.get(wallet=self.w1)

        partial_observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="bsc",
            txid="0xbscpartial0001",
            log_index=1,
            block_number=987654,
            from_address="0xffffffffffffffffffffffffffffffffffffffff",
            to_address=session.deposit_address,
            token_contract_address="0x55d398326f99059ff775485246999027b3197955",
            asset_code="USDT",
            amount=4 * 10**18,
            confirmations=session.required_confirmations,
            raw_payload={"source": "bsc-partial-payment-test"},
        )

        session.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(session.observed_amount, 4_000_000)
        self.assertNotEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.w1.balance, 0)

        with self.assertRaises(Exception):
            credit_confirmed_deposit_session(
                actor=self.operator,
                deposit_session=session,
                observed_transfer=partial_observed,
                created_by=self.u1,
            )

        session.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertNotEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.w1.balance, 0)