from datetime import timedelta
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from ledger.models import DepositAddress, DepositSession

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

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_deposit_session_reuses_existing_active_session(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "m/44'/60'/0'/0/12",
        )

        self.client.force_login(self.u1)

        first = self.client.post(
            reverse("wallet_deposit_request"),
            {"deposit_option_key": "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7"},
        )
        second = self.client.post(
            reverse("wallet_deposit_request"),
            {"deposit_option_key": "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7"},
        )

        self.assertEqual(DepositSession.objects.count(), 1)
        session = DepositSession.objects.get(wallet=self.w1)

        expected_url = reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        self.assertRedirects(first, expected_url)
        self.assertRedirects(second, expected_url)
        self.assertEqual(session.derivation_index, 0)
        self.assertEqual(session.derivation_path, "m/44'/60'/0'/0/12")
        self.assertEqual(session.route_key, "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7")

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_deposit_session_does_not_reuse_expired_session(self, mocked_derive):
        mocked_derive.side_effect = [
            ("0xcccccccccccccccccccccccccccccccccccccccc", "m/44'/60'/0'/0/13"),
            ("0xdddddddddddddddddddddddddddddddddddddddd", "m/44'/60'/0'/0/14"),
        ]

        self.client.force_login(self.u1)

        first = self.client.post(
            reverse("wallet_deposit_request"),
            {"deposit_option_key": "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7"},
        )
        session = DepositSession.objects.get(wallet=self.w1)
        DepositSession.objects.filter(id=session.id).update(
            expires_at=timezone.now() - timedelta(seconds=1)
        )

        second = self.client.post(
            reverse("wallet_deposit_request"),
            {"deposit_option_key": "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7"},
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
            deposit_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/12",
            expires_at=timezone.now() + timedelta(hours=1),
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
            deposit_address="0xcccccccccccccccccccccccccccccccccccccccc",
            address_derivation_ref="m/44'/60'/0'/0/13",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=100,
        )

        self.client.force_login(self.u1)
        response = self.client.get(
            reverse("wallet_deposit_session", kwargs={"public_id": session.public_id})
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "0xcccccccccccccccccccccccccccccccccccccccc")
        self.assertContains(response, "100")

    def test_deposit_session_status_endpoint_returns_payload(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0xdddddddddddddddddddddddddddddddddddddddd",
            address_derivation_ref="m/44'/60'/0'/0/14",
            expires_at=timezone.now() + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=12,
            min_amount=100,
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
        self.assertEqual(payload["min_amount"], 100)
        self.assertIn("expires_at", payload)
        self.assertIn("expires_at_iso", payload)