import json
from datetime import datetime, timedelta, timezone as dt_timezone
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from django.contrib.auth import get_user_model

from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositSession, InternalAPIRequestNonce, ObservedOnchainTransfer
from ledger.services import create_deposit_session, get_external_asset_clearing_wallet
from .base import BaseLedgerTestCase

@override_settings(
    LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME="deposit-service",
    LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET="super-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
)
class TestInternalDepositObservationAPI(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.deposit_service_user = get_user_model().objects.create_user(
            username="deposit-service",
            email="deposit-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.deposit_service_user, "can_record_onchain_observations")
        self.grant_perm(self.deposit_service_user, "can_credit_confirmed_deposits")

        self.session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x9999999999999999999999999999999999999999",
            address_derivation_ref="m/44'/60'/0'/0/99",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )

    def _post_signed(self, payload, *, nonce="nonce-1", timestamp="1775476800"):
        body = json.dumps(payload).encode("utf-8")
        signature = build_internal_request_signature(
            service_name="deposit-service",
            timestamp=timestamp,
            nonce=nonce,
            body_bytes=body,
            shared_secret="super-secret",
        )
        return self.client.post(
            reverse("internal_deposit_observation"),
            data=body,
            content_type="application/json",
            HTTP_X_LEDGER_SERVICE="deposit-service",
            HTTP_X_LEDGER_TIMESTAMP=timestamp,
            HTTP_X_LEDGER_NONCE=nonce,
            HTTP_X_LEDGER_SIGNATURE=signature,
        )

    @patch("ledger.internal_api.timezone.now")
    def test_valid_confirmed_observation_is_recorded_and_credited_exactly_once(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        payload = {
            "session_public_id": str(self.session.public_id),
            "chain": "ethereum",
            "txid": "0xabc",
            "log_index": 7,
            "block_number": 123456,
            "from_address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "deposit_address": "0x9999999999999999999999999999999999999999",
            "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "asset_code": "USDT",
            "amount": 250,
            "confirmations": 12,
            "raw_payload": {"provider": "deposit-service"},
        }

        response = self._post_signed(payload)
        self.assertEqual(response.status_code, 200)

        self.session.refresh_from_db()
        self.w1.refresh_from_db()

        clearing_wallet = get_external_asset_clearing_wallet()
        clearing_wallet.refresh_from_db()

        self.assertEqual(self.session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.w1.balance, 250)
        self.assertEqual(clearing_wallet.balance, -250)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 1)
        self.assertEqual(InternalAPIRequestNonce.objects.count(), 1)

    @patch("ledger.internal_api.timezone.now")
    def test_same_event_with_new_nonce_is_idempotent(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        payload = {
            "session_public_id": str(self.session.public_id),
            "chain": "ethereum",
            "txid": "0xdef",
            "log_index": 8,
            "block_number": 123457,
            "from_address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "deposit_address": "0x9999999999999999999999999999999999999999",
            "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "asset_code": "USDT",
            "amount": 300,
            "confirmations": 12,
        }

        first = self._post_signed(payload, nonce="nonce-a")
        second = self._post_signed(payload, nonce="nonce-b")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 300)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 1)

    @patch("ledger.internal_api.timezone.now")
    def test_replayed_nonce_is_rejected(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        payload = {
            "session_public_id": str(self.session.public_id),
            "chain": "ethereum",
            "txid": "0x123",
            "log_index": 9,
            "block_number": 123458,
            "from_address": "0xcccccccccccccccccccccccccccccccccccccccc",
            "deposit_address": "0x9999999999999999999999999999999999999999",
            "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "asset_code": "USDT",
            "amount": 300,
            "confirmations": 12,
        }

        first = self._post_signed(payload, nonce="nonce-replay")
        second = self._post_signed(payload, nonce="nonce-replay")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 403)
        self.assertContains(second, "Replay detected", status_code=403)

    @patch("ledger.internal_api.timezone.now")
    def test_invalid_signature_is_rejected(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        payload = {
            "session_public_id": str(self.session.public_id),
            "chain": "ethereum",
            "txid": "0x456",
            "log_index": 10,
            "block_number": 123459,
            "from_address": "0xdddddddddddddddddddddddddddddddddddddddd",
            "deposit_address": "0x9999999999999999999999999999999999999999",
            "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "asset_code": "USDT",
            "amount": 300,
            "confirmations": 12,
        }

        body = json.dumps(payload).encode("utf-8")
        response = self.client.post(
            reverse("internal_deposit_observation"),
            data=body,
            content_type="application/json",
            HTTP_X_LEDGER_SERVICE="deposit-service",
            HTTP_X_LEDGER_TIMESTAMP="1712448000",
            HTTP_X_LEDGER_NONCE="bad-signature",
            HTTP_X_LEDGER_SIGNATURE="deadbeef",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 0)