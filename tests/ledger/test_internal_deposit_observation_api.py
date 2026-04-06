import json
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

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

        user_model = get_user_model()
        self.deposit_service_user = user_model.objects.create_user(
            username="deposit-service",
            email="deposit-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.deposit_service_user, "can_record_onchain_observations")
        self.grant_perm(self.deposit_service_user, "can_credit_confirmed_deposits")
        self.grant_perm(self.deposit_service_user, "can_apply_raw_ledger_transaction")
        self.grant_perm(self.deposit_service_user, "can_manage_deposit_sweep_jobs")

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
        self.deposit_service_user.refresh_from_db()

        self.assertTrue(
            self.deposit_service_user.has_perm("ledger.can_record_onchain_observations")
        )
        self.assertTrue(
            self.deposit_service_user.has_perm("ledger.can_credit_confirmed_deposits")
        )
        self.assertTrue(
            self.deposit_service_user.has_perm("ledger.can_apply_raw_ledger_transaction")
        )
        self.assertTrue(
            self.deposit_service_user.has_perm("ledger.can_manage_deposit_sweep_jobs")
        )
    def _build_payload(
        self,
        *,
        txid="0xabc",
        log_index=7,
        amount=250,
        confirmations=12,
        block_number=123456,
        from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ):
        return {
            "session_public_id": str(self.session.public_id),
            "chain": "ethereum",
            "txid": txid,
            "log_index": log_index,
            "block_number": block_number,
            "from_address": from_address,
            "deposit_address": "0x9999999999999999999999999999999999999999",
            "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "asset_code": "USDT",
            "amount": amount,
            "confirmations": confirmations,
        }

    def _post_signed(self, payload, *, nonce="nonce-1", timestamp=None, signature=None):
        if timestamp is None:
            timestamp = str(int(timezone.now().timestamp()))

        body_text = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        body_bytes = body_text.encode("utf-8")

        if signature is None:
            signature = build_internal_request_signature(
                service_name="deposit-service",
                timestamp=timestamp,
                nonce=nonce,
                body_bytes=body_bytes,
                shared_secret="super-secret",
            )

        return self.client.post(
            reverse("internal_deposit_observation"),
            data=body_text,
            content_type="application/json",
            headers={
                "X-Ledger-Service": "deposit-service",
                "X-Ledger-Timestamp": timestamp,
                "X-Ledger-Nonce": nonce,
                "X-Ledger-Signature": signature,
            },
        )

    def test_valid_confirmed_observation_is_recorded_and_credited_exactly_once(self):
        payload = self._build_payload(
            txid="0xabc",
            log_index=7,
            amount=250,
            confirmations=12,
        )

        response = self._post_signed(payload, nonce="nonce-valid")

        self.assertEqual(response.status_code, 200, response.content.decode())
        self.session.refresh_from_db()
        self.w1.refresh_from_db()

        clearing_wallet = get_external_asset_clearing_wallet()
        clearing_wallet.refresh_from_db()

        self.assertEqual(self.session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.session.confirmations, 12)
        self.assertEqual(self.session.observed_txid, "0xabc")
        self.assertEqual(self.w1.balance, 250)
        self.assertEqual(clearing_wallet.balance, -250)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 1)
        self.assertEqual(InternalAPIRequestNonce.objects.count(), 1)

        observed = ObservedOnchainTransfer.objects.get()
        self.assertEqual(observed.status, ObservedOnchainTransfer.STATUS_CREDITED)
        self.assertEqual(observed.confirmations, 12)
        self.assertEqual(observed.amount, 250)

    def test_same_event_with_new_nonce_is_idempotent(self):
        payload = self._build_payload(
            txid="0xdef",
            log_index=8,
            amount=300,
            confirmations=12,
            block_number=123457,
            from_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )

        first = self._post_signed(payload, nonce="nonce-a")
        second = self._post_signed(payload, nonce="nonce-b")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)

        self.session.refresh_from_db()
        self.w1.refresh_from_db()

        clearing_wallet = get_external_asset_clearing_wallet()
        clearing_wallet.refresh_from_db()

        self.assertEqual(self.session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.w1.balance, 300)
        self.assertEqual(clearing_wallet.balance, -300)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 1)
        self.assertEqual(InternalAPIRequestNonce.objects.count(), 2)

        observed = ObservedOnchainTransfer.objects.get()
        self.assertEqual(observed.txid, "0xdef")
        self.assertEqual(observed.log_index, 8)
        self.assertEqual(observed.amount, 300)
        self.assertEqual(observed.confirmations, 12)

    def test_replayed_nonce_is_rejected(self):
        payload = self._build_payload(
            txid="0x123",
            log_index=9,
            amount=300,
            confirmations=12,
            block_number=123458,
            from_address="0xcccccccccccccccccccccccccccccccccccccccc",
        )

        first = self._post_signed(payload, nonce="nonce-replay")
        second = self._post_signed(payload, nonce="nonce-replay")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 403)

        self.session.refresh_from_db()
        self.w1.refresh_from_db()

        clearing_wallet = get_external_asset_clearing_wallet()
        clearing_wallet.refresh_from_db()

        self.assertEqual(self.session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(self.w1.balance, 300)
        self.assertEqual(clearing_wallet.balance, -300)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 1)
        self.assertEqual(InternalAPIRequestNonce.objects.count(), 1)

    def test_invalid_signature_is_rejected(self):
        payload = self._build_payload(
            txid="0x456",
            log_index=10,
            amount=300,
            confirmations=12,
            block_number=123459,
            from_address="0xdddddddddddddddddddddddddddddddddddddddd",
        )

        response = self._post_signed(
            payload,
            nonce="bad-signature",
            signature="deadbeef",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 0)
        self.assertEqual(InternalAPIRequestNonce.objects.count(), 0)

        self.session.refresh_from_db()
        self.w1.refresh_from_db()

        clearing_wallet = get_external_asset_clearing_wallet()
        clearing_wallet.refresh_from_db()

        self.assertEqual(self.session.status, DepositSession.STATUS_AWAITING_PAYMENT)
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(clearing_wallet.balance, 0)