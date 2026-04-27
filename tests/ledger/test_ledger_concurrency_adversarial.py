import json
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from datetime import timezone as dt_timezone

from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositSweepJob, EvmSenderState
from ledger.services import (
    create_deposit_session,
    credit_confirmed_deposit_session,
    record_onchain_observation,
)
from .base import BaseLedgerTestCase


USDT_ETH = "0xdac17f958d2ee523a2206206994597c13d831ec7"


@override_settings(
    LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME="sweeper-service",
    LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET="sweeper-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
    LEDGER_SWEEP_JOB_CLAIM_LEASE_SECONDS=120,
    LEDGER_SWEEP_JOB_CLAIM_MAX_BATCH=50,
    LEDGER_EVM_SENDER_LOCK_SECONDS=120,
    LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED=False,
)
class TestLedgerConcurrencyAdversarial(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        user_model = get_user_model()
        self.sweeper_service_user = user_model.objects.create_user(
            username="sweeper-service",
            email="sweeper-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.sweeper_service_user, "can_manage_deposit_sweep_jobs")
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")

    def _post_signed(self, url_name, payload, *, nonce, now_value, public_id=None):
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        timestamp = str(int(now_value.timestamp()))
        signature = build_internal_request_signature(
            service_name="sweeper-service",
            timestamp=timestamp,
            nonce=nonce,
            body_bytes=body,
            shared_secret="sweeper-secret",
        )

        if public_id is None:
            url = reverse(url_name)
        else:
            url = reverse(url_name, kwargs={"public_id": public_id})

        return self.client.post(
            url,
            data=body,
            content_type="application/json",
            HTTP_X_LEDGER_SERVICE="sweeper-service",
            HTTP_X_LEDGER_TIMESTAMP=timestamp,
            HTTP_X_LEDGER_NONCE=nonce,
            HTTP_X_LEDGER_SIGNATURE=signature,
        )

    def _create_credited_job(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address=USDT_ETH,
            deposit_address="0x3333333333333333333333333333333333333333",
            address_derivation_ref="evm:ethereum:external:202",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xadversarial987",
            log_index=9,
            block_number=123458,
            from_address="0xcccccccccccccccccccccccccccccccccccccccc",
            to_address="0x3333333333333333333333333333333333333333",
            token_contract_address=USDT_ETH,
            asset_code="USDT",
            amount=300,
            confirmations=6,
            raw_payload={"txid": "0xadversarial987", "log_index": 9},
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        return DepositSweepJob.objects.get(observed_transfer=observed)

    def _claim_job(self, *, nonce, now_value):
        return self._post_signed(
            "internal_sweep_jobs_claim",
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": USDT_ETH,
                    }
                ],
                "limit": 10,
            },
            nonce=nonce,
            now_value=now_value,
        )

    @patch("ledger.internal_api.timezone.now")
    def test_stale_claim_token_is_rejected_after_reclaim(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        job = self._create_credited_job()

        first_claim_response = self._claim_job(
            nonce="claim-stale-1",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(first_claim_response.status_code, 200)
        old_claim_token = first_claim_response.json()["results"][0]["claim_token"]
        self.assertTrue(old_claim_token)

        job.refresh_from_db()
        job.claim_expires_at = timezone.now() - timedelta(seconds=1)
        job.save(update_fields=["claim_expires_at", "updated_at"])

        second_claim_response = self._claim_job(
            nonce="claim-stale-2",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(second_claim_response.status_code, 200)
        new_claim_token = second_claim_response.json()["results"][0]["claim_token"]
        self.assertTrue(new_claim_token)
        self.assertNotEqual(old_claim_token, new_claim_token)

        stale_requests = [
            (
                "internal_sweep_job_funding_broadcasted",
                {
                    "claim_token": old_claim_token,
                    "gas_funding_txid": "0xstalegas",
                    "destination_address": "0x9999999999999999999999999999999999999999",
                },
                "stale-funding",
            ),
            (
                "internal_sweep_job_ready_to_sweep",
                {"claim_token": old_claim_token},
                "stale-ready",
            ),
            (
                "internal_sweep_job_sweep_broadcasted",
                {
                    "claim_token": old_claim_token,
                    "sweep_txid": "0xstalesweep",
                    "destination_address": "0x9999999999999999999999999999999999999999",
                },
                "stale-sweep",
            ),
            (
                "internal_sweep_job_confirmed",
                {"claim_token": old_claim_token},
                "stale-confirmed",
            ),
            (
                "internal_sweep_job_reschedule",
                {
                    "claim_token": old_claim_token,
                    "next_retry_in_seconds": 30,
                },
                "stale-reschedule",
            ),
            (
                "internal_sweep_job_failed",
                {
                    "claim_token": old_claim_token,
                    "error": "stale worker failure",
                },
                "stale-failed",
            ),
        ]

        for url_name, payload, nonce in stale_requests:
            response = self._post_signed(
                url_name,
                payload,
                nonce=nonce,
                now_value=mocked_now.return_value,
                public_id=job.public_id,
            )
            self.assertEqual(response.status_code, 400, url_name)

        job.refresh_from_db()
        self.assertEqual(job.claim_token, new_claim_token)
        self.assertEqual(job.status, DepositSweepJob.STATUS_PENDING)
        self.assertEqual(job.gas_funding_txid, "")
        self.assertEqual(job.sweep_txid, "")
        self.assertEqual(job.last_error, "")

        response = self._post_signed(
            "internal_sweep_job_ready_to_sweep",
            {"claim_token": new_claim_token},
            nonce="fresh-ready",
            now_value=mocked_now.return_value,
            public_id=job.public_id,
        )
        self.assertEqual(response.status_code, 200)

        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_READY_TO_SWEEP)
        self.assertEqual(job.claim_token, new_claim_token)

    @patch("ledger.internal_api.timezone.now")
    def test_stale_evm_sender_lock_token_is_rejected_after_reclaim(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        chain = "ethereum"
        address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

        first_response = self._post_signed(
            "internal_evm_sender_lock_acquire",
            {
                "chain": chain,
                "address": address,
                "lock_seconds": 120,
            },
            nonce="sender-acquire-1",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(first_response.status_code, 200)
        old_lock_token = first_response.json()["lock_token"]
        self.assertTrue(old_lock_token)

        state = EvmSenderState.objects.get(chain=chain, address=address)
        state.lock_expires_at = timezone.now() - timedelta(seconds=1)
        state.save(update_fields=["lock_expires_at", "updated_at"])

        second_response = self._post_signed(
            "internal_evm_sender_lock_acquire",
            {
                "chain": chain,
                "address": address,
                "lock_seconds": 120,
            },
            nonce="sender-acquire-2",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(second_response.status_code, 200)
        new_lock_token = second_response.json()["lock_token"]
        self.assertTrue(new_lock_token)
        self.assertNotEqual(old_lock_token, new_lock_token)

        stale_confirm_response = self._post_signed(
            "internal_evm_sender_lock_confirm",
            {
                "chain": chain,
                "address": address,
                "lock_token": old_lock_token,
                "nonce": 7,
                "txid": "0xstale",
            },
            nonce="sender-stale-confirm",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(stale_confirm_response.status_code, 400)

        state.refresh_from_db()
        self.assertIsNone(state.next_nonce)
        self.assertEqual(state.lock_token, new_lock_token)

        stale_release_response = self._post_signed(
            "internal_evm_sender_lock_release",
            {
                "chain": chain,
                "address": address,
                "lock_token": old_lock_token,
            },
            nonce="sender-stale-release",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(stale_release_response.status_code, 400)

        state.refresh_from_db()
        self.assertIsNone(state.next_nonce)
        self.assertEqual(state.lock_token, new_lock_token)

        fresh_confirm_response = self._post_signed(
            "internal_evm_sender_lock_confirm",
            {
                "chain": chain,
                "address": address,
                "lock_token": new_lock_token,
                "nonce": 7,
                "txid": "0xfresh",
            },
            nonce="sender-fresh-confirm",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(fresh_confirm_response.status_code, 200)

        state.refresh_from_db()
        self.assertEqual(state.next_nonce, 8)
        self.assertEqual(state.lock_token, "")
        self.assertEqual(state.locked_by_service, "")
        self.assertIsNone(state.lock_expires_at)

    @patch("ledger.internal_api.timezone.now")
    def test_active_evm_sender_lock_blocks_second_lock_for_same_sender(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        chain = "ethereum"
        address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

        first_response = self._post_signed(
            "internal_evm_sender_lock_acquire",
            {
                "chain": chain,
                "address": address,
                "lock_seconds": 120,
            },
            nonce="sender-contention-1",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(first_response.status_code, 200)
        first_token = first_response.json()["lock_token"]

        second_response = self._post_signed(
            "internal_evm_sender_lock_acquire",
            {
                "chain": chain,
                "address": address,
                "lock_seconds": 120,
            },
            nonce="sender-contention-2",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(second_response.status_code, 400)

        state = EvmSenderState.objects.get(chain=chain, address=address)
        self.assertEqual(state.lock_token, first_token)
        self.assertEqual(state.locked_by_service, "sweeper-service")
        self.assertIsNone(state.next_nonce)

    @patch("ledger.internal_api.timezone.now")
    def test_evm_sender_locks_are_independent_for_different_addresses(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        chain = "ethereum"
        address_a = "0xcccccccccccccccccccccccccccccccccccccccc"
        address_b = "0xdddddddddddddddddddddddddddddddddddddddd"

        response_a = self._post_signed(
            "internal_evm_sender_lock_acquire",
            {
                "chain": chain,
                "address": address_a,
                "lock_seconds": 120,
            },
            nonce="sender-independent-a",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(response_a.status_code, 200)

        response_b = self._post_signed(
            "internal_evm_sender_lock_acquire",
            {
                "chain": chain,
                "address": address_b,
                "lock_seconds": 120,
            },
            nonce="sender-independent-b",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(response_b.status_code, 200)

        token_a = response_a.json()["lock_token"]
        token_b = response_b.json()["lock_token"]
        self.assertTrue(token_a)
        self.assertTrue(token_b)
        self.assertNotEqual(token_a, token_b)

        state_a = EvmSenderState.objects.get(chain=chain, address=address_a)
        state_b = EvmSenderState.objects.get(chain=chain, address=address_b)

        self.assertEqual(state_a.lock_token, token_a)
        self.assertEqual(state_b.lock_token, token_b)
        self.assertEqual(EvmSenderState.objects.filter(chain=chain).count(), 2)
