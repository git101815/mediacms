import json
from datetime import timedelta
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from datetime import datetime, timezone as dt_timezone
from django.contrib.auth import get_user_model

from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositSweepJob
from ledger.services import (
    create_deposit_session,
    record_onchain_observation,
    credit_confirmed_deposit_session,
)
from .base import BaseLedgerTestCase


@override_settings(
    LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME="sweeper-service",
    LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET="sweeper-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
    LEDGER_SWEEP_JOB_CLAIM_LEASE_SECONDS=120,
    LEDGER_SWEEP_JOB_CLAIM_MAX_BATCH=50,
    LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED=False,
)
class TestInternalSweepJobsAPI(BaseLedgerTestCase):
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
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x3333333333333333333333333333333333333333",
            address_derivation_ref="evm:ethereum:external:102",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0x987",
            log_index=9,
            block_number=123458,
            from_address="0xcccccccccccccccccccccccccccccccccccccccc",
            to_address="0x3333333333333333333333333333333333333333",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=300,
            confirmations=6,
            raw_payload={"txid": "0x987", "log_index": 9},
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        return DepositSweepJob.objects.get(observed_transfer=observed)

    @patch("ledger.internal_api.timezone.now")
    def test_claim_returns_pending_job(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        job = self._create_credited_job()

        response = self._post_signed(
            "internal_sweep_jobs_claim",
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    }
                ],
                "limit": 10,
            },
            nonce="claim-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(data["results"][0]["public_id"], str(job.public_id))

        job.refresh_from_db()
        self.assertEqual(job.claimed_by_service, "sweeper-service")
        self.assertIsNotNone(job.claim_expires_at)

    @patch("ledger.internal_api.timezone.now")
    def test_claim_skips_non_expired_claimed_job(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        job = self._create_credited_job()
        job.claimed_by_service = "other-service"
        job.claim_expires_at = mocked_now.return_value + timedelta(seconds=60)
        job.save(update_fields=["claimed_by_service", "claim_expires_at", "updated_at"])

        response = self._post_signed(
            "internal_sweep_jobs_claim",
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    }
                ],
                "limit": 10,
            },
            nonce="claim-2",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["results"], [])

    @patch("ledger.internal_api.timezone.now")
    def test_full_success_lifecycle(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        job = self._create_credited_job()

        claim_response = self._post_signed(
            "internal_sweep_jobs_claim",
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    }
                ],
                "limit": 10,
            },
            nonce="claim-3",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(claim_response.status_code, 200)
        claim_token = claim_response.json()["results"][0]["claim_token"]
        self.assertTrue(claim_token)
        response = self._post_signed(
            "internal_sweep_job_funding_broadcasted",
            {
                "claim_token": claim_token,
                "gas_funding_txid": "0xgas123",
                "destination_address": "0x9999999999999999999999999999999999999999",
            },
            nonce="funding-1",
            now_value=mocked_now.return_value,
            public_id=job.public_id,
        )
        self.assertEqual(response.status_code, 200)

        response = self._post_signed(
            "internal_sweep_job_ready_to_sweep",
            {"claim_token": claim_token,},
            nonce="ready-1",
            now_value=mocked_now.return_value,
            public_id=job.public_id,
        )
        self.assertEqual(response.status_code, 200)

        response = self._post_signed(
            "internal_sweep_job_sweep_broadcasted",
            {
                "claim_token": claim_token,
                "sweep_txid": "0xsweep123",
                "destination_address": "0x9999999999999999999999999999999999999999",
            },
            nonce="sweep-1",
            now_value=mocked_now.return_value,
            public_id=job.public_id,
        )
        self.assertEqual(response.status_code, 200)

        response = self._post_signed(
            "internal_sweep_job_confirmed",
            {"claim_token": claim_token,},
            nonce="confirmed-1",
            now_value=mocked_now.return_value,
            public_id=job.public_id,
        )
        self.assertEqual(response.status_code, 200)

        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_CONFIRMED)
        self.assertEqual(job.gas_funding_txid, "0xgas123")
        self.assertEqual(job.sweep_txid, "0xsweep123")
        self.assertEqual(job.destination_address, "0x9999999999999999999999999999999999999999")
        self.assertEqual(job.claimed_by_service, "")
        self.assertIsNone(job.claim_expires_at)
        self.assertIsNotNone(job.confirmed_at)

    @patch("ledger.internal_api.timezone.now")
    def test_failed_endpoint_marks_job_failed(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        job = self._create_credited_job()

        claim_response = self._post_signed(
            "internal_sweep_jobs_claim",
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    }
                ],
                "limit": 10,
            },
            nonce="claim-4",
            now_value=mocked_now.return_value,
        )
        self.assertEqual(claim_response.status_code, 200)
        claim_token = claim_response.json()["results"][0]["claim_token"]
        self.assertTrue(claim_token)

        response = self._post_signed(
            "internal_sweep_job_failed",
            {
                "claim_token": claim_token,
                "error": "gas funding transaction dropped",
            },
            nonce="failed-1",
            now_value=mocked_now.return_value,
            public_id=job.public_id,
        )
        self.assertEqual(response.status_code, 200)

        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_FAILED)
        self.assertEqual(job.last_error, "gas funding transaction dropped")
        self.assertEqual(job.claimed_by_service, "")
        self.assertIsNone(job.claim_expires_at)