import json
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositSession, OrphanDepositRecoveryAudit
from ledger.orphan_recovery import CLAIM_METADATA_KEY

from .base import BaseLedgerTestCase


TOKEN_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
DEPOSIT_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"


@override_settings(
    LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME="sweeper-service",
    LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET="sweeper-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
    LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED=False,
)
class TestInternalOrphanRecoveryAPI(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        user_model = get_user_model()
        self.sweeper_service_user = user_model.objects.create_user(
            username="sweeper-service",
            email="sweeper-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.sweeper_service_user, "can_manage_deposit_sweep_jobs")
        self.route_decimals = patch(
            "ledger.orphan_recovery._get_route_onchain_decimals",
            return_value=6,
        )
        self.route_decimals.start()
        self.addCleanup(self.route_decimals.stop)

    def _make_session(self, *, now):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address=TOKEN_CONTRACT,
            deposit_address=DEPOSIT_ADDRESS,
            address_derivation_ref="m/44'/60'/0'/0/7",
            derivation_index=7,
            expires_at=now - timedelta(days=9),
            status=DepositSession.STATUS_SWEPT,
            required_confirmations=12,
            min_amount=1_000_000,
        )
        DepositSession.objects.filter(pk=session.pk).update(
            updated_at=now - timedelta(days=10)
        )
        session.refresh_from_db()
        return session

    @staticmethod
    def _claim_payload(*, lease_seconds=120):
        return {
            "options": [
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": TOKEN_CONTRACT,
                }
            ],
            "limit": 10,
            "older_than_hours": 24,
            "lease_seconds": lease_seconds,
        }

    def _post_signed(self, url_name, payload, *, nonce, now_value, url_kwargs=None):
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        timestamp = str(int(now_value.timestamp()))
        signature = build_internal_request_signature(
            service_name="sweeper-service",
            timestamp=timestamp,
            nonce=nonce,
            body_bytes=body,
            shared_secret="sweeper-secret",
        )
        return self.client.post(
            reverse(url_name, kwargs=url_kwargs),
            data=body,
            content_type="application/json",
            HTTP_X_LEDGER_SERVICE="sweeper-service",
            HTTP_X_LEDGER_TIMESTAMP=timestamp,
            HTTP_X_LEDGER_NONCE=nonce,
            HTTP_X_LEDGER_SIGNATURE=signature,
        )

    def _claim(self, *, now_value, nonce, lease_seconds=120):
        with patch("ledger.internal_api.timezone.now", return_value=now_value), patch(
            "ledger.orphan_recovery.timezone.now", return_value=now_value
        ):
            return self._post_signed(
                "internal_orphan_recovery_claim",
                self._claim_payload(lease_seconds=lease_seconds),
                nonce=nonce,
                now_value=now_value,
            )

    def test_claim_lease_is_exclusive_then_reclaimable_after_expiry(self):
        now = timezone.now()
        session = self._make_session(now=now)

        first = self._claim(now_value=now, nonce="orphan-claim-1")
        self.assertEqual(first.status_code, 200)
        first_rows = first.json()["results"]
        self.assertEqual(len(first_rows), 1)
        self.assertEqual(first_rows[0]["session_public_id"], str(session.public_id))
        first_token = first_rows[0]["claim_token"]
        self.assertTrue(first_token)

        still_leased = self._claim(
            now_value=now + timedelta(seconds=60),
            nonce="orphan-claim-2",
        )
        self.assertEqual(still_leased.status_code, 200)
        self.assertEqual(still_leased.json()["results"], [])

        reclaimed = self._claim(
            now_value=now + timedelta(seconds=121),
            nonce="orphan-claim-3",
        )
        self.assertEqual(reclaimed.status_code, 200)
        reclaimed_rows = reclaimed.json()["results"]
        self.assertEqual(len(reclaimed_rows), 1)
        self.assertEqual(reclaimed_rows[0]["session_public_id"], str(session.public_id))
        self.assertNotEqual(reclaimed_rows[0]["claim_token"], first_token)

    def test_result_rejects_wrong_claim_token_then_accepts_current_token(self):
        now = timezone.now()
        session = self._make_session(now=now)
        claim = self._claim(now_value=now, nonce="orphan-result-claim")
        self.assertEqual(claim.status_code, 200)
        claim_token = claim.json()["results"][0]["claim_token"]

        result_payload = {
            "claim_token": "wrong-token",
            "status": OrphanDepositRecoveryAudit.STATUS_DUST_FINAL,
            "decision_reason": "below_profit_threshold",
            "token_balance": 1_000_000,
            "native_balance": 0,
            "token_value_usd": "1",
            "native_value_usd": "0",
            "token_recovery_cost_usd": "0.10",
            "native_recovery_cost_usd": "0.01",
            "funding_txid": "",
            "token_sweep_txid": "",
            "native_sweep_txid": "",
            "metadata": {"test": True},
        }
        with patch("ledger.internal_api.timezone.now", return_value=now):
            rejected = self._post_signed(
                "internal_orphan_recovery_result",
                result_payload,
                nonce="orphan-result-wrong",
                now_value=now,
                url_kwargs={"session_public_id": session.public_id},
            )
        self.assertEqual(rejected.status_code, 400)

        audit = OrphanDepositRecoveryAudit.objects.get(
            chain="ethereum",
            deposit_address=DEPOSIT_ADDRESS,
        )
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK)
        self.assertIn(CLAIM_METADATA_KEY, audit.metadata)

        result_payload["claim_token"] = claim_token
        with patch("ledger.internal_api.timezone.now", return_value=now):
            accepted = self._post_signed(
                "internal_orphan_recovery_result",
                result_payload,
                nonce="orphan-result-correct",
                now_value=now,
                url_kwargs={"session_public_id": session.public_id},
            )
        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(
            accepted.json()["status"],
            OrphanDepositRecoveryAudit.STATUS_DUST_FINAL,
        )

        audit.refresh_from_db()
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_DUST_FINAL)
        self.assertNotIn(CLAIM_METADATA_KEY, audit.metadata)
        self.assertIsNotNone(audit.finalized_at)

    def test_claim_requires_authenticated_sweeper_signature(self):
        now = timezone.now()
        self._make_session(now=now)
        response = self.client.post(
            reverse("internal_orphan_recovery_claim"),
            data=json.dumps(self._claim_payload()),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 403)


    def test_pending_result_renews_claim_lease(self):
        now = timezone.now()
        session = self._make_session(now=now)
        claim = self._claim(now_value=now, nonce="orphan-renew-claim", lease_seconds=120)
        token = claim.json()["results"][0]["claim_token"]
        payload = {
            "claim_token": token,
            "status": OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK,
            "decision_reason": "broadcast_progress",
            "token_balance": 1,
            "native_balance": 0,
            "metadata": {"step": "funding"},
        }
        report_time = now + timedelta(seconds=100)
        with patch("ledger.internal_api.timezone.now", return_value=report_time), patch(
            "ledger.orphan_recovery.timezone.now", return_value=report_time
        ):
            response = self._post_signed(
                "internal_orphan_recovery_result",
                payload,
                nonce="orphan-renew-result",
                now_value=report_time,
                url_kwargs={"session_public_id": session.public_id},
            )
        self.assertEqual(response.status_code, 200)
        still_leased = self._claim(
            now_value=now + timedelta(seconds=130),
            nonce="orphan-renew-second-claim",
            lease_seconds=120,
        )
        self.assertEqual(still_leased.json()["results"], [])

    def test_result_rejects_negative_balances_and_reserved_metadata(self):
        now = timezone.now()
        session = self._make_session(now=now)
        claim = self._claim(now_value=now, nonce="orphan-validation-claim")
        token = claim.json()["results"][0]["claim_token"]
        base = {
            "claim_token": token,
            "status": OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK,
            "decision_reason": "validation",
            "token_balance": 0,
            "native_balance": 0,
            "metadata": {},
        }
        negative = dict(base, token_balance=-1)
        response = self._post_signed(
            "internal_orphan_recovery_result",
            negative,
            nonce="orphan-negative-result",
            now_value=now,
            url_kwargs={"session_public_id": session.public_id},
        )
        self.assertEqual(response.status_code, 400)

        fractional = dict(base, token_balance=1.5)
        response = self._post_signed(
            "internal_orphan_recovery_result",
            fractional,
            nonce="orphan-fractional-result",
            now_value=now,
            url_kwargs={"session_public_id": session.public_id},
        )
        self.assertEqual(response.status_code, 400)

        reserved = dict(base, metadata={CLAIM_METADATA_KEY: {"token": "forged"}})
        response = self._post_signed(
            "internal_orphan_recovery_result",
            reserved,
            nonce="orphan-reserved-result",
            now_value=now,
            url_kwargs={"session_public_id": session.public_id},
        )
        self.assertEqual(response.status_code, 400)
