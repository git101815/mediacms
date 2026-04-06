import json
from datetime import timedelta
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositSession
from ledger.services import create_deposit_session
from .base import BaseLedgerTestCase


@override_settings(
    LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME="deposit-service",
    LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET="super-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
)
class TestInternalDepositWatchlistAPI(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.deposit_service_user = self.user_model.objects.create_user(
            username="deposit-service",
            email="deposit-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.deposit_service_user, "can_view_deposit_sessions")

        create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="evm:ethereum:external:10",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )

    def _post_signed(self, payload, *, nonce="nonce-watchlist-1", now_value=None):
        now_value = now_value or timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        timestamp = str(int(now_value.timestamp()))
        signature = build_internal_request_signature(
            service_name="deposit-service",
            timestamp=timestamp,
            nonce=nonce,
            body_bytes=body,
            shared_secret="super-secret",
        )
        return self.client.post(
            reverse("internal_deposit_watchlist"),
            data=body,
            content_type="application/json",
            HTTP_X_LEDGER_SERVICE="deposit-service",
            HTTP_X_LEDGER_TIMESTAMP=timestamp,
            HTTP_X_LEDGER_NONCE=nonce,
            HTTP_X_LEDGER_SIGNATURE=signature,
        )

    @patch("ledger.internal_api.timezone.now")
    def test_watchlist_returns_active_targets(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)

        response = self._post_signed(
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    }
                ]
            },
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data["results"]), 1)
        self.assertEqual(len(data["results"][0]["targets"]), 1)
        self.assertEqual(
            data["results"][0]["targets"][0]["deposit_address"],
            "0x1111111111111111111111111111111111111111",
        )