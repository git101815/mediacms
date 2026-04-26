import json
from datetime import timedelta, timezone as dt_timezone
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from deposit_service.app.erc20_logs import (
    address_to_topic,
    decode_address_from_topic,
    decode_uint256,
)
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
        self.deposit_service_user = get_user_model().objects.create_user(
            username="deposit-service",
            email="deposit-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.deposit_service_user, "can_view_deposit_sessions")

    def _post_signed(self, payload, *, nonce="nonce-watchlist-1", now_value=None):
        now_value = now_value or timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
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
    def test_watchlist_returns_only_active_non_expired_targets(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        active = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/10",
            expires_at=mocked_now.return_value + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )
        create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x2222222222222222222222222222222222222222",
            address_derivation_ref="m/44'/60'/0'/0/11",
            expires_at=mocked_now.return_value - timedelta(seconds=1),
            required_confirmations=12,
            min_amount=100,
        )
        DepositSession.objects.filter(deposit_address="0x2222222222222222222222222222222222222222").update(
            status=DepositSession.STATUS_AWAITING_PAYMENT
        )
        recent_swept = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x3333333333333333333333333333333333333333",
            address_derivation_ref="m/44'/60'/0'/0/12",
            expires_at=mocked_now.return_value + timedelta(hours=1),
            status=DepositSession.STATUS_SWEPT,
            required_confirmations=12,
            min_amount=100,
        )
        old_swept = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x6666666666666666666666666666666666666666",
            address_derivation_ref="m/44'/60'/0'/0/15",
            expires_at=mocked_now.return_value + timedelta(hours=1),
            status=DepositSession.STATUS_SWEPT,
            required_confirmations=12,
            min_amount=100,
        )
        DepositSession.objects.filter(id=old_swept.id).update(
            updated_at=mocked_now.return_value - timedelta(days=8)
        )

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

        self.assertEqual(response.status_code, 200, response.content.decode())
        data = response.json()
        self.assertEqual(len(data["results"]), 1)
        targets = data["results"][0]["targets"]
        self.assertEqual(len(targets), 2)
        self.assertEqual(
            targets[0]["session_public_id"],
            str(active.public_id),
        )
        self.assertEqual(
            targets[0]["deposit_address"],
            "0x1111111111111111111111111111111111111111",
        )
        self.assertEqual(targets[0]["watch_reason"], "active")
        self.assertTrue(targets[0]["auto_credit"])

        self.assertEqual(
            targets[1]["session_public_id"],
            str(recent_swept.public_id),
        )
        self.assertEqual(
            targets[1]["deposit_address"],
            "0x3333333333333333333333333333333333333333",
        )
        self.assertEqual(targets[1]["watch_reason"], "residual")
        self.assertFalse(targets[1]["auto_credit"])

    @patch("ledger.internal_api.timezone.now")
    def test_watchlist_separates_routes_by_token_contract_address(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x4444444444444444444444444444444444444444",
            address_derivation_ref="m/44'/60'/0'/0/13",
            expires_at=mocked_now.return_value + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )
        create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            deposit_address="0x5555555555555555555555555555555555555555",
            address_derivation_ref="m/44'/60'/0'/0/14",
            expires_at=mocked_now.return_value + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )

        response = self._post_signed(
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    }
                ]
            },
            nonce="nonce-watchlist-2",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        targets = response.json()["results"][0]["targets"]
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["deposit_address"], "0x5555555555555555555555555555555555555555")

    @patch("ledger.internal_api.timezone.now")
    def test_watchlist_rejects_invalid_option_rows(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        response = self._post_signed(
            {"options": [{"chain": "", "asset_code": "USDT", "token_contract_address": ""}]},
            nonce="nonce-watchlist-invalid",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("chain and asset_code", response.json()["error"])


class TestErc20LogHelpers(BaseLedgerTestCase):
    def test_address_to_topic_left_pads_address(self):
        topic = address_to_topic("0x1111111111111111111111111111111111111111")
        self.assertEqual(
            topic,
            "0x0000000000000000000000001111111111111111111111111111111111111111",
        )

    def test_decode_address_from_topic_returns_lowercase_address(self):
        topic = "0x0000000000000000000000001111111111111111111111111111111111111111"
        decoded = decode_address_from_topic(topic)
        self.assertEqual(decoded, "0x1111111111111111111111111111111111111111")

    def test_decode_uint256_decodes_hex_value(self):
        value = decode_uint256("0x00000000000000000000000000000000000000000000000000000000000000ff")
        self.assertEqual(value, 255)