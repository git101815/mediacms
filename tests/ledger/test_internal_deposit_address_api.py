import json
from datetime import datetime, timedelta, timezone as dt_timezone
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone

from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositAddress, DepositRouteCounter, DepositSession
from ledger.services import build_deposit_option_key

from .base import BaseLedgerTestCase


@override_settings(
    LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME="deposit-service",
    LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET="super-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
    LEDGER_INTERNAL_ADDRESS_BATCH_MAX_SIZE=50,
    LEDGER_INTERNAL_ADDRESS_STATS_MAX_SIZE=50,
    LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED=False,
)
class TestInternalDepositAddressAPI(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        user_model = get_user_model()
        self.deposit_service_user = user_model.objects.create_user(
            username="deposit-service",
            email="deposit-service@example.com",
            password="test-password-123",
        )
        self.grant_perm(self.deposit_service_user, "can_manage_deposit_addresses")
        self.grant_perm(self.deposit_service_user, "can_manage_deposit_sessions")

    def _post_signed(self, url_name, payload, *, nonce="nonce-1", now_value=None):
        now_value = now_value or datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
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
            reverse(url_name),
            data=body,
            content_type="application/json",
            HTTP_X_LEDGER_SERVICE="deposit-service",
            HTTP_X_LEDGER_TIMESTAMP=timestamp,
            HTTP_X_LEDGER_NONCE=nonce,
            HTTP_X_LEDGER_SIGNATURE=signature,
        )

    @patch("ledger.internal_api.timezone.now")
    def test_provision_batch_creates_addresses(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        payload = {
            "addresses": [
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "display_label": "Ethereum · USDT",
                    "address": "0x1111111111111111111111111111111111111111",
                    "address_derivation_ref": "m/44'/60'/0'/0/0",
                    "required_confirmations": 12,
                    "min_amount": 100,
                    "session_ttl_seconds": 3600,
                    "metadata": {"provisioned_by": "deposit-service"},
                    "derivation_index": 0,
                },
                {
                    "chain": "bsc",
                    "asset_code": "USDT",
                    "token_contract_address": "0x55d398326f99059ff775485246999027b3197955",
                    "display_label": "BNB Chain · USDT",
                    "address": "0x2222222222222222222222222222222222222222",
                    "address_derivation_ref": "m/44'/60'/0'/0/1",
                    "required_confirmations": 12,
                    "min_amount": 100,
                    "session_ttl_seconds": 3600,
                    "metadata": {"provisioned_by": "deposit-service"},
                    "derivation_index": 1,
                },
            ]
        }

        response = self._post_signed(
            "internal_deposit_address_provision",
            payload,
            nonce="nonce-provision-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200, response.content.decode())
        self.assertEqual(DepositAddress.objects.count(), 2)

        data = response.json()
        self.assertEqual(data["created_count"], 2)
        self.assertEqual(data["existing_count"], 0)
        self.assertEqual(len(data["rows"]), 2)

    @patch("ledger.internal_api.timezone.now")
    def test_provision_batch_is_idempotent_with_new_nonce(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        payload = {
            "addresses": [
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "display_label": "Ethereum · USDT",
                    "address": "0x3333333333333333333333333333333333333333",
                    "address_derivation_ref": "m/44'/60'/0'/0/2",
                    "required_confirmations": 12,
                    "min_amount": 100,
                    "session_ttl_seconds": 3600,
                    "derivation_index": 2,
                }
            ]
        }

        first = self._post_signed(
            "internal_deposit_address_provision",
            payload,
            nonce="nonce-provision-a",
            now_value=mocked_now.return_value,
        )
        second = self._post_signed(
            "internal_deposit_address_provision",
            payload,
            nonce="nonce-provision-b",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(DepositAddress.objects.count(), 1)

        second_payload = second.json()
        self.assertEqual(second_payload["created_count"], 0)
        self.assertEqual(second_payload["existing_count"], 1)

    @patch("ledger.internal_api.timezone.now")
    def test_stats_returns_route_key_next_index_and_session_counts(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        route = DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x4444444444444444444444444444444444444444",
            address_derivation_ref="m/44'/60'/0'/0/2",
            derivation_index=2,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
            status=DepositAddress.STATUS_AVAILABLE,
        )

        route_key = "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7"
        DepositRouteCounter.objects.create(
            route_key=route_key,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            next_derivation_index=8,
        )

        DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain=route.chain,
            asset_code=route.asset_code,
            token_contract_address=route.token_contract_address,
            route_key=route_key,
            display_label="Ethereum · USDT",
            deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            address_derivation_ref="m/44'/60'/0'/0/5",
            derivation_index=5,
            derivation_path="m/44'/60'/0'/0/5",
            required_confirmations=12,
            min_amount=100,
            expires_at=mocked_now.return_value + timedelta(hours=1),
            status=DepositSession.STATUS_AWAITING_PAYMENT,
        )
        DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain=route.chain,
            asset_code=route.asset_code,
            token_contract_address=route.token_contract_address,
            route_key=route_key,
            display_label="Ethereum · USDT",
            deposit_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            address_derivation_ref="m/44'/60'/0'/0/6",
            derivation_index=6,
            derivation_path="m/44'/60'/0'/0/6",
            required_confirmations=12,
            min_amount=100,
            expires_at=mocked_now.return_value + timedelta(hours=1),
            status=DepositSession.STATUS_SWEPT,
        )

        payload = {
            "options": [
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                }
            ]
        }

        response = self._post_signed(
            "internal_deposit_address_stats",
            payload,
            nonce="nonce-stats-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200, response.content.decode())
        data = response.json()
        self.assertEqual(len(data["results"]), 1)

        result = data["results"][0]
        self.assertEqual(result["chain"], "ethereum")
        self.assertEqual(result["asset_code"], "USDT")
        self.assertEqual(result["token_contract_address"], "0xdac17f958d2ee523a2206206994597c13d831ec7")
        self.assertEqual(result["route_key"], route_key)
        self.assertEqual(result["next_derivation_index"], 8)
        self.assertEqual(result["active_session_count"], 1)
        self.assertEqual(result["total_session_count"], 2)

    @patch("ledger.internal_api.timezone.now")
    def test_stats_defaults_next_index_to_zero_when_counter_is_missing(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        DepositAddress.objects.create(
            chain="bsc",
            asset_code="USDT",
            token_contract_address="0x55d398326f99059ff775485246999027b3197955",
            display_label="BNB Chain · USDT",
            address="0x5555555555555555555555555555555555555555",
            address_derivation_ref="m/44'/60'/0'/0/7",
            derivation_index=7,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
            status=DepositAddress.STATUS_AVAILABLE,
        )

        payload = {
            "options": [
                {
                    "chain": "bsc",
                    "asset_code": "USDT",
                    "token_contract_address": "0x55d398326f99059ff775485246999027b3197955",
                }
            ]
        }

        response = self._post_signed(
            "internal_deposit_address_stats",
            payload,
            nonce="nonce-stats-2",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["results"][0]["next_derivation_index"], 0)
        self.assertEqual(data["results"][0]["active_session_count"], 0)
        self.assertEqual(data["results"][0]["total_session_count"], 0)

    @patch("ledger.internal_api.timezone.now")
    def test_stats_rejects_missing_chain_or_asset_code(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        response = self._post_signed(
            "internal_deposit_address_stats",
            {
                "options": [
                    {
                        "chain": "",
                        "asset_code": "USDT",
                        "token_contract_address": "",
                    }
                ]
            },
            nonce="nonce-stats-invalid-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("chain and asset_code", response.json()["error"])

    @patch("ledger.internal_api.timezone.now")
    def test_provision_rejects_conflicting_existing_rows(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x6666666666666666666666666666666666666666",
            address_derivation_ref="m/44'/60'/0'/0/8",
            derivation_index=8,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )
        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x7777777777777777777777777777777777777777",
            address_derivation_ref="m/44'/60'/0'/0/9",
            derivation_index=9,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        response = self._post_signed(
            "internal_deposit_address_provision",
            {
                "addresses": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "display_label": "Ethereum · USDT",
                        "address": "0x6666666666666666666666666666666666666666",
                        "address_derivation_ref": "m/44'/60'/0'/0/9",
                        "required_confirmations": 12,
                        "min_amount": 100,
                        "session_ttl_seconds": 3600,
                        "derivation_index": 9,
                    }
                ]
            },
            nonce="nonce-provision-conflict-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("different rows", response.json()["error"])