import json
from unittest.mock import patch

from django.test import override_settings
from django.urls import reverse
from datetime import datetime, timezone as dt_timezone
from django.contrib.auth import get_user_model
from ledger.internal_api import build_internal_request_signature
from ledger.models import DepositAddress
from .base import BaseLedgerTestCase


@override_settings(
    LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME="deposit-service",
    LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET="super-secret",
    LEDGER_INTERNAL_API_MAX_SKEW_SECONDS=300,
    LEDGER_INTERNAL_NONCE_TTL_SECONDS=900,
    LEDGER_INTERNAL_ADDRESS_BATCH_MAX_SIZE=50,
    LEDGER_INTERNAL_ADDRESS_STATS_MAX_SIZE=50,
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
                    "address_derivation_ref": "evm:ethereum:external:0",
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
                    "display_label": "BSC · USDT",
                    "address": "0x2222222222222222222222222222222222222222",
                    "address_derivation_ref": "evm:bsc:external:0",
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

        self.assertEqual(response.status_code, 200)
        self.assertEqual(DepositAddress.objects.count(), 2)
        payload = response.json()
        self.assertEqual(payload["created_count"], 2)
        self.assertEqual(payload["existing_count"], 0)

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
                    "address_derivation_ref": "evm:ethereum:external:1",
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
    def test_pool_stats_returns_counts(self, mocked_now):
        mocked_now.return_value = datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)

        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x4444444444444444444444444444444444444444",
            address_derivation_ref="evm:ethereum:external:2",
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
            status=DepositAddress.STATUS_AVAILABLE,
        )

        payload = {
            "options": [
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "start_index": 0,
                }
            ]
        }

        response = self._post_signed(
            "internal_deposit_address_stats",
            payload,
            nonce="nonce-stats-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["results"][0]["available_count"], 1)
        self.assertEqual(data["results"][0]["allocated_count"], 0)
        self.assertEqual(data["results"][0]["retired_count"], 0)
        self.assertEqual(data["results"][0]["max_derivation_index"], 2)
        self.assertEqual(data["results"][0]["next_derivation_index"], 3)

    @patch("ledger.internal_api.timezone.now")
    def test_pool_stats_falls_back_to_derivation_ref_when_index_is_null(self, mocked_now):
        mocked_now.return_value = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=timezone.utc)

        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x5555555555555555555555555555555555555555",
            address_derivation_ref="evm:ethereum:external:7",
            derivation_index=None,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
            status=DepositAddress.STATUS_AVAILABLE,
        )

        payload = {
            "options": [
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "start_index": 0,
                }
            ]
        }

        response = self._post_signed(
            "internal_deposit_address_stats",
            payload,
            nonce="nonce-stats-fallback-1",
            now_value=mocked_now.return_value,
        )

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["results"][0]["max_derivation_index"], 7)
        self.assertEqual(data["results"][0]["next_derivation_index"], 8)