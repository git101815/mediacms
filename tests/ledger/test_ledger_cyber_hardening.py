import tempfile
from datetime import timedelta
from unittest.mock import patch

from django.core.exceptions import PermissionDenied, ValidationError
from django.test import RequestFactory, override_settings
from django.utils import timezone

from ledger.internal_api import authenticate_internal_deposit_request
from ledger.models import DepositSession
from ledger.services import (
    LEDGER_OPERATION_FLAG_DEPOSIT_OPEN,
    create_deposit_session,
    list_ledger_operation_flags,
    open_user_deposit_session,
    set_ledger_operation_flag,
)

from .base import BaseLedgerTestCase


class TestLedgerCyberHardening(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.flags_file = tempfile.NamedTemporaryFile(delete=True)
        self.flags_file.close()

    def _flags_override(self):
        return override_settings(LEDGER_OPERATIONAL_FLAGS_PATH=self.flags_file.name)

    def test_ledger_flag_file_can_disable_deposit_open_without_restart(self):
        with self._flags_override():
            self.assertTrue(list_ledger_operation_flags()[LEDGER_OPERATION_FLAG_DEPOSIT_OPEN]["enabled"])
            set_ledger_operation_flag(
                key=LEDGER_OPERATION_FLAG_DEPOSIT_OPEN,
                enabled=False,
                reason="test incident",
                actor=self.operator,
            )

            with self.assertRaises(ValidationError):
                open_user_deposit_session(
                    actor=self.u1,
                    wallet=self.w1,
                    option_key=self.default_deposit_option_key(),
                    token_pack=self.default_token_pack,
                )

    @override_settings(
        LEDGER_DEPOSIT_OPEN_COOLDOWN_THRESHOLD=3,
        LEDGER_DEPOSIT_OPEN_COOLDOWN_WINDOW_SECONDS=5 * 60,
        LEDGER_DEPOSIT_OPEN_COOLDOWN_SECONDS=15 * 60,
    )
    @patch("ledger.services._derive_session_deposit_address")
    def test_deposit_open_cooldown_blocks_fourth_new_session_after_three_recent_sessions(self, mocked_derive):
        now = timezone.now()
        for index in range(3):
            create_deposit_session(
                actor=self.u1,
                wallet=self.w1,
                chain="ethereum",
                asset_code="USDT",
                token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
                deposit_address=f"0x{index + 1:040x}",
                address_derivation_ref=f"m/44'/60'/0'/0/{1000 + index}",
                expires_at=now + timedelta(hours=1),
                required_confirmations=12,
                min_amount=self.default_token_pack.gross_stable_amount,
            )

        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "m/44'/60'/0'/0/9999",
        )

        with self.assertRaises(ValidationError):
            open_user_deposit_session(
                actor=self.u1,
                wallet=self.w1,
                option_key=self.default_deposit_option_key(),
                token_pack=self.default_token_pack,
            )

        mocked_derive.assert_not_called()

    @override_settings(
        LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED=True,
        LEDGER_INTERNAL_GATEWAY_SECRET="gateway-secret-for-tests",
        LEDGER_INTERNAL_API_ALLOWED_CIDRS=["127.0.0.1/32"],
    )
    def test_internal_api_network_guard_rejects_missing_gateway_secret_before_hmac(self):
        request = RequestFactory().post(
            "/api/internal/ledger/deposit-watchlist",
            data=b"{}",
            content_type="application/json",
            REMOTE_ADDR="127.0.0.1",
        )

        with self.assertRaises(PermissionDenied):
            authenticate_internal_deposit_request(request)

    @override_settings(
        LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED=True,
        LEDGER_INTERNAL_GATEWAY_SECRET="gateway-secret-for-tests",
        LEDGER_INTERNAL_API_ALLOWED_CIDRS=["10.0.0.0/8"],
    )
    def test_internal_api_network_guard_rejects_disallowed_source_address(self):
        request = RequestFactory().post(
            "/api/internal/ledger/deposit-watchlist",
            data=b"{}",
            content_type="application/json",
            HTTP_X_LEDGER_INTERNAL_GATEWAY="gateway-secret-for-tests",
            REMOTE_ADDR="203.0.113.10",
        )

        with self.assertRaises(PermissionDenied):
            authenticate_internal_deposit_request(request)
