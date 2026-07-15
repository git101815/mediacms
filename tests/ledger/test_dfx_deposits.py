from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import SimpleTestCase, TransactionTestCase

from ledger.dfx_deposits import (
    _preflight_dfx_purchase,
    open_dfx_deposit_session,
)
from ledger.models import TokenPack


class TestDfxDepositPreflight(SimpleTestCase):
    def _route(self):
        return {
            "key": (
                "arbitrum:USDC:"
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            ),
            "chain": "arbitrum",
            "asset_code": "USDC",
            "token_contract_address": (
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            ),
        }

    def _run_preflight(self, source_amount):
        with (
            patch(
                "ledger.dfx_deposits.list_available_deposit_options",
                return_value=[self._route()],
            ),
            patch(
                "ledger.dfx_deposits.find_dfx_asset_for_route",
                return_value={"id": 123},
            ),
            patch(
                "ledger.dfx_deposits._build_token_pack_snapshot",
                return_value={"gross_stable_amount": 10_000_000},
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_buy_quote",
                return_value={
                    "sourceAmount": str(source_amount),
                    "requestedTargetAmount": "10",
                    "estimatedTargetAmount": "10",
                },
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_fiat_currency",
                return_value="EUR",
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_fiat",
                return_value={"name": "EUR"},
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_bank_limits",
                return_value=(Decimal("10"), Decimal("100")),
            ),
        ):
            return _preflight_dfx_purchase(
                option_key=self._route()["key"],
                token_pack=SimpleNamespace(),
            )

    def test_preflight_rejects_quote_below_bank_minimum(self):
        with self.assertRaisesRegex(
            ValidationError,
            "below DFX's minimum",
        ):
            self._run_preflight("9.99")

    def test_preflight_rejects_quote_above_bank_maximum(self):
        with self.assertRaisesRegex(
            ValidationError,
            "above DFX's maximum",
        ):
            self._run_preflight("100.01")

    def test_preflight_accepts_quote_inside_bank_limits(self):
        result = self._run_preflight("12.34")
        self.assertEqual(result["asset"]["id"], 123)
        self.assertEqual(result["source_amount"], "12.34")

    def test_invalid_preflight_never_allocates_a_deposit_address(self):
        with (
            patch(
                "ledger.dfx_deposits.dfx_enabled",
                return_value=True,
            ),
            patch(
                "ledger.dfx_deposits._preflight_dfx_purchase",
                side_effect=ValidationError("DFX limit rejected"),
            ),
            patch(
                "ledger.dfx_deposits.open_user_deposit_session",
            ) as mocked_open,
        ):
            with self.assertRaisesRegex(ValidationError, "DFX limit rejected"):
                open_dfx_deposit_session(
                    actor=SimpleNamespace(),
                    wallet=SimpleNamespace(),
                    option_key=self._route()["key"],
                    token_pack=SimpleNamespace(),
                )

        mocked_open.assert_not_called()


class TestDfxDepositPreflightTransactions(TransactionTestCase):
    def test_real_pack_snapshot_runs_without_an_outer_transaction(self):
        token_pack = TokenPack.objects.create(
            code="dfx-preflight-transaction",
            name="DFX preflight transaction",
            description="",
            badge_text="",
            token_amount=1_000_000_000,
            gross_stable_amount=10_000_000,
            is_active=True,
            sort_order=0,
        )
        route = {
            "key": (
                "arbitrum:USDC:"
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            ),
            "chain": "arbitrum",
            "asset_code": "USDC",
            "token_contract_address": (
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            ),
        }

        with (
            patch(
                "ledger.dfx_deposits.list_available_deposit_options",
                return_value=[route],
            ),
            patch(
                "ledger.dfx_deposits.find_dfx_asset_for_route",
                return_value={"id": 123},
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_buy_quote",
                return_value={
                    "sourceAmount": "12.34",
                    "requestedTargetAmount": "10",
                    "estimatedTargetAmount": "10",
                },
            ) as mocked_quote,
            patch(
                "ledger.dfx_deposits.get_dfx_fiat_currency",
                return_value="EUR",
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_fiat",
                return_value={"name": "EUR"},
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_bank_limits",
                return_value=(Decimal("10"), Decimal("100")),
            ),
        ):
            result = _preflight_dfx_purchase(
                option_key=route["key"],
                token_pack=token_pack,
            )

        self.assertEqual(result["source_amount"], "12.34")
        mocked_quote.assert_called_once_with(
            asset_id=123,
            target_canonical_amount=10_000_000,
            fiat_currency="EUR",
        )
