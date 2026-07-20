from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase, TransactionTestCase

from ledger.dfx_deposits import (
    _preflight_dfx_purchase,
    open_dfx_deposit_session,
)
from ledger.models import TokenPack


class TestDfxDepositPreflight(TestCase):
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
                return_value="CHF",
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_fiat",
                return_value={"name": "CHF"},
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
                return_value="CHF",
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_fiat",
                return_value={"name": "CHF"},
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
            fiat_currency="CHF",
        )


from datetime import timedelta as _timedelta

from django.test import override_settings as _override_settings
from django.utils import timezone as _timezone

from ledger.dfx_deposits import prepare_dfx_browser_launch as _prepare_dfx_browser_launch
from ledger.models import DepositSession as _DepositSession

from .base import BaseLedgerTestCase as _BaseLedgerTestCase


@_override_settings(
    DFX_API_BASE_URL="https://api.example",
    DFX_APP_BASE_URL="https://app.example",
    DFX_PUBLIC_BASE_URL="https://site.example",
    DFX_FIAT_CURRENCY="CHF",
    DFX_LANGUAGE="en",
    DFX_WALLET_POOL_JSON='[{"id":67,"name":"Edge"}]',
    DFX_LAUNCH_QUOTE_MAX_AGE_SECONDS=1800,
    WALLET_FIAT_USD_RATES={"CHF": "1.12"},
)
class TestDfxLaunchSnapshotReuse(_BaseLedgerTestCase):
    route_key = (
        "arbitrum:USDC:"
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    )
    deposit_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    def _quote(self, *, source="12.34", target="10"):
        return {
            "sourceAmount": source,
            "requestedTargetAmount": target,
            "estimatedTargetAmount": target,
        }

    def _asset(self, asset_id=123):
        return {
            "id": asset_id,
            "uniqueName": "Arbitrum/USDC",
        }

    def _session(
        self,
        *,
        prepared_at=None,
        snapshot_route_key=None,
    ):
        prepared_at = prepared_at or _timezone.now()
        snapshot_route_key = snapshot_route_key or self.route_key
        return _DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="arbitrum",
            asset_code="USDC",
            token_contract_address=(
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            ),
            route_key=self.route_key,
            display_label="Bank transfer (DFX)",
            deposit_address=self.deposit_address,
            address_derivation_ref="m/44'/60'/0'/0/18",
            derivation_index=18,
            derivation_path="m/44'/60'/0'/0/18",
            expires_at=_timezone.now() + _timedelta(days=7),
            status=_DepositSession.STATUS_AWAITING_PAYMENT,
            required_confirmations=1,
            min_amount=10_000_000,
            metadata={
                "payment_provider": {
                    "key": "dfx",
                    "label": "Bank transfer (DFX)",
                    "status": "READY_TO_LAUNCH",
                },
                "dfx_launch_snapshot": {
                    "route_key": snapshot_route_key,
                    "chain": "arbitrum",
                    "target_canonical_amount": 10_000_000,
                    "currency": "CHF",
                    "checkout_amount": "12.34",
                    "asset": self._asset(),
                    "quote": self._quote(),
                    "prepared_at": prepared_at.isoformat(),
                },
            },
        )

    def _signer_result(self):
        return {
            "address": self.deposit_address,
            "signature": "0x" + ("11" * 65),
            "message": "signed",
        }

    def test_fresh_snapshot_skips_asset_lookup_and_quote(self):
        session = self._session()
        with (
            patch(
                "ledger.dfx_deposits.sign_dfx_auth_message",
                return_value=self._signer_result(),
            ),
            patch(
                "ledger.dfx_deposits.find_dfx_asset_for_route",
            ) as mocked_find,
            patch(
                "ledger.dfx_deposits.get_dfx_buy_quote",
            ) as mocked_quote,
        ):
            launch = _prepare_dfx_browser_launch(
                session=session,
                actor=self.u1,
            )

        mocked_find.assert_not_called()
        mocked_quote.assert_not_called()
        self.assertEqual(launch["checkout_params"]["amount-in"], "12.34")
        self.assertEqual(launch["checkout_params"]["asset-out"], "123")
        self.assertEqual(launch["auth_payload"]["walletId"], 67)
        session.refresh_from_db()
        self.assertEqual(session.metadata["payment_provider"]["dfx_wallet_id"], 67)

    def _assert_snapshot_refreshes(self, session):
        new_asset = self._asset(asset_id=456)
        new_quote = self._quote(source="13.01", target="10")
        with (
            patch(
                "ledger.dfx_deposits.sign_dfx_auth_message",
                return_value=self._signer_result(),
            ),
            patch(
                "ledger.dfx_deposits.find_dfx_asset_for_route",
                return_value=new_asset,
            ) as mocked_find,
            patch(
                "ledger.dfx_deposits.get_dfx_buy_quote",
                return_value=new_quote,
            ) as mocked_quote,
        ):
            launch = _prepare_dfx_browser_launch(
                session=session,
                actor=self.u1,
            )

        mocked_find.assert_called_once()
        mocked_quote.assert_called_once_with(
            asset_id=456,
            target_canonical_amount=10_000_000,
            fiat_currency="CHF",
        )
        self.assertEqual(launch["checkout_params"]["amount-in"], "13.01")

        session.refresh_from_db()
        snapshot = session.metadata["dfx_launch_snapshot"]
        self.assertEqual(snapshot["asset"]["id"], 456)
        self.assertEqual(snapshot["checkout_amount"], "13.01")

    def test_expired_snapshot_fetches_a_new_quote(self):
        session = self._session(
            prepared_at=_timezone.now() - _timedelta(minutes=31),
        )
        self._assert_snapshot_refreshes(session)

    def test_snapshot_for_another_route_fetches_a_new_quote(self):
        session = self._session(snapshot_route_key="wrong:route:key")
        self._assert_snapshot_refreshes(session)

