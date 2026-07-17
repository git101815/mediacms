from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from ledger.dfx_deposits import get_dfx_deposit_options
from ledger.providers.dfx import get_dfx_settlement_route_preferences


ARBITRUM_USDC_KEY = (
    "arbitrum:USDC:"
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
)
BASE_USDC_KEY = (
    "base:USDC:"
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
)


def route(key, chain):
    return {
        "key": key,
        "chain": chain,
        "asset_code": "USDC",
        "token_contract_address": key.rsplit(":", 1)[-1],
        "required_confirmations": 1,
        "min_amount": 1_000_000,
    }


@override_settings(
    DFX_SETTLEMENT_ROUTE_PREFERENCES=(
        "base:USDC",
        "arbitrum:USDC",
    ),
    WALLET_FIAT_USD_RATES={"EUR": "1.12"},
)
class TestDfxProductModel(SimpleTestCase):
    def _common_patches(self):
        return (
            patch("ledger.dfx_deposits.dfx_enabled", return_value=True),
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
                return_value=(Decimal("10"), Decimal("1000")),
            ),
            patch(
                "ledger.dfx_deposits.get_dfx_assets_for_blockchain",
                return_value=[],
            ),
            patch(
                "ledger.dfx_deposits.list_available_deposit_options",
                return_value=[
                    route(ARBITRUM_USDC_KEY, "arbitrum"),
                    route(BASE_USDC_KEY, "base"),
                ],
            ),
        )

    def test_route_preferences_accept_comma_separated_configuration(self):
        with override_settings(
            DFX_SETTLEMENT_ROUTE_PREFERENCES=(
                "base:USDC, arbitrum:USDC"
            )
        ):
            self.assertEqual(
                get_dfx_settlement_route_preferences(),
                ("base:USDC", "arbitrum:USDC"),
            )

    def test_dfx_is_one_direct_provider_using_first_healthy_preference(self):
        def match_asset(*, chain, **kwargs):
            return {
                "id": 123,
                "uniqueName": f"{chain}/USDC",
            }

        patches = self._common_patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4],
            patches[5],
            patch(
                "ledger.dfx_deposits.find_dfx_asset_for_route",
                side_effect=match_asset,
            ),
        ):
            options = get_dfx_deposit_options()

        self.assertEqual(len(options), 1)
        option = options[0]
        self.assertEqual(option["deposit_route_key"], BASE_USDC_KEY)
        self.assertEqual(option["payment_method_type"], "provider")
        self.assertEqual(option["provider_key"], "dfx")
        self.assertFalse(option["payment_requires_route_selection"])
        self.assertEqual(option["payment_price_mode"], "fixed")
        self.assertEqual(option["payment_currency"], "EUR")
        self.assertEqual(option["label"], "Bank transfer (DFX)")
        self.assertNotIn("USDC", option["label"])
        self.assertNotIn("Base", option["label"])

    def test_unavailable_preference_falls_back_without_exposing_routes(self):
        def match_asset(*, chain, **kwargs):
            if chain == "base":
                return None
            return {
                "id": 456,
                "uniqueName": f"{chain}/USDC",
            }

        patches = self._common_patches()
        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4],
            patches[5],
            patch(
                "ledger.dfx_deposits.find_dfx_asset_for_route",
                side_effect=match_asset,
            ),
        ):
            options = get_dfx_deposit_options()

        self.assertEqual(len(options), 1)
        option = options[0]
        self.assertEqual(option["deposit_route_key"], ARBITRUM_USDC_KEY)
        self.assertFalse(option["payment_requires_route_selection"])
        self.assertEqual(option["route_label"], "Bank transfer (DFX)")
