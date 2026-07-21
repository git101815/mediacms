from unittest.mock import patch

from django.test import SimpleTestCase

from files.views import _build_wallet_deposit_options
from ledger.providers.dfx import (
    DFX_PAYMENT_METHOD_KEY,
    DFX_PROVIDER_KEY,
)
from ledger.providers.paygate import PAYGATE_PROVIDER_KEY


class TestWalletProviderRouting(SimpleTestCase):
    def test_direct_crypto_and_dfx_are_distinct_payment_groups(self):
        paygate_options = [
            {
                "key": "paygate:usd:paypal:hosted_checkout",
                "chain": "paygate",
                "asset_code": "USD",
                "min_amount": 1_000_000,
                "payment_method_key": "paygate:paypal",
                "payment_method_label": "PayPal",
                "payment_method_type": "provider",
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": "paypal",
            },
            {
                "key": "paygate:eur:revolut:hosted_checkout",
                "chain": "paygate",
                "asset_code": "EUR",
                "min_amount": 1_000_000,
                "payment_method_key": "paygate:revolut",
                "payment_method_label": "Revolut",
                "payment_method_type": "provider",
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": "revolut",
            },
            {
                "key": "paygate:usd:transak:hosted_checkout",
                "chain": "paygate",
                "asset_code": "USD",
                "min_amount": 15_000_000,
                "payment_method_key": "paygate:transak",
                "payment_method_label": "Transak",
                "payment_method_type": "provider",
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": "transak",
            },
        ]
        dfx_options = [
            {
                "key": (
                    "dfx:arbitrum:USDC:"
                    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                ),
                "deposit_route_key": (
                    "arbitrum:USDC:"
                    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                ),
                "chain": "arbitrum",
                "asset_code": "USDC",
                "token_contract_address": (
                    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                ),
                "min_amount": 10_000_000,
                "payment_method_key": DFX_PAYMENT_METHOD_KEY,
                "payment_method_label": "Bank transfer (DFX)",
                "payment_method_type": "provider",
                "provider_key": DFX_PROVIDER_KEY,
                "payment_currency": "EUR",
                "payment_currency_usd_rate": "1.12",
                "payment_requires_route_selection": False,
                "payment_price_mode": "fixed",
            },
        ]
        direct_crypto_options = [
            {
                "key": (
                    "arbitrum:USDC:"
                    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                ),
                "chain": "arbitrum",
                "asset_code": "USDC",
                "token_contract_address": (
                    "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                ),
                "min_amount": 1_000_000,
            },
            {
                "key": (
                    "ethereum:USDT:"
                    "0xdac17f958d2ee523a2206206994597c13d831ec7"
                ),
                "chain": "ethereum",
                "asset_code": "USDT",
                "token_contract_address": (
                    "0xdac17f958d2ee523a2206206994597c13d831ec7"
                ),
                "min_amount": 1_000_000,
            },
        ]

        with (
            patch(
                "files.views.get_malum_deposit_option",
                return_value=None,
            ),
            patch(
                "files.views.get_paygate_deposit_options",
                return_value=paygate_options,
            ),
            patch(
                "files.views.get_dfx_deposit_options",
                return_value=dfx_options,
            ),
            patch(
                "files.views.list_available_deposit_options",
                return_value=direct_crypto_options,
            ),
        ):
            options = _build_wallet_deposit_options()

        groups = {}
        for option in options:
            group_key = option["payment_group_key"]
            groups.setdefault(group_key, []).append(option)

        self.assertEqual(
            set(groups),
            {"crypto", "paypal_us", "revolut_eu", "transak_card", "dfx_bank"},
        )

        direct_crypto_rows = groups["crypto"]
        self.assertEqual(len(direct_crypto_rows), 2)
        self.assertTrue(
            all(
                option["payment_method_type"] == "crypto"
                and option["payment_requires_route_selection"]
                for option in direct_crypto_rows
            )
        )

        dfx_rows = groups["dfx_bank"]
        self.assertEqual(len(dfx_rows), 1)
        self.assertEqual(
            dfx_rows[0]["payment_method_key"],
            DFX_PAYMENT_METHOD_KEY,
        )
        self.assertEqual(dfx_rows[0]["payment_method_type"], "provider")
        self.assertEqual(dfx_rows[0]["provider_key"], DFX_PROVIDER_KEY)
        self.assertFalse(dfx_rows[0]["payment_requires_route_selection"])
        self.assertEqual(dfx_rows[0]["payment_price_mode"], "fixed")

        self.assertEqual(
            groups["paypal_us"][0]["payment_method_type"],
            "provider",
        )
        self.assertEqual(
            groups["transak_card"][0]["payment_method_type"],
            "provider",
        )
        self.assertEqual(
            groups["revolut_eu"][0]["payment_method_type"],
            "provider",
        )

        for group_key, provider_id in {
            "paypal_us": "paypal",
            "revolut_eu": "revolut",
            "transak_card": "transak",
        }.items():
            option = groups[group_key][0]
            self.assertEqual(option["payment_method_type"], "provider")
            self.assertEqual(option["provider_key"], PAYGATE_PROVIDER_KEY)
            self.assertEqual(option["paygate_provider_id"], provider_id)
