from django.test import SimpleTestCase

from ledger.dashboard import config as wallet_config
from ledger.models import DepositAddress
from ledger.services import list_available_deposit_options

from .base import BaseLedgerTestCase


class TestWalletPaymentVisualCatalog(SimpleTestCase):
    def test_checkout_wallets_use_their_dedicated_logos(self):
        methods = wallet_config.get_wallet_checkout_methods()

        self.assertEqual(
            methods["apple_pay"]["icon_path"],
            "images/wallet/apple_pay.svg",
        )
        self.assertEqual(
            methods["google_pay"]["icon_path"],
            "images/wallet/google_pay.svg",
        )

    def test_crypto_catalog_contains_stablecoins_and_native_assets(self):
        assets = wallet_config.get_wallet_crypto_asset_groups()

        self.assertEqual(set(assets), {"USDC", "USDT", "ETH", "BNB", "POL"})


class TestWalletCryptoRouteCatalog(BaseLedgerTestCase):
    def test_available_and_allocated_templates_are_visible_but_retired_is_not(self):
        routes = (
            ("base", "USDC", "0x0000000000000000000000000000000000000101", DepositAddress.STATUS_AVAILABLE, {}),
            ("ethereum", "USDT", "0x0000000000000000000000000000000000000102", DepositAddress.STATUS_AVAILABLE, {}),
            ("ethereum", "ETH", "", DepositAddress.STATUS_ALLOCATED, {"amount_semantics": "native_quoted"}),
            ("bsc", "BNB", "", DepositAddress.STATUS_ALLOCATED, {"amount_semantics": "native_quoted"}),
            ("polygon", "POL", "", DepositAddress.STATUS_ALLOCATED, {"amount_semantics": "native_quoted"}),
        )

        for index, (chain, asset, contract, status, metadata) in enumerate(
            routes,
            start=101,
        ):
            DepositAddress.objects.create(
                chain=chain,
                asset_code=asset,
                token_contract_address=contract,
                display_label=f"{chain} · {asset}",
                address=f"0x{index:040x}",
                address_derivation_ref=f"m/44'/60'/0'/0/{index}",
                derivation_index=index,
                required_confirmations=1,
                min_amount=1_000_000,
                session_ttl_seconds=3600,
                status=status,
                metadata=metadata,
            )

        DepositAddress.objects.create(
            chain="base",
            asset_code="ETH",
            token_contract_address="",
            display_label="Base · ETH retired",
            address="0x0000000000000000000000000000000000000201",
            address_derivation_ref="m/44'/60'/0'/0/201",
            derivation_index=201,
            required_confirmations=1,
            min_amount=1_000_000,
            session_ttl_seconds=3600,
            status=DepositAddress.STATUS_RETIRED,
            metadata={"amount_semantics": "native_quoted"},
        )

        options = list_available_deposit_options()

        self.assertEqual(
            {option["asset_code"] for option in options},
            {"USDC", "USDT", "ETH", "BNB", "POL"},
        )
        self.assertNotIn(
            "base:ETH:native",
            {option["key"] for option in options},
        )
