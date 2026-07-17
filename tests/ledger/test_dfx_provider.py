from decimal import Decimal
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from django.test import SimpleTestCase, override_settings

from ledger.providers.dfx import (
    build_dfx_auth_payload,
    build_dfx_checkout_url,
    find_dfx_asset_for_route,
    get_dfx_buy_quote,
    round_dfx_source_amount,
)


@override_settings(
    DFX_API_BASE_URL="https://api.example",
    DFX_APP_BASE_URL="https://app.example",
    DFX_PUBLIC_BASE_URL="https://site.example",
    DFX_FIAT_CURRENCY="EUR",
    DFX_PAYMENT_METHOD="Bank",
    DFX_LANGUAGE="en",
    WALLET_FIAT_USD_RATES={"EUR": "1.12"},
)
class TestDfxProvider(SimpleTestCase):
    def test_route_matches_blockchain_asset_and_contract(self):
        asset = find_dfx_asset_for_route(
            chain="arbitrum",
            asset_code="USDC",
            token_contract_address=(
                "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
            ),
            assets=[
                {
                    "id": 123,
                    "name": "USDC",
                    "dexName": "USDC",
                    "uniqueName": "Arbitrum/USDC",
                    "chainId": (
                        "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
                    ),
                    "blockchain": "Arbitrum",
                    "buyable": True,
                    "comingSoon": False,
                }
            ],
        )
        self.assertEqual(asset["id"], 123)

    @patch("ledger.providers.dfx.call_dfx_json")
    def test_quote_uses_target_amount_not_source_amount(self, mocked_call):
        mocked_call.return_value = {
            "isValid": True,
            "amount": 12.34,
            "estimatedAmount": 10,
        }
        quote = get_dfx_buy_quote(
            asset_id=123,
            target_canonical_amount=10_000_000,
        )
        payload = mocked_call.call_args.kwargs["payload"]
        self.assertNotIn("amount", payload)
        self.assertEqual(payload["targetAmount"], 10.0)
        self.assertEqual(payload["paymentMethod"], "Bank")
        self.assertEqual(quote["sourceAmount"], "12.34")


    def test_auth_payload_contains_exact_dfx_fields(self):
        payload = build_dfx_auth_payload(
            address="0x1111111111111111111111111111111111111111",
            signature="0x" + "11" * 65,
            chain="arbitrum",
        )
        self.assertEqual(payload["blockchain"], "Arbitrum")
        self.assertEqual(payload["language"], "EN")
        self.assertNotIn("wallet", payload)

    def test_source_amount_rounds_up(self):
        self.assertEqual(
            round_dfx_source_amount(Decimal("12.3401")),
            "12.35",
        )

    def test_checkout_url_contains_tracking_and_route(self):
        url = build_dfx_checkout_url(
            access_token="jwt-token",
            asset={"id": 123},
            chain="arbitrum",
            fiat_currency="EUR",
            source_amount="12.35",
            external_transaction_id="session-id",
            redirect_uri="https://site.example/return",
            customer_email="buyer@example.com",
        )
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        self.assertEqual(parsed.path, "/buy")
        self.assertEqual(query["session"], ["jwt-token"])
        self.assertEqual(query["asset-out"], ["123"])
        self.assertEqual(query["blockchain"], ["Arbitrum"])
        self.assertEqual(query["asset-in"], ["EUR"])
        self.assertEqual(query["amount-in"], ["12.35"])
        self.assertEqual(query["payment-method"], ["bank"])
        self.assertNotIn("mail", query)
        self.assertEqual(
            query["external-transaction-id"],
            ["session-id"],
        )


class _DfxTestCache:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, timeout=None):
        self.values[key] = value


@override_settings(
    DFX_API_BASE_URL="https://api.example",
    DFX_FIAT_CURRENCY="EUR",
    DFX_PAYMENT_METHOD="Bank",
    DFX_QUOTE_CACHE_SECONDS=60,
    DFX_WALLET_NAME="",
    WALLET_FIAT_USD_RATES={"EUR": "1.12", "CHF": "1.10"},
)
class TestDfxQuoteCache(SimpleTestCase):
    def _response(self):
        return {
            "isValid": True,
            "amount": 12.34,
            "estimatedAmount": 10,
        }

    def test_identical_quotes_reuse_the_short_cache(self):
        fake_cache = _DfxTestCache()
        with (
            patch("ledger.providers.dfx.cache", fake_cache),
            patch(
                "ledger.providers.dfx.call_dfx_json",
                return_value=self._response(),
            ) as mocked_call,
        ):
            first = get_dfx_buy_quote(
                asset_id=123,
                target_canonical_amount=10_000_000,
                fiat_currency="EUR",
            )
            second = get_dfx_buy_quote(
                asset_id=123,
                target_canonical_amount=10_000_000,
                fiat_currency="EUR",
            )

        self.assertEqual(first, second)
        mocked_call.assert_called_once()

    def test_quote_cache_isolated_by_asset_amount_and_currency(self):
        fake_cache = _DfxTestCache()
        with (
            patch("ledger.providers.dfx.cache", fake_cache),
            patch(
                "ledger.providers.dfx.call_dfx_json",
                return_value=self._response(),
            ) as mocked_call,
        ):
            get_dfx_buy_quote(
                asset_id=123,
                target_canonical_amount=10_000_000,
                fiat_currency="EUR",
            )
            get_dfx_buy_quote(
                asset_id=124,
                target_canonical_amount=10_000_000,
                fiat_currency="EUR",
            )
            get_dfx_buy_quote(
                asset_id=124,
                target_canonical_amount=11_000_000,
                fiat_currency="EUR",
            )
            get_dfx_buy_quote(
                asset_id=124,
                target_canonical_amount=11_000_000,
                fiat_currency="CHF",
            )

        self.assertEqual(mocked_call.call_count, 4)

