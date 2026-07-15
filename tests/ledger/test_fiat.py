from decimal import Decimal

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings

from ledger.fiat import (
    canonical_stable_to_fiat_amount,
    get_fiat_usd_rate,
)


class TestFiatConversion(SimpleTestCase):
    @override_settings(WALLET_FIAT_USD_RATES={"EUR": "1.12"})
    def test_eur_usd_rate_converts_canonical_usd_to_eur(self):
        self.assertEqual(get_fiat_usd_rate("EUR"), Decimal("1.12"))
        self.assertEqual(
            canonical_stable_to_fiat_amount(13_000_000, currency="EUR"),
            "11.61",
        )

    def test_usd_does_not_require_explicit_rate(self):
        self.assertEqual(
            canonical_stable_to_fiat_amount(13_000_000, currency="USD"),
            "13.00",
        )

    @override_settings(WALLET_FIAT_USD_RATES={})
    def test_non_usd_currency_requires_configured_rate(self):
        with self.assertRaises(ImproperlyConfigured):
            canonical_stable_to_fiat_amount(13_000_000, currency="EUR")
