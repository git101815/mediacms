from urllib.parse import parse_qs, urlparse

from django.test import SimpleTestCase, override_settings

from ledger.providers.paygate import (
    build_paygate_checkout_url,
    canonical_stable_to_paygate_amount,
    get_paygate_provider_currency,
)


@override_settings(
    PAYGATE_CURRENCY="USD",
    PAYGATE_PROVIDER_CURRENCIES={
        "paypal": "USD",
        "revolut": "EUR",
    },
    PAYGATE_CHECKOUT_BASE_URL="https://checkout.example",
    WALLET_FIAT_USD_RATES={
        "USD": "1",
        "EUR": "1.12",
    },
)
class TestPayGateFiat(SimpleTestCase):
    def test_provider_currency_mapping(self):
        self.assertEqual(get_paygate_provider_currency("paypal"), "USD")
        self.assertEqual(get_paygate_provider_currency("revolut"), "EUR")

    def test_revolut_amount_is_converted_from_canonical_usd_to_eur(self):
        self.assertEqual(
            canonical_stable_to_paygate_amount(
                13_000_000,
                currency=get_paygate_provider_currency("revolut"),
            ),
            "11.61",
        )

    def test_revolut_checkout_uses_eur_amount_and_currency(self):
        checkout_url = build_paygate_checkout_url(
            address_in="wallet-reference",
            amount="11.61",
            customer_email="buyer@example.com",
            currency=get_paygate_provider_currency("revolut"),
            provider_id="revolut",
        )

        parsed = urlparse(checkout_url)
        query = parse_qs(parsed.query)

        self.assertEqual(parsed.path, "/process-payment.php")
        self.assertEqual(query["amount"], ["11.61"])
        self.assertEqual(query["currency"], ["EUR"])
        self.assertEqual(query["provider"], ["revolut"])
