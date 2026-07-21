from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings

from ledger.paygate_deposits import get_paygate_deposit_options
from ledger.providers.paygate import (
    get_paygate_provider_min_canonical_stable_amount,
    get_paygate_provider_min_canonical_stable_amounts,
)


class TestPayGateProviderMinimums(SimpleTestCase):
    @override_settings(
        PAYGATE_MIN_CANONICAL_STABLE_AMOUNT=1_000_000,
        PAYGATE_PROVIDER_MIN_CANONICAL_STABLE_AMOUNTS={
            "Transak": "15000000",
        },
    )
    def test_provider_specific_minimum_overrides_global_minimum(self):
        self.assertEqual(
            get_paygate_provider_min_canonical_stable_amount("transak"),
            15_000_000,
        )
        self.assertEqual(
            get_paygate_provider_min_canonical_stable_amount("paypal"),
            1_000_000,
        )

    @override_settings(
        PAYGATE_PROVIDER_MIN_CANONICAL_STABLE_AMOUNTS={
            "transak": "invalid",
        },
    )
    def test_invalid_provider_minimum_fails_closed(self):
        with self.assertRaises(ImproperlyConfigured):
            get_paygate_provider_min_canonical_stable_amounts()

    @override_settings(
        PAYGATE_PROVIDER_IDS=("paypal", "transak"),
        PAYGATE_PROVIDER_LABELS={
            "paypal": "PayPal",
            "transak": "Transak",
        },
        PAYGATE_PROVIDER_CURRENCIES={
            "paypal": "USD",
            "transak": "USD",
        },
        PAYGATE_MIN_CANONICAL_STABLE_AMOUNT=1_000_000,
        PAYGATE_PROVIDER_MIN_CANONICAL_STABLE_AMOUNTS={
            "transak": 15_000_000,
        },
        WALLET_FIAT_USD_RATES={"USD": "1"},
    )
    def test_deposit_options_expose_each_provider_minimum(self):
        with patch(
            "ledger.paygate_deposits.paygate_enabled",
            return_value=True,
        ):
            options = get_paygate_deposit_options()

        by_provider = {
            option["paygate_provider_id"]: option
            for option in options
        }
        self.assertEqual(by_provider["paypal"]["min_amount"], 1_000_000)
        self.assertEqual(by_provider["transak"]["min_amount"], 15_000_000)
        self.assertEqual(by_provider["transak"]["min_amount_display"], "15")
