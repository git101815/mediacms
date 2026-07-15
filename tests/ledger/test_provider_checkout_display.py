import uuid
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase
from django.utils import timezone

from files.views import _build_deposit_session_payload


class TestProviderCheckoutDisplay(SimpleTestCase):
    def test_paygate_eur_checkout_keeps_fiat_and_settlement_units_separate(self):
        session = SimpleNamespace(
            public_id=uuid.uuid4(),
            status="awaiting_payment",
            chain="paygate",
            asset_code="EUR",
            deposit_address="paygate:reference",
            required_confirmations=1,
            confirmations=0,
            min_amount=13_000_000,
            expected_onchain_raw_amount=13_000_000,
            observed_txid="",
            observed_amount=13_000_000,
            expires_at=timezone.now(),
            metadata={
                "display_label": "Revolut (EU only)",
                "token_pack": {
                    "name": "Starter",
                    "token_amount": 500,
                    "gross_stable_amount": 13_000_000,
                },
                "payment_method": {
                    "label": "Revolut (EU only)",
                    "show_network_step": False,
                },
                "payment_provider": {
                    "key": "paygate",
                    "label": "Revolut (EU only)",
                    "checkout_currency": "EUR",
                    "checkout_amount": "11.61",
                    "checkout_url": "https://checkout.example/session",
                    "reference": "provider-reference",
                },
            },
        )

        with (
            patch("files.views._is_provider_checkout_session", return_value=True),
            patch("files.views._get_public_deposit_status", return_value="awaiting_payment"),
            patch("files.views.reverse", return_value="/wallet/"),
        ):
            payload = _build_deposit_session_payload(session)

        self.assertEqual(payload["expected_payment_amount_display"], "11.61")
        self.assertEqual(payload["expected_payment_currency"], "EUR")
        self.assertEqual(payload["observed_amount_display"], "13")
        self.assertEqual(payload["observed_asset_code"], "USDC")
        self.assertIn("€11.61", payload["token_pack_label"])
        self.assertEqual(payload["asset_code"], "EUR")
