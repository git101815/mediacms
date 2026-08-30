import hashlib
import hmac
import json
import time
from unittest.mock import patch

from django.core.exceptions import PermissionDenied, ValidationError
from django.test import SimpleTestCase, override_settings
from django.urls import reverse

from ledger.providers.skillflow import (
    canonical_stable_to_skillflow_amount,
    create_skillflow_checkout,
    get_skillflow_min_canonical_stable_amount,
    verify_skillflow_webhook_signature,
)


@override_settings(
    SKILLFLOW_PARTNER_KEY="partner-secret",
    SKILLFLOW_WEBHOOK_SECRET="webhook-secret",
    SKILLFLOW_API_BASE_URL="https://payments.skillflow.store",
    SKILLFLOW_PUBLIC_BASE_URL="https://site.example",
    WALLET_FIAT_USD_RATES={"EUR": "1.12"},
)
class TestSkillflowProvider(SimpleTestCase):
    @patch("ledger.providers.skillflow._post_skillflow_json")
    def test_checkout_uses_every_partner_api_field(self, mocked_post):
        mocked_post.return_value = {
            "url": "https://www.mollie.com/checkout/select-method/example",
            "paymentId": "partner_payment_1",
            "amount": 0.90,
            "currency": "EUR",
            "description": "Pack Templates Design",
        }
        metadata = {
            "provider": "skillflow",
            "depositSessionPublicId": "7a00d825-76a7-448e-9c4b-a9568e96aeb3",
            "tokenPackCode": "starter-pack",
        }

        response = create_skillflow_checkout(
            user_id="42",
            amount_eur="0.90",
            email="buyer@example.com",
            redirect_url="https://site.example/wallet/deposits/session/",
            cancel_url="https://site.example/wallet/deposits/session/",
            metadata=metadata,
        )

        self.assertEqual(
            mocked_post.call_args.args[0],
            {
                "userId": "42",
                "amount": 0.9,
                "redirectUrl": "https://site.example/wallet/deposits/session/",
                "cancelUrl": "https://site.example/wallet/deposits/session/",
                "metadata": metadata,
                "email": "buyer@example.com",
            },
        )
        self.assertEqual(response["paymentId"], "partner_payment_1")
        self.assertEqual(response["amount"], "0.90")
        self.assertEqual(response["currency"], "EUR")
        self.assertEqual(response["description"], "Pack Templates Design")

    @patch("ledger.providers.skillflow._post_skillflow_json")
    def test_checkout_rejects_response_amount_or_currency_mismatch(self, mocked_post):
        base_response = {
            "url": "https://www.mollie.com/checkout/select-method/example",
            "paymentId": "partner_payment_1",
            "amount": 0.90,
            "currency": "EUR",
            "description": "Pack Templates Design",
        }
        for overrides, message in (
            ({"amount": 0.89}, "amount does not match"),
            ({"currency": "USD"}, "currency must be EUR"),
            ({"url": "https://attacker.example/pay"}, "HTTPS Mollie URL"),
        ):
            mocked_post.return_value = {**base_response, **overrides}
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValidationError, message):
                    create_skillflow_checkout(
                        user_id="42",
                        amount_eur="0.90",
                        redirect_url="https://site.example/success",
                        cancel_url="https://site.example/cancel",
                        metadata={},
                    )

    def test_eur_conversion_rounds_up_and_respects_provider_minimum(self):
        self.assertEqual(canonical_stable_to_skillflow_amount(1_000_000), "0.90")
        self.assertEqual(get_skillflow_min_canonical_stable_amount(), 560_000)

    def test_webhook_signature_uses_timestamp_dot_exact_raw_body(self):
        raw_body = b'{"event":"payment.succeeded","amount":0.90}'
        timestamp = 1_787_900_000
        signature = hmac.new(
            b"webhook-secret",
            str(timestamp).encode("ascii") + b"." + raw_body,
            hashlib.sha256,
        ).hexdigest()

        verify_skillflow_webhook_signature(
            raw_body=raw_body,
            signature_header=signature,
            timestamp_header=str(timestamp),
            now_epoch_seconds=timestamp + 300,
        )

        with self.assertRaises(PermissionDenied):
            verify_skillflow_webhook_signature(
                raw_body=raw_body + b" ",
                signature_header=signature,
                timestamp_header=str(timestamp),
                now_epoch_seconds=timestamp,
            )

        with self.assertRaises(PermissionDenied):
            verify_skillflow_webhook_signature(
                raw_body=raw_body,
                signature_header=signature,
                timestamp_header=str(timestamp),
                now_epoch_seconds=timestamp + 301,
            )

    @patch("files.skillflow_webhooks.process_skillflow_webhook")
    def test_webhook_view_verifies_raw_body_and_returns_documented_ack(self, mocked_process):
        payload = {"event": "payment.succeeded", "paymentId": "partner_payment_1"}
        raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        timestamp = int(time.time())
        signature = hmac.new(
            b"webhook-secret",
            str(timestamp).encode("ascii") + b"." + raw_body,
            hashlib.sha256,
        ).hexdigest()

        response = self.client.generic(
            "POST",
            reverse("skillflow_webhook"),
            raw_body,
            content_type="application/json",
            HTTP_X_SKILLFLOW_SIGNATURE=signature,
            HTTP_X_SKILLFLOW_TIMESTAMP=str(timestamp),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"received": True})
        mocked_process.assert_called_once_with(payload)

    @patch("files.skillflow_webhooks.process_skillflow_webhook")
    def test_webhook_view_rejects_invalid_signature_before_parsing(self, mocked_process):
        response = self.client.generic(
            "POST",
            reverse("skillflow_webhook"),
            b"not-json",
            content_type="application/json",
            HTTP_X_SKILLFLOW_SIGNATURE="00" * hashlib.sha256().digest_size,
            HTTP_X_SKILLFLOW_TIMESTAMP=str(int(time.time())),
        )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json(), {"received": False, "error": "invalid_signature"})
        mocked_process.assert_not_called()
