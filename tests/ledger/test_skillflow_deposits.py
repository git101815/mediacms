from copy import deepcopy
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import override_settings
from django.urls import reverse

from ledger.models import DepositSession, LedgerTransaction, TokenWallet
from ledger.services import get_external_asset_clearing_wallet, get_system_wallet
from ledger.skillflow_deposits import (
    open_skillflow_deposit_session,
    process_skillflow_webhook,
)

from .base import BaseLedgerTestCase


@override_settings(
    SKILLFLOW_ENABLED=True,
    SKILLFLOW_PARTNER_KEY="partner-secret",
    SKILLFLOW_WEBHOOK_SECRET="webhook-secret",
    SKILLFLOW_API_BASE_URL="https://payments.skillflow.store",
    SKILLFLOW_PUBLIC_BASE_URL="https://site.example",
    SKILLFLOW_PAYMENT_TTL_SECONDS=3600,
    SKILLFLOW_API_TIMEOUT_SECONDS=20,
    LEDGER_DEPOSIT_OPEN_COOLDOWN_THRESHOLD=1000,
    WALLET_FIAT_USD_RATES={"EUR": "1.12"},
)
class TestSkillflowDeposits(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")

    def _checkout_response(self, payment_id="partner_payment_1", amount="0.90"):
        return {
            "url": "https://www.mollie.com/checkout/select-method/example",
            "paymentId": payment_id,
            "amount": amount,
            "currency": "EUR",
            "description": "Pack Templates Design",
        }

    def _open_session(self):
        payment_number = DepositSession.objects.filter(chain="skillflow").count() + 1
        with patch(
            "ledger.skillflow_deposits.create_skillflow_checkout",
            return_value=self._checkout_response(
                payment_id=f"partner_payment_{payment_number}"
            ),
        ) as mocked_checkout:
            session = open_skillflow_deposit_session(
                actor=self.u1,
                wallet=self.w1,
                token_pack=self.default_token_pack,
            )
        return session, mocked_checkout

    def _webhook_payload(self, session, **overrides):
        provider = (session.metadata or {})["payment_provider"]
        payload = {
            "event": "payment.succeeded",
            "paymentId": provider["reference"],
            "mollieId": "tr_WDqYK6vllg",
            "userId": str(self.u1.id),
            "amount": 0.90,
            "currency": "EUR",
            "email": self.u1.email,
            "status": "paid",
            "metadata": deepcopy(provider["request_metadata"]),
            "timestamp": "2026-08-28T18:45:00.000Z",
        }
        payload.update(overrides)
        return payload

    def test_open_session_freezes_fx_and_uses_full_skillflow_checkout_contract(self):
        session, mocked_checkout = self._open_session()
        provider = session.metadata["payment_provider"]
        request_metadata = provider["request_metadata"]

        self.assertEqual(session.chain, "skillflow")
        self.assertEqual(session.asset_code, "EUR")
        self.assertEqual(session.status, DepositSession.STATUS_AWAITING_PAYMENT)
        self.assertEqual(session.min_amount, 1_000_000)
        self.assertIsNone(session.expected_onchain_raw_amount)
        self.assertEqual(provider["reference"], "partner_payment_1")
        self.assertEqual(provider["checkout_amount"], "0.90")
        self.assertEqual(provider["checkout_currency"], "EUR")
        self.assertEqual(provider["checkout_currency_usd_rate"], "1.12")
        self.assertEqual(provider["description"], "Pack Templates Design")
        self.assertEqual(request_metadata["provider"], "skillflow")
        self.assertEqual(request_metadata["depositSessionPublicId"], str(session.public_id))
        self.assertEqual(request_metadata["tokenPackCode"], self.default_token_pack.code)
        self.assertEqual(request_metadata["expectedCanonicalStableAmount"], 1_000_000)
        self.assertEqual(request_metadata["checkoutAmount"], "0.90")
        self.assertEqual(request_metadata["checkoutCurrency"], "EUR")

        kwargs = mocked_checkout.call_args.kwargs
        self.assertEqual(kwargs["user_id"], str(self.u1.id))
        self.assertEqual(kwargs["amount_eur"], "0.90")
        self.assertEqual(kwargs["email"], self.u1.email)
        self.assertEqual(kwargs["redirect_url"], kwargs["cancel_url"])
        self.assertIn(str(session.public_id), kwargs["redirect_url"])
        self.assertEqual(kwargs["metadata"], request_metadata)

    def test_active_session_is_reused_without_creating_a_second_checkout(self):
        with patch(
            "ledger.skillflow_deposits.create_skillflow_checkout",
            return_value=self._checkout_response(),
        ) as mocked_checkout:
            first = open_skillflow_deposit_session(
                actor=self.u1,
                wallet=self.w1,
                token_pack=self.default_token_pack,
            )
            second = open_skillflow_deposit_session(
                actor=self.u1,
                wallet=self.w1,
                token_pack=self.default_token_pack,
            )

        self.assertEqual(first.id, second.id)
        self.assertEqual(DepositSession.objects.filter(chain="skillflow").count(), 1)
        mocked_checkout.assert_called_once()

    @override_settings(
        WALLET_PAYMENT_METHOD_PRICE_BPS={"skillflow_card": 250},
        WALLET_PAYMENT_METHOD_PRICE_FIXED_CANONICAL={"skillflow_card": 0.30},
    )
    def test_wallet_request_applies_skillflow_group_price_and_redirects_to_mollie(self):
        self.client.force_login(self.u1)
        with patch(
            "ledger.skillflow_deposits.create_skillflow_checkout",
            return_value=self._checkout_response(amount="1.19"),
        ) as mocked_checkout:
            response = self.client.post(
                reverse("wallet_deposit_request"),
                {
                    "deposit_option_key": "skillflow:eur:hosted_checkout",
                    "token_pack_key": self.default_token_pack.code,
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.url,
            "https://www.mollie.com/checkout/select-method/example",
        )
        session = DepositSession.objects.get(chain="skillflow")
        token_pack = session.metadata["token_pack"]
        self.assertEqual(token_pack["net_stable_amount"], 1_000_000)
        self.assertEqual(token_pack["fixed_fee_stable_amount"], 300_000)
        self.assertEqual(token_pack["percentage_fee_stable_amount"], 25_000)
        self.assertEqual(token_pack["gross_stable_amount"], 1_325_000)
        self.assertEqual(mocked_checkout.call_args.kwargs["amount_eur"], "1.19")

    def test_succeeded_webhook_credits_once_and_books_fx_rounding_as_fee(self):
        session, _ = self._open_session()
        payload = self._webhook_payload(session)

        with patch(
            "ledger.skillflow_deposits._get_internal_deposit_service_actor",
            return_value=self.operator,
        ):
            first = process_skillflow_webhook(payload)
            second = process_skillflow_webhook(payload)

        session.refresh_from_db()
        self.w1.refresh_from_db()
        platform_fees = get_system_wallet(
            TokenWallet.SYSTEM_PLATFORM_FEES,
            allow_negative=False,
        )
        platform_fees.refresh_from_db()
        clearing = get_external_asset_clearing_wallet()
        clearing.refresh_from_db()

        self.assertEqual(first["ledger_txn_id"], second["ledger_txn_id"])
        self.assertEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(session.observed_txid, "partner_payment_1")
        self.assertEqual(session.observed_amount, 1_008_000)
        self.assertEqual(self.w1.balance, self.default_token_pack.token_amount)
        self.assertEqual(platform_fees.balance, 800_000)
        self.assertEqual(clearing.balance, -100_800_000)
        self.assertEqual(
            LedgerTransaction.objects.filter(
                external_id="skillflow-deposit-credit:partner_payment_1"
            ).count(),
            1,
        )
        provider = session.metadata["payment_provider"]
        self.assertEqual(provider["status"], "PAID")
        self.assertEqual(provider["mollie_id"], "tr_WDqYK6vllg")
        self.assertEqual(provider["last_event"], "payment.succeeded")
        self.assertEqual(provider["credited_ledger_txn_id"], first["ledger_txn_id"])

    def test_webhook_must_match_frozen_amount_user_payment_and_metadata(self):
        mismatches = (
            {"amount": 0.89},
            {"userId": str(self.u2.id)},
            {"paymentId": "partner_payment_other"},
            {"metadata": {"provider": "skillflow"}},
            {"currency": "USD"},
            {"status": "open"},
            {"event": "payment.failed"},
        )

        for index, overrides in enumerate(mismatches):
            session, _ = self._open_session()
            payload = self._webhook_payload(session, **overrides)
            with self.subTest(overrides=overrides):
                with (
                    patch(
                        "ledger.skillflow_deposits._get_internal_deposit_service_actor",
                        return_value=self.operator,
                    ),
                    self.assertRaises(ValidationError),
                ):
                    process_skillflow_webhook(payload)

            session.status = DepositSession.STATUS_CANCELED
            session.save(update_fields=["status", "updated_at"])
            self.assertEqual(
                LedgerTransaction.objects.filter(
                    external_id__startswith="skillflow-deposit-credit:"
                ).count(),
                0,
                msg=f"mismatch #{index} credited the ledger",
            )

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)

    def test_checkout_failure_marks_session_failed_without_crediting(self):
        with patch(
            "ledger.skillflow_deposits.create_skillflow_checkout",
            side_effect=ValidationError("provider unavailable"),
        ):
            with self.assertRaisesRegex(ValidationError, "provider unavailable"):
                open_skillflow_deposit_session(
                    actor=self.u1,
                    wallet=self.w1,
                    token_pack=self.default_token_pack,
                )

        session = DepositSession.objects.get(chain="skillflow")
        self.assertEqual(session.status, DepositSession.STATUS_FAILED)
        self.assertEqual(session.metadata["payment_provider"]["status"], "CREATE_FAILED")
        self.assertIn("provider unavailable", session.metadata["payment_provider"]["last_error"])
