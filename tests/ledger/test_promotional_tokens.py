from unittest import mock

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings

from ledger.dashboard import bonus_vault, referrals
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from ledger.services import (
    apply_ledger_transaction,
    consume_promotional_tokens_for_internal_spend,
    create_wallet_withdrawal_request,
    get_wallet_available_balance,
    get_wallet_withdrawable_balance,
)


SCALE = 10 ** 6


class PromotionalTokenAccountingTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="promo-accounting-user",
            email="promo-accounting@example.com",
            password="test-password",
        )
        self.wallet = TokenWallet.objects.get(user=self.user)
        self.wallet.balance = 1_000 * SCALE
        self.wallet.promotional_balance = 700 * SCALE
        self.wallet.held_balance = 100 * SCALE
        self.wallet.allow_negative = False
        self.wallet.save(
            update_fields=[
                "balance",
                "promotional_balance",
                "held_balance",
                "allow_negative",
                "updated_at",
            ]
        )

    def test_available_and_withdrawable_balances_are_distinct(self):
        self.assertEqual(get_wallet_available_balance(self.wallet), 900 * SCALE)
        self.assertEqual(get_wallet_withdrawable_balance(self.wallet), 200 * SCALE)

    def test_internal_spend_consumes_promotional_first(self):
        spent = consume_promotional_tokens_for_internal_spend(
            self.wallet,
            250 * SCALE,
        )
        self.assertEqual(spent, 250 * SCALE)
        self.assertEqual(self.wallet.promotional_balance, 450 * SCALE)

    def test_withdrawal_cannot_reserve_promotional_tokens(self):
        with self.assertRaisesMessage(ValidationError, "Insufficient withdrawable balance"):
            create_wallet_withdrawal_request(
                actor=self.user,
                wallet=self.wallet,
                amount="201",
                destination_address="test-destination",
            )

    def test_user_to_user_purchase_preserves_promotional_provenance(self):
        user_model = get_user_model()
        seller = user_model.objects.create_user(
            username="promo-accounting-seller",
            email="promo-seller@example.com",
            password="test-password",
        )
        seller_wallet = TokenWallet.objects.get(user=seller)
        admin = user_model.objects.create_superuser(
            username="promo-accounting-admin",
            email="promo-admin@example.com",
            password="test-password",
        )

        apply_ledger_transaction(
            actor=admin,
            kind="purchase",
            entries=[
                (self.wallet, -500 * SCALE),
                (seller_wallet, 500 * SCALE),
            ],
            created_by=self.user,
            external_id="promo-accounting:user-sale",
        )

        self.wallet.refresh_from_db()
        seller_wallet.refresh_from_db()
        self.assertEqual(self.wallet.promotional_balance, 200 * SCALE)
        self.assertEqual(seller_wallet.promotional_balance, 500 * SCALE)
        self.assertEqual(get_wallet_withdrawable_balance(seller_wallet), 0)

    def test_bonus_vault_counts_only_money_backed_purchase_spend(self):
        self.wallet.held_balance = 0
        self.wallet.save(update_fields=["held_balance"])
        txn = LedgerTransaction.objects.create(
            kind="purchase",
            created_by=self.user,
            metadata={},
        )
        LedgerEntry.objects.create(
            txn=txn,
            wallet=self.wallet,
            delta=-500 * SCALE,
            promotional_delta=-300 * SCALE,
            balance_after=500 * SCALE,
        )
        with mock.patch.object(bonus_vault.config, "BONUS_VAULT_START_AT", None):
            eligible = bonus_vault._get_total_eligible_spend_units(wallet=self.wallet)
        self.assertEqual(eligible, 200 * SCALE)

    def test_referral_purchase_threshold_ignores_promotional_spend(self):
        txn = LedgerTransaction.objects.create(
            kind="purchase",
            created_by=self.user,
            metadata={"price_tokens": 500 * SCALE},
        )
        LedgerEntry.objects.create(
            txn=txn,
            wallet=self.wallet,
            delta=-500 * SCALE,
            promotional_delta=-450 * SCALE,
            balance_after=500 * SCALE,
        )
        self.assertEqual(
            referrals._purchase_amount_units(txn, self.user.pk),
            50 * SCALE,
        )


class PromotionalReservationAccountingTests(TestCase):
    def test_unsettled_ads_spend_uses_promotional_balance_before_cash(self):
        user_model = get_user_model()
        advertiser = user_model.objects.create_superuser(
            username="promo-reservation-advertiser",
            email="promo-reservation@example.com",
            password="test-password",
        )
        wallet = TokenWallet.objects.get(user=advertiser)
        wallet.balance = 1_000 * SCALE
        wallet.promotional_balance = 700 * SCALE
        wallet.held_balance = 100 * SCALE
        wallet.allow_negative = False
        wallet.save(
            update_fields=[
                "balance",
                "promotional_balance",
                "held_balance",
                "allow_negative",
                "updated_at",
            ]
        )

        with mock.patch(
            "ads.runtime.get_account_unsettled_microtokens",
            return_value=200 * SCALE,
        ):
            self.assertEqual(
                get_wallet_available_balance(wallet),
                700 * SCALE,
            )
            self.assertEqual(
                get_wallet_withdrawable_balance(wallet),
                200 * SCALE,
            )
