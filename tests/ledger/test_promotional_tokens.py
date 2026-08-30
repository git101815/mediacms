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

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=50)
    def test_available_and_withdrawable_balances_are_distinct(self):
        self.assertEqual(get_wallet_available_balance(self.wallet), 900 * SCALE)
        self.assertEqual(get_wallet_withdrawable_balance(self.wallet), 540 * SCALE)

    def test_internal_spend_preserves_wallet_provenance_ratio(self):
        spent = consume_promotional_tokens_for_internal_spend(
            self.wallet,
            250 * SCALE,
        )
        self.assertEqual(spent, 175 * SCALE)
        self.assertEqual(self.wallet.promotional_balance, 525 * SCALE)

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=50)
    def test_withdrawal_reserves_promotional_tokens_up_to_ratio(self):
        request = create_wallet_withdrawal_request(
            actor=self.user,
            wallet=self.wallet,
            amount="400",
            destination_address="test-destination",
        )
        self.wallet.refresh_from_db()
        request.hold.refresh_from_db()
        self.assertEqual(self.wallet.held_balance, 500 * SCALE)
        self.assertEqual(request.metadata["cash_reserved_units"], 200 * SCALE)
        self.assertEqual(request.metadata["promotional_reserved_units"], 200 * SCALE)
        self.assertEqual(request.metadata["promotional_withdrawal_percent"], 50)
        self.assertEqual(request.hold.metadata["promotional_reserved_units"], 200 * SCALE)

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=50)
    def test_withdrawal_above_ratio_capacity_is_rejected(self):
        with self.assertRaisesMessage(ValidationError, "Insufficient withdrawable balance"):
            create_wallet_withdrawal_request(
                actor=self.user,
                wallet=self.wallet,
                amount="541",
                destination_address="test-destination",
            )

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=0)
    def test_zero_percent_is_cash_only(self):
        self.assertEqual(get_wallet_withdrawable_balance(self.wallet), 270 * SCALE)

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=100)
    def test_hundred_percent_allows_all_available_promo(self):
        self.assertEqual(get_wallet_withdrawable_balance(self.wallet), 900 * SCALE)

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=50)
    def test_pending_withdrawal_cannot_reuse_reserved_promo_or_cash(self):
        self.wallet.held_balance = 0
        self.wallet.save(update_fields=["held_balance", "updated_at"])
        first = create_wallet_withdrawal_request(
            actor=self.user,
            wallet=self.wallet,
            amount="600",
            destination_address="first-destination",
        )
        self.assertEqual(first.metadata["cash_reserved_units"], 300 * SCALE)
        self.assertEqual(first.metadata["promotional_reserved_units"], 300 * SCALE)
        self.wallet.refresh_from_db()
        self.assertEqual(get_wallet_withdrawable_balance(self.wallet), 0)
        with self.assertRaisesMessage(ValidationError, "Insufficient withdrawable balance"):
            create_wallet_withdrawal_request(
                actor=self.user,
                wallet=self.wallet,
                amount="1",
                destination_address="second-destination",
            )

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=101)
    def test_invalid_promotional_withdrawal_percent_fails_closed(self):
        with self.assertRaisesMessage(
            ValidationError,
            "LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT must be between 0 and 100",
        ):
            get_wallet_withdrawable_balance(self.wallet)

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
        self.assertEqual(self.wallet.promotional_balance, 350 * SCALE)
        self.assertEqual(seller_wallet.promotional_balance, 350 * SCALE)
        self.assertEqual(get_wallet_withdrawable_balance(seller_wallet), 300 * SCALE)

    def test_user_to_user_transfer_preserves_promotional_proportion(self):
        user_model = get_user_model()
        recipient = user_model.objects.create_user(
            username="promo-accounting-transfer-recipient",
            email="promo-transfer@example.com",
            password="test-password",
        )
        recipient_wallet = TokenWallet.objects.get(user=recipient)
        admin = user_model.objects.create_superuser(
            username="promo-accounting-transfer-admin",
            email="promo-transfer-admin@example.com",
            password="test-password",
        )

        apply_ledger_transaction(
            actor=admin,
            kind="transfer",
            entries=[
                (self.wallet, -500 * SCALE),
                (recipient_wallet, 500 * SCALE),
            ],
            created_by=self.user,
            external_id="promo-accounting:user-transfer",
        )

        self.wallet.refresh_from_db()
        recipient_wallet.refresh_from_db()
        self.assertEqual(self.wallet.promotional_balance, 350 * SCALE)
        self.assertEqual(recipient_wallet.promotional_balance, 350 * SCALE)

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
    def test_unsettled_ads_reservation_preserves_funding_proportion(self):
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
                420 * SCALE,
            )
