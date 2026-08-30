from io import StringIO

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase

from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet


SCALE = 10 ** 6


class PromotionalBackfillCommandTests(TestCase):
    def _run(self, *args):
        stdout = StringIO()
        call_command("rebuild_promotional_accounting", *args, stdout=stdout)
        return stdout.getvalue()

    def test_dry_run_is_read_only_and_apply_is_idempotent(self):
        user_model = get_user_model()
        buyer = user_model.objects.create_user(
            username="legacy-promo-buyer",
            email="legacy-promo-buyer@example.com",
            password="test-password",
        )
        seller = user_model.objects.create_user(
            username="legacy-promo-seller",
            email="legacy-promo-seller@example.com",
            password="test-password",
        )
        buyer_wallet = TokenWallet.objects.get(user=buyer)
        seller_wallet = TokenWallet.objects.get(user=seller)

        reward_txn = LedgerTransaction.objects.create(
            kind="daily_reward",
            created_by=buyer,
        )
        reward_entry = LedgerEntry.objects.create(
            txn=reward_txn,
            wallet=buyer_wallet,
            delta=500 * SCALE,
            promotional_delta=0,
            balance_after=500 * SCALE,
        )

        deposit_txn = LedgerTransaction.objects.create(
            kind="deposit",
            created_by=buyer,
        )
        LedgerEntry.objects.create(
            txn=deposit_txn,
            wallet=buyer_wallet,
            delta=300 * SCALE,
            promotional_delta=0,
            balance_after=800 * SCALE,
        )

        sale_txn = LedgerTransaction.objects.create(
            kind="purchase",
            created_by=buyer,
        )
        buyer_sale_entry = LedgerEntry.objects.create(
            txn=sale_txn,
            wallet=buyer_wallet,
            delta=-600 * SCALE,
            promotional_delta=0,
            balance_after=200 * SCALE,
        )
        seller_sale_entry = LedgerEntry.objects.create(
            txn=sale_txn,
            wallet=seller_wallet,
            delta=600 * SCALE,
            promotional_delta=0,
            balance_after=600 * SCALE,
        )

        spend_txn = LedgerTransaction.objects.create(
            kind="ad_spend",
            created_by=seller,
        )
        seller_spend_entry = LedgerEntry.objects.create(
            txn=spend_txn,
            wallet=seller_wallet,
            delta=-200 * SCALE,
            promotional_delta=0,
            balance_after=400 * SCALE,
        )

        TokenWallet.objects.filter(pk=buyer_wallet.pk).update(
            balance=200 * SCALE,
            promotional_balance=0,
        )
        TokenWallet.objects.filter(pk=seller_wallet.pk).update(
            balance=400 * SCALE,
            promotional_balance=0,
        )

        dry_run_output = self._run("--dry-run")
        self.assertIn("ledger entries to change: 4", dry_run_output)
        buyer_wallet.refresh_from_db()
        seller_wallet.refresh_from_db()
        reward_entry.refresh_from_db()
        self.assertEqual(buyer_wallet.promotional_balance, 0)
        self.assertEqual(seller_wallet.promotional_balance, 0)
        self.assertEqual(reward_entry.promotional_delta, 0)

        self._run("--apply")

        buyer_wallet.refresh_from_db()
        seller_wallet.refresh_from_db()
        reward_entry.refresh_from_db()
        buyer_sale_entry.refresh_from_db()
        seller_sale_entry.refresh_from_db()
        seller_spend_entry.refresh_from_db()

        self.assertEqual(buyer_wallet.promotional_balance, 0)
        self.assertEqual(seller_wallet.promotional_balance, 300 * SCALE)
        self.assertEqual(reward_entry.promotional_delta, 500 * SCALE)
        self.assertEqual(buyer_sale_entry.promotional_delta, -500 * SCALE)
        self.assertEqual(seller_sale_entry.promotional_delta, 500 * SCALE)
        self.assertEqual(seller_spend_entry.promotional_delta, -200 * SCALE)

        second_output = self._run("--apply")
        self.assertIn("ledger entries to change: 0", second_output)
        self.assertIn("wallets to change: 0", second_output)

    def test_internal_spend_reversal_restores_promotional_provenance(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="legacy-promo-refund",
            email="legacy-promo-refund@example.com",
            password="test-password",
        )
        wallet = TokenWallet.objects.get(user=user)

        reward_txn = LedgerTransaction.objects.create(
            kind="daily_reward", created_by=user
        )
        LedgerEntry.objects.create(
            txn=reward_txn,
            wallet=wallet,
            delta=500 * SCALE,
            promotional_delta=0,
            balance_after=500 * SCALE,
        )

        purchase_txn = LedgerTransaction.objects.create(
            kind="ai_generation_purchase", created_by=user
        )
        purchase_entry = LedgerEntry.objects.create(
            txn=purchase_txn,
            wallet=wallet,
            delta=-500 * SCALE,
            promotional_delta=0,
            balance_after=0,
        )

        reversal_txn = LedgerTransaction.objects.create(
            kind="ai_generation_purchase_reversal",
            status=LedgerTransaction.STATUS_REVERSED,
            reversal_of=purchase_txn,
            created_by=user,
        )
        reversal_entry = LedgerEntry.objects.create(
            txn=reversal_txn,
            wallet=wallet,
            delta=500 * SCALE,
            promotional_delta=0,
            balance_after=500 * SCALE,
        )

        TokenWallet.objects.filter(pk=wallet.pk).update(
            balance=500 * SCALE,
            promotional_balance=0,
        )

        self._run("--apply")

        wallet.refresh_from_db()
        purchase_entry.refresh_from_db()
        reversal_entry.refresh_from_db()
        self.assertEqual(wallet.promotional_balance, 500 * SCALE)
        self.assertEqual(purchase_entry.promotional_delta, -500 * SCALE)
        self.assertEqual(reversal_entry.promotional_delta, 500 * SCALE)

    def test_cash_deposit_reversal_preserves_unrelated_promo(self):
        user_model = get_user_model()
        user = user_model.objects.create_user(
            username="legacy-cash-reversal",
            email="legacy-cash-reversal@example.com",
            password="test-password",
        )
        wallet = TokenWallet.objects.get(user=user)

        deposit_txn = LedgerTransaction.objects.create(
            kind="deposit", created_by=user
        )
        LedgerEntry.objects.create(
            txn=deposit_txn,
            wallet=wallet,
            delta=500 * SCALE,
            promotional_delta=0,
            balance_after=500 * SCALE,
        )

        reward_txn = LedgerTransaction.objects.create(
            kind="daily_reward", created_by=user
        )
        LedgerEntry.objects.create(
            txn=reward_txn,
            wallet=wallet,
            delta=100 * SCALE,
            promotional_delta=0,
            balance_after=600 * SCALE,
        )

        reversal_txn = LedgerTransaction.objects.create(
            kind="deposit_reversal",
            status=LedgerTransaction.STATUS_REVERSED,
            reversal_of=deposit_txn,
            created_by=user,
        )
        reversal_entry = LedgerEntry.objects.create(
            txn=reversal_txn,
            wallet=wallet,
            delta=-500 * SCALE,
            promotional_delta=0,
            balance_after=100 * SCALE,
        )

        TokenWallet.objects.filter(pk=wallet.pk).update(
            balance=100 * SCALE,
            promotional_balance=0,
        )

        self._run("--apply")

        wallet.refresh_from_db()
        reversal_entry.refresh_from_db()
        self.assertEqual(wallet.promotional_balance, 100 * SCALE)
        self.assertEqual(reversal_entry.promotional_delta, 0)
