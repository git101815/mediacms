import importlib

from django.apps import apps as django_apps
from django.contrib.auth import get_user_model
from django.test import TestCase

from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet


SCALE = 10 ** 6


class PromotionalBackfillTests(TestCase):
    def test_legacy_rewards_are_replayed_through_spend_and_user_sale(self):
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

        # Simulate the production state immediately before 0039: balances
        # already include legacy activity, while the new provenance columns
        # added by 0038 still contain their defaults.
        TokenWallet.objects.filter(pk=buyer_wallet.pk).update(
            balance=200 * SCALE,
            promotional_balance=0,
        )
        TokenWallet.objects.filter(pk=seller_wallet.pk).update(
            balance=400 * SCALE,
            promotional_balance=0,
        )

        migration = importlib.import_module(
            "ledger.migrations.0039_backfill_promotional_accounting"
        )
        migration.backfill_promotional_accounting(django_apps, None)

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
