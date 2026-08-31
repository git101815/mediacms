from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from ledger.services import (
    apply_ledger_transaction,
    consume_promotional_tokens_for_internal_spend_provenance,
    create_wallet_withdrawal_request,
    get_wallet_withdrawable_balance,
    reverse_ledger_transaction,
)
from ledger.management.commands.rebuild_restricted_promotional_accounting import (
    rebuild_restricted_promotional_accounting,
)
from tests.ledger.base import BaseLedgerTestCase

SCALE = 10 ** 6


class RestrictedPromotionalRuntimeTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="restricted-user",
            email="restricted@example.com",
            password="test-password",
        )
        self.wallet = self.user.token_wallet
        self.wallet.balance = 1_000 * SCALE
        self.wallet.promotional_balance = 700 * SCALE
        self.wallet.restricted_promotional_balance = 500 * SCALE
        self.wallet.held_balance = 100 * SCALE
        self.wallet.save(
            update_fields=[
                "balance",
                "promotional_balance",
                "restricted_promotional_balance",
                "held_balance",
                "updated_at",
            ]
        )

    @override_settings(LEDGER_MAX_PROMOTIONAL_WITHDRAWAL_PERCENT=50)
    def test_creator_cannot_withdraw_own_restricted_rewards(self):
        self.user.advancedUser = True
        self.user.save(update_fields=["advancedUser"])

        # Generic 100-token hold reserves 30 cash, 20 earned promo and 50
        # restricted promo. Creator withdrawal can use only 270 cash + 180
        # earned promo = 450.
        self.assertEqual(get_wallet_withdrawable_balance(self.wallet), 450 * SCALE)
        request = create_wallet_withdrawal_request(
            actor=self.user,
            wallet=self.wallet,
            amount="450",
            destination_address="creator-earned-only",
            payout_asset_code="USDT",
            payout_chain="ethereum",
        )
        self.assertEqual(request.metadata["cash_reserved_units"], 270 * SCALE)
        self.assertEqual(request.metadata["promotional_reserved_units"], 180 * SCALE)
        self.assertEqual(request.metadata["restricted_promotional_reserved_units"], 0)

    def test_internal_spend_consumes_restricted_inside_promotional_proportion(self):
        self.wallet.held_balance = 0
        self.wallet.save(update_fields=["held_balance", "updated_at"])
        promo, restricted = consume_promotional_tokens_for_internal_spend_provenance(
            self.wallet,
            500 * SCALE,
        )
        self.assertEqual(promo, 350 * SCALE)
        self.assertEqual(restricted, 250 * SCALE)
        self.assertEqual(self.wallet.promotional_balance, 350 * SCALE)
        self.assertEqual(self.wallet.restricted_promotional_balance, 250 * SCALE)


class RestrictedPromotionalLedgerTests(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.w1.balance = 1_000 * SCALE
        self.w1.promotional_balance = 700 * SCALE
        self.w1.restricted_promotional_balance = 350 * SCALE
        self.w1.held_balance = 0
        self.w1.save(
            update_fields=[
                "balance",
                "promotional_balance",
                "restricted_promotional_balance",
                "held_balance",
                "updated_at",
            ]
        )

    def test_transfer_propagates_nested_restricted_provenance(self):
        txn = apply_ledger_transaction(
            actor=self.operator,
            kind="transfer",
            entries=[(self.w1, -500 * SCALE), (self.w2, 500 * SCALE)],
            created_by=self.u1,
            external_id="restricted:transfer",
        )
        self.w1.refresh_from_db()
        self.w2.refresh_from_db()
        self.assertEqual(self.w1.promotional_balance, 350 * SCALE)
        self.assertEqual(self.w1.restricted_promotional_balance, 175 * SCALE)
        self.assertEqual(self.w2.promotional_balance, 350 * SCALE)
        self.assertEqual(self.w2.restricted_promotional_balance, 175 * SCALE)
        self.assertEqual(
            txn.entries.get(wallet=self.w1).restricted_promotional_delta,
            -175 * SCALE,
        )
        self.assertEqual(
            txn.entries.get(wallet=self.w2).restricted_promotional_delta,
            175 * SCALE,
        )

    def test_creator_transfer_does_not_move_restricted_rewards(self):
        self.u1.advancedUser = True
        self.u1.save(update_fields=["advancedUser"])

        txn = apply_ledger_transaction(
            actor=self.operator,
            kind="transfer",
            entries=[(self.w1, -500 * SCALE), (self.w2, 500 * SCALE)],
            created_by=self.u1,
            external_id="restricted:creator-transfer",
        )
        self.w1.refresh_from_db()
        self.w2.refresh_from_db()
        self.assertEqual(
            txn.entries.get(wallet=self.w1).restricted_promotional_delta, 0
        )
        self.assertEqual(
            txn.entries.get(wallet=self.w2).restricted_promotional_delta, 0
        )
        self.assertEqual(
            self.w1.restricted_promotional_balance, 350 * SCALE
        )
        self.assertEqual(self.w2.restricted_promotional_balance, 0)

    def test_reversal_restores_exact_restricted_provenance(self):
        txn = apply_ledger_transaction(
            actor=self.operator,
            kind="transfer",
            entries=[(self.w1, -500 * SCALE), (self.w2, 500 * SCALE)],
            created_by=self.u1,
            external_id="restricted:transfer:reverse-source",
        )
        reverse_ledger_transaction(
            actor=self.operator,
            original_txn=txn,
            created_by=self.u1,
            external_id="restricted:transfer:reverse",
        )
        self.w1.refresh_from_db()
        self.w2.refresh_from_db()
        self.assertEqual(self.w1.promotional_balance, 700 * SCALE)
        self.assertEqual(self.w1.restricted_promotional_balance, 350 * SCALE)
        self.assertEqual(self.w2.promotional_balance, 0)
        self.assertEqual(self.w2.restricted_promotional_balance, 0)

    def test_historical_rebuild_marks_rewards_and_converts_creator_revenue(self):
        # Current wallet state is the result of the historical rows below.
        self.w1.balance = 0
        self.w1.promotional_balance = 0
        self.w1.restricted_promotional_balance = 0
        self.w2.balance = 400 * SCALE
        self.w2.promotional_balance = 400 * SCALE
        self.w2.restricted_promotional_balance = 0
        self.w1.save(update_fields=["balance", "promotional_balance", "restricted_promotional_balance"])
        self.w2.save(update_fields=["balance", "promotional_balance", "restricted_promotional_balance"])

        self.issuance.balance = -500 * SCALE
        self.issuance.save(update_fields=["balance", "updated_at"])
        reward = LedgerTransaction.objects.create(
            kind="daily_reward",
            status=LedgerTransaction.STATUS_POSTED,
            metadata={},
        )
        LedgerEntry.objects.create(
            txn=reward,
            wallet=self.issuance,
            delta=-500 * SCALE,
            balance_after=-500 * SCALE,
        )
        LedgerEntry.objects.create(
            txn=reward,
            wallet=self.w1,
            delta=500 * SCALE,
            promotional_delta=500 * SCALE,
            balance_after=500 * SCALE,
        )
        purchase = LedgerTransaction.objects.create(
            kind="purchase",
            status=LedgerTransaction.STATUS_POSTED,
            metadata={"product": "premium_media"},
        )
        LedgerEntry.objects.create(
            txn=purchase,
            wallet=self.w1,
            delta=-500 * SCALE,
            promotional_delta=-500 * SCALE,
            balance_after=0,
        )
        LedgerEntry.objects.create(
            txn=purchase,
            wallet=self.w2,
            delta=400 * SCALE,
            promotional_delta=400 * SCALE,
            balance_after=400 * SCALE,
        )
        platform = TokenWallet.objects.create(
            wallet_type=TokenWallet.TYPE_SYSTEM,
            system_key=TokenWallet.SYSTEM_PLATFORM_FEES,
            balance=100 * SCALE,
        ) if not TokenWallet.objects.filter(system_key=TokenWallet.SYSTEM_PLATFORM_FEES).exists() else TokenWallet.objects.get(system_key=TokenWallet.SYSTEM_PLATFORM_FEES)
        platform.balance = 100 * SCALE
        platform.save(update_fields=["balance", "updated_at"])
        LedgerEntry.objects.create(
            txn=purchase,
            wallet=platform,
            delta=100 * SCALE,
            balance_after=platform.balance,
        )

        stats = rebuild_restricted_promotional_accounting(apply=True)
        self.assertGreaterEqual(stats.entry_updates, 2)
        reward_entry = reward.entries.get(wallet=self.w1)
        buyer_entry = purchase.entries.get(wallet=self.w1)
        creator_entry = purchase.entries.get(wallet=self.w2)
        reward_entry.refresh_from_db()
        buyer_entry.refresh_from_db()
        creator_entry.refresh_from_db()
        self.assertEqual(reward_entry.restricted_promotional_delta, 500 * SCALE)
        self.assertEqual(buyer_entry.restricted_promotional_delta, -500 * SCALE)
        self.assertEqual(creator_entry.restricted_promotional_delta, 0)
        self.w2.refresh_from_db()
        self.assertEqual(self.w2.restricted_promotional_balance, 0)
