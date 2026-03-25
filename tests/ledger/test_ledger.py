from django.core.exceptions import ValidationError
from django.test import TestCase

from files.tests import create_account
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from ledger.services import apply_ledger_transaction, get_system_wallet


class TestLedger(TestCase):
    def setUp(self):
        self.u1 = create_account(password="pass12345")
        self.u2 = create_account(password="pass12345")

        # Signal auto-creates user wallets.
        self.w1 = self.u1.token_wallet
        self.w2 = self.u2.token_wallet

        # Reuse the canonical system issuance wallet across tests.
        self.issuance = get_system_wallet(
            TokenWallet.SYSTEM_ISSUANCE,
            allow_negative=True,
        )

    def test_wallet_auto_created_on_user_save(self):
        self.assertTrue(TokenWallet.objects.filter(user=self.u1).exists())
        self.assertTrue(TokenWallet.objects.filter(user=self.u2).exists())
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w2.balance, 0)

        # A re-save must not create a second wallet.
        self.u1.save()
        self.assertEqual(TokenWallet.objects.filter(user=self.u1).count(), 1)

    def test_apply_transaction_double_entry_updates_balances_and_creates_rows(self):
        txn = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -50), (self.w1, 50)],
            external_id="mint-1",
            created_by=self.u1,
            memo="test mint",
            metadata={"source": "test"},
        )

        self.issuance.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(self.issuance.balance, -50)
        self.assertEqual(self.w1.balance, 50)

        self.assertEqual(LedgerTransaction.objects.count(), 1)
        self.assertEqual(LedgerEntry.objects.count(), 2)

        entries = list(LedgerEntry.objects.filter(txn=txn).order_by("wallet_id"))
        self.assertEqual(len(entries), 2)
        self.assertEqual(sum(entry.delta for entry in entries), 0)

        user_entry = LedgerEntry.objects.get(txn=txn, wallet=self.w1)
        self.assertEqual(user_entry.delta, 50)
        self.assertEqual(user_entry.balance_after, 50)

        issuance_entry = LedgerEntry.objects.get(txn=txn, wallet=self.issuance)
        self.assertEqual(issuance_entry.delta, -50)
        self.assertEqual(issuance_entry.balance_after, -50)

        self.assertEqual(txn.external_id, "mint-1")
        self.assertIsNotNone(txn.request_hash)

    def test_transfer_two_wallets_balanced(self):
        apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="mint-2",
        )
        self.issuance.refresh_from_db()
        self.w1.refresh_from_db()
        self.assertEqual(self.issuance.balance, -100)
        self.assertEqual(self.w1.balance, 100)

        txn = apply_ledger_transaction(
            kind="transfer",
            entries=[(self.w1, -30), (self.w2, 30)],
            external_id="xfer-1",
        )

        self.w1.refresh_from_db()
        self.w2.refresh_from_db()
        self.assertEqual(self.w1.balance, 70)
        self.assertEqual(self.w2.balance, 30)

        entries = list(LedgerEntry.objects.filter(txn=txn).order_by("wallet_id"))
        self.assertEqual(len(entries), 2)
        self.assertEqual(sum(entry.delta for entry in entries), 0)

    def test_insufficient_funds_rolls_back(self):
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="transfer",
                entries=[(self.w1, -1), (self.w2, 1)],
                external_id="insufficient-1",
            )

        self.w1.refresh_from_db()
        self.w2.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w2.balance, 0)

        # Atomic rollback: nothing must remain in database.
        self.assertEqual(LedgerTransaction.objects.count(), 0)
        self.assertEqual(LedgerEntry.objects.count(), 0)

    def test_idempotency_same_external_id_returns_same_txn(self):
        txn1 = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="idem-1",
        )
        self.issuance.refresh_from_db()
        self.w1.refresh_from_db()
        self.assertEqual(self.issuance.balance, -10)
        self.assertEqual(self.w1.balance, 10)

        txn2 = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="idem-1",
        )
        self.issuance.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(txn1.id, txn2.id)
        self.assertEqual(self.issuance.balance, -10)
        self.assertEqual(self.w1.balance, 10)
        self.assertEqual(LedgerTransaction.objects.count(), 1)
        self.assertEqual(LedgerEntry.objects.count(), 2)

    def test_idempotency_external_id_payload_mismatch_rejected(self):
        apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="idem-2",
            memo="A",
        )

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="mint",
                entries=[(self.issuance, -11), (self.w1, 11)],
                external_id="idem-2",
                memo="A",
            )

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="mint",
                entries=[(self.issuance, -10), (self.w1, 10)],
                external_id="idem-2",
                memo="B",
            )

    def test_reject_unsaved_wallet_in_entries(self):
        unsaved = TokenWallet(user=self.u1)
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="mint",
                entries=[(unsaved, 10), (self.w1, -10)],
                external_id="bad-1",
            )

    def test_ledger_entry_is_immutable(self):
        txn = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -5), (self.w1, 5)],
            external_id="imm-1",
        )
        entry = LedgerEntry.objects.get(txn=txn, wallet=self.w1)

        entry.delta = 999
        with self.assertRaises(ValidationError):
            entry.save()

        with self.assertRaises(ValidationError):
            entry.delete()

    def test_queryset_update_delete_blocked(self):
        txn = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -5), (self.w1, 5)],
            external_id="imm-2",
        )
        entry = LedgerEntry.objects.get(txn=txn, wallet=self.w1)

        with self.assertRaises(ValidationError):
            LedgerEntry.objects.filter(id=entry.id).update(delta=1)

        with self.assertRaises(ValidationError):
            LedgerEntry.objects.filter(id=entry.id).delete()

        with self.assertRaises(ValidationError):
            LedgerTransaction.objects.filter(id=txn.id).update(kind="x")

        with self.assertRaises(ValidationError):
            LedgerTransaction.objects.filter(id=txn.id).delete()

    def test_reject_unbalanced_transaction(self):
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="bad_unbalanced",
                entries=[(self.w1, 10), (self.w2, -9)],
                external_id="bad-unbalanced-1",
            )

    def test_reject_single_entry_transaction(self):
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="bad_single",
                entries=[(self.w1, 10)],
                external_id="bad-single-1",
            )

    def test_system_issuance_wallet_can_go_negative(self):
        apply_ledger_transaction(
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="mint-double-1",
        )

        self.issuance.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(self.issuance.balance, -100)
        self.assertEqual(self.w1.balance, 100)

    def test_purchase_with_platform_fee_is_balanced(self):
        fees = get_system_wallet(
            TokenWallet.SYSTEM_PLATFORM_FEES,
            allow_negative=False,
        )

        apply_ledger_transaction(
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="mint-before-purchase",
        )

        txn = apply_ledger_transaction(
            kind="video_purchase",
            entries=[(self.w1, -100), (self.w2, 80), (fees, 20)],
            external_id="purchase-1",
        )

        self.w1.refresh_from_db()
        self.w2.refresh_from_db()
        fees.refresh_from_db()

        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w2.balance, 80)
        self.assertEqual(fees.balance, 20)

        entries = list(LedgerEntry.objects.filter(txn=txn))
        self.assertEqual(sum(entry.delta for entry in entries), 0)

    def test_get_system_wallet_rejects_allow_negative_mismatch():
        get_system_wallet(TokenWallet.SYSTEM_ISSUANCE, allow_negative=True)

        with pytest.raises(ValidationError, match="expected False"):
            get_system_wallet(TokenWallet.SYSTEM_ISSUANCE, allow_negative=False)
