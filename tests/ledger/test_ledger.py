from django.core.exceptions import ValidationError
from django.test import TestCase

from files.tests import create_account  # creates a user then user.save()
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from ledger.services import apply_ledger_transaction


class TestLedger(TestCase):
    def setUp(self):
        self.u1 = create_account(password="pass12345")
        self.u2 = create_account(password="pass12345")

        # signal auto creates wallets
        self.w1 = self.u1.token_wallet
        self.w2 = self.u2.token_wallet

    def test_wallet_auto_created_on_user_save(self):
        self.assertTrue(TokenWallet.objects.filter(user=self.u1).exists())
        self.assertTrue(TokenWallet.objects.filter(user=self.u2).exists())
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w2.balance, 0)

        # idempotent: a resave does not create a second wallet
        self.u1.save()
        self.assertEqual(TokenWallet.objects.filter(user=self.u1).count(), 1)

    def test_apply_transaction_single_entry_updates_balance_and_creates_rows(self):
        txn = apply_ledger_transaction(
            kind="mint",
            entries=[(self.w1, 50)],
            external_id="mint-1",
            created_by=self.u1,
            memo="test mint",
            metadata={"source": "test"},
        )

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 50)

        self.assertEqual(LedgerTransaction.objects.count(), 1)
        self.assertEqual(LedgerEntry.objects.count(), 1)

        entry = LedgerEntry.objects.get(txn=txn)
        self.assertEqual(entry.wallet_id, self.w1.id)
        self.assertEqual(entry.delta, 50)
        self.assertEqual(entry.balance_after, 50)

        self.assertEqual(txn.external_id, "mint-1")
        self.assertIsNotNone(txn.request_hash)  # fills when external_id is defined

    def test_transfer_two_wallets_balanced(self):
        apply_ledger_transaction(kind="mint", entries=[(self.w1, 100)], external_id="mint-2")
        self.w1.refresh_from_db()
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
        self.assertEqual(sum(e.delta for e in entries), 0)  # transaction équilibrée par construction ici

    def test_insufficient_funds_rolls_back(self):
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(kind="burn", entries=[(self.w1, -1)], external_id="burn-1")

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)

        # atomic: nothing stays in base
        self.assertEqual(LedgerTransaction.objects.count(), 0)
        self.assertEqual(LedgerEntry.objects.count(), 0)

    def test_idempotency_same_external_id_returns_same_txn(self):
        txn1 = apply_ledger_transaction(kind="mint", entries=[(self.w1, 10)], external_id="idem-1")
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 10)

        txn2 = apply_ledger_transaction(kind="mint", entries=[(self.w1, 10)], external_id="idem-1")
        self.w1.refresh_from_db()

        self.assertEqual(txn1.id, txn2.id)
        self.assertEqual(self.w1.balance, 10)  # not applied twice
        self.assertEqual(LedgerTransaction.objects.count(), 1)
        self.assertEqual(LedgerEntry.objects.count(), 1)

    def test_idempotency_external_id_payload_mismatch_rejected(self):
        apply_ledger_transaction(kind="mint", entries=[(self.w1, 10)], external_id="idem-2", memo="A")

        with self.assertRaises(ValidationError):
            # same external_id but different payload → reject
            apply_ledger_transaction(kind="mint", entries=[(self.w1, 11)], external_id="idem-2", memo="A")

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(kind="mint", entries=[(self.w1, 10)], external_id="idem-2", memo="B")

    def test_reject_unsaved_wallet_in_entries(self):
        unsaved = TokenWallet(user=self.u1)  # not saved → id None
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(kind="mint", entries=[(unsaved, 10)], external_id="bad-1")

    def test_ledger_entry_is_immutable(self):
        txn = apply_ledger_transaction(kind="mint", entries=[(self.w1, 5)], external_id="imm-1")
        entry = LedgerEntry.objects.get(txn=txn)

        entry.delta = 999
        with self.assertRaises(ValidationError):
            entry.save()  # ImmutableLedgerRow.save

        with self.assertRaises(ValidationError):
            entry.delete()  # ImmutableLedgerRow.delete

    def test_queryset_update_delete_blocked(self):
        txn = apply_ledger_transaction(kind="mint", entries=[(self.w1, 5)], external_id="imm-2")
        entry = LedgerEntry.objects.get(txn=txn)

        # QuerySet.update/delete blocked via LedgerImmutableQuerySet
        with self.assertRaises(ValidationError):
            LedgerEntry.objects.filter(id=entry.id).update(delta=1)

        with self.assertRaises(ValidationError):
            LedgerEntry.objects.filter(id=entry.id).delete()

        with self.assertRaises(ValidationError):
            LedgerTransaction.objects.filter(id=txn.id).update(kind="x")

        with self.assertRaises(ValidationError):
            LedgerTransaction.objects.filter(id=txn.id).delete()