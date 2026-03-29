from django.core.exceptions import ValidationError
from django.test import TestCase

from files.tests import create_account
from ledger.models import (
    LEDGER_METADATA_VERSION,
    LEDGER_OUTBOX_MAX_RETRIES,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
)
from ledger.services import (
    apply_ledger_transaction,
    create_pending_ledger_transaction,
    get_dispatchable_outbox_events,
    get_system_wallet,
    mark_outbox_event_failed,
    move_outbox_event_to_dlq,
    reverse_ledger_transaction,
)


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

        self.assertEqual(txn.status, LedgerTransaction.STATUS_POSTED)

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

        self.assertEqual(txn.status, LedgerTransaction.STATUS_POSTED)

    def test_get_system_wallet_rejects_allow_negative_mismatch(self):
        get_system_wallet(TokenWallet.SYSTEM_ISSUANCE, allow_negative=True)

        with self.assertRaisesRegex(ValidationError, "expected False"):
            get_system_wallet(TokenWallet.SYSTEM_ISSUANCE, allow_negative=False)

    def test_can_create_pending_transaction_without_entries(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="pending-1",
            created_by=self.u1,
            memo="awaiting confirmations",
            metadata={"confirmations": 0},
        )
        self.assertEqual(txn.status, LedgerTransaction.STATUS_PENDING)
        self.assertEqual(txn.entries.count(), 0)
        self.assertEqual(LedgerEntry.objects.count(), 0)

    def test_pending_transaction_is_idempotent(self):
        txn1 = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="pending-2",
            memo="awaiting confirmations",
            metadata={"confirmations": 0},
        )
        txn2 = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="pending-2",
            memo="awaiting confirmations",
            metadata={"confirmations": 0},
        )
        self.assertEqual(txn1.id, txn2.id)
        self.assertEqual(txn2.status, LedgerTransaction.STATUS_PENDING)

    def test_reverse_posted_transaction_creates_compensation_entries(self):
        original = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -25), (self.w1, 25)],
            external_id="rev-src-1",
        )
        reversal = reverse_ledger_transaction(
            original_txn=original,
            external_id="rev-1",
            created_by=self.u1,
            memo="deposit failed",
        )

        self.issuance.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(reversal.status, LedgerTransaction.STATUS_REVERSED)
        self.assertEqual(reversal.reversal_of_id, original.id)
        self.assertEqual(self.issuance.balance, 0)
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(reversal.entries.count(), 2)
        self.assertEqual(sum(entry.delta for entry in reversal.entries.all()), 0)

    def test_reverse_is_idempotent(self):
        original = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -15), (self.w1, 15)],
            external_id="rev-src-2",
        )
        reversal1 = reverse_ledger_transaction(
            original_txn=original,
            external_id="rev-2",
        )
        reversal2 = reverse_ledger_transaction(
            original_txn=original,
            external_id="rev-2",
        )
        self.assertEqual(reversal1.id, reversal2.id)

    def test_cannot_reverse_pending_transaction(self):
        pending = create_pending_ledger_transaction(
            kind="crypto_withdrawal",
            external_id="pending-3",
        )
        with self.assertRaises(ValidationError):
            reverse_ledger_transaction(
                original_txn=pending,
                external_id="rev-pending-1",
            )

    def test_posted_transaction_enqueues_outbox_event(self):
        txn = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -20), (self.w1, 20)],
            external_id="outbox-posted-1",
        )

        event = LedgerOutbox.objects.get(txn=txn)
        self.assertEqual(event.topic, "ledger.transaction.posted")
        self.assertEqual(event.status, LedgerOutbox.STATUS_PENDING)
        self.assertEqual(event.aggregate_type, "ledger_transaction")
        self.assertEqual(event.aggregate_id, txn.id)
        self.assertEqual(event.payload["txn_id"], txn.id)
        self.assertEqual(event.payload["status"], txn.status)

    def test_pending_transaction_enqueues_outbox_event(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="outbox-pending-1",
            created_by=self.u1,
            metadata={"confirmations": 0},
        )

        event = LedgerOutbox.objects.get(txn=txn)
        self.assertEqual(event.topic, "ledger.transaction.pending")
        self.assertEqual(event.status, LedgerOutbox.STATUS_PENDING)
        self.assertEqual(event.payload["txn_id"], txn.id)
        self.assertEqual(event.payload["status"], txn.status)

    def test_reversed_transaction_enqueues_outbox_event(self):
        original = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -12), (self.w1, 12)],
            external_id="outbox-rev-src-1",
        )

        reversal = reverse_ledger_transaction(
            original_txn=original,
            external_id="outbox-rev-1",
            created_by=self.u1,
        )

        event = LedgerOutbox.objects.get(txn=reversal)
        self.assertEqual(event.topic, "ledger.transaction.reversed")
        self.assertEqual(event.status, LedgerOutbox.STATUS_PENDING)
        self.assertEqual(event.payload["txn_id"], reversal.id)
        self.assertEqual(event.payload["reversal_of_id"], original.id)

    def test_outbox_is_rolled_back_with_failed_transaction(self):
        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                kind="transfer",
                entries=[(self.w1, -1), (self.w2, 1)],
                external_id="outbox-rollback-1",
            )

        self.assertEqual(LedgerTransaction.objects.count(), 0)
        self.assertEqual(LedgerEntry.objects.count(), 0)
        self.assertEqual(LedgerOutbox.objects.count(), 0)

    def test_posted_transaction_sets_metadata_version(self):
        txn = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="meta-posted-1",
            metadata={"source": "test"},
        )
        self.assertEqual(txn.metadata_version, LEDGER_METADATA_VERSION)

        event = LedgerOutbox.objects.get(txn=txn)
        self.assertEqual(event.metadata_version, LEDGER_METADATA_VERSION)

    def test_pending_transaction_sets_metadata_version(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="meta-pending-1",
            metadata={"confirmations": 0},
        )
        self.assertEqual(txn.metadata_version, LEDGER_METADATA_VERSION)

        event = LedgerOutbox.objects.get(txn=txn)
        self.assertEqual(event.metadata_version, LEDGER_METADATA_VERSION)

    def test_reversed_transaction_sets_metadata_version(self):
        original = apply_ledger_transaction(
            kind="mint",
            entries=[(self.issuance, -14), (self.w1, 14)],
            external_id="meta-rev-src-1",
        )
        reversal = reverse_ledger_transaction(
            original_txn=original,
            external_id="meta-rev-1",
        )

        self.assertEqual(reversal.metadata_version, LEDGER_METADATA_VERSION)

        event = LedgerOutbox.objects.get(txn=reversal)
        self.assertEqual(event.metadata_version, LEDGER_METADATA_VERSION)

    def test_outbox_event_failed_before_dlq_threshold(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="dlq-fail-1",
            metadata={"confirmations": 0},
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(event, "temporary network error")
        event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_FAILED)
        self.assertEqual(event.fail_count, 1)
        self.assertEqual(event.dead_lettered_at, None)
        self.assertEqual(event.dead_letter_reason, "")

    def test_outbox_event_moves_to_dlq_after_max_retries(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="dlq-max-1",
            metadata={"confirmations": 0},
        )
        event = LedgerOutbox.objects.get(txn=txn)

        for i in range(LEDGER_OUTBOX_MAX_RETRIES):
            mark_outbox_event_failed(event, f"error-{i}")
            event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_DEAD_LETTERED)
        self.assertEqual(event.fail_count, LEDGER_OUTBOX_MAX_RETRIES)
        self.assertIsNotNone(event.dead_lettered_at)
        self.assertEqual(event.dead_letter_reason, f"error-{LEDGER_OUTBOX_MAX_RETRIES - 1}")

    def test_move_outbox_event_to_dlq_sets_terminal_state(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_withdrawal",
            external_id="dlq-explicit-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        move_outbox_event_to_dlq(event, "manual dead lettering")
        event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_DEAD_LETTERED)
        self.assertIsNotNone(event.dead_lettered_at)
        self.assertEqual(event.dead_letter_reason, "manual dead lettering")

    def test_dead_lettered_events_are_not_dispatchable(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="dlq-dispatchable-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        move_outbox_event_to_dlq(event, "poison message")
        ids = [e.id for e in get_dispatchable_outbox_events(limit=100)]

        self.assertNotIn(event.id, ids)

    def test_failed_events_remain_dispatchable_before_dlq(self):
        txn = create_pending_ledger_transaction(
            kind="crypto_deposit",
            external_id="dlq-retryable-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(event, "temporary timeout")
        ids = [e.id for e in get_dispatchable_outbox_events(limit=100)]

        self.assertIn(event.id, ids)

    def test_create_ledger_saga_is_idempotent(self):
        saga1 = create_ledger_saga(
            saga_type="crypto_withdrawal",
            external_id="saga-1",
            created_by=self.u1,
        )
        saga2 = create_ledger_saga(
            saga_type="crypto_withdrawal",
            external_id="saga-1",
            created_by=self.u1,
        )

        self.assertEqual(saga1.id, saga2.id)
        self.assertEqual(saga1.status, LedgerSaga.STATUS_PENDING)

    def test_saga_steps_can_be_started_and_completed(self):
        saga = create_ledger_saga(
            saga_type="crypto_withdrawal",
            external_id="saga-steps-1",
        )
        start_ledger_saga(saga)

        step = add_saga_step(
            saga=saga,
            step_key="reserve_balance",
            step_order=1,
        )
        start_saga_step(step)

        txn = apply_ledger_transaction(
            kind="withdrawal_reserve",
            entries=[(self.w1, -10), (self.w2, 10)],
            external_id="saga-step-txn-1",
        )
        complete_saga_step(step, txn=txn)
        step.refresh_from_db()

        self.assertEqual(step.status, LedgerSagaStep.STATUS_COMPLETED)
        self.assertEqual(step.txn_id, txn.id)

    def test_complete_ledger_saga_marks_completed(self):
        saga = create_ledger_saga(
            saga_type="crypto_withdrawal",
            external_id="saga-complete-1",
        )
        start_ledger_saga(saga)

        step = add_saga_step(saga=saga, step_key="noop", step_order=1)
        start_saga_step(step)
        complete_saga_step(step)

        complete_ledger_saga(saga)
        saga.refresh_from_db()

        self.assertEqual(saga.status, LedgerSaga.STATUS_COMPLETED)
        self.assertIsNotNone(saga.completed_at)

        def test_fail_saga_step_marks_saga_failed(self):
            saga = create_ledger_saga(
                saga_type="crypto_withdrawal",
                external_id="saga-fail-1",
            )
            start_ledger_saga(saga)

            step = add_saga_step(saga=saga, step_key="broadcast", step_order=1)
            start_saga_step(step)
            fail_saga_step(step, error_message="broadcast failed")

            step.refresh_from_db()
            saga.refresh_from_db()

            self.assertEqual(step.status, LedgerSagaStep.STATUS_FAILED)
            self.assertEqual(saga.status, LedgerSaga.STATUS_FAILED)
            self.assertEqual(saga.last_error, "broadcast failed")

        def test_compensate_ledger_saga_reverses_completed_steps_in_reverse_order(self):
            saga = create_ledger_saga(
                saga_type="crypto_withdrawal",
                external_id="saga-comp-1",
            )
            start_ledger_saga(saga)

            step1 = add_saga_step(saga=saga, step_key="reserve_1", step_order=1)
            step2 = add_saga_step(saga=saga, step_key="reserve_2", step_order=2)

            txn1 = apply_ledger_transaction(
                kind="reserve_a",
                entries=[(self.issuance, -5), (self.w1, 5)],
                external_id="saga-comp-txn-1",
            )
            txn2 = apply_ledger_transaction(
                kind="reserve_b",
                entries=[(self.issuance, -7), (self.w1, 7)],
                external_id="saga-comp-txn-2",
            )

            complete_saga_step(step1, txn=txn1)
            complete_saga_step(step2, txn=txn2)
            fail_saga_step(step2, error_message="later external failure")

            compensate_ledger_saga(
                saga=saga,
                created_by=self.u1,
                reason="rollback workflow",
            )

            saga.refresh_from_db()
            step1.refresh_from_db()
            step2.refresh_from_db()
            self.w1.refresh_from_db()
            self.issuance.refresh_from_db()

            self.assertEqual(saga.status, LedgerSaga.STATUS_COMPENSATED)
            self.assertEqual(step1.status, LedgerSagaStep.STATUS_COMPENSATED)
            self.assertEqual(step2.status, LedgerSagaStep.STATUS_COMPENSATED)
            self.assertIsNotNone(step1.compensation_txn_id)
            self.assertIsNotNone(step2.compensation_txn_id)
            self.assertEqual(self.w1.balance, 0)
            self.assertEqual(self.issuance.balance, 0)

