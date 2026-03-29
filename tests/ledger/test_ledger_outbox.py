from django.core.exceptions import ValidationError
from datetime import timedelta
from django.utils import timezone

from ledger.models import (
    LEDGER_METADATA_VERSION,
    LEDGER_OUTBOX_MAX_RETRIES,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
)
from ledger.services import (
    apply_ledger_transaction,
    create_pending_ledger_transaction,
    get_dispatchable_outbox_events,
    mark_outbox_event_failed,
    move_outbox_event_to_dlq,
    reverse_ledger_transaction,
    get_dead_lettered_outbox_events,
    get_failed_outbox_events,
    get_stale_pending_outbox_events,
    redrive_dead_lettered_outbox_event,
    replay_failed_outbox_event,
)

from tests.ledger.base import BaseLedgerTestCase


class TestLedgerOutbox(BaseLedgerTestCase):
    def test_posted_transaction_enqueues_outbox_event(self):
        txn = apply_ledger_transaction(
            actor=self.operator,
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
            actor=self.operator,
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
            actor=self.operator,
            kind="mint",
            entries=[(self.issuance, -12), (self.w1, 12)],
            external_id="outbox-rev-src-1",
        )

        reversal = reverse_ledger_transaction(
            actor=self.operator,
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
                actor=self.operator,
                kind="transfer",
                entries=[(self.w1, -1), (self.w2, 1)],
                external_id="outbox-rollback-1",
            )

        self.assertEqual(LedgerTransaction.objects.count(), 0)
        self.assertEqual(LedgerEntry.objects.count(), 0)
        self.assertEqual(LedgerOutbox.objects.count(), 0)

    def test_posted_transaction_sets_metadata_version(self):
        txn = apply_ledger_transaction(
            actor=self.operator,
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
            actor=self.operator,
            kind="crypto_deposit",
            external_id="meta-pending-1",
            metadata={"confirmations": 0},
        )
        self.assertEqual(txn.metadata_version, LEDGER_METADATA_VERSION)

        event = LedgerOutbox.objects.get(txn=txn)
        self.assertEqual(event.metadata_version, LEDGER_METADATA_VERSION)

    def test_reversed_transaction_sets_metadata_version(self):
        original = apply_ledger_transaction(
            actor=self.operator,
            kind="mint",
            entries=[(self.issuance, -14), (self.w1, 14)],
            external_id="meta-rev-src-1",
        )
        reversal = reverse_ledger_transaction(
            actor=self.operator,
            original_txn=original,
            external_id="meta-rev-1",
        )

        self.assertEqual(reversal.metadata_version, LEDGER_METADATA_VERSION)

        event = LedgerOutbox.objects.get(txn=reversal)
        self.assertEqual(event.metadata_version, LEDGER_METADATA_VERSION)

    def test_outbox_event_failed_before_dlq_threshold(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="dlq-fail-1",
            metadata={"confirmations": 0},
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(
            actor=self.operator,
            event=event,
            error_message="temporary network error",
        )
        event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_FAILED)
        self.assertEqual(event.fail_count, 1)
        self.assertEqual(event.dead_lettered_at, None)
        self.assertEqual(event.dead_letter_reason, "")

    def test_outbox_event_moves_to_dlq_after_max_retries(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="dlq-max-1",
            metadata={"confirmations": 0},
        )
        event = LedgerOutbox.objects.get(txn=txn)

        for i in range(LEDGER_OUTBOX_MAX_RETRIES):
            mark_outbox_event_failed(
                actor=self.operator,
                event=event,
                error_message=f"error-{i}",
            )
            event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_DEAD_LETTERED)
        self.assertEqual(event.fail_count, LEDGER_OUTBOX_MAX_RETRIES)
        self.assertIsNotNone(event.dead_lettered_at)
        self.assertEqual(event.dead_letter_reason, f"error-{LEDGER_OUTBOX_MAX_RETRIES - 1}")

    def test_move_outbox_event_to_dlq_sets_terminal_state(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_withdrawal",
            external_id="dlq-explicit-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        move_outbox_event_to_dlq(
            actor=self.operator,
            event=event,
            reason="manual dead lettering",
        )
        event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_DEAD_LETTERED)
        self.assertIsNotNone(event.dead_lettered_at)
        self.assertEqual(event.dead_letter_reason, "manual dead lettering")

    def test_dead_lettered_events_are_not_dispatchable(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="dlq-dispatchable-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        move_outbox_event_to_dlq(
            actor=self.operator,
            event=event,
            reason="poison message",
        )
        ids = [e.id for e in get_dispatchable_outbox_events(actor=self.operator, limit=100)]

        self.assertNotIn(event.id, ids)

    def test_failed_events_are_not_dispatchable_until_next_retry_at(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="dlq-retryable-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(
            actor=self.operator,
            event=event,
            error_message="temporary timeout",
        )
        event.refresh_from_db()

        ids = [e.id for e in get_dispatchable_outbox_events(actor=self.operator, limit=100)]

        self.assertNotIn(event.id, ids)
        self.assertEqual(event.status, LedgerOutbox.STATUS_FAILED)
        self.assertIsNotNone(event.next_retry_at)

    def test_failed_event_becomes_dispatchable_when_next_retry_at_is_reached(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="dlq-retry-window-2",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(
            actor=self.operator,
            event=event,
            error_message="temporary timeout",
        )

        LedgerOutbox.objects.filter(id=event.id).update(
            next_retry_at=timezone.now() - timedelta(seconds=1)
        )

        ids = [e.id for e in get_dispatchable_outbox_events(actor=self.operator, limit=100)]

        self.assertIn(event.id, ids)

    def test_failed_event_is_not_dispatchable_before_next_retry_at(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="retry-window-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(
            actor=self.operator,
            event=event,
            error_message="temporary timeout",
        )
        ids = [e.id for e in get_dispatchable_outbox_events(actor=self.operator, limit=100)]

        self.assertNotIn(event.id, ids)
        self.assertIsNotNone(event.next_retry_at)

    def test_replay_failed_outbox_event_moves_back_to_pending(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="replay-failed-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(
            actor=self.operator,
            event=event,
            error_message="temporary timeout",
        )
        replay_failed_outbox_event(actor=self.operator, event=event)
        event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_PENDING)
        self.assertIsNone(event.next_retry_at)

    def test_redrive_dead_lettered_outbox_event_moves_back_to_pending(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="redrive-dlq-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        move_outbox_event_to_dlq(
            actor=self.operator,
            event=event,
            reason="poison message",
        )
        redrive_dead_lettered_outbox_event(actor=self.operator, event=event)
        event.refresh_from_db()

        self.assertEqual(event.status, LedgerOutbox.STATUS_PENDING)
        self.assertEqual(event.redrive_count, 1)
        self.assertIsNotNone(event.last_redriven_at)
        self.assertEqual(event.dead_letter_reason, "")
        self.assertIsNone(event.dead_lettered_at)

    def test_get_failed_outbox_events_returns_failed_only(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="failed-list-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        mark_outbox_event_failed(
            actor=self.operator,
            event=event,
            error_message="temporary timeout",
        )

        ids = [e.id for e in get_failed_outbox_events(actor=self.operator, limit=100)]
        self.assertIn(event.id, ids)

    def test_get_dead_lettered_outbox_events_returns_dead_lettered_only(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="dlq-list-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        move_outbox_event_to_dlq(
            actor=self.operator,
            event=event,
            reason="poison message",
        )

        ids = [e.id for e in get_dead_lettered_outbox_events(actor=self.operator, limit=100)]
        self.assertIn(event.id, ids)

    def test_get_stale_pending_outbox_events_returns_old_pending_only(self):
        txn = create_pending_ledger_transaction(
            actor=self.operator,
            kind="crypto_deposit",
            external_id="stale-pending-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        LedgerOutbox.objects.filter(id=event.id).update(
            created_at=timezone.now() - timedelta(seconds=1200)
        )

        ids = [e.id for e in get_stale_pending_outbox_events(actor=self.operator, older_than_seconds=900, limit=100)]
        self.assertIn(event.id, ids)