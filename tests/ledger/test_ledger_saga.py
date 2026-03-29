from ledger.models import LedgerSaga, LedgerSagaStep
from ledger.services import (
    add_saga_step,
    apply_ledger_transaction,
    complete_ledger_saga,
    complete_saga_step,
    compensate_ledger_saga,
    create_ledger_saga,
    fail_saga_step,
    start_ledger_saga,
    start_saga_step,
    get_failed_sagas,
    get_stale_compensating_sagas,
)
from datetime import timedelta
from django.utils import timezone
from tests.ledger.base import BaseLedgerTestCase


class TestLedgerSaga(BaseLedgerTestCase):
    def test_create_ledger_saga_is_idempotent(self):
        saga1 = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="saga-1",
            created_by=self.u1,
        )
        saga2 = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="saga-1",
            created_by=self.u1,
        )

        self.assertEqual(saga1.id, saga2.id)
        self.assertEqual(saga1.status, LedgerSaga.STATUS_PENDING)

    def test_saga_steps_can_be_started_and_completed(self):
        saga = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="saga-steps-1",
        )
        start_ledger_saga(actor=self.operator, saga=saga)

        step = add_saga_step(
            actor=self.operator,
            saga=saga,
            step_key="reserve_balance",
            step_order=1,
        )
        start_saga_step(actor=self.operator, step=step)

        apply_ledger_transaction(
            actor=self.operator,
            kind="test_funding",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="saga-step-fund-1",
        )
        txn = apply_ledger_transaction(
            actor=self.operator,
            kind="withdrawal_reserve",
            entries=[(self.w1, -10), (self.w2, 10)],
            external_id="saga-step-txn-1",
        )
        complete_saga_step(actor=self.operator, step=step, txn=txn)
        step.refresh_from_db()

        self.assertEqual(step.status, LedgerSagaStep.STATUS_COMPLETED)
        self.assertEqual(step.txn_id, txn.id)

    def test_complete_ledger_saga_marks_completed(self):
        saga = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="saga-complete-1",
        )
        start_ledger_saga(actor=self.operator, saga=saga)

        step = add_saga_step(actor=self.operator, saga=saga, step_key="noop", step_order=1)
        start_saga_step(actor=self.operator, step=step)
        complete_saga_step(actor=self.operator, step=step)

        complete_ledger_saga(actor=self.operator, saga=saga)
        saga.refresh_from_db()

        self.assertEqual(saga.status, LedgerSaga.STATUS_COMPLETED)
        self.assertIsNotNone(saga.completed_at)

    def test_fail_saga_step_marks_saga_failed(self):
        saga = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="saga-fail-1",
        )
        start_ledger_saga(actor=self.operator, saga=saga)

        step = add_saga_step(actor=self.operator, saga=saga, step_key="broadcast", step_order=1)
        start_saga_step(actor=self.operator, step=step)
        fail_saga_step(actor=self.operator, step=step, error_message="broadcast failed")

        step.refresh_from_db()
        saga.refresh_from_db()

        self.assertEqual(step.status, LedgerSagaStep.STATUS_FAILED)
        self.assertEqual(saga.status, LedgerSaga.STATUS_FAILED)
        self.assertEqual(saga.last_error, "broadcast failed")

    def test_compensate_ledger_saga_reverses_completed_steps_in_reverse_order(self):
        saga = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="saga-comp-1",
        )
        start_ledger_saga(actor=self.operator, saga=saga)

        step1 = add_saga_step(actor=self.operator, saga=saga, step_key="reserve_1", step_order=1)
        step2 = add_saga_step(actor=self.operator, saga=saga, step_key="reserve_2", step_order=2)

        txn1 = apply_ledger_transaction(
            actor=self.operator,
            kind="reserve_a",
            entries=[(self.issuance, -5), (self.w1, 5)],
            external_id="saga-comp-txn-1",
        )
        txn2 = apply_ledger_transaction(
            actor=self.operator,
            kind="reserve_b",
            entries=[(self.issuance, -7), (self.w1, 7)],
            external_id="saga-comp-txn-2",
        )

        complete_saga_step(actor=self.operator, step=step1, txn=txn1)
        complete_saga_step(actor=self.operator, step=step2, txn=txn2)
        fail_saga_step(actor=self.operator, step=step2, error_message="later external failure")

        compensate_ledger_saga(
            actor=self.operator,
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

    def test_get_failed_sagas_returns_failed_only(self):
        saga = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="failed-saga-list-1",
        )
        start_ledger_saga(actor=self.operator, saga=saga)

        step = add_saga_step(actor=self.operator, saga=saga, step_key="broadcast", step_order=1)
        start_saga_step(actor=self.operator, step=step)
        fail_saga_step(actor=self.operator, step=step, error_message="broadcast failed")

        ids = [s.id for s in get_failed_sagas(actor=self.operator, limit=100)]
        self.assertIn(saga.id, ids)

    def test_get_stale_compensating_sagas_returns_old_compensating_only(self):
        saga = create_ledger_saga(
            actor=self.operator,
            saga_type="crypto_withdrawal",
            external_id="stale-compensating-1",
        )

        LedgerSaga.objects.filter(id=saga.id).update(
            status=LedgerSaga.STATUS_COMPENSATING,
            created_at=timezone.now() - timedelta(seconds=1200),
        )

        ids = [s.id for s in get_stale_compensating_sagas(actor=self.operator, older_than_seconds=900, limit=100)]
        self.assertIn(saga.id, ids)