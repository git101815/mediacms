from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied

from ledger.models import LedgerOutbox
from ledger.services import (
    add_saga_step,
    apply_ledger_transaction,
    complete_saga_step,
    compensate_ledger_saga,
    create_ledger_saga,
    create_pending_ledger_transaction,
    fail_saga_step,
    mark_outbox_event_failed,
    start_ledger_saga,
)

from tests.ledger.base import BaseLedgerTestCase


class TestLedgerPermissions(BaseLedgerTestCase):
    def test_apply_raw_ledger_transaction_requires_permission(self):
        with self.assertRaises(PermissionDenied):
            apply_ledger_transaction(
                actor=self.u1,
                kind="mint",
                entries=[(self.issuance, -10), (self.w1, 10)],
                external_id="perm-raw-1",
            )

    def test_apply_raw_ledger_transaction_allowed_with_permission(self):
        self.grant_perm(self.u1, "can_apply_raw_ledger_transaction")

        txn = apply_ledger_transaction(
            actor=self.u1,
            kind="mint",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="perm-raw-2",
        )

        self.assertEqual(txn.created_by_id, self.u1.id)

    def test_cannot_impersonate_created_by_without_permission(self):
        User = get_user_model()
        other = User.objects.create_user(username="other_perm_test", password="x")

        self.grant_perm(self.u1, "can_apply_raw_ledger_transaction")

        with self.assertRaises(PermissionDenied):
            apply_ledger_transaction(
                actor=self.u1,
                created_by=other,
                kind="mint",
                entries=[(self.issuance, -10), (self.w1, 10)],
                external_id="perm-raw-3",
            )

    def test_can_impersonate_created_by_with_permission(self):
        User = get_user_model()
        other = User.objects.create_user(username="other_perm_test_2", password="x")

        self.grant_perm(self.u1, "can_apply_raw_ledger_transaction")
        self.grant_perm(self.u1, "can_impersonate_ledger_creator")

        txn = apply_ledger_transaction(
            actor=self.u1,
            created_by=other,
            kind="mint",
            entries=[(self.issuance, -10), (self.w1, 10)],
            external_id="perm-raw-4",
        )

        self.assertEqual(txn.created_by_id, other.id)

    def test_manage_outbox_requires_permission(self):
        self.grant_perm(self.u1, "can_create_pending_ledger_transaction")
        txn = create_pending_ledger_transaction(
            actor=self.u1,
            kind="crypto_deposit",
            external_id="perm-outbox-1",
        )
        event = LedgerOutbox.objects.get(txn=txn)

        with self.assertRaises(PermissionDenied):
            mark_outbox_event_failed(
                actor=self.u2,
                event=event,
                error_message="forbidden",
            )

    def test_create_ledger_saga_requires_permission(self):
        with self.assertRaises(PermissionDenied):
            create_ledger_saga(
                actor=self.u1,
                saga_type="crypto_withdrawal",
                external_id="perm-saga-1",
            )

    def test_compensate_ledger_saga_requires_dedicated_permission(self):
        self.grant_perm(self.u1, "can_manage_ledger_sagas")
        self.grant_perm(self.u1, "can_apply_raw_ledger_transaction")
        self.grant_perm(self.u1, "can_reverse_ledger_transaction")

        saga = create_ledger_saga(
            actor=self.u1,
            saga_type="crypto_withdrawal",
            external_id="perm-saga-2",
        )
        start_ledger_saga(actor=self.u1, saga=saga)

        step = add_saga_step(actor=self.u1, saga=saga, step_key="reserve", step_order=1)

        txn = apply_ledger_transaction(
            actor=self.u1,
            kind="reserve_test",
            entries=[(self.issuance, -5), (self.w1, 5)],
            external_id="perm-saga-txn-1",
        )

        complete_saga_step(actor=self.u1, step=step, txn=txn)
        fail_saga_step(actor=self.u1, step=step, error_message="failure")

        with self.assertRaises(PermissionDenied):
            compensate_ledger_saga(
                actor=self.u1,
                saga=saga,
                reason="forbidden compensation",
            )