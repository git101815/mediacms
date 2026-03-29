from django.core.exceptions import PermissionDenied, ValidationError

from ledger.models import (
    LEDGER_ACTION_TRANSFER,
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    LedgerHold,
    LedgerVelocityWindow,
)
from ledger.services import (
    apply_ledger_transaction,
    create_wallet_hold,
    get_wallet_available_balance,
    get_wallet_velocity_amount,
    record_wallet_velocity,
    release_wallet_hold,
    set_wallet_risk_status,
    set_wallet_velocity_limits,
)

from tests.ledger.base import BaseLedgerTestCase


class TestLedgerRisk(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.operator.is_superuser = True
        self.operator.is_staff = True
        self.operator.save(update_fields=["is_superuser", "is_staff"])

    def test_create_wallet_hold_reduces_available_balance(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="risk-hold-fund-1",
        )

        hold = create_wallet_hold(
            actor=self.operator,
            wallet=self.w1,
            amount=40,
            reason="withdrawal review",
        )

        self.w1.refresh_from_db()
        self.assertEqual(hold.amount, 40)
        self.assertEqual(self.w1.balance, 100)
        self.assertEqual(self.w1.held_balance, 40)
        self.assertEqual(get_wallet_available_balance(self.w1), 60)

    def test_release_wallet_hold_restores_available_balance(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="risk-hold-fund-2",
        )

        hold = create_wallet_hold(
            actor=self.operator,
            wallet=self.w1,
            amount=40,
            reason="withdrawal review",
        )
        release_wallet_hold(actor=self.operator, hold=hold)

        self.w1.refresh_from_db()
        hold.refresh_from_db()

        self.assertTrue(hold.released)
        self.assertEqual(self.w1.held_balance, 0)
        self.assertEqual(get_wallet_available_balance(self.w1), 100)

    def test_cannot_spend_held_balance(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="risk-held-fund-1",
        )

        create_wallet_hold(
            actor=self.operator,
            wallet=self.w1,
            amount=90,
            reason="review",
        )

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                actor=self.operator,
                kind="transfer",
                entries=[(self.w1, -20), (self.w2, 20)],
                external_id="risk-held-spend-1",
            )

    def test_blocked_wallet_cannot_transact(self):
        set_wallet_risk_status(
            actor=self.operator,
            wallet=self.w1,
            risk_status=LEDGER_RISK_STATUS_BLOCKED,
            reason="fraud",
        )

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                actor=self.operator,
                kind="mint",
                entries=[(self.issuance, -10), (self.w1, 10)],
                external_id="risk-blocked-1",
            )

    def test_review_wallet_cannot_transact(self):
        set_wallet_risk_status(
            actor=self.operator,
            wallet=self.w1,
            risk_status=LEDGER_RISK_STATUS_REVIEW,
            reason="manual review",
            review_required=True,
        )

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                actor=self.operator,
                kind="mint",
                entries=[(self.issuance, -10), (self.w1, 10)],
                external_id="risk-review-1",
            )

    def test_set_wallet_velocity_limits_blocks_excess_outflow(self):
        set_wallet_velocity_limits(
            actor=self.operator,
            wallet=self.w1,
            hourly_outflow_limit=50,
            daily_outflow_limit=100,
        )

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="risk-velocity-fund-1",
        )

        apply_ledger_transaction(
            actor=self.operator,
            kind="transfer",
            entries=[(self.w1, -40), (self.w2, 40)],
            external_id="risk-velocity-ok-1",
        )

        with self.assertRaises(ValidationError):
            apply_ledger_transaction(
                actor=self.operator,
                kind="transfer",
                entries=[(self.w1, -20), (self.w2, 20)],
                external_id="risk-velocity-block-1",
            )

    def test_record_wallet_velocity_accumulates_window_amount(self):
        record_wallet_velocity(wallet=self.w1, action=LEDGER_ACTION_TRANSFER, amount=10)
        record_wallet_velocity(wallet=self.w1, action=LEDGER_ACTION_TRANSFER, amount=15)

        amount = get_wallet_velocity_amount(
            wallet=self.w1,
            action=LEDGER_ACTION_TRANSFER,
            window_seconds=3600,
        )
        self.assertEqual(amount, 25)

    def test_manage_wallet_holds_requires_permission(self):
        with self.assertRaises(PermissionDenied):
            create_wallet_hold(
                actor=self.u1,
                wallet=self.w1,
                amount=10,
                reason="forbidden hold",
            )

    def test_manage_wallet_risk_requires_permission(self):
        with self.assertRaises(PermissionDenied):
            set_wallet_risk_status(
                actor=self.u1,
                wallet=self.w1,
                risk_status=LEDGER_RISK_STATUS_BLOCKED,
                reason="forbidden risk update",
            )

    def test_velocity_window_rows_are_created(self):
        record_wallet_velocity(wallet=self.w1, action=LEDGER_ACTION_TRANSFER, amount=12)

        self.assertTrue(
            LedgerVelocityWindow.objects.filter(
                wallet=self.w1,
                action=LEDGER_ACTION_TRANSFER,
            ).exists()
        )

    def test_hold_rows_are_created(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit_mint",
            entries=[(self.issuance, -100), (self.w1, 100)],
            external_id="risk-hold-row-1",
        )

        create_wallet_hold(
            actor=self.operator,
            wallet=self.w1,
            amount=25,
            reason="hold row",
        )

        self.assertTrue(LedgerHold.objects.filter(wallet=self.w1, released=False).exists())