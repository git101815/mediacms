from ledger.models import WalletRequest
from ledger.services import (
    apply_ledger_transaction,
    complete_wallet_withdrawal_request,
    create_wallet_withdrawal_request,
    get_external_asset_clearing_wallet,
    reject_wallet_request,
)

from .base import BaseLedgerTestCase


class TestWalletRequestReview(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_review_wallet_requests")

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -5_000_000),
                (self.w1, 5_000_000),
            ],
            created_by=self.u1,
            memo="Initial funding for withdrawal request review tests",
        )

    def test_reject_wallet_request_releases_hold_and_marks_request_rejected(self):
        wallet_request = create_wallet_withdrawal_request(
            actor=self.u1,
            wallet=self.w1,
            amount="1.5",
            destination_address="0xreject000000000000000000000000000000000001",
            notes="Reject this one",
        )

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 5_000_000)
        self.assertEqual(self.w1.held_balance, 1_500_000)

        rejected_request = reject_wallet_request(
            actor=self.operator,
            wallet_request=wallet_request,
            rejection_reason="Manual review rejected the payout",
        )

        rejected_request.refresh_from_db()
        self.w1.refresh_from_db()
        rejected_request.hold.refresh_from_db()

        self.assertEqual(rejected_request.status, WalletRequest.STATUS_REJECTED)
        self.assertEqual(rejected_request.reviewed_by_id, self.operator.id)
        self.assertIsNotNone(rejected_request.reviewed_at)
        self.assertEqual(rejected_request.rejection_reason, "Manual review rejected the payout")
        self.assertEqual(rejected_request.payout_txid, "")
        self.assertIsNone(rejected_request.linked_ledger_txn)

        self.assertTrue(rejected_request.hold.released)
        self.assertEqual(rejected_request.hold.released_by_id, self.operator.id)
        self.assertIsNotNone(rejected_request.hold.released_at)

        self.assertEqual(self.w1.balance, 5_000_000)
        self.assertEqual(self.w1.held_balance, 0)

    def test_complete_wallet_withdrawal_request_releases_hold_and_posts_ledger_txn(self):
        wallet_request = create_wallet_withdrawal_request(
            actor=self.u1,
            wallet=self.w1,
            amount="2",
            destination_address="0xcomplete0000000000000000000000000000000001",
            notes="Complete this one",
        )

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 5_000_000)
        self.assertEqual(self.w1.held_balance, 2_000_000)

        external_asset_clearing_wallet = get_external_asset_clearing_wallet()
        external_asset_clearing_wallet.refresh_from_db()
        self.assertEqual(external_asset_clearing_wallet.balance, 0)

        completed_request = complete_wallet_withdrawal_request(
            actor=self.operator,
            wallet_request=wallet_request,
            payout_txid="0xpaid000000000000000000000000000000000001",
        )

        completed_request.refresh_from_db()
        self.w1.refresh_from_db()
        external_asset_clearing_wallet.refresh_from_db()
        completed_request.hold.refresh_from_db()

        self.assertEqual(completed_request.status, WalletRequest.STATUS_COMPLETED)
        self.assertEqual(completed_request.reviewed_by_id, self.operator.id)
        self.assertIsNotNone(completed_request.reviewed_at)
        self.assertIsNotNone(completed_request.completed_at)
        self.assertEqual(completed_request.rejection_reason, "")
        self.assertEqual(completed_request.payout_txid, "0xpaid000000000000000000000000000000000001")
        self.assertIsNotNone(completed_request.linked_ledger_txn)

        self.assertTrue(completed_request.hold.released)
        self.assertEqual(completed_request.hold.released_by_id, self.operator.id)
        self.assertIsNotNone(completed_request.hold.released_at)

        self.assertEqual(self.w1.balance, 3_000_000)
        self.assertEqual(self.w1.held_balance, 0)
        self.assertEqual(external_asset_clearing_wallet.balance, 2_000_000)