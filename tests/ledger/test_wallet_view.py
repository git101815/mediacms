from django.urls import reverse

from ledger.models import LEDGER_RISK_STATUS_REVIEW
from ledger.services import (
    apply_ledger_transaction,
    create_pending_ledger_transaction,
    create_wallet_hold,
    reverse_ledger_transaction,
    set_wallet_risk_status,
)

from .base import BaseLedgerTestCase


class TestWalletView(BaseLedgerTestCase):
    def test_wallet_page_requires_login(self):
        response = self.client.get(reverse("wallet"))
        self.assertEqual(response.status_code, 302)

    def test_wallet_page_shows_balances_holds_and_entries(self):
        self.grant_perm(self.operator, "can_manage_wallet_holds")

        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -500),
                (self.w1, 500),
            ],
            created_by=self.u1,
            memo="Initial top-up",
        )
        create_wallet_hold(
            actor=self.operator,
            wallet=self.w1,
            amount=120,
            reason="Payout reserve",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Wallet")
        self.assertContains(response, "500")
        self.assertContains(response, "380")
        self.assertContains(response, "120")
        self.assertContains(response, "Initial top-up")
        self.assertContains(response, "Payout reserve")

    def test_wallet_page_hides_internal_risk_reason_without_permission(self):
        self.grant_perm(self.operator, "can_manage_wallet_risk")
        set_wallet_risk_status(
            actor=self.operator,
            wallet=self.w1,
            risk_status=LEDGER_RISK_STATUS_REVIEW,
            reason="Manual review required",
            review_required=True,
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertContains(response, "Wallet under review")
        self.assertNotContains(response, "Manual review required")

    def test_wallet_page_shows_internal_risk_reason_with_permission(self):
        self.grant_perm(self.operator, "can_manage_wallet_risk")
        self.grant_perm(self.u1, "can_view_wallet_risk")
        set_wallet_risk_status(
            actor=self.operator,
            wallet=self.w1,
            risk_status=LEDGER_RISK_STATUS_REVIEW,
            reason="Manual review required",
            review_required=True,
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertContains(response, "Manual review required")

    def test_wallet_page_filters_transactions_by_tab(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -600),
                (self.w1, 600),
            ],
            created_by=self.u1,
            memo="Deposit row",
        )
        apply_ledger_transaction(
            actor=self.operator,
            kind="purchase",
            entries=[
                (self.w1, -150),
                (self.issuance, 150),
            ],
            created_by=self.u1,
            memo="Purchase row",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "deposits"})

        self.assertContains(response, "Deposit row")
        self.assertNotContains(response, "Purchase row")
        self.assertRegex(
            response.content.decode(),
            r'class="wallet-filter-pill wallet-filter-pill--active"[^>]*>\s*Deposits\s*</a>',
        )

    def test_wallet_page_filters_transactions_by_status(self):
        posted_txn = apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -400),
                (self.w1, 400),
            ],
            created_by=self.u1,
            memo="Posted deposit",
        )
        reverse_ledger_transaction(
            actor=self.operator,
            original_txn=posted_txn,
            created_by=self.u1,
            memo="Reversed deposit",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"status": "reversed"})

        self.assertContains(response, "Reversed deposit")
        self.assertNotContains(response, "Posted deposit")
        self.assertContains(response, 'option value="reversed" selected', html=False)

    def test_wallet_page_pending_filter_shows_empty_state_when_no_pending_entry_exists(self):
        create_pending_ledger_transaction(
            actor=self.operator,
            kind="withdrawal",
            created_by=self.u1,
            memo="Pending withdrawal",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"status": "pending"})

        self.assertContains(response, 'option value="pending" selected', html=False)
        self.assertContains(response, "No activity yet")
        self.assertContains(response, "No pending transaction matches this filter yet.")

    def test_wallet_page_invalid_filters_fallback_to_all(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -250),
                (self.w1, 250),
            ],
            created_by=self.u1,
            memo="Fallback row",
        )

        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"), {"tab": "nope", "status": "???"})

        self.assertContains(response, "Fallback row")
        self.assertRegex(
            response.content.decode(),
            r'class="wallet-filter-pill wallet-filter-pill--active"[^>]*>\s*All\s*</a>',
        )
        self.assertContains(response, 'option value="all" selected', html=False)

    def test_wallet_page_paginates_activity(self):
        for index in range(25):
            apply_ledger_transaction(
                actor=self.operator,
                kind="deposit",
                entries=[
                    (self.issuance, -(index + 1)),
                    (self.w1, index + 1),
                ],
                created_by=self.u1,
                memo=f"Deposit {index}",
            )

        self.client.force_login(self.u1)
        page_one = self.client.get(reverse("wallet"))
        page_two = self.client.get(reverse("wallet"), {"page": 2})

        self.assertContains(page_one, "Deposit 24")
        self.assertContains(page_one, "Deposit 5")
        self.assertNotContains(page_one, "Deposit 4")
        self.assertContains(page_two, "Deposit 4")
        self.assertContains(page_two, "Deposit 0")
        self.assertContains(page_one, "Page 1 of 2")
        self.assertContains(page_two, "Page 2 of 2")