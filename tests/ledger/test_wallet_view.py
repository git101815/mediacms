from django.urls import reverse

from ledger.models import LEDGER_RISK_STATUS_REVIEW
from ledger.services import apply_ledger_transaction, create_wallet_hold, set_wallet_risk_status

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