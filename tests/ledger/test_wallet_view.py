from django.urls import reverse

from ledger.models import LEDGER_RISK_STATUS_REVIEW, WalletRequest, DepositAddress, DepositSession
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

    def test_wallet_page_shows_request_actions(self):
        self.client.force_login(self.u1)
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Deposit")
        self.assertContains(response, "Withdraw")

    def test_user_can_open_deposit_session_from_wallet(self):
        DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/10",
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_deposit_request"),
            {
                "deposit_option_key": "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
                "return_tab": "all",
                "return_status": "all",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(WalletRequest.objects.filter(request_type=WalletRequest.REQUEST_TYPE_DEPOSIT).count(), 0)

        session = DepositSession.objects.get(wallet=self.w1)
        self.assertRedirects(response, reverse("wallet_deposit_session", kwargs={"public_id": session.public_id}))

        wallet_response = self.client.get(reverse("wallet"))
        self.assertContains(wallet_response, "Recent Deposit Sessions")
        self.assertContains(wallet_response, "Ethereum · USDT")

    def test_user_can_create_withdrawal_request_from_wallet(self):
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

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "120",
                "destination_address": "0xabc123",
                "notes": "First withdrawal",
                "return_tab": "all",
                "return_status": "all",
            },
        )

        self.assertEqual(response.status_code, 302)

        wallet_request = WalletRequest.objects.get(
            wallet=self.w1,
            request_type=WalletRequest.REQUEST_TYPE_WITHDRAWAL,
        )
        self.assertEqual(wallet_request.status, WalletRequest.STATUS_PENDING)
        self.assertEqual(wallet_request.amount, 120)
        self.assertEqual(wallet_request.destination_address, "0xabc123")
        self.assertEqual(wallet_request.notes, "First withdrawal")
        self.assertIsNotNone(wallet_request.hold)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 500)
        self.assertEqual(self.w1.held_balance, 120)

        wallet_response = self.client.get(reverse("wallet"))
        self.assertContains(wallet_response, "First withdrawal")
        self.assertContains(wallet_response, "0xabc123")
        self.assertContains(wallet_response, "Reserved 120")

    def test_withdrawal_request_rejected_when_amount_exceeds_available_balance(self):
        apply_ledger_transaction(
            actor=self.operator,
            kind="deposit",
            entries=[
                (self.issuance, -100),
                (self.w1, 100),
            ],
            created_by=self.u1,
            memo="Small top-up",
        )

        self.client.force_login(self.u1)

        response = self.client.post(
            reverse("wallet_withdrawal_request"),
            {
                "amount": "150",
                "destination_address": "0xoverflow",
                "notes": "Too much",
                "return_tab": "all",
                "return_status": "all",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(WalletRequest.objects.count(), 0)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.held_balance, 0)