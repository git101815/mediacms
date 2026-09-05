
from decimal import Decimal

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.exceptions import ValidationError
from django.test import TestCase, override_settings

from ledger.models import (
    P2PAgentAssignment,
    P2PMakerProfile,
    P2POrder,
    TokenPack,
    TokenWallet,
)
from ledger.p2p_services import (
    P2PNoAgentAvailable,
    create_p2p_order_for_checkout,
    find_new_p2p_agent,
    mark_p2p_fiat_received,
    mark_p2p_fiat_sent,
    preview_p2p_checkout,
    respond_to_p2p_agent_offer,
)


@override_settings(
    P2P_INTERNAL_SERVICE_USERNAME="p2p-test-service",
    P2P_AGENT_RESPONSE_TIMEOUT_SECONDS=300,
    P2P_TRADE_TIMEOUT_SECONDS=3600,
)
class P2PTransactionFlowTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.service = users.objects.create_user(username="p2p-test-service")
        self.service.user_permissions.add(
            Permission.objects.get(content_type__app_label="ledger", codename="can_apply_raw_ledger_transaction"),
            Permission.objects.get(content_type__app_label="ledger", codename="can_reverse_ledger_transaction"),
        )
        self.customer = users.objects.create_user(username="p2p_customer")
        self.agent_a_user = users.objects.create_user(username="p2p_agent_a")
        self.agent_b_user = users.objects.create_user(username="p2p_agent_b")
        self.pack = TokenPack.objects.create(
            code="p2p-test-pack",
            name="P2P test",
            token_amount=5_000 * 1_000_000,
            gross_stable_amount=50 * 1_000_000,
            is_active=True,
        )
        self.agent_a = P2PMakerProfile.objects.create(
            user=self.agent_a_user,
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            card_enabled=True,
            telegram_user_id="1001",
            commission_percent=Decimal("4.00"),
        )
        self.agent_b = P2PMakerProfile.objects.create(
            user=self.agent_b_user,
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            card_enabled=True,
            telegram_user_id="1002",
            commission_percent=Decimal("6.00"),
        )

    def _fund_agent(self, user, amount):
        TokenWallet.objects.update_or_create(
            user=user,
            defaults={"wallet_type": TokenWallet.TYPE_USER, "allow_negative": False, "balance": amount},
        )

    def test_declined_agent_is_excluded_and_transaction_value_updates(self):
        order = create_p2p_order_for_checkout(
            buyer=self.customer,
            token_pack=self.pack,
            payment_method=P2POrder.PAYMENT_METHOD_CARD,
        )
        first = order.assignments.get()
        self.assertEqual(order.platform_amount, 52_000_000)

        state, _ = respond_to_p2p_agent_offer(
            action_token=first.action_token,
            action="decline",
            channel="telegram",
            external_user_id="1001",
        )
        self.assertEqual(state, "declined")
        order.refresh_from_db()
        self.assertEqual(order.status, P2POrder.STATUS_WAITING_NEW_AGENT)
        self.assertIsNone(order.maker_id)

        order = find_new_p2p_agent(order_id=order.id, buyer_id=self.customer.id)
        self.assertEqual(order.maker_id, self.agent_b.id)
        self.assertEqual(order.platform_amount, 53_000_000)
        self.assertEqual(set(order.assignments.values_list("maker_id", flat=True)), {self.agent_a.id, self.agent_b.id})

    def test_accept_funds_then_two_locks_settle_to_customer(self):
        order = create_p2p_order_for_checkout(
            buyer=self.customer,
            token_pack=self.pack,
            payment_method=P2POrder.PAYMENT_METHOD_CARD,
        )
        assignment = order.assignments.get()
        self._fund_agent(self.agent_a_user, order.token_amount)

        state, order = respond_to_p2p_agent_offer(
            action_token=assignment.action_token,
            action="accept",
            channel="telegram",
            external_user_id="1001",
        )
        self.assertEqual(state, "accepted")
        self.assertEqual(order.status, P2POrder.STATUS_CHAT_OPEN)
        agent_wallet = TokenWallet.objects.get(user=self.agent_a_user)
        customer_wallet, _ = TokenWallet.objects.get_or_create(user=self.customer)
        settlement_wallet = TokenWallet.objects.get(system_key=TokenWallet.SYSTEM_P2P_SETTLEMENT)
        agent_wallet.refresh_from_db()
        settlement_wallet.refresh_from_db()
        self.assertEqual(agent_wallet.balance, 0)
        self.assertEqual(settlement_wallet.balance, order.token_amount)

        order = mark_p2p_fiat_sent(order_id=order.id, user_id=self.customer.id)
        self.assertEqual(order.status, P2POrder.STATUS_FIAT_SENT)
        order = mark_p2p_fiat_received(order_id=order.id, user_id=self.agent_a_user.id)
        self.assertEqual(order.status, P2POrder.STATUS_COMPLETED)
        customer_wallet.refresh_from_db()
        settlement_wallet.refresh_from_db()
        self.assertEqual(customer_wallet.balance, order.token_amount)
        self.assertEqual(settlement_wallet.balance, 0)


    def test_price_change_between_preview_and_submit_requires_reconfirmation(self):
        preview = preview_p2p_checkout(
            buyer=self.customer,
            token_pack=self.pack,
            payment_method=P2POrder.PAYMENT_METHOD_CARD,
        )
        self.assertEqual(preview["transaction_amount"], 52_000_000)

        self.agent_a.accepting_orders = False
        self.agent_a.save(update_fields=["accepting_orders", "updated_at"])

        with self.assertRaisesMessage(
            ValidationError,
            "P2P transaction value changed. Please review the updated price.",
        ):
            create_p2p_order_for_checkout(
                buyer=self.customer,
                token_pack=self.pack,
                payment_method=P2POrder.PAYMENT_METHOD_CARD,
                expected_transaction_amount=preview["transaction_amount"],
            )
        self.assertEqual(P2POrder.objects.count(), 0)

    def test_agents_without_notification_identity_are_not_selectable(self):
        P2PMakerProfile.objects.update(
            telegram_user_id="",
            discord_user_id="",
        )
        with self.assertRaises(P2PNoAgentAvailable):
            preview_p2p_checkout(
                buyer=self.customer,
                token_pack=self.pack,
                payment_method=P2POrder.PAYMENT_METHOD_CARD,
            )

    def test_offered_agent_cannot_access_chat_until_acceptance(self):
        order = create_p2p_order_for_checkout(
            buyer=self.customer,
            token_pack=self.pack,
            payment_method=P2POrder.PAYMENT_METHOD_CARD,
        )
        self.client.force_login(self.agent_a_user)
        response = self.client.get(f"/wallet/p2p/{order.public_id}/")
        self.assertEqual(response.status_code, 404)
