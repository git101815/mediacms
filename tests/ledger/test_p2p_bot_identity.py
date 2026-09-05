
import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.test import TestCase, override_settings

from ledger.models import P2PAgentAssignment, P2PMakerProfile, P2POrder
from ledger.p2p_services import respond_to_p2p_agent_offer


class P2PBotIdentityTests(TestCase):
    def test_external_identity_cannot_decline_someone_elses_offer(self):
        users = get_user_model()
        buyer = users.objects.create_user(username="p2p_bot_buyer")
        agent_user = users.objects.create_user(username="p2p_bot_agent")
        maker = P2PMakerProfile.objects.create(
            user=agent_user,
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            card_enabled=True,
            telegram_user_id="123",
            commission_percent=Decimal("1.00"),
        )
        order = P2POrder.objects.create(
            buyer=buyer,
            maker=maker,
            payment_method=P2POrder.PAYMENT_METHOD_CARD,
            platform_amount=10_100_000,
            status=P2POrder.STATUS_WAITING_AGENT,
        )
        assignment = P2PAgentAssignment.objects.create(
            order=order,
            maker=maker,
            expires_at=order.created_at + __import__("datetime").timedelta(minutes=5),
            transaction_amount_snapshot=order.platform_amount,
        )
        with self.assertRaises(PermissionDenied):
            respond_to_p2p_agent_offer(
                action_token=assignment.action_token,
                action="decline",
                channel="telegram",
                external_user_id="999",
            )

    @override_settings(P2P_N8N_ACTION_SECRET="dedicated-p2p-secret")
    def test_n8n_callback_requires_dedicated_p2p_action_secret(self):
        users = get_user_model()
        buyer = users.objects.create_user(username="p2p_n8n_buyer")
        agent_user = users.objects.create_user(username="p2p_n8n_agent")
        maker = P2PMakerProfile.objects.create(
            user=agent_user,
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            card_enabled=True,
            telegram_user_id="456",
            commission_percent=Decimal("1.00"),
        )
        order = P2POrder.objects.create(
            buyer=buyer,
            maker=maker,
            payment_method=P2POrder.PAYMENT_METHOD_CARD,
            platform_amount=10_100_000,
            status=P2POrder.STATUS_WAITING_AGENT,
        )
        assignment = P2PAgentAssignment.objects.create(
            order=order,
            maker=maker,
            expires_at=order.created_at + __import__("datetime").timedelta(minutes=5),
            transaction_amount_snapshot=order.platform_amount,
        )
        body = json.dumps(
            {
                "action_token": str(assignment.action_token),
                "action": "decline",
                "channel": "telegram",
                "external_user_id": "456",
            }
        )

        legacy = self.client.post(
            "/api/p2p/n8n/agent-response",
            data=body,
            content_type="application/json",
            HTTP_X_NOTIFICATION_SECRET="dedicated-p2p-secret",
        )
        self.assertEqual(legacy.status_code, 403)

        response = self.client.post(
            "/api/p2p/n8n/agent-response",
            data=body,
            content_type="application/json",
            HTTP_X_P2P_ACTION_SECRET="dedicated-p2p-secret",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["state"], "declined")

