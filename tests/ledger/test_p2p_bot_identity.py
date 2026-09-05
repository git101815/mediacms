
import json
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.cache import cache
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


@override_settings(P2P_N8N_ACTION_SECRET="dedicated-p2p-secret")
class P2PTelegramBotLoginTests(TestCase):
    def setUp(self):
        cache.clear()
        users = get_user_model()
        self.agent_user = users.objects.create_user(
            username="telegram_agent",
            password="correct horse battery staple",
        )
        self.profile = P2PMakerProfile.objects.create(
            user=self.agent_user,
            status=P2PMakerProfile.STATUS_PAUSED,
            accepting_orders=False,
            card_enabled=True,
            commission_percent=Decimal("1.00"),
        )

    def _post(self, payload):
        return self.client.post(
            "/api/p2p/n8n/telegram-auth",
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_X_P2P_ACTION_SECRET="dedicated-p2p-secret",
        )

    def test_start_username_password_binds_telegram_identity(self):
        start = self._post(
            {
                "action": "start",
                "telegram_user_id": "987654321",
                "telegram_chat_id": "987654321",
            }
        )
        self.assertEqual(start.status_code, 200)
        self.assertEqual(start.json()["state"], "need_username")

        username = self._post(
            {
                "action": "input",
                "telegram_user_id": "987654321",
                "telegram_chat_id": "987654321",
                "text": "telegram_agent",
            }
        )
        self.assertEqual(username.status_code, 200)
        self.assertEqual(username.json()["state"], "need_password")

        password = self._post(
            {
                "action": "input",
                "telegram_user_id": "987654321",
                "telegram_chat_id": "987654321",
                "text": "correct horse battery staple",
            }
        )
        self.assertEqual(password.status_code, 200)
        self.assertTrue(password.json()["ok"])
        self.assertEqual(password.json()["state"], "authenticated")
        self.assertEqual(password.json()["message"], "Hello telegram_agent")
        self.assertTrue(password.json()["sensitive_input"])

        self.profile.refresh_from_db()
        self.assertEqual(self.profile.telegram_user_id, "987654321")

    def test_wrong_password_is_not_bound(self):
        self._post(
            {
                "action": "start",
                "telegram_user_id": "111",
                "telegram_chat_id": "111",
            }
        )
        self._post(
            {
                "action": "input",
                "telegram_user_id": "111",
                "telegram_chat_id": "111",
                "text": "telegram_agent",
            }
        )
        response = self._post(
            {
                "action": "input",
                "telegram_user_id": "111",
                "telegram_chat_id": "111",
                "text": "wrong-password",
            }
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["state"], "invalid_credentials")
        self.assertTrue(response.json()["sensitive_input"])

        self.profile.refresh_from_db()
        self.assertEqual(self.profile.telegram_user_id, "")

    def test_private_chat_identity_is_required(self):
        response = self._post(
            {
                "action": "start",
                "telegram_user_id": "123",
                "telegram_chat_id": "-100123",
            }
        )
        self.assertEqual(response.status_code, 400)

    def test_unrelated_notification_secret_is_not_accepted(self):
        response = self.client.post(
            "/api/p2p/n8n/telegram-auth",
            data=json.dumps(
                {
                    "action": "start",
                    "telegram_user_id": "123",
                    "telegram_chat_id": "123",
                }
            ),
            content_type="application/json",
            HTTP_X_NOTIFICATION_SECRET="dedicated-p2p-secret",
        )
        self.assertEqual(response.status_code, 403)

