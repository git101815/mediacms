from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from ledger.models import P2PMakerProfile, P2PMessage, P2POrder


class P2PExchangeTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.buyer = users.objects.create_user(username="p2p-buyer", password="x")
        self.maker_user = users.objects.create_user(username="p2p-maker", password="x")
        self.outsider = users.objects.create_user(username="p2p-outsider", password="x")
        self.maker = P2PMakerProfile.objects.create(
            user=self.maker_user,
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            revolut_enabled=True,
            commission_percent=Decimal("4.00"),
        )
        self.order = P2POrder.objects.create(
            buyer=self.buyer,
            maker=self.maker,
            payment_method=P2POrder.PAYMENT_METHOD_REVOLUT,
            platform_amount=50_000_000,
        )

    def _page_url(self):
        return reverse("p2p_exchange", args=[self.order.public_id])

    def _messages_url(self):
        return reverse("p2p_exchange_messages", args=[self.order.public_id])

    def _send_url(self):
        return reverse("p2p_exchange_send_message", args=[self.order.public_id])

    def test_buyer_and_maker_can_open_private_exchange_page(self):
        self.client.force_login(self.buyer)
        response = self.client.get(self._page_url())
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Order #")
        self.assertContains(response, "p2p-maker")

        self.client.force_login(self.maker_user)
        response = self.client.get(self._page_url())
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "p2p-buyer")

    def test_outsider_cannot_discover_exchange(self):
        self.client.force_login(self.outsider)
        self.assertEqual(self.client.get(self._page_url()).status_code, 404)
        self.assertEqual(self.client.get(self._messages_url()).status_code, 404)

    def test_message_sender_is_authenticated_user_and_peer_can_poll_it(self):
        self.client.force_login(self.maker_user)
        response = self.client.post(
            self._send_url(),
            data='{"message":"Send 48 EUR to my Revolut handle."}',
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 201)
        message = P2PMessage.objects.get(order=self.order)
        self.assertEqual(message.sender, self.maker_user)
        self.assertEqual(message.body, "Send 48 EUR to my Revolut handle.")

        self.client.force_login(self.buyer)
        response = self.client.get(self._messages_url(), {"after_id": 0})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["messages"]), 1)
        self.assertEqual(payload["messages"][0]["sender_name"], "p2p-maker")
        self.assertFalse(payload["messages"][0]["is_mine"])

    def test_poll_after_id_only_returns_newer_messages(self):
        first = P2PMessage.objects.create(order=self.order, sender=self.buyer, body="first")
        second = P2PMessage.objects.create(order=self.order, sender=self.maker_user, body="second")
        self.client.force_login(self.buyer)
        response = self.client.get(self._messages_url(), {"after_id": first.id})
        self.assertEqual(response.status_code, 200)
        ids = [item["id"] for item in response.json()["messages"]]
        self.assertEqual(ids, [second.id])

    def test_terminal_order_is_read_only(self):
        self.order.status = P2POrder.STATUS_COMPLETED
        self.order.save(update_fields=["status", "updated_at"])
        self.client.force_login(self.buyer)
        response = self.client.post(
            self._send_url(),
            data='{"message":"too late"}',
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 409)
        self.assertFalse(P2PMessage.objects.filter(order=self.order).exists())

    def test_empty_and_oversized_messages_are_rejected(self):
        self.client.force_login(self.buyer)
        response = self.client.post(
            self._send_url(), data='{"message":"   "}', content_type="application/json"
        )
        self.assertEqual(response.status_code, 400)

        response = self.client.post(
            self._send_url(),
            data='{"message":"' + ("x" * 4001) + '"}',
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
