from asgiref.sync import sync_to_async
from channels.routing import URLRouter
from channels.testing import WebsocketCommunicator
from django.contrib.auth import get_user_model
from django.test import TransactionTestCase, override_settings

from ledger.models import P2PMakerProfile, P2PMessage, P2POrder
from ledger.p2p_routing import websocket_urlpatterns


IN_MEMORY_CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    }
}


@override_settings(CHANNEL_LAYERS=IN_MEMORY_CHANNEL_LAYERS)
class P2PWebSocketTests(TransactionTestCase):
    reset_sequences = True

    def setUp(self):
        User = get_user_model()
        self.buyer = User.objects.create_user(
            username="p2p_ws_buyer",
            password="testpass123",
        )
        self.maker_user = User.objects.create_user(
            username="p2p_ws_maker",
            password="testpass123",
        )
        self.outsider = User.objects.create_user(
            username="p2p_ws_outsider",
            password="testpass123",
        )
        self.maker = P2PMakerProfile.objects.create(
            user=self.maker_user,
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            revolut_enabled=True,
        )
        self.order = P2POrder.objects.create(
            buyer=self.buyer,
            maker=self.maker,
            payment_method=P2POrder.PAYMENT_METHOD_REVOLUT,
            platform_amount=50_000_000,
        )
        P2PMessage.objects.create(
            order=self.order,
            kind=P2PMessage.KIND_SYSTEM,
            body="Order created",
        )

    def communicator(self, user):
        app = URLRouter(websocket_urlpatterns)
        communicator = WebsocketCommunicator(
            app,
            f"/ws/p2p/{self.order.public_id}/",
        )
        communicator.scope["user"] = user
        return communicator

    async def test_participants_receive_snapshot_and_realtime_message(self):
        buyer_socket = self.communicator(self.buyer)
        maker_socket = self.communicator(self.maker_user)

        buyer_connected, _ = await buyer_socket.connect()
        maker_connected, _ = await maker_socket.connect()
        self.assertTrue(buyer_connected)
        self.assertTrue(maker_connected)

        buyer_snapshot = await buyer_socket.receive_json_from()
        maker_snapshot = await maker_socket.receive_json_from()
        self.assertEqual(buyer_snapshot["type"], "snapshot")
        self.assertEqual(maker_snapshot["type"], "snapshot")
        self.assertEqual(buyer_snapshot["messages"][0]["body"], "Order created")

        await buyer_socket.send_json_to(
            {
                "type": "message.send",
                "message": "Hello maker",
                "client_id": "test-client-id",
            }
        )

        buyer_event = await buyer_socket.receive_json_from()
        maker_event = await maker_socket.receive_json_from()
        self.assertEqual(buyer_event["type"], "message")
        self.assertEqual(maker_event["type"], "message")
        self.assertTrue(buyer_event["message"]["is_mine"])
        self.assertFalse(maker_event["message"]["is_mine"])
        self.assertEqual(maker_event["message"]["body"], "Hello maker")

        count = await sync_to_async(P2PMessage.objects.filter(order=self.order).count)()
        self.assertEqual(count, 2)

        await buyer_socket.disconnect()
        await maker_socket.disconnect()

    async def test_outsider_cannot_connect(self):
        socket = self.communicator(self.outsider)
        connected, close_code = await socket.connect()
        self.assertFalse(connected)
        self.assertEqual(close_code, 4404)

    async def test_completed_order_rejects_new_messages(self):
        await sync_to_async(
            P2POrder.objects.filter(pk=self.order.pk).update
        )(status=P2POrder.STATUS_COMPLETED)

        socket = self.communicator(self.buyer)
        connected, _ = await socket.connect()
        self.assertTrue(connected)
        snapshot = await socket.receive_json_from()
        self.assertFalse(snapshot["can_send"])

        await socket.send_json_to(
            {
                "type": "message.send",
                "message": "should fail",
            }
        )
        error = await socket.receive_json_from()
        self.assertEqual(error["type"], "error")
        self.assertEqual(error["code"], "read_only")
        await socket.disconnect()
