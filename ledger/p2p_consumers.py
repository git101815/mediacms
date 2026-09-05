from __future__ import annotations

import logging

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer

from .models import P2PMessage, P2POrder
from .p2p_realtime import p2p_group_name


logger = logging.getLogger(__name__)

MAX_CHAT_MESSAGE_CHARS = 4000
MAX_INITIAL_MESSAGES = 500


def _serialize_message(message: P2PMessage, *, user_id: int) -> dict:
    sender = message.sender
    return {
        "id": int(message.id),
        "kind": message.kind,
        "body": message.body,
        "created_at": message.created_at.isoformat(),
        "sender_id": getattr(sender, "id", None),
        "sender_name": sender.username if sender is not None else "System",
        "is_mine": bool(sender is not None and sender.id == user_id),
    }


class P2PExchangeConsumer(AsyncJsonWebsocketConsumer):
    """Authenticated realtime room for one P2P order.

    PostgreSQL is the message source of truth. Redis/Channels is only the live
    fan-out transport between web replicas and connected browsers.
    """

    async def connect(self):
        user = self.scope.get("user")
        if user is None or not user.is_authenticated:
            await self.close(code=4401)
            return

        public_id = self.scope["url_route"]["kwargs"]["public_id"]
        state = await self._load_order_state(public_id, user.id)
        if state is None:
            # Keep private-order existence opaque to non-participants.
            await self.close(code=4404)
            return

        self.order_id = state["order_id"]
        self.public_id = state["public_id"]
        self.group_name = p2p_group_name(self.public_id)

        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name,
        )
        await self.accept()

        messages = await self._load_initial_messages(
            self.order_id,
            user.id,
        )
        await self.send_json(
            {
                "type": "snapshot",
                "messages": messages,
                "order_status": state["status"],
                "can_send": state["can_send"],
                "history_limited": len(messages) >= MAX_INITIAL_MESSAGES,
            }
        )

    async def disconnect(self, close_code):
        group_name = getattr(self, "group_name", None)
        if group_name:
            try:
                await self.channel_layer.group_discard(
                    group_name,
                    self.channel_name,
                )
            except Exception:
                logger.exception(
                    "Failed to discard P2P websocket group %s",
                    group_name,
                )

    async def receive_json(self, content, **kwargs):
        event_type = str(content.get("type") or "").strip()

        if event_type == "ping":
            await self.send_json({"type": "pong"})
            return

        if event_type != "message.send":
            await self.send_json(
                {
                    "type": "error",
                    "code": "unsupported_event",
                    "detail": "Unsupported WebSocket event.",
                }
            )
            return

        body = str(content.get("message") or "").strip()
        if not body:
            await self.send_json(
                {
                    "type": "error",
                    "code": "empty_message",
                    "detail": "Message cannot be empty.",
                }
            )
            return
        if len(body) > MAX_CHAT_MESSAGE_CHARS:
            await self.send_json(
                {
                    "type": "error",
                    "code": "message_too_long",
                    "detail": (
                        f"Message cannot exceed {MAX_CHAT_MESSAGE_CHARS} characters."
                    ),
                }
            )
            return

        user = self.scope["user"]
        result = await self._create_message(
            self.order_id,
            user.id,
            body,
        )
        if result["error"] == "not_found":
            await self.close(code=4404)
            return
        if result["error"] == "read_only":
            await self.send_json(
                {
                    "type": "error",
                    "code": "read_only",
                    "detail": "This P2P conversation is read-only.",
                    "order_status": result["status"],
                    "can_send": False,
                }
            )
            return

        message = {
            "id": result["id"],
            "kind": P2PMessage.KIND_USER,
            "body": body,
            "created_at": result["created_at"],
            "sender_id": user.id,
            "sender_name": user.username,
        }
        client_id = str(content.get("client_id") or "")[:64]

        try:
            await self.channel_layer.group_send(
                self.group_name,
                {
                    "type": "p2p.message",
                    "message": message,
                    "client_id": client_id,
                },
            )
        except Exception:
            # The DB write already succeeded. Show it to the sender locally;
            # the peer will recover it from PostgreSQL after reconnect.
            logger.exception(
                "Failed to fan out P2P message %s",
                result["id"],
            )
            local_message = dict(message)
            local_message["is_mine"] = True
            await self.send_json(
                {
                    "type": "message",
                    "message": local_message,
                    "client_id": client_id,
                }
            )

    async def p2p_message(self, event):
        message = dict(event["message"])
        message["is_mine"] = (
            message.get("sender_id") == self.scope["user"].id
        )
        await self.send_json(
            {
                "type": "message",
                "message": message,
                "client_id": (
                    event.get("client_id", "")
                    if message["is_mine"]
                    else ""
                ),
            }
        )

    async def p2p_status(self, event):
        await self.send_json(
            {
                "type": "status",
                "order_status": event["order_status"],
                "can_send": bool(event["can_send"]),
            }
        )

    @database_sync_to_async
    def _load_order_state(self, public_id, user_id):
        try:
            order = P2POrder.objects.select_related("maker__user").get(
                public_id=public_id
            )
        except P2POrder.DoesNotExist:
            return None

        if user_id not in {order.buyer_id, order.maker.user_id}:
            return None

        return {
            "order_id": order.id,
            "public_id": str(order.public_id),
            "status": order.status,
            "can_send": order.chat_writable,
        }

    @database_sync_to_async
    def _load_initial_messages(self, order_id, user_id):
        order = (
            P2POrder.objects.select_related("maker__user")
            .filter(id=order_id)
            .first()
        )
        if order is None or user_id not in {order.buyer_id, order.maker.user_id}:
            return []

        messages = list(
            P2PMessage.objects.filter(order_id=order_id)
            .select_related("sender")
            .order_by("-id")[:MAX_INITIAL_MESSAGES]
        )
        messages.reverse()
        return [
            _serialize_message(message, user_id=user_id)
            for message in messages
        ]

    @database_sync_to_async
    def _create_message(self, order_id, user_id, body):
        try:
            order = P2POrder.objects.select_related("maker__user").get(
                id=order_id
            )
        except P2POrder.DoesNotExist:
            return {"error": "not_found"}

        if user_id not in {order.buyer_id, order.maker.user_id}:
            return {"error": "not_found"}
        if not order.chat_writable:
            return {
                "error": "read_only",
                "status": order.status,
            }

        message = P2PMessage.objects.create(
            order=order,
            sender_id=user_id,
            kind=P2PMessage.KIND_USER,
            body=body,
        )
        return {
            "error": None,
            "id": int(message.id),
            "created_at": message.created_at.isoformat(),
        }
