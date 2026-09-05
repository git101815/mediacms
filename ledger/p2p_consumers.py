
from __future__ import annotations

import logging

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.core.exceptions import PermissionDenied, ValidationError

from .models import P2PMessage, P2POrder
from .p2p_realtime import p2p_group_name
from .p2p_services import mark_p2p_fiat_received, mark_p2p_fiat_sent


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
    async def connect(self):
        user = self.scope.get("user")
        if user is None or not user.is_authenticated:
            await self.close(code=4401)
            return
        public_id = self.scope["url_route"]["kwargs"]["public_id"]
        state = await self._load_order_state(public_id, user.id)
        if state is None:
            await self.close(code=4404)
            return
        self.order_id = state["order_id"]
        self.public_id = state["public_id"]
        self.role = state["role"]
        self.group_name = p2p_group_name(self.public_id)
        await self.channel_layer.group_add(self.group_name, self.channel_name)
        await self.accept()
        messages = await self._load_initial_messages(self.order_id, user.id, state["chat_started"])
        await self.send_json(
            {
                "type": "snapshot",
                "messages": messages,
                "order_status": state["status"],
                "can_send": state["can_send"],
                "role": self.role,
                "actions": self._actions_for_status(state["status"]),
                "history_limited": len(messages) >= MAX_INITIAL_MESSAGES,
            }
        )

    async def disconnect(self, close_code):
        if getattr(self, "group_name", None):
            try:
                await self.channel_layer.group_discard(self.group_name, self.channel_name)
            except Exception:
                logger.exception("Failed to discard P2P websocket group %s", self.group_name)

    def _actions_for_status(self, status):
        return {
            "can_mark_sent": self.role == "buyer" and status == P2POrder.STATUS_CHAT_OPEN,
            "can_mark_received": self.role == "agent" and status == P2POrder.STATUS_FIAT_SENT,
        }

    async def receive_json(self, content, **kwargs):
        event_type = str(content.get("type") or "").strip()
        if event_type == "ping":
            await self.send_json({"type": "pong"})
            return
        if event_type == "order.fiat_sent":
            await self._handle_order_action("sent")
            return
        if event_type == "order.fiat_received":
            await self._handle_order_action("received")
            return
        if event_type != "message.send":
            await self.send_json({"type": "error", "code": "unsupported_event", "detail": "Unsupported WebSocket event."})
            return

        body = str(content.get("message") or "").strip()
        if not body:
            await self.send_json({"type": "error", "code": "empty_message", "detail": "Message cannot be empty."})
            return
        if len(body) > MAX_CHAT_MESSAGE_CHARS:
            await self.send_json({"type": "error", "code": "message_too_long", "detail": f"Message cannot exceed {MAX_CHAT_MESSAGE_CHARS} characters."})
            return

        user = self.scope["user"]
        result = await self._create_message(self.order_id, user.id, body)
        if result["error"] == "not_found":
            await self.close(code=4404)
            return
        if result["error"] == "read_only":
            await self.send_json({"type": "error", "code": "read_only", "detail": "This P2P conversation is read-only.", "order_status": result["status"], "can_send": False})
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
                {"type": "p2p.message", "message": message, "client_id": client_id},
            )
        except Exception:
            logger.exception("Failed to fan out P2P message %s", result["id"])
            local_message = dict(message)
            local_message["is_mine"] = True
            await self.send_json({"type": "message", "message": local_message, "client_id": client_id})

    async def _handle_order_action(self, action):
        try:
            if action == "sent":
                order = await database_sync_to_async(mark_p2p_fiat_sent)(
                    order_id=self.order_id,
                    user_id=self.scope["user"].id,
                )
            else:
                order = await database_sync_to_async(mark_p2p_fiat_received)(
                    order_id=self.order_id,
                    user_id=self.scope["user"].id,
                )
        except (ValidationError, PermissionDenied) as exc:
            await self.send_json({"type": "error", "code": "invalid_action", "detail": str(exc)})
            return
        await self.send_json({"type": "action_ack", "order_status": order.status})

    async def p2p_message(self, event):
        message = dict(event["message"])
        message["is_mine"] = message.get("sender_id") == self.scope["user"].id
        await self.send_json(
            {
                "type": "message",
                "message": message,
                "client_id": event.get("client_id", "") if message["is_mine"] else "",
            }
        )

    async def p2p_status(self, event):
        status = event["order_status"]
        await self.send_json(
            {
                "type": "status",
                "order_status": status,
                "can_send": bool(event["can_send"]),
                "actions": self._actions_for_status(status),
            }
        )

    @database_sync_to_async
    def _load_order_state(self, public_id, user_id):
        order = P2POrder.objects.select_related("maker__user").filter(public_id=public_id).first()
        if order is None:
            return None
        if user_id == order.buyer_id:
            role = "buyer"
        elif order.maker_id and order.maker.user_id == user_id and order.chat_started:
            role = "agent"
        else:
            return None
        return {
            "order_id": order.id,
            "public_id": str(order.public_id),
            "status": order.status,
            "can_send": order.chat_writable,
            "chat_started": order.chat_started,
            "role": role,
        }

    @database_sync_to_async
    def _load_initial_messages(self, order_id, user_id, chat_started):
        if not chat_started:
            return []
        order = P2POrder.objects.select_related("maker__user").filter(id=order_id).first()
        if order is None:
            return []
        allowed = user_id == order.buyer_id or (
            order.maker_id and order.maker.user_id == user_id and order.chat_started
        )
        if not allowed:
            return []
        messages = list(
            P2PMessage.objects.filter(order_id=order_id).select_related("sender").order_by("-id")[:MAX_INITIAL_MESSAGES]
        )
        messages.reverse()
        return [_serialize_message(message, user_id=user_id) for message in messages]

    @database_sync_to_async
    def _create_message(self, order_id, user_id, body):
        order = P2POrder.objects.select_related("maker__user").filter(id=order_id).first()
        if order is None:
            return {"error": "not_found"}
        allowed = user_id == order.buyer_id or (
            order.maker_id and order.maker.user.id == user_id and order.chat_started
        )
        if not allowed:
            return {"error": "not_found"}
        if not order.chat_writable:
            return {"error": "read_only", "status": order.status}
        message = P2PMessage.objects.create(order=order, sender_id=user_id, kind=P2PMessage.KIND_USER, body=body)
        return {"error": None, "id": int(message.id), "created_at": message.created_at.isoformat()}
