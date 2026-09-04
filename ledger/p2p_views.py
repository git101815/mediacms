from __future__ import annotations

import json
from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.db.models import Q
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from .models import P2PMessage, P2POrder


PLATFORM_VALUE_SCALE = Decimal("1000000")
MAX_CHAT_MESSAGE_CHARS = 4000
MAX_MESSAGES_PER_POLL = 100


def _format_platform_value(value: int) -> str:
    amount = Decimal(int(value or 0)) / PLATFORM_VALUE_SCALE
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _order_for_user(*, public_id, user) -> P2POrder:
    try:
        return (
            P2POrder.objects.select_related("buyer", "maker__user")
            .get(
                Q(buyer=user) | Q(maker__user=user),
                public_id=public_id,
            )
        )
    except P2POrder.DoesNotExist as exc:
        # Do not disclose whether a private P2P order exists to outsiders.
        raise Http404 from exc


def _serialize_message(message: P2PMessage, *, user_id: int) -> dict:
    sender = message.sender
    return {
        "id": int(message.id),
        "kind": message.kind,
        "body": message.body,
        "created_at": message.created_at.isoformat(),
        "sender_name": str(sender) if sender is not None else "System",
        "is_mine": bool(sender is not None and sender.id == user_id),
    }


@login_required
@ensure_csrf_cookie
@require_GET
def p2p_exchange(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    is_buyer = order.buyer_id == request.user.id
    counterparty = order.maker.user if is_buyer else order.buyer

    context = {
        "p2p_order": order,
        "p2p_role": "buyer" if is_buyer else "maker",
        "p2p_counterparty": counterparty,
        "p2p_order_reference": str(order.public_id).split("-")[0].upper(),
        "p2p_platform_amount_display": _format_platform_value(order.platform_amount),
        "p2p_payment_method_display": order.get_payment_method_display(),
        "p2p_can_send": order.chat_writable,
        "p2p_maker_profile": order.maker,
    }
    return render(request, "cms/p2p_exchange.html", context)


@login_required
@require_GET
def p2p_exchange_messages(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    raw_after_id = (request.GET.get("after_id") or "0").strip()
    try:
        after_id = max(0, int(raw_after_id))
    except (TypeError, ValueError):
        return JsonResponse({"detail": "Invalid after_id."}, status=400)

    messages = list(
        order.messages.select_related("sender")
        .filter(id__gt=after_id)
        .order_by("id")[:MAX_MESSAGES_PER_POLL]
    )
    return JsonResponse(
        {
            "messages": [
                _serialize_message(message, user_id=request.user.id)
                for message in messages
            ],
            "order_status": order.status,
            "can_send": order.chat_writable,
        }
    )


@login_required
@require_POST
def p2p_exchange_send_message(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    if not order.chat_writable:
        return JsonResponse(
            {"detail": "This P2P conversation is read-only."},
            status=409,
        )

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"detail": "Invalid JSON body."}, status=400)

    body = str(payload.get("message") or "").strip()
    if not body:
        return JsonResponse({"detail": "Message cannot be empty."}, status=400)
    if len(body) > MAX_CHAT_MESSAGE_CHARS:
        return JsonResponse(
            {"detail": f"Message cannot exceed {MAX_CHAT_MESSAGE_CHARS} characters."},
            status=400,
        )

    message = P2PMessage.objects.create(
        order=order,
        sender=request.user,
        kind=P2PMessage.KIND_USER,
        body=body,
    )
    return JsonResponse(
        {"message": _serialize_message(message, user_id=request.user.id)},
        status=201,
    )
