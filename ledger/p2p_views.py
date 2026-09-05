
from __future__ import annotations

import json
from decimal import Decimal, ROUND_HALF_UP

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import Http404, JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from .models import P2PMessage, P2POrder, P2PReview
from .p2p_services import cancel_p2p_order, find_new_p2p_agent, submit_p2p_review


PLATFORM_VALUE_SCALE = Decimal("1000000")
MAX_CHAT_MESSAGE_CHARS = 4000
MAX_MESSAGES_PER_POLL = 100


def _format_platform_value(value: int) -> str:
    amount = Decimal(int(value or 0)) / PLATFORM_VALUE_SCALE
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _rounded_rating(value) -> Decimal | None:
    if value in (None, ""):
        return None
    rating = Decimal(str(value))
    rating = min(Decimal("5"), max(Decimal("0"), rating))
    return (rating * 2).quantize(Decimal("1"), rounding=ROUND_HALF_UP) / 2


def _rating_stars(value) -> list[str]:
    rounded = _rounded_rating(value)
    if rounded is None:
        return []
    stars = []
    remaining = rounded
    for _ in range(5):
        if remaining >= 1:
            stars.append("full")
        elif remaining >= Decimal("0.5"):
            stars.append("half")
        else:
            stars.append("empty")
        remaining -= 1
    return stars


def _order_for_user(*, public_id, user) -> P2POrder:
    order = (
        P2POrder.objects.select_related("buyer", "maker__user", "token_pack")
        .filter(public_id=public_id)
        .first()
    )
    if order is None:
        raise Http404
    if order.buyer_id == user.id:
        return order
    if (
        order.maker_id
        and order.maker.user_id == user.id
        and order.chat_started
    ):
        return order
    # An offered-but-not-accepted agent gets no MediaCMS room access. Their
    # only controls are the Accept/Decline buttons in Telegram/Discord.
    raise Http404


def _serialize_message(message: P2PMessage, *, user_id: int) -> dict:
    sender = message.sender
    return {
        "id": int(message.id),
        "kind": message.kind,
        "body": message.body,
        "created_at": message.created_at.isoformat(),
        "sender_name": sender.username if sender is not None else "System",
        "is_mine": bool(sender is not None and sender.id == user_id),
    }


@login_required
@ensure_csrf_cookie
@require_GET
def p2p_exchange(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    is_buyer = order.buyer_id == request.user.id
    counterparty = None
    if is_buyer:
        if order.maker_id:
            counterparty = order.maker.user
    else:
        counterparty = order.buyer

    maker_profile = order.maker if order.maker_id else None
    existing_review = None
    if order.status == P2POrder.STATUS_COMPLETED:
        existing_review = P2PReview.objects.filter(order=order, reviewer=request.user).first()

    context = {
        "p2p_order": order,
        "p2p_role": "buyer" if is_buyer else "agent",
        "p2p_counterparty": counterparty,
        "p2p_order_reference": str(order.public_id).split("-")[0].upper(),
        "p2p_platform_amount_display": _format_platform_value(order.platform_amount),
        "p2p_base_amount_display": _format_platform_value(order.base_amount),
        "p2p_commission_amount_display": _format_platform_value(order.commission_amount),
        "p2p_token_amount_display": _format_platform_value(order.token_amount),
        "p2p_payment_method_display": order.get_payment_method_display(),
        "p2p_can_send": order.chat_writable,
        "p2p_chat_started": order.chat_started,
        "p2p_is_pretrade": not order.chat_started,
        "p2p_can_find_new_agent": is_buyer and order.status == P2POrder.STATUS_WAITING_NEW_AGENT,
        "p2p_can_cancel": is_buyer and order.status in {
            P2POrder.STATUS_WAITING_AGENT,
            P2POrder.STATUS_WAITING_NEW_AGENT,
            P2POrder.STATUS_NO_AGENT_AVAILABLE,
        },
        "p2p_maker_profile": maker_profile,
        "p2p_rating_value": _rounded_rating(maker_profile.rating) if maker_profile else None,
        "p2p_rating_stars": _rating_stars(maker_profile.rating) if maker_profile else [],
        "p2p_existing_review": existing_review,
        "p2p_review_target": counterparty,
    }
    return render(request, "cms/p2p_exchange.html", context)


@login_required
@require_POST
def p2p_exchange_find_agent(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    try:
        find_new_p2p_agent(order_id=order.id, buyer_id=request.user.id)
    except (ValidationError, PermissionDenied) as exc:
        messages.error(request, str(exc))
    return redirect("p2p_exchange", public_id=public_id)


@login_required
@require_POST
def p2p_exchange_cancel(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    try:
        cancel_p2p_order(order_id=order.id, buyer_id=request.user.id)
    except (ValidationError, PermissionDenied) as exc:
        messages.error(request, str(exc))
    return redirect("p2p_exchange", public_id=public_id)


@login_required
@require_POST
def p2p_exchange_review(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    try:
        submit_p2p_review(
            order_id=order.id,
            reviewer_id=request.user.id,
            ratings={
                "communication": request.POST.get("communication"),
                "responsiveness": request.POST.get("responsiveness"),
                "reliability": request.POST.get("reliability"),
                "payment_experience": request.POST.get("payment_experience"),
                "cooperation": request.POST.get("cooperation"),
            },
        )
        messages.success(request, "Review submitted.")
    except (ValidationError, PermissionDenied) as exc:
        messages.error(request, str(exc))
    return redirect("p2p_exchange", public_id=public_id)


@login_required
@require_GET
def p2p_exchange_messages(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    if not order.chat_started:
        return JsonResponse({"messages": [], "order_status": order.status, "can_send": False})
    raw_after_id = (request.GET.get("after_id") or "0").strip()
    try:
        after_id = max(0, int(raw_after_id))
    except (TypeError, ValueError):
        return JsonResponse({"detail": "Invalid after_id."}, status=400)

    messages_qs = list(
        order.messages.select_related("sender").filter(id__gt=after_id).order_by("id")[:MAX_MESSAGES_PER_POLL]
    )
    return JsonResponse(
        {
            "messages": [_serialize_message(message, user_id=request.user.id) for message in messages_qs],
            "order_status": order.status,
            "can_send": order.chat_writable,
        }
    )


@login_required
@require_POST
def p2p_exchange_send_message(request, public_id):
    order = _order_for_user(public_id=public_id, user=request.user)
    if not order.chat_writable:
        return JsonResponse({"detail": "This P2P conversation is read-only."}, status=409)
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"detail": "Invalid JSON body."}, status=400)
    body = str(payload.get("message") or "").strip()
    if not body:
        return JsonResponse({"detail": "Message cannot be empty."}, status=400)
    if len(body) > MAX_CHAT_MESSAGE_CHARS:
        return JsonResponse({"detail": f"Message cannot exceed {MAX_CHAT_MESSAGE_CHARS} characters."}, status=400)
    message = P2PMessage.objects.create(order=order, sender=request.user, kind=P2PMessage.KIND_USER, body=body)
    return JsonResponse({"message": _serialize_message(message, user_id=request.user.id)}, status=201)
