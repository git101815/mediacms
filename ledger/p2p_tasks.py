
from __future__ import annotations

import logging

import requests
from celery import shared_task
from django.conf import settings
from django.urls import reverse

from .models import P2PAgentAssignment


logger = logging.getLogger(__name__)


def p2p_order_url(order) -> str:
    base = str(getattr(settings, "SSL_FRONTEND_HOST", "") or getattr(settings, "FRONTEND_HOST", "")).rstrip("/")
    return base + reverse("p2p_exchange", kwargs={"public_id": order.public_id})


def _human_tokens(value: int) -> str:
    amount = int(value or 0) / 1_000_000
    return f"{amount:,.2f}".rstrip("0").rstrip(".")


def _human_usd(value: int) -> str:
    return f"${int(value or 0) / 1_000_000:,.2f}"


def _offer_text(assignment: P2PAgentAssignment) -> str:
    order = assignment.order
    return (
        "New P2P transaction\n\n"
        f"Order: #{str(order.public_id).split('-')[0].upper()}\n"
        f"Customer buys: {_human_tokens(order.token_amount)} tokens\n"
        f"Payment method: {order.get_payment_method_display()}\n"
        f"Transaction value: {_human_usd(order.platform_amount)}\n"
        f"Your commission: {_human_usd(order.commission_amount)}\n"
        f"Required funding to accept: {_human_tokens(order.token_amount)} tokens"
    )


def _telegram_request(method: str, payload: dict):
    token = str(getattr(settings, "P2P_TELEGRAM_BOT_TOKEN", "") or "").strip()
    if not token:
        return None
    response = requests.post(
        f"https://api.telegram.org/bot{token}/{method}",
        json=payload,
        timeout=5,
    )
    response.raise_for_status()
    return response.json()


def telegram_answer_callback(callback_query_id: str, text: str) -> None:
    try:
        _telegram_request(
            "answerCallbackQuery",
            {"callback_query_id": callback_query_id, "text": text[:180], "show_alert": False},
        )
    except requests.RequestException:
        logger.exception("Failed to answer P2P Telegram callback")


def telegram_remove_buttons(chat_id, message_id) -> None:
    try:
        _telegram_request(
            "editMessageReplyMarkup",
            {"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}},
        )
    except requests.RequestException:
        logger.exception("Failed to remove P2P Telegram buttons")


def telegram_send_chat_link(chat_id, order) -> None:
    try:
        _telegram_request(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": f"P2P transaction accepted. Open the private conversation:\n{p2p_order_url(order)}",
                "disable_web_page_preview": True,
            },
        )
    except requests.RequestException:
        logger.exception("Failed to send P2P Telegram chat link")


def _send_telegram_offer(assignment: P2PAgentAssignment) -> bool:
    chat_id = str(assignment.maker.telegram_user_id or "").strip()
    token = str(getattr(settings, "P2P_TELEGRAM_BOT_TOKEN", "") or "").strip()
    if not chat_id or not token:
        return False
    _telegram_request(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": _offer_text(assignment),
            "reply_markup": {
                "inline_keyboard": [[
                    {
                        "text": "Accept",
                        "callback_data": f"p2p:a:{assignment.action_token.hex}",
                    },
                    {
                        "text": "Decline",
                        "callback_data": f"p2p:d:{assignment.action_token.hex}",
                    },
                ]]
            },
        },
    )
    return True


def _discord_headers() -> dict:
    token = str(getattr(settings, "P2P_DISCORD_BOT_TOKEN", "") or "").strip()
    return {"Authorization": f"Bot {token}", "Content-Type": "application/json"}


def _send_discord_offer(assignment: P2PAgentAssignment) -> bool:
    user_id = str(assignment.maker.discord_user_id or "").strip()
    token = str(getattr(settings, "P2P_DISCORD_BOT_TOKEN", "") or "").strip()
    if not user_id or not token:
        return False
    channel_response = requests.post(
        "https://discord.com/api/v10/users/@me/channels",
        headers=_discord_headers(),
        json={"recipient_id": user_id},
        timeout=5,
    )
    channel_response.raise_for_status()
    channel_id = channel_response.json()["id"]
    message_response = requests.post(
        f"https://discord.com/api/v10/channels/{channel_id}/messages",
        headers=_discord_headers(),
        json={
            "content": _offer_text(assignment),
            "components": [
                {
                    "type": 1,
                    "components": [
                        {
                            "type": 2,
                            "style": 3,
                            "label": "Accept",
                            "custom_id": f"p2p:a:{assignment.action_token.hex}",
                        },
                        {
                            "type": 2,
                            "style": 4,
                            "label": "Decline",
                            "custom_id": f"p2p:d:{assignment.action_token.hex}",
                        },
                    ],
                }
            ],
        },
        timeout=5,
    )
    message_response.raise_for_status()
    return True


@shared_task(
    bind=True,
    name="ledger.notify_p2p_agent_offer",
    queue="short_tasks",
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def notify_p2p_agent_offer(self, assignment_id: int):
    if getattr(settings, "TESTING", False):
        return False
    assignment = (
        P2PAgentAssignment.objects.select_related("maker__user", "order")
        .filter(pk=assignment_id, status=P2PAgentAssignment.STATUS_OFFERED)
        .first()
    )
    if assignment is None:
        return False

    delivered = False
    delivered = _send_telegram_offer(assignment) or delivered
    delivered = _send_discord_offer(assignment) or delivered
    if not delivered:
        logger.warning(
            "P2P assignment %s has no configured Telegram/Discord destination",
            assignment.id,
        )
    return delivered


@shared_task(name="ledger.expire_p2p_agent_offer", queue="short_tasks")
def expire_p2p_agent_offer(assignment_id: int):
    from .p2p_services import expire_p2p_agent_assignment

    state, _order, remaining = expire_p2p_agent_assignment(assignment_id=assignment_id)
    if state == "not_yet" and remaining:
        expire_p2p_agent_offer.apply_async(args=[assignment_id], countdown=remaining)
    return state


@shared_task(name="ledger.expire_p2p_trade", queue="short_tasks")
def expire_p2p_trade(order_id: int):
    from .p2p_services import expire_p2p_trade_if_due

    state, _order, remaining = expire_p2p_trade_if_due(order_id=order_id)
    if state == "not_yet" and remaining:
        expire_p2p_trade.apply_async(args=[order_id], countdown=remaining)
    return state
