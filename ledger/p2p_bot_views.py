
from __future__ import annotations

import hmac
import json
import re

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

from .p2p_services import respond_to_p2p_agent_offer
from .p2p_tasks import (
    p2p_order_url,
    telegram_answer_callback,
    telegram_remove_buttons,
    telegram_send_chat_link,
)


ACTION_RE = re.compile(r"^p2p:(?P<action>[ad]):(?P<token>[0-9a-f]{32})$")


def _parse_action(value: str):
    match = ACTION_RE.match(str(value or "").strip())
    if not match:
        return None, None
    return ("accept" if match.group("action") == "a" else "decline"), match.group("token")


@csrf_exempt
@require_POST
def p2p_telegram_webhook(request):
    configured_secret = str(getattr(settings, "P2P_TELEGRAM_WEBHOOK_SECRET", "") or "").strip()
    if not configured_secret:
        return HttpResponse(status=503)
    supplied_secret = str(request.headers.get("X-Telegram-Bot-Api-Secret-Token", "") or "")
    if not hmac.compare_digest(configured_secret, supplied_secret):
        return HttpResponse(status=403)

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return HttpResponse(status=400)

    callback = payload.get("callback_query") or {}
    action, action_token = _parse_action(callback.get("data"))
    if not action:
        return JsonResponse({"ok": True})
    external_user_id = (callback.get("from") or {}).get("id")
    callback_id = str(callback.get("id") or "")
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")

    try:
        state, order = respond_to_p2p_agent_offer(
            action_token=action_token,
            action=action,
            channel="telegram",
            external_user_id=external_user_id,
        )
    except PermissionDenied:
        telegram_answer_callback(callback_id, "This Telegram account is not authorized for that P2P offer.")
        return JsonResponse({"ok": True})
    except ValidationError as exc:
        telegram_answer_callback(callback_id, str(exc))
        return JsonResponse({"ok": True})

    if state == "accepted":
        telegram_answer_callback(callback_id, "Accepted. Your tokens were funded and the private conversation is open.")
        if chat_id is not None and message_id is not None:
            telegram_remove_buttons(chat_id, message_id)
        telegram_send_chat_link(chat_id or external_user_id, order)
    elif state == "declined":
        telegram_answer_callback(callback_id, "Declined.")
        if chat_id is not None and message_id is not None:
            telegram_remove_buttons(chat_id, message_id)
    elif state == "expired":
        telegram_answer_callback(callback_id, "This P2P offer has expired.")
        if chat_id is not None and message_id is not None:
            telegram_remove_buttons(chat_id, message_id)
    else:
        telegram_answer_callback(callback_id, f"This offer is already {state}.")
    return JsonResponse({"ok": True})


def _verify_discord_request(request) -> bool:
    public_key = str(getattr(settings, "P2P_DISCORD_PUBLIC_KEY", "") or "").strip()
    signature = str(request.headers.get("X-Signature-Ed25519", "") or "").strip()
    timestamp = str(request.headers.get("X-Signature-Timestamp", "") or "").strip()
    if not public_key or not signature or not timestamp:
        return False
    try:
        VerifyKey(bytes.fromhex(public_key)).verify(
            timestamp.encode("utf-8") + request.body,
            bytes.fromhex(signature),
        )
        return True
    except (ValueError, BadSignatureError):
        return False


def _discord_ephemeral(content: str):
    return JsonResponse({"type": 4, "data": {"content": content[:1900], "flags": 64}})


@csrf_exempt
@require_POST
def p2p_discord_interactions(request):
    if not _verify_discord_request(request):
        return HttpResponse(status=401)
    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return HttpResponse(status=400)

    interaction_type = int(payload.get("type") or 0)
    if interaction_type == 1:  # Discord endpoint verification PING.
        return JsonResponse({"type": 1})
    if interaction_type != 3:
        return _discord_ephemeral("Unsupported P2P bot interaction.")

    action, action_token = _parse_action((payload.get("data") or {}).get("custom_id"))
    if not action:
        return _discord_ephemeral("Unknown P2P action.")
    user = ((payload.get("member") or {}).get("user") or payload.get("user") or {})
    external_user_id = user.get("id")

    try:
        state, order = respond_to_p2p_agent_offer(
            action_token=action_token,
            action=action,
            channel="discord",
            external_user_id=external_user_id,
        )
    except PermissionDenied:
        return _discord_ephemeral("This Discord account is not authorized for that P2P offer.")
    except ValidationError as exc:
        return _discord_ephemeral(str(exc))

    if state == "accepted":
        content = (
            "Accepted. Your tokens were funded and the private conversation is open.\n"
            f"{p2p_order_url(order)}"
        )
    elif state == "declined":
        content = "P2P transaction declined."
    elif state == "expired":
        content = "This P2P offer has expired."
    else:
        content = f"This P2P offer is already {state}."

    # UPDATE_MESSAGE also removes the Accept/Decline buttons from the original DM.
    return JsonResponse({"type": 7, "data": {"content": content, "components": []}})
