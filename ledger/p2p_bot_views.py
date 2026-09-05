from __future__ import annotations

import hmac
import json

from django.conf import settings
from django.contrib.auth import authenticate
from django.core.cache import cache
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import HttpResponse, JsonResponse
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .models import P2PMakerProfile
from .p2p_services import respond_to_p2p_agent_offer


def _configured_n8n_action_secret() -> str:
    return str(getattr(settings, "P2P_N8N_ACTION_SECRET", "") or "").strip()


def _supplied_n8n_action_secret(request) -> str:
    return str(request.headers.get("X-P2P-Action-Secret") or "").strip()


_TELEGRAM_AUTH_TTL_SECONDS = 10 * 60
_TELEGRAM_AUTH_FAILURE_WINDOW_SECONDS = 15 * 60
_TELEGRAM_AUTH_MAX_FAILURES = 5


def _telegram_auth_state_key(telegram_user_id: str) -> str:
    return f"p2p:telegram-auth:state:{telegram_user_id}"


def _telegram_auth_failures_key(telegram_user_id: str) -> str:
    return f"p2p:telegram-auth:failures:{telegram_user_id}"


def _parse_json_body(request):
    try:
        return json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _require_n8n_action_secret(request):
    configured_secret = _configured_n8n_action_secret()
    if not configured_secret:
        return HttpResponse(status=503)
    if not hmac.compare_digest(
        configured_secret,
        _supplied_n8n_action_secret(request),
    ):
        return HttpResponse(status=403)
    return None


def _normalize_telegram_identity(payload):
    telegram_user_id = str(payload.get("telegram_user_id") or "").strip()
    telegram_chat_id = str(payload.get("telegram_chat_id") or "").strip()

    if (
        not telegram_user_id
        or not telegram_chat_id
        or not telegram_user_id.isdigit()
        or not telegram_chat_id.isdigit()
        or telegram_user_id != telegram_chat_id
    ):
        raise ValidationError("P2P bot sign-in must be performed in a private Telegram chat.")

    return telegram_user_id, telegram_chat_id


@csrf_exempt
@require_POST
def p2p_n8n_telegram_auth(request):
    # Authenticate a P2P agent through the dedicated Telegram/n8n workflow.
    # Conversation state is kept server-side in Redis. The password is never stored.
    secret_error = _require_n8n_action_secret(request)
    if secret_error is not None:
        return secret_error

    payload = _parse_json_body(request)
    if payload is None:
        return JsonResponse({"detail": "Invalid JSON body."}, status=400)

    try:
        telegram_user_id, _telegram_chat_id = _normalize_telegram_identity(payload)
    except ValidationError as exc:
        return JsonResponse({"detail": str(exc)}, status=400)

    action = str(payload.get("action") or "").strip().lower()
    incoming_text = str(payload.get("text") or "")
    state_key = _telegram_auth_state_key(telegram_user_id)
    failures_key = _telegram_auth_failures_key(telegram_user_id)

    if action == "start":
        cache.set(
            state_key,
            {"stage": "username"},
            timeout=_TELEGRAM_AUTH_TTL_SECONDS,
        )
        return JsonResponse(
            {
                "ok": True,
                "state": "need_username",
                "message": "Send your MediaCMS username.",
                "sensitive_input": False,
            }
        )

    if action != "input":
        return JsonResponse({"detail": "Invalid Telegram auth action."}, status=400)

    state = cache.get(state_key)
    if not isinstance(state, dict):
        return JsonResponse(
            {
                "ok": False,
                "state": "need_start",
                "message": "Send /start to sign in.",
                "sensitive_input": False,
            }
        )

    stage = str(state.get("stage") or "")
    if stage == "username":
        username = incoming_text.strip()
        if not username or len(username) > 150:
            return JsonResponse(
                {
                    "ok": False,
                    "state": "need_username",
                    "message": "Send a valid MediaCMS username.",
                    "sensitive_input": False,
                }
            )

        cache.set(
            state_key,
            {"stage": "password", "username": username},
            timeout=_TELEGRAM_AUTH_TTL_SECONDS,
        )
        return JsonResponse(
            {
                "ok": True,
                "state": "need_password",
                "message": "Send your MediaCMS password.",
                "sensitive_input": False,
            }
        )

    if stage != "password":
        cache.delete(state_key)
        return JsonResponse(
            {
                "ok": False,
                "state": "need_start",
                "message": "Send /start to sign in.",
                "sensitive_input": False,
            }
        )

    username = str(state.get("username") or "")
    failures = int(cache.get(failures_key) or 0)
    if failures >= _TELEGRAM_AUTH_MAX_FAILURES:
        cache.delete(state_key)
        return JsonResponse(
            {
                "ok": False,
                "state": "rate_limited",
                "message": "Too many failed attempts. Try again in 15 minutes.",
                "sensitive_input": True,
                "retry_after_seconds": _TELEGRAM_AUTH_FAILURE_WINDOW_SECONDS,
            }
        )

    password = incoming_text
    user = authenticate(request=None, username=username, password=password)
    if user is None:
        failures += 1
        cache.set(
            failures_key,
            failures,
            timeout=_TELEGRAM_AUTH_FAILURE_WINDOW_SECONDS,
        )
        if failures >= _TELEGRAM_AUTH_MAX_FAILURES:
            cache.delete(state_key)
            message = "Too many failed attempts. Try again in 15 minutes."
            state_name = "rate_limited"
        else:
            message = "Invalid username or password. Send your password again."
            state_name = "invalid_credentials"

        return JsonResponse(
            {
                "ok": False,
                "state": state_name,
                "message": message,
                "sensitive_input": True,
            }
        )

    profile = P2PMakerProfile.objects.filter(user=user).first()
    if profile is None:
        cache.delete(state_key)
        cache.delete(failures_key)
        return JsonResponse(
            {
                "ok": False,
                "state": "not_p2p_agent",
                "message": "This MediaCMS account is not registered as a P2P agent.",
                "sensitive_input": True,
            }
        )

    conflict = (
        P2PMakerProfile.objects.exclude(pk=profile.pk)
        .filter(telegram_user_id=telegram_user_id)
        .exists()
    )
    if conflict:
        cache.delete(state_key)
        cache.delete(failures_key)
        return JsonResponse(
            {
                "ok": False,
                "state": "telegram_already_linked",
                "message": "This Telegram account is already linked to another P2P agent.",
                "sensitive_input": True,
            }
        )

    if profile.telegram_user_id != telegram_user_id:
        profile.telegram_user_id = telegram_user_id
        profile.save(update_fields=["telegram_user_id", "updated_at"])

    cache.delete(state_key)
    cache.delete(failures_key)
    return JsonResponse(
        {
            "ok": True,
            "state": "authenticated",
            "username": user.username,
            "message": f"Hello {user.username}",
            "sensitive_input": True,
        }
    )


@csrf_exempt
@require_POST
def p2p_n8n_agent_response(request):
    secret_error = _require_n8n_action_secret(request)
    if secret_error is not None:
        return secret_error

    payload = _parse_json_body(request)
    if payload is None:
        return JsonResponse({"detail": "Invalid JSON body."}, status=400)

    action_token = str(payload.get("action_token") or "").strip()
    action = str(payload.get("action") or "").strip().lower()
    channel = str(payload.get("channel") or "").strip().lower()
    external_user_id = payload.get("external_user_id")
    if not action_token or action not in {"accept", "decline"}:
        return JsonResponse({"detail": "Invalid P2P agent action."}, status=400)
    if channel not in {"telegram", "discord"}:
        return JsonResponse({"detail": "Invalid P2P notification channel."}, status=400)

    try:
        state, order = respond_to_p2p_agent_offer(
            action_token=action_token,
            action=action,
            channel=channel,
            external_user_id=external_user_id,
        )
    except PermissionDenied as exc:
        return JsonResponse({"detail": str(exc)}, status=403)
    except ValidationError as exc:
        return JsonResponse(
            {
                "ok": False,
                "state": "error",
                "message": str(exc),
            }
        )

    response = {
        "ok": True,
        "state": state,
        "order_public_id": str(order.public_id),
    }
    if state == "accepted":
        response["message"] = "Accepted. The private conversation is now open."
        response["order_path"] = reverse("p2p_exchange", kwargs={"public_id": order.public_id})
        response["order_url"] = request.build_absolute_uri(response["order_path"])
    elif state == "declined":
        response["message"] = "Declined."
    elif state == "expired":
        response["message"] = "This P2P offer has expired."
    else:
        response["message"] = f"This P2P offer is already {state}."
    return JsonResponse(response)
