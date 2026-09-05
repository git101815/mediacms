from __future__ import annotations

import hmac
import json
import os

from django.conf import settings
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import HttpResponse, JsonResponse
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .p2p_services import respond_to_p2p_agent_offer


def _configured_n8n_action_secret() -> str:
    configured = str(getattr(settings, "P2P_N8N_ACTION_SECRET", "") or "").strip()
    if configured:
        return configured
    return str(os.environ.get("NOTIFICATION_WEBHOOK_SECRET", "") or "").strip()


def _supplied_n8n_action_secret(request) -> str:
    return str(
        request.headers.get("X-P2P-Action-Secret")
        or request.headers.get("X-Notification-Secret")
        or ""
    ).strip()


@csrf_exempt
@require_POST
def p2p_n8n_agent_response(request):
    configured_secret = _configured_n8n_action_secret()
    if not configured_secret:
        return HttpResponse(status=503)
    if not hmac.compare_digest(configured_secret, _supplied_n8n_action_secret(request)):
        return HttpResponse(status=403)

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
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
        return JsonResponse({"detail": str(exc)}, status=400)

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
