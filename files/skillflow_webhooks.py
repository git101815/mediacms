import json

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from ledger.providers.skillflow import (
    SKILLFLOW_MAX_WEBHOOK_BODY_BYTES,
    verify_skillflow_webhook_signature,
)
from ledger.skillflow_deposits import process_skillflow_webhook


@csrf_exempt
@require_POST
def skillflow_webhook(request):
    raw_body = request.body or b""
    if len(raw_body) > SKILLFLOW_MAX_WEBHOOK_BODY_BYTES:
        return JsonResponse({"received": False, "error": "payload_too_large"}, status=413)

    try:
        verify_skillflow_webhook_signature(
            raw_body=raw_body,
            signature_header=request.headers.get("x-skillflow-signature", ""),
            timestamp_header=request.headers.get("x-skillflow-timestamp", ""),
        )
    except PermissionDenied:
        return JsonResponse({"received": False, "error": "invalid_signature"}, status=401)
    except ImproperlyConfigured:
        return JsonResponse({"received": False, "error": "configuration_error"}, status=500)

    try:
        payload = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"received": False, "error": "invalid_json"}, status=400)
    if not isinstance(payload, dict):
        return JsonResponse({"received": False, "error": "invalid_payload"}, status=400)

    try:
        process_skillflow_webhook(payload)
    except ImproperlyConfigured:
        return JsonResponse({"received": False, "error": "configuration_error"}, status=500)
    except (PermissionDenied, ValidationError) as exc:
        message = exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc)
        return JsonResponse({"received": False, "error": message}, status=400)

    return JsonResponse({"received": True})
