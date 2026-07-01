import json

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from ledger.malum import process_malum_webhook


@csrf_exempt
@require_POST
def malum_webhook(request):
    try:
        payload = json.loads((request.body or b"{}").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return JsonResponse({"status": "error", "detail": "invalid_json"}, status=400)

    if not isinstance(payload, dict):
        return JsonResponse({"status": "error", "detail": "invalid_payload"}, status=400)

    try:
        result = process_malum_webhook(payload)
    except PermissionDenied as exc:
        return JsonResponse({"status": "error", "detail": str(exc)}, status=401)
    except ImproperlyConfigured as exc:
        return JsonResponse({"status": "error", "detail": str(exc)}, status=500)
    except ValidationError as exc:
        message = exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc)
        return JsonResponse({"status": "error", "detail": message}, status=400)

    return JsonResponse({"status": "ok", **result})
