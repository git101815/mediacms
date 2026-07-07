from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET

from ledger.paygate_deposits import process_paygate_callback


@csrf_exempt
@require_GET
def paygate_callback(request):
    payload = {key: value for key, value in request.GET.items()}

    try:
        result = process_paygate_callback(payload)
    except PermissionDenied as exc:
        return JsonResponse({"status": "error", "detail": str(exc)}, status=401)
    except ImproperlyConfigured as exc:
        return JsonResponse({"status": "error", "detail": str(exc)}, status=500)
    except ValidationError as exc:
        message = exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc)
        return JsonResponse({"status": "error", "detail": message}, status=400)

    return JsonResponse({"status": "ok", **result})