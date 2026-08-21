
from __future__ import annotations

import json
import logging

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.exceptions import (
    ImproperlyConfigured,
    ObjectDoesNotExist,
    PermissionDenied,
    ValidationError,
)
from django.http import FileResponse, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .internal_auth import authenticate_ai_generation_service
from .models import AIGenerationRequest
from .services import (
    ai_generation_available,
    claim_next_generation,
    complete_generation_from_url,
    create_generation_request,
    download_provider_image,
    format_token_amount,
    generation_price_tokens,
    generation_provider_payload,
    get_user_wallet,
    heartbeat_generation,
    fail_generation,
    serialize_generation,
)

logger = logging.getLogger(__name__)


def _json_body(request) -> dict:
    try:
        payload = json.loads((request.body or b"{}").decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("Request body must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValidationError("Request body must be a JSON object")
    return payload


def _validation_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError) and getattr(exc, "messages", None):
        return str(exc.messages[0])
    return str(exc)


def _internal_error_response(exc: Exception):
    if isinstance(exc, ObjectDoesNotExist):
        return JsonResponse({"success": False, "error": "Generation not found"}, status=404)
    if isinstance(exc, PermissionDenied):
        logger.warning("Permission denied in internal AI generation endpoint", exc_info=exc)
        return JsonResponse({"success": False, "error": "Permission denied"}, status=403)
    if isinstance(exc, ImproperlyConfigured):
        return JsonResponse(
            {"success": False, "error": "Internal AI service is not configured"},
            status=503,
        )
    if isinstance(exc, ValidationError):
        return JsonResponse(
            {"success": False, "error": _validation_message(exc)},
            status=400,
        )
    raise exc


@login_required
@require_GET
def generation_page(request):
    wallet = get_user_wallet(request.user)
    return render(
        request,
        "ai_generation/generate.html",
        {
            "ai_generation_price": format_token_amount(generation_price_tokens()),
            "ai_generation_balance": format_token_amount(wallet.balance),
            "ai_generation_max_prompt_chars": int(
                getattr(settings, "AI_GENERATION_MAX_PROMPT_CHARS", 1200)
            ),
            "ai_generation_enabled": ai_generation_available(),
            "ai_generation_default_resolution": str(
                getattr(settings, "AI_GENERATION_PROVIDER_RESOLUTION", "512x768")
            ),
        },
    )


@login_required
@require_POST
def generation_create_api(request):
    try:
        payload = _json_body(request)
        generation = create_generation_request(
            actor=request.user,
            prompt=payload.get("prompt", ""),
            resolution=payload.get("resolution", ""),
        )
        wallet = get_user_wallet(request.user)
        wallet.refresh_from_db(fields=["balance"])
        return JsonResponse(
            {
                "success": True,
                "generation": serialize_generation(generation, request=request),
                "balance_tokens": int(wallet.balance),
                "balance_display": format_token_amount(wallet.balance),
            },
            status=201,
        )
    except ValidationError:
        return JsonResponse(
            {
                "success": False,
                "error": "Invalid request payload",
            },
            status=400,
        )


@login_required
@require_GET
def generation_list_api(request):
    generations = AIGenerationRequest.objects.filter(user=request.user).order_by(
        "-created_at"
    )[:50]
    return JsonResponse(
        {
            "success": True,
            "generations": [
                serialize_generation(item, request=request)
                for item in generations
            ],
        }
    )


@login_required
@require_GET
def generation_detail_api(request, public_id):
    generation = get_object_or_404(
        AIGenerationRequest,
        public_id=public_id,
        user=request.user,
    )
    return JsonResponse(
        {
            "success": True,
            "generation": serialize_generation(generation, request=request),
        }
    )


@login_required
@require_GET
def generation_image(request, public_id):
    generation = get_object_or_404(
        AIGenerationRequest,
        public_id=public_id,
        user=request.user,
        status=AIGenerationRequest.STATUS_SUCCESS,
    )

    provider_result_url = ""
    if isinstance(generation.result_metadata, dict):
        provider_result_url = str(
            generation.result_metadata.get("image_download_url", "") or ""
        ).strip()

    if provider_result_url:
        try:
            image_bytes, content_type, extension = download_provider_image(
                provider_result_url
            )
        except ValidationError as exc:
            return JsonResponse(
                {
                    "success": False,
                    "error": _validation_message(exc),
                },
                status=502,
            )

        response = HttpResponse(
            image_bytes,
            content_type=content_type,
        )
        response["Content-Disposition"] = (
            f'inline; filename="{generation.public_id}.{extension}"'
        )
        response["Content-Length"] = str(len(image_bytes))
        response["Cache-Control"] = "private, no-store"
        return response

    # Compatibility for pre-diskless generations that already have a local
    # result_file. New n8n/Perchance generations never enter this branch.
    if generation.result_file:
        handle = generation.result_file.open("rb")
        response = FileResponse(
            handle,
            content_type=(
                generation.result_content_type
                or "application/octet-stream"
            ),
        )
        response["Content-Disposition"] = (
            f'inline; filename="{generation.public_id}"'
        )
        response["Cache-Control"] = "private, no-store"
        return response

    return JsonResponse(
        {"success": False, "error": "Generated image is unavailable"},
        status=404,
    )


@csrf_exempt
@require_POST
def internal_generation_claim(request):
    try:
        _actor, _payload, service_name = authenticate_ai_generation_service(request)
        generation = claim_next_generation(service_name=service_name)
        return JsonResponse(
            {
                "success": True,
                "job": (
                    generation_provider_payload(generation)
                    if generation is not None
                    else None
                ),
            }
        )
    except Exception as exc:
        return _internal_error_response(exc)


@csrf_exempt
@require_POST
def internal_generation_heartbeat(request, public_id):
    try:
        _actor, payload, service_name = authenticate_ai_generation_service(request)
        generation = heartbeat_generation(
            public_id=public_id,
            service_name=service_name,
            claim_token=str(payload.get("claim_token", "") or ""),
        )
        return JsonResponse(
            {
                "success": True,
                "claim_expires_at": generation.claim_expires_at.isoformat(),
            }
        )
    except Exception as exc:
        return _internal_error_response(exc)


@csrf_exempt
@require_POST
def internal_generation_success(request, public_id):
    try:
        _actor, payload, service_name = authenticate_ai_generation_service(request)
        claim_token = str(payload.get("claim_token", "") or "")
        provider_request_id = str(
            payload.get("provider_request_id", "") or ""
        )
        provider_metadata = (
            payload.get("provider_metadata")
            if isinstance(payload.get("provider_metadata"), dict)
            else {}
        )

        if str(payload.get("image_base64", "") or "").strip():
            raise ValidationError(
                "Inline image results are no longer supported; provide a result URL"
            )

        result_url = str(payload.get("result_url", "") or "").strip()
        if not result_url:
            raise ValidationError("Provider result URL is required")

        generation = complete_generation_from_url(
            public_id=public_id,
            service_name=service_name,
            claim_token=claim_token,
            result_url=result_url,
            provider_request_id=provider_request_id,
            provider_metadata=provider_metadata,
        )
        return JsonResponse(
            {
                "success": True,
                "generation_id": str(generation.public_id),
                "status": generation.status,
            }
        )
    except Exception as exc:
        return _internal_error_response(exc)


@csrf_exempt
@require_POST
def internal_generation_failed(request, public_id):
    try:
        _actor, payload, service_name = authenticate_ai_generation_service(request)
        generation = fail_generation(
            public_id=public_id,
            service_name=service_name,
            claim_token=str(payload.get("claim_token", "") or ""),
            error_code=str(
                payload.get("error_code", "provider_failed") or "provider_failed"
            ),
            error_message=str(payload.get("error_message", "") or ""),
        )
        return JsonResponse(
            {
                "success": True,
                "generation_id": str(generation.public_id),
                "status": generation.status,
            }
        )
    except Exception as exc:
        return _internal_error_response(exc)
