
from __future__ import annotations

import json

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.exceptions import (
    ImproperlyConfigured,
    ObjectDoesNotExist,
    PermissionDenied,
    ValidationError,
)
from django.http import FileResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .internal_auth import authenticate_ai_generation_service
from .models import AIGenerationRequest
from .services import (
    claim_next_generation,
    complete_generation,
    complete_generation_from_url,
    create_generation_request,
    decode_provider_image_base64,
    format_token_amount,
    generation_price_tokens,
    generation_provider_payload,
    get_user_wallet,
    heartbeat_generation,
    fail_generation,
    serialize_generation,
    setting_enabled,
)


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
        return JsonResponse({"success": False, "error": str(exc)}, status=403)
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
    generations = AIGenerationRequest.objects.filter(user=request.user).order_by(
        "-created_at"
    )[:20]
    return render(
        request,
        "ai_generation/generate.html",
        {
            "ai_generation_price": format_token_amount(generation_price_tokens()),
            "ai_generation_balance": format_token_amount(wallet.balance),
            "ai_generation_max_prompt_chars": int(
                getattr(settings, "AI_GENERATION_MAX_PROMPT_CHARS", 1200)
            ),
            "ai_generation_enabled": setting_enabled(
                "AI_GENERATION_ENABLED",
                True,
            ),
            "ai_generation_initial": [
                serialize_generation(item, request=request)
                for item in generations
            ],
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
    except ValidationError as exc:
        return JsonResponse(
            {
                "success": False,
                "error": _validation_message(exc),
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
    if not generation.result_file:
        return JsonResponse(
            {"success": False, "error": "Generated image is unavailable"},
            status=404,
        )

    handle = generation.result_file.open("rb")
    response = FileResponse(
        handle,
        content_type=generation.result_content_type or "application/octet-stream",
    )
    response["Content-Disposition"] = (
        f'inline; filename="{generation.public_id}"'
    )
    response["Cache-Control"] = "private, max-age=3600"
    return response


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

        image_base64 = str(payload.get("image_base64", "") or "").strip()
        if image_base64:
            image_bytes, extension = decode_provider_image_base64(
                image_base64,
                str(payload.get("content_type", "") or ""),
            )
            generation = complete_generation(
                public_id=public_id,
                service_name=service_name,
                claim_token=claim_token,
                image_bytes=image_bytes,
                content_type=str(payload.get("content_type", "") or ""),
                extension=extension,
                provider_request_id=provider_request_id,
                provider_metadata=provider_metadata,
            )
        else:
            generation = complete_generation_from_url(
                public_id=public_id,
                service_name=service_name,
                claim_token=claim_token,
                result_url=str(payload.get("result_url", "") or ""),
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
