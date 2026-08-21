import json
import logging

import requests
from celery import shared_task
from django.conf import settings
from django.core.exceptions import ValidationError

from .models import AIGenerationRequest
from .services import (
    claim_next_generation,
    complete_generation_from_url,
    fail_generation,
    fail_stale_generations,
    generation_provider_payload,
)


logger = logging.getLogger(__name__)


def _provider_timeout_seconds() -> int:
    configured = max(
        1,
        int(getattr(settings, "AI_GENERATION_N8N_PROVIDER_TIMEOUT_SECONDS", 240)),
    )
    lease_seconds = max(
        1,
        int(getattr(settings, "AI_GENERATION_CLAIM_LEASE_SECONDS", 300)),
    )

    # The one-way provider flow is diskless after n8n returns. Keep only a
    # small lease margin for validating/finalizing the provider URL.
    safe_max = max(1, lease_seconds - 30)
    return min(configured, safe_max)


def _provider_service_name() -> str:
    return str(
        getattr(
            settings,
            "AI_GENERATION_INTERNAL_SERVICE_USERNAME",
            "ai-generation-service",
        )
        or "ai-generation-service"
    )[:64]


def _provider_error_message(value, default: str) -> str:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        return value[:2000]
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))[:2000]
    except (TypeError, ValueError):
        return str(value)[:2000]


def _validation_message(exc: ValidationError) -> str:
    if getattr(exc, "messages", None):
        return str(exc.messages[0])[:2000]
    return str(exc)[:2000]


def _fail_claimed_generation(
    generation: AIGenerationRequest,
    *,
    service_name: str,
    claim_token: str,
    error_code: str,
    error_message: str,
) -> None:
    try:
        fail_generation(
            public_id=generation.public_id,
            service_name=service_name,
            claim_token=claim_token,
            error_code=error_code,
            error_message=error_message,
        )
    except Exception:
        # If the claim expired while the provider request was in flight, the
        # periodic stale-generation task will close it. Do not let an old task
        # mutate a claim it no longer owns.
        logger.exception(
            "Could not mark AI generation %s as failed",
            generation.public_id,
        )


def _queue_next_generation() -> None:
    if AIGenerationRequest.objects.filter(
        status=AIGenerationRequest.STATUS_QUEUED
    ).exists():
        wake_ai_generation_worker.apply_async(queue="long_tasks")


@shared_task(queue="long_tasks")
def wake_ai_generation_worker(generation_id: str = ""):
    """Claim one job locally, send it to n8n, then finalize it locally."""
    url = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_WEBHOOK_URL", "") or ""
    ).strip()

    service_name = _provider_service_name()
    generation = claim_next_generation(service_name=service_name)
    if generation is None:
        return {"processed": False, "reason": "no_job"}

    claim_token = generation.claim_token
    secret = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_SECRET", "") or ""
    ).strip()

    try:
        if not url or not secret:
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_not_configured",
                error_message="AI generation provider is not configured.",
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_not_configured",
            }

        response = requests.post(
            url,
            json=generation_provider_payload(generation),
            headers={
                "Content-Type": "application/json",
                "X-AI-Generation-Wake-Secret": secret,
            },
            timeout=_provider_timeout_seconds(),
        )
        response.raise_for_status()

        try:
            provider_response = response.json()
        except ValueError:
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_invalid_response",
                error_message="Image generation provider returned invalid JSON.",
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_invalid_response",
            }

        if not isinstance(provider_response, dict):
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_invalid_response",
                error_message="Image generation provider returned an invalid response.",
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_invalid_response",
            }

        if str(provider_response.get("public_id") or "") != str(generation.public_id):
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_invalid_response",
                error_message="Image generation provider returned a mismatched job id.",
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_invalid_response",
            }

        if provider_response.get("ok") is not True:
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_failed",
                error_message=_provider_error_message(
                    provider_response.get("error"),
                    "Image generation provider failed.",
                ),
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_failed",
            }

        result = provider_response.get("result")
        if not isinstance(result, dict):
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_invalid_response",
                error_message="Image generation provider response is missing result metadata.",
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_invalid_response",
            }

        result_url = str(result.get("image_download_url") or "").strip()
        if not result_url:
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_invalid_response",
                error_message="Image generation provider did not return an image URL.",
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_invalid_response",
            }

        try:
            completed = complete_generation_from_url(
                public_id=generation.public_id,
                service_name=service_name,
                claim_token=claim_token,
                result_url=result_url,
                provider_request_id=str(result.get("request_id") or ""),
                provider_metadata=result,
            )
        except ValidationError as exc:
            _fail_claimed_generation(
                generation,
                service_name=service_name,
                claim_token=claim_token,
                error_code="provider_result_error",
                error_message=_validation_message(exc),
            )
            return {
                "processed": True,
                "generation_id": str(generation.public_id),
                "status": "failed",
                "error_code": "provider_result_error",
            }

        return {
            "processed": True,
            "generation_id": str(completed.public_id),
            "status": completed.status,
        }

    except requests.Timeout:
        _fail_claimed_generation(
            generation,
            service_name=service_name,
            claim_token=claim_token,
            error_code="provider_timeout",
            error_message="Image generation provider timed out.",
        )
        return {
            "processed": True,
            "generation_id": str(generation.public_id),
            "status": "failed",
            "error_code": "provider_timeout",
        }
    except requests.RequestException:
        logger.exception(
            "Could not contact n8n for AI generation %s",
            generation.public_id,
        )
        _fail_claimed_generation(
            generation,
            service_name=service_name,
            claim_token=claim_token,
            error_code="provider_http_error",
            error_message="Could not contact image generation provider.",
        )
        return {
            "processed": True,
            "generation_id": str(generation.public_id),
            "status": "failed",
            "error_code": "provider_http_error",
        }
    except Exception:
        logger.exception(
            "Unexpected AI generation provider error for %s",
            generation.public_id,
        )
        _fail_claimed_generation(
            generation,
            service_name=service_name,
            claim_token=claim_token,
            error_code="provider_error",
            error_message="Image generation provider processing failed.",
        )
        return {
            "processed": True,
            "generation_id": str(generation.public_id),
            "status": "failed",
            "error_code": "provider_error",
        }
    finally:
        _queue_next_generation()


@shared_task(queue="short_tasks")
def nudge_ai_generation_worker():
    pending = (
        AIGenerationRequest.objects.filter(
            status=AIGenerationRequest.STATUS_QUEUED
        )
        .order_by("created_at")
        .values_list("public_id", flat=True)
        .first()
    )
    if pending is None:
        return {"queued": False}
    wake_ai_generation_worker.apply_async(
        args=[str(pending)],
        queue="long_tasks",
    )
    return {"queued": True, "generation_id": str(pending)}


@shared_task(queue="short_tasks")
def fail_stale_ai_generations():
    result = fail_stale_generations()
    if result["failed_running"] or result["failed_queued"]:
        logger.warning("Failed stale AI generations: %s", result)
    return result
