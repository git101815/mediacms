
import logging

import requests
from celery import shared_task
from django.conf import settings

from .models import AIGenerationRequest
from .services import fail_stale_generations


logger = logging.getLogger(__name__)


@shared_task(
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def wake_ai_generation_worker(generation_id: str = ""):
    url = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_WEBHOOK_URL", "") or ""
    ).strip()
    if not url:
        return {"sent": False, "reason": "not_configured"}

    timeout = int(
        getattr(settings, "AI_GENERATION_N8N_WAKE_TIMEOUT_SECONDS", 5)
    )
    secret = str(
        getattr(settings, "AI_GENERATION_N8N_WAKE_SECRET", "") or ""
    ).strip()

    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-AI-Generation-Wake-Secret"] = secret

    response = requests.post(
        url,
        json={
            "reason": "generation_queued",
            "generation_id": str(generation_id or ""),
        },
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return {"sent": True, "status_code": response.status_code}


@shared_task
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
    wake_ai_generation_worker.delay(str(pending))
    return {"queued": True, "generation_id": str(pending)}


@shared_task
def refund_stale_ai_generations():
    result = fail_stale_generations()
    if result["refunded_running"] or result["refunded_queued"]:
        logger.warning("Refunded stale AI generations: %s", result)
    return result
