from __future__ import annotations

import logging

import requests
from celery import shared_task
from django.conf import settings


logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name="ledger.notify_p2p_agent_offer",
    queue="short_tasks",
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def notify_p2p_agent_offer(self, payload, event_id=""):
    """Deliver a P2P offer event to the dedicated n8n workflow.

    MediaCMS never talks to Telegram/Discord directly. n8n owns those credentials.
    """
    if getattr(settings, "TESTING", False):
        return False

    webhook_url = str(getattr(settings, "P2P_N8N_WEBHOOK_URL", "") or "").strip()
    webhook_secret = str(
        getattr(settings, "P2P_N8N_WEBHOOK_SECRET", "") or ""
    ).strip()
    if not webhook_url or not webhook_secret:
        logger.warning(
            "P2P n8n webhook is not fully configured; skipping agent offer event"
        )
        return False

    body = dict(payload or {})
    body["event"] = "p2p.agent_offer"
    body["event_id"] = str(event_id or "")

    response = requests.post(
        webhook_url,
        json=body,
        headers={
            "Content-Type": "application/json",
            "X-P2P-Webhook-Secret": webhook_secret,
            "X-P2P-Event-ID": str(event_id or ""),
        },
        timeout=5.0,
    )
    response.raise_for_status()
    return True


@shared_task(name="ledger.expire_p2p_agent_offer", queue="short_tasks")
def expire_p2p_agent_offer(assignment_id: int):
    from .p2p_services import expire_p2p_agent_assignment

    state, _order, remaining = expire_p2p_agent_assignment(assignment_id=assignment_id)
    if state == "not_yet" and remaining:
        expire_p2p_agent_offer.apply_async(args=[assignment_id], countdown=remaining)
    return state


@shared_task(name="ledger.expire_p2p_trade", queue="short_tasks")
def expire_p2p_trade(order_id: int):
    from .p2p_services import expire_p2p_trade_if_due

    state, _order, remaining = expire_p2p_trade_if_due(order_id=order_id)
    if state == "not_yet" and remaining:
        expire_p2p_trade.apply_async(args=[order_id], countdown=remaining)
    return state
