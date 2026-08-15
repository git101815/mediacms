import logging
import os
import uuid

import requests
from celery import shared_task
from django.conf import settings


logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    name="ledger.notify_admin_event",
    queue="short_tasks",
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def notify_admin_event(self, event, payload, event_id=""):
    # The normal repository test command may run against a production-shaped
    # stack. Tests must never call n8n/Telegram, even if prod env vars exist.
    if getattr(settings, "TESTING", False):
        return False

    webhook_url = str(
        os.environ.get("NOTIFICATION_WEBHOOK_URL", "")
    ).strip()
    if not webhook_url:
        logger.info(
            "Notification webhook not configured; skipping %s",
            event,
        )
        return False

    event = str(event or "").strip()
    if not event:
        logger.warning("Refusing notification with empty event name")
        return False

    event_id = str(event_id or uuid.uuid4().hex)
    body = dict(payload or {})
    body["event"] = event
    body["event_id"] = event_id

    headers = {
        "Content-Type": "application/json",
        "X-Notification-Event": event_id,
    }
    secret = str(
        os.environ.get("NOTIFICATION_WEBHOOK_SECRET", "")
    ).strip()
    if secret:
        headers["X-Notification-Secret"] = secret

    response = requests.post(
        webhook_url,
        json=body,
        headers=headers,
        timeout=5.0,
    )
    response.raise_for_status()
    return True
