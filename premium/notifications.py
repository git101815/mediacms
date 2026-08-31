import logging
from decimal import Decimal

from django.conf import settings
from django.core.mail import EmailMultiAlternatives, get_connection
from django.db import transaction
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone

from ledger.models import LEDGER_METADATA_VERSION, LedgerOutbox, LedgerTransaction
from ledger.services import _create_outbox_event


logger = logging.getLogger(__name__)

CREATOR_EMAIL_TOPIC = "premium.creator_transactional_email"
CREATOR_EMAIL_EVENT_MEDIA_PURCHASE = "media_purchase"
CREATOR_EMAIL_EVENT_SUBSCRIPTION_STARTED = "subscription_started"
CREATOR_EMAIL_EVENT_SUBSCRIPTION_RENEWED = "subscription_renewed"

DEFAULT_CREATOR_PURCHASE_EMAIL_ENABLED = True
DEFAULT_CREATOR_NEW_SUBSCRIPTION_EMAIL_ENABLED = True
DEFAULT_CREATOR_RENEWAL_EMAIL_ENABLED = False
PLATFORM_TOKEN_DECIMALS = 6


def _setting_enabled(name: str, default: bool) -> bool:
    value = getattr(settings, name, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return bool(value)


def creator_email_event_enabled(event_type: str) -> bool:
    if event_type == CREATOR_EMAIL_EVENT_MEDIA_PURCHASE:
        return _setting_enabled(
            "PREMIUM_CREATOR_PURCHASE_EMAIL_ENABLED",
            DEFAULT_CREATOR_PURCHASE_EMAIL_ENABLED,
        )
    if event_type == CREATOR_EMAIL_EVENT_SUBSCRIPTION_STARTED:
        return _setting_enabled(
            "PREMIUM_CREATOR_NEW_SUBSCRIPTION_EMAIL_ENABLED",
            DEFAULT_CREATOR_NEW_SUBSCRIPTION_EMAIL_ENABLED,
        )
    if event_type == CREATOR_EMAIL_EVENT_SUBSCRIPTION_RENEWED:
        return _setting_enabled(
            "PREMIUM_CREATOR_RENEWAL_EMAIL_ENABLED",
            DEFAULT_CREATOR_RENEWAL_EMAIL_ENABLED,
        )
    return False


def _format_token_units(value) -> str:
    amount = Decimal(int(value or 0)) / Decimal(10**PLATFORM_TOKEN_DECIMALS)
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _absolute_frontend_url(path: str) -> str:
    path = str(path or "").strip()
    if not path:
        return ""
    base = str(getattr(settings, "FRONTEND_HOST", "") or "").strip()
    if not base:
        return path
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def _build_email_context(payload: dict) -> dict:
    event_type = str(payload.get("event_type") or "")
    creator_username = str(payload.get("creator_username") or "")
    creator_name = str(payload.get("creator_name") or "").strip()

    if event_type == CREATOR_EMAIL_EVENT_MEDIA_PURCHASE:
        subject = "You made a sale"
        headline = "You made a sale"
    elif event_type == CREATOR_EMAIL_EVENT_SUBSCRIPTION_STARTED:
        subject = "You have a new subscriber"
        headline = "You have a new subscriber"
    elif event_type == CREATOR_EMAIL_EVENT_SUBSCRIPTION_RENEWED:
        subject = "A subscription renewed"
        headline = "A subscription renewed"
    else:
        raise ValueError(f"Unsupported creator email event type: {event_type}")

    return {
        **payload,
        "event_type": event_type,
        "subject": subject,
        "headline": headline,
        "creator_display_name": creator_name or creator_username or "Creator",
        "price_tokens_display": _format_token_units(payload.get("price_tokens")),
        "creator_amount_display": _format_token_units(payload.get("creator_amount")),
        "wallet_url": _absolute_frontend_url(reverse("wallet")),
        "media_url": _absolute_frontend_url(payload.get("media_url_path", "")),
    }


def queue_creator_transactional_email(
    *,
    txn: LedgerTransaction,
    event_type: str,
    creator,
    payload: dict,
) -> LedgerOutbox | None:
    if not creator_email_event_enabled(event_type):
        return None

    recipient_email = str(getattr(creator, "email", "") or "").strip()
    if not recipient_email:
        return None

    event = _create_outbox_event(
        txn=txn,
        topic=CREATOR_EMAIL_TOPIC,
        payload={
            **dict(payload or {}),
            "event_type": event_type,
            "recipient_email": recipient_email,
            "creator_user_id": int(creator.pk),
            "creator_username": str(getattr(creator, "username", "") or ""),
            "creator_name": str(getattr(creator, "name", "") or ""),
            "transaction_id": int(txn.pk),
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    def _enqueue(event_id=event.pk):
        from .tasks import dispatch_creator_email_outbox_event

        dispatch_creator_email_outbox_event.delay(event_id)

    transaction.on_commit(_enqueue)
    return event


def deliver_creator_email_outbox_event(event_id: int) -> dict:
    event = LedgerOutbox.objects.get(pk=event_id)
    if event.topic != CREATOR_EMAIL_TOPIC:
        raise ValueError("Outbox event is not a creator transactional email")
    if event.status == LedgerOutbox.STATUS_DISPATCHED:
        return {"sent": False, "reason": "already_dispatched", "event_id": event.id}
    if event.status == LedgerOutbox.STATUS_DEAD_LETTERED:
        return {"sent": False, "reason": "dead_lettered", "event_id": event.id}

    payload = dict(event.payload or {})
    recipient_email = str(payload.get("recipient_email") or "").strip()
    if not recipient_email:
        raise ValueError("Creator transactional email recipient is missing")

    context = _build_email_context(payload)
    subject = render_to_string(
        "premium/email/creator_transactional_subject.txt",
        context,
    ).strip().replace("\n", " ")
    text_body = render_to_string(
        "premium/email/creator_transactional.txt",
        context,
    )
    html_body = render_to_string(
        "premium/email/creator_transactional.html",
        context,
    )

    direct_backend = str(
        getattr(
            settings,
            "CELERY_EMAIL_BACKEND",
            "django.core.mail.backends.smtp.EmailBackend",
        )
    )
    connection = get_connection(backend=direct_backend)
    message = EmailMultiAlternatives(
        subject=subject,
        body=text_body,
        from_email=settings.DEFAULT_FROM_EMAIL,
        to=[recipient_email],
        connection=connection,
    )
    message.attach_alternative(html_body, "text/html")

    sent_count = message.send(fail_silently=False)
    if sent_count != 1:
        raise RuntimeError("Creator transactional email was not accepted by the email backend")

    now = timezone.now()
    LedgerOutbox.objects.filter(pk=event.pk).update(
        status=LedgerOutbox.STATUS_DISPATCHED,
        dispatched_at=now,
        last_attempt_at=now,
        next_retry_at=None,
        last_error="",
    )
    return {"sent": True, "event_id": event.id}
