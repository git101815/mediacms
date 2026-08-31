import logging
from datetime import timedelta
from decimal import Decimal

from celery import current_app
from django.conf import settings
from django.core.mail import EmailMultiAlternatives, get_connection
from django.db import transaction
from django.db.models import Q
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
DEFAULT_CREATOR_RENEWAL_EMAIL_ENABLED = True

CREATOR_EMAIL_PREFERENCE_FIELDS = {
    CREATOR_EMAIL_EVENT_MEDIA_PURCHASE: (
        "notification_on_premium_purchases",
        True,
    ),
    CREATOR_EMAIL_EVENT_SUBSCRIPTION_STARTED: (
        "notification_on_new_subscriptions",
        True,
    ),
    CREATOR_EMAIL_EVENT_SUBSCRIPTION_RENEWED: (
        "notification_on_subscription_renewals",
        False,
    ),
}
DEFAULT_CREATOR_EMAIL_RECOVERY_ENABLED = True
DEFAULT_CREATOR_EMAIL_RECOVERY_GRACE_SECONDS = 120
DEFAULT_CREATOR_EMAIL_RECOVERY_BATCH_SIZE = 100
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


def creator_email_preference_enabled(*, creator, event_type: str) -> bool:
    field = CREATOR_EMAIL_PREFERENCE_FIELDS.get(event_type)
    if field is None:
        return False
    field_name, default = field
    return bool(getattr(creator, field_name, default))


def _non_negative_int_setting(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(getattr(settings, name, default))
    except (TypeError, ValueError):
        value = int(default)
    return max(int(minimum), value)


def get_recoverable_creator_email_event_ids(*, at=None) -> list[int]:
    if not _setting_enabled(
        "PREMIUM_CREATOR_EMAIL_RECOVERY_ENABLED",
        DEFAULT_CREATOR_EMAIL_RECOVERY_ENABLED,
    ):
        return []

    now = at or timezone.now()
    grace_seconds = _non_negative_int_setting(
        "PREMIUM_CREATOR_EMAIL_RECOVERY_GRACE_SECONDS",
        DEFAULT_CREATOR_EMAIL_RECOVERY_GRACE_SECONDS,
    )
    batch_size = _non_negative_int_setting(
        "PREMIUM_CREATOR_EMAIL_RECOVERY_BATCH_SIZE",
        DEFAULT_CREATOR_EMAIL_RECOVERY_BATCH_SIZE,
        minimum=1,
    )
    stale_before = now - timedelta(seconds=grace_seconds)

    return list(
        LedgerOutbox.objects.filter(topic=CREATOR_EMAIL_TOPIC)
        .filter(
            Q(
                status=LedgerOutbox.STATUS_PENDING,
                created_at__lte=stale_before,
            )
            | Q(
                status=LedgerOutbox.STATUS_FAILED,
                next_retry_at__isnull=False,
                next_retry_at__lte=stale_before,
            )
        )
        .order_by("created_at", "id")
        .values_list("id", flat=True)[:batch_size]
    )


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


def _safe_enqueue_creator_email_outbox_event(event_id: int) -> bool:
    try:
        current_app.send_task(
            "premium.tasks.dispatch_creator_email_outbox_event",
            args=[int(event_id)],
            queue="short_tasks",
        )
        return True
    except Exception:
        # The purchase/subscription has already committed at this point.
        # A broker outage must not turn a successful financial operation into
        # an HTTP 500. Celery beat will recover the still-pending outbox row.
        logger.exception(
            "Failed to enqueue creator transactional email event_id=%s; "
            "periodic recovery will retry it",
            event_id,
        )
        return False


def queue_creator_transactional_email(
    *,
    txn: LedgerTransaction,
    event_type: str,
    creator,
    payload: dict,
) -> LedgerOutbox | None:
    if not creator_email_event_enabled(event_type):
        return None
    if not creator_email_preference_enabled(
        creator=creator,
        event_type=event_type,
    ):
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

    transaction.on_commit(
        lambda event_id=event.pk: _safe_enqueue_creator_email_outbox_event(event_id),
        robust=True,
    )
    return event


@transaction.atomic
def record_creator_email_delivery_failure(
    *,
    event_id: int,
    error_message: str,
    final_failure: bool,
    retry_at=None,
) -> LedgerOutbox:
    event = LedgerOutbox.objects.select_for_update().get(
        pk=int(event_id),
        topic=CREATOR_EMAIL_TOPIC,
    )
    if event.status in {
        LedgerOutbox.STATUS_DISPATCHED,
        LedgerOutbox.STATUS_DEAD_LETTERED,
    }:
        return event

    now = timezone.now()
    error_text = str(error_message or "")[:4000]
    event.fail_count = int(event.fail_count or 0) + 1
    event.last_error = error_text
    event.last_attempt_at = now

    update_fields = [
        "status",
        "fail_count",
        "last_error",
        "last_attempt_at",
        "next_retry_at",
    ]
    if final_failure:
        event.status = LedgerOutbox.STATUS_DEAD_LETTERED
        event.dead_lettered_at = now
        event.dead_letter_reason = (
            f"Creator transactional email retries exhausted: {error_text}"
        )[:2000]
        event.next_retry_at = None
        update_fields.extend(["dead_lettered_at", "dead_letter_reason"])
    else:
        event.status = LedgerOutbox.STATUS_FAILED
        event.next_retry_at = retry_at

    # Use save(), not QuerySet.update(): ledger.signals relies on pre_save /
    # post_save to emit the critical admin alert on a dead-letter transition.
    event.save(update_fields=update_fields)
    return event


@transaction.atomic
def deliver_creator_email_outbox_event(event_id: int) -> dict:
    # Serialize delivery attempts for one outbox row. Recovery and a normal
    # Celery retry can overlap; only one worker may perform the SMTP send.
    event = LedgerOutbox.objects.select_for_update().get(pk=event_id)
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
    event.status = LedgerOutbox.STATUS_DISPATCHED
    event.dispatched_at = now
    event.last_attempt_at = now
    event.next_retry_at = None
    event.last_error = ""
    event.save(
        update_fields=[
            "status",
            "dispatched_at",
            "last_attempt_at",
            "next_retry_at",
            "last_error",
        ]
    )
    return {"sent": True, "event_id": event.id}
