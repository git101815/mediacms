import hashlib
import hmac
import json
from datetime import datetime, timezone as dt_timezone

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from .models import InternalAPIRequestNonce


def build_internal_request_signature(
    *,
    service_name: str,
    timestamp: str,
    nonce: str,
    body_bytes: bytes,
    shared_secret: str,
) -> str:
    body_sha256 = hashlib.sha256(body_bytes).hexdigest()
    signing_payload = "\n".join(
        [
            service_name,
            timestamp,
            nonce,
            body_sha256,
        ]
    ).encode("utf-8")
    return hmac.new(
        shared_secret.encode("utf-8"),
        signing_payload,
        hashlib.sha256,
    ).hexdigest()


def _get_internal_deposit_service_actor():
    username = settings.LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME
    if not username:
        raise ImproperlyConfigured("LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME is not configured")

    user_model = get_user_model()
    actor = user_model.objects.filter(username=username).first()
    if actor is None:
        raise ImproperlyConfigured("Configured internal deposit service actor does not exist")

    return actor


def _validate_internal_request_timestamp(timestamp_value: str) -> datetime:
    try:
        timestamp_int = int(timestamp_value)
    except (TypeError, ValueError) as exc:
        raise PermissionDenied("Invalid timestamp header") from exc

    request_time = datetime.fromtimestamp(timestamp_int, tz=dt_timezone.utc)
    skew = abs((timezone.now() - request_time).total_seconds())
    if skew > settings.LEDGER_INTERNAL_API_MAX_SKEW_SECONDS:
        raise PermissionDenied("Request timestamp is outside the allowed skew window")

    return request_time

@transaction.atomic
def authenticate_internal_service_request(
    request,
    *,
    expected_service_name: str,
    username_setting_name: str,
    shared_secret_setting_name: str,
):
    service_name = (request.headers.get("X-Ledger-Service") or "").strip()
    timestamp_value = (request.headers.get("X-Ledger-Timestamp") or "").strip()
    nonce = (request.headers.get("X-Ledger-Nonce") or "").strip()
    provided_signature = (request.headers.get("X-Ledger-Signature") or "").strip().lower()

    if service_name != expected_service_name:
        raise PermissionDenied("Invalid internal service name")

    if not timestamp_value or not nonce or not provided_signature:
        raise PermissionDenied("Missing internal authentication headers")

    shared_secret = getattr(settings, shared_secret_setting_name, "").strip()
    if not shared_secret:
        raise ImproperlyConfigured(f"{shared_secret_setting_name} is not configured")

    body_bytes = request.body or b""
    expected_signature = build_internal_request_signature(
        service_name=service_name,
        timestamp=timestamp_value,
        nonce=nonce,
        body_bytes=body_bytes,
        shared_secret=shared_secret,
    )

    if not hmac.compare_digest(provided_signature, expected_signature):
        raise PermissionDenied("Invalid internal request signature")

    _validate_internal_request_timestamp(timestamp_value)

    request_sha256 = hashlib.sha256(body_bytes).hexdigest()
    expires_at = timezone.now() + timezone.timedelta(seconds=settings.LEDGER_INTERNAL_NONCE_TTL_SECONDS)

    try:
        InternalAPIRequestNonce.objects.create(
            service_name=service_name,
            nonce=nonce,
            request_sha256=request_sha256,
            expires_at=expires_at,
        )
    except IntegrityError as exc:
        raise PermissionDenied("Replay detected for internal request nonce") from exc

    try:
        payload = json.loads(body_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("Request body must be valid JSON") from exc

    if not isinstance(payload, dict):
        raise ValidationError("Request body must be a JSON object")

    username = getattr(settings, username_setting_name, "").strip()
    if not username:
        raise ImproperlyConfigured(f"{username_setting_name} is not configured")

    user_model = get_user_model()
    actor = user_model.objects.filter(username=username).first()
    if actor is None:
        raise ImproperlyConfigured("Configured internal service actor does not exist")

    return actor, payload, service_name

def authenticate_internal_deposit_request(request):
    return authenticate_internal_service_request(
        request,
        expected_service_name="deposit-service",
        username_setting_name="LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME",
        shared_secret_setting_name="LEDGER_INTERNAL_DEPOSIT_SERVICE_SHARED_SECRET",
    )


def authenticate_internal_sweeper_request(request):
    return authenticate_internal_service_request(
        request,
        expected_service_name="sweeper-service",
        username_setting_name="LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME",
        shared_secret_setting_name="LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET",
    )