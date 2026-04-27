import hashlib
import hmac
import ipaddress
import json
import os
from datetime import datetime, timezone as dt_timezone

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.utils import timezone

from .models import InternalAPIRequestNonce


DEFAULT_INTERNAL_API_ALLOWED_CIDRS = [
    "127.0.0.1/32",
    "::1/128",
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
]
DEFAULT_INTERNAL_GATEWAY_HEADER = "X-Ledger-Internal-Gateway"


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


def _setting_enabled(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, default)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _internal_allowed_cidrs() -> list[str]:
    configured = getattr(settings, "LEDGER_INTERNAL_API_ALLOWED_CIDRS", DEFAULT_INTERNAL_API_ALLOWED_CIDRS)
    if isinstance(configured, str):
        return [item.strip() for item in configured.split(",") if item.strip()]
    return [str(item).strip() for item in configured if str(item).strip()]


def _internal_gateway_secret() -> str:
    return (
        str(getattr(settings, "LEDGER_INTERNAL_GATEWAY_SECRET", "") or "").strip()
        or os.environ.get("LEDGER_INTERNAL_GATEWAY_SECRET", "").strip()
        or os.environ.get("MEDIACMS_INTERNAL_GATEWAY_SECRET", "").strip()
    )


def _internal_gateway_header_name() -> str:
    return str(
        getattr(settings, "LEDGER_INTERNAL_GATEWAY_HEADER", DEFAULT_INTERNAL_GATEWAY_HEADER)
        or DEFAULT_INTERNAL_GATEWAY_HEADER
    ).strip()


def _request_remote_addr(request) -> str:
    return str(request.META.get("REMOTE_ADDR", "") or "").strip()


def _remote_addr_allowed(remote_addr: str) -> bool:
    if not remote_addr:
        return False

    try:
        ip_addr = ipaddress.ip_address(remote_addr)
    except ValueError:
        return False

    for raw_network in _internal_allowed_cidrs():
        try:
            network = ipaddress.ip_network(raw_network, strict=False)
        except ValueError as exc:
            raise ImproperlyConfigured(f"Invalid LEDGER_INTERNAL_API_ALLOWED_CIDRS entry: {raw_network}") from exc
        if ip_addr in network:
            return True
    return False


def _enforce_internal_network_guard(request) -> None:
    if not _setting_enabled("LEDGER_INTERNAL_API_NETWORK_GUARD_ENABLED", False):
        return

    if not _internal_allowed_cidrs():
        raise ImproperlyConfigured("LEDGER_INTERNAL_API_ALLOWED_CIDRS must not be empty when guard is enabled")

    remote_addr = _request_remote_addr(request)
    if not _remote_addr_allowed(remote_addr):
        raise PermissionDenied("Internal ledger API source address is not allowed")

    gateway_secret = _internal_gateway_secret()
    require_gateway_secret = _setting_enabled("LEDGER_INTERNAL_GATEWAY_SECRET_REQUIRED", True)
    if not gateway_secret:
        if require_gateway_secret:
            raise ImproperlyConfigured("LEDGER_INTERNAL_GATEWAY_SECRET must be configured when internal guard is enabled")
        return

    header_name = _internal_gateway_header_name()
    provided_gateway_secret = (request.headers.get(header_name) or "").strip()
    if not hmac.compare_digest(provided_gateway_secret, gateway_secret):
        raise PermissionDenied("Invalid internal ledger gateway header")


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
    _enforce_internal_network_guard(request)

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