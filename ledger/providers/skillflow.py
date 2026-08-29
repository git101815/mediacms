from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError

from ledger.fiat import (
    canonical_stable_to_fiat_amount,
    fiat_amount_to_canonical_stable_units,
    get_fiat_usd_rate,
)


SKILLFLOW_PROVIDER_KEY = "skillflow"
SKILLFLOW_CHAIN = "skillflow"
SKILLFLOW_CURRENCY = "EUR"
SKILLFLOW_ROUTE_SLUG = "hosted_checkout"
SKILLFLOW_ROUTE_KEY_PREFIX = "skillflow"
SKILLFLOW_PAYMENT_METHOD_KEY = "skillflow:card"
SKILLFLOW_PAYMENT_METHOD_TYPE = "provider"
SKILLFLOW_PAYMENT_METHOD_LABEL = "Card (Skillflow)"
SKILLFLOW_NETWORK_DISPLAY = "Hosted checkout"

SKILLFLOW_DEFAULT_API_BASE_URL = "https://payments.skillflow.store"
SKILLFLOW_CHECKOUT_PATH = "/api/partner/checkout"
SKILLFLOW_DEFAULT_API_TIMEOUT_SECONDS = 20
SKILLFLOW_DEFAULT_PAYMENT_TTL_SECONDS = 60 * 60
SKILLFLOW_MIN_EUR_AMOUNT = Decimal("0.50")
SKILLFLOW_WEBHOOK_TOLERANCE_SECONDS = 300
SKILLFLOW_MAX_RESPONSE_BODY_BYTES = 64 * 1024
SKILLFLOW_MAX_WEBHOOK_BODY_BYTES = 64 * 1024


def _setting_bool(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _setting_str(name: str, default: str = "") -> str:
    return str(getattr(settings, name, os.environ.get(name, default)) or "").strip()


def get_skillflow_api_base_url() -> str:
    value = _setting_str("SKILLFLOW_API_BASE_URL", SKILLFLOW_DEFAULT_API_BASE_URL).rstrip("/")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ImproperlyConfigured("SKILLFLOW_API_BASE_URL must be an HTTPS origin")
    return value


def get_skillflow_public_base_url() -> str:
    return (
        _setting_str("SKILLFLOW_PUBLIC_BASE_URL")
        or _setting_str("FRONTEND_HOST")
        or _setting_str("SITE_URL")
    ).rstrip("/")


def get_skillflow_partner_key() -> str:
    value = _setting_str("SKILLFLOW_PARTNER_KEY")
    if not value:
        raise ImproperlyConfigured("SKILLFLOW_PARTNER_KEY is not configured")
    return value


def get_skillflow_webhook_secret() -> str:
    value = _setting_str("SKILLFLOW_WEBHOOK_SECRET")
    if not value:
        raise ImproperlyConfigured("SKILLFLOW_WEBHOOK_SECRET is not configured")
    return value


def get_skillflow_api_timeout_seconds() -> int:
    try:
        value = int(
            getattr(
                settings,
                "SKILLFLOW_API_TIMEOUT_SECONDS",
                SKILLFLOW_DEFAULT_API_TIMEOUT_SECONDS,
            )
        )
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured("SKILLFLOW_API_TIMEOUT_SECONDS must be an integer") from exc
    if value < 1 or value > 60:
        raise ImproperlyConfigured("SKILLFLOW_API_TIMEOUT_SECONDS must be between 1 and 60")
    return value


def get_skillflow_payment_ttl_seconds() -> int:
    try:
        value = int(
            getattr(
                settings,
                "SKILLFLOW_PAYMENT_TTL_SECONDS",
                SKILLFLOW_DEFAULT_PAYMENT_TTL_SECONDS,
            )
        )
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured("SKILLFLOW_PAYMENT_TTL_SECONDS must be an integer") from exc
    return max(300, value)


def skillflow_enabled() -> bool:
    if not _setting_bool("SKILLFLOW_ENABLED", False):
        return False
    try:
        get_skillflow_api_base_url()
        get_skillflow_public_base_url_or_error()
        get_skillflow_partner_key()
        get_skillflow_webhook_secret()
        get_skillflow_api_timeout_seconds()
        get_fiat_usd_rate(SKILLFLOW_CURRENCY)
    except ImproperlyConfigured:
        return False
    return True


def get_skillflow_public_base_url_or_error() -> str:
    value = get_skillflow_public_base_url()
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ImproperlyConfigured(
            "SKILLFLOW_PUBLIC_BASE_URL, FRONTEND_HOST, or SITE_URL must be an absolute HTTP(S) origin"
        )
    return value


def skillflow_route_key() -> str:
    return f"{SKILLFLOW_ROUTE_KEY_PREFIX}:{SKILLFLOW_CURRENCY.lower()}:{SKILLFLOW_ROUTE_SLUG}"


def is_skillflow_deposit_option_key(option_key: str) -> bool:
    return str(option_key or "").strip() == skillflow_route_key()


def get_skillflow_min_canonical_stable_amount() -> int:
    return fiat_amount_to_canonical_stable_units(
        SKILLFLOW_MIN_EUR_AMOUNT,
        currency=SKILLFLOW_CURRENCY,
    )


def canonical_stable_to_skillflow_amount(value: int) -> str:
    return canonical_stable_to_fiat_amount(
        int(value),
        currency=SKILLFLOW_CURRENCY,
        decimal_places=2,
        rounding=ROUND_CEILING,
    )


def skillflow_amount_to_canonical_stable_units(value, *, currency_usd_rate) -> int:
    try:
        amount = Decimal(str(value))
        rate = Decimal(str(currency_usd_rate))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("Invalid Skillflow amount or frozen EUR/USD rate") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValidationError("Skillflow amount must be finite and positive")
    if not rate.is_finite() or rate <= 0:
        raise ValidationError("Frozen Skillflow EUR/USD rate must be finite and positive")

    canonical = amount * rate * Decimal(1_000_000)
    return int(canonical.to_integral_value(rounding=ROUND_CEILING))


def _validate_absolute_url(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise ValidationError(f"{field_name} must be an absolute HTTP(S) URL")
    return normalized


def _validate_skillflow_checkout_url(value: str) -> str:
    normalized = str(value or "").strip()
    parsed = urlparse(normalized)
    hostname = str(parsed.hostname or "").rstrip(".").lower()
    if (
        parsed.scheme != "https"
        or not hostname
        or parsed.username
        or parsed.password
        or not (hostname == "mollie.com" or hostname.endswith(".mollie.com"))
    ):
        raise ValidationError("Skillflow checkout URL must be an HTTPS Mollie URL")
    return normalized


def _post_skillflow_json(payload: dict) -> dict:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = urllib_request.Request(
        f"{get_skillflow_api_base_url()}{SKILLFLOW_CHECKOUT_PATH}",
        data=body,
        method="POST",
        headers={
            "User-Agent": "MediaCMS-Skillflow/1.0",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "x-partner-key": get_skillflow_partner_key(),
        },
    )

    try:
        with urllib_request.urlopen(
            request,
            timeout=get_skillflow_api_timeout_seconds(),
        ) as response:
            raw_body = response.read(SKILLFLOW_MAX_RESPONSE_BODY_BYTES + 1)
    except HTTPError as exc:
        error_body = exc.read(1024).decode("utf-8", errors="replace")
        raise ValidationError(f"Skillflow API error {exc.code}: {error_body[:500]}") from exc
    except (TimeoutError, URLError) as exc:
        reason = getattr(exc, "reason", exc)
        raise ValidationError(f"Skillflow API request failed: {reason}") from exc

    if len(raw_body) > SKILLFLOW_MAX_RESPONSE_BODY_BYTES:
        raise ValidationError("Skillflow API response is too large")

    try:
        parsed = json.loads(raw_body.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValidationError("Skillflow API returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValidationError("Skillflow API returned an invalid response")
    return parsed


def create_skillflow_checkout(
    *,
    user_id: str,
    amount_eur,
    redirect_url: str,
    cancel_url: str,
    email: str = "",
    metadata: dict | None = None,
) -> dict:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise ValidationError("Skillflow userId is required")

    try:
        amount = Decimal(str(amount_eur))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("Skillflow amount must be a decimal number") from exc
    if not amount.is_finite() or amount < SKILLFLOW_MIN_EUR_AMOUNT:
        raise ValidationError("Skillflow amount must be at least 0.50 EUR")
    if amount != amount.quantize(Decimal("0.01")):
        raise ValidationError("Skillflow amount must have at most two decimal places")

    normalized_metadata = metadata or {}
    if not isinstance(normalized_metadata, dict):
        raise ValidationError("Skillflow metadata must be an object")

    payload = {
        "userId": normalized_user_id,
        "amount": float(amount),
        "redirectUrl": _validate_absolute_url(redirect_url, field_name="Skillflow redirectUrl"),
        "cancelUrl": _validate_absolute_url(cancel_url, field_name="Skillflow cancelUrl"),
        "metadata": normalized_metadata,
    }
    normalized_email = str(email or "").strip()
    if normalized_email:
        payload["email"] = normalized_email

    response = _post_skillflow_json(payload)
    checkout_url = _validate_skillflow_checkout_url(response.get("url"))
    payment_id = str(response.get("paymentId") or "").strip()
    currency = str(response.get("currency") or "").strip().upper()
    description = str(response.get("description") or "").strip()

    try:
        response_amount = Decimal(str(response.get("amount")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("Skillflow checkout response amount is invalid") from exc

    if not payment_id or len(payment_id) > 128:
        raise ValidationError("Skillflow checkout response paymentId is invalid")
    if response_amount != amount:
        raise ValidationError("Skillflow checkout response amount does not match the request")
    if currency != SKILLFLOW_CURRENCY:
        raise ValidationError("Skillflow checkout response currency must be EUR")
    if not description:
        raise ValidationError("Skillflow checkout response description is missing")

    return {
        **response,
        "url": checkout_url,
        "paymentId": payment_id,
        "amount": format(response_amount, ".2f"),
        "currency": currency,
        "description": description,
    }


def verify_skillflow_webhook_signature(
    *,
    raw_body: bytes,
    signature_header: str,
    timestamp_header: str,
    now_epoch_seconds: int | None = None,
) -> None:
    if not isinstance(raw_body, bytes):
        raise PermissionDenied("Invalid Skillflow webhook body")
    if len(raw_body) > SKILLFLOW_MAX_WEBHOOK_BODY_BYTES:
        raise PermissionDenied("Skillflow webhook body is too large")

    timestamp_text = str(timestamp_header or "").strip()
    if (
        not timestamp_text.isascii()
        or not timestamp_text.isdigit()
        or len(timestamp_text) > 16
    ):
        raise PermissionDenied("Invalid Skillflow webhook timestamp")
    timestamp = int(timestamp_text)
    now = int(time.time() if now_epoch_seconds is None else now_epoch_seconds)
    if timestamp <= 0 or abs(now - timestamp) > SKILLFLOW_WEBHOOK_TOLERANCE_SECONDS:
        raise PermissionDenied("Expired Skillflow webhook timestamp")

    signature_text = str(signature_header or "").strip().lower()
    if len(signature_text) != hashlib.sha256().digest_size * 2:
        raise PermissionDenied("Invalid Skillflow webhook signature")
    try:
        provided_signature = bytes.fromhex(signature_text)
    except ValueError as exc:
        raise PermissionDenied("Invalid Skillflow webhook signature") from exc

    signed_payload = timestamp_text.encode("ascii") + b"." + raw_body
    expected_signature = hmac.new(
        get_skillflow_webhook_secret().encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(provided_signature, expected_signature):
        raise PermissionDenied("Invalid Skillflow webhook signature")


__all__ = [
    "SKILLFLOW_CHAIN",
    "SKILLFLOW_CURRENCY",
    "SKILLFLOW_MAX_WEBHOOK_BODY_BYTES",
    "SKILLFLOW_MIN_EUR_AMOUNT",
    "SKILLFLOW_NETWORK_DISPLAY",
    "SKILLFLOW_PAYMENT_METHOD_KEY",
    "SKILLFLOW_PAYMENT_METHOD_LABEL",
    "SKILLFLOW_PAYMENT_METHOD_TYPE",
    "SKILLFLOW_PROVIDER_KEY",
    "SKILLFLOW_WEBHOOK_TOLERANCE_SECONDS",
    "canonical_stable_to_skillflow_amount",
    "create_skillflow_checkout",
    "get_skillflow_min_canonical_stable_amount",
    "get_skillflow_payment_ttl_seconds",
    "get_skillflow_public_base_url_or_error",
    "is_skillflow_deposit_option_key",
    "skillflow_amount_to_canonical_stable_units",
    "skillflow_enabled",
    "skillflow_route_key",
    "verify_skillflow_webhook_signature",
]
