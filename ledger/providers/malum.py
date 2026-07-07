from __future__ import annotations

import hashlib
import hmac
import json
import os
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError

MALUM_PROVIDER_KEY = "malum"
MALUM_CHAIN = "malum"
MALUM_ROUTE_SLUG = "hosted_checkout"
MALUM_PAYMENT_METHOD_KEY = "malum:hosted_checkout"
MALUM_PAYMENT_METHOD_TYPE = "provider"
MALUM_PAYMENT_METHOD_LABEL = "Malum Payments (Credit card accepted)"
MALUM_NETWORK_DISPLAY = "Hosted checkout"
MALUM_ROUTE_KEY_PREFIX = "malum"
MALUM_DEFAULT_API_BASE_URL = "https://malum.to"
MALUM_DEFAULT_PAYMENT_TTL_SECONDS = 60 * 60
MALUM_MIN_CANONICAL_STABLE_AMOUNT = 50_000  # $0.50, canonical stable units with 6 decimals.
MALUM_CREATE_PAYMENT_PATH = "/api/v2/payment/create"
MALUM_LOOKUP_PATH = "/api/v2/payment/lookup"

MALUM_STATUS_CREATED = "CREATED"
MALUM_STATUS_PROCESSING = "PROCESSING"
MALUM_STATUS_COMPLETED = "COMPLETED"
MALUM_STATUS_EXPIRED = "EXPIRED"
MALUM_STATUS_CANCELLED = "CANCELLED"
MALUM_STATUS_CANCELED = "CANCELED"


def _setting_bool(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _setting_str(name: str, default: str = "") -> str:
    return str(getattr(settings, name, os.environ.get(name, default)) or "").strip()


def get_malum_currency() -> str:
    value = _setting_str("MALUM_CURRENCY", "USD").upper()
    if not value:
        raise ImproperlyConfigured("MALUM_CURRENCY is not configured")
    return value


def get_malum_api_base_url() -> str:
    return _setting_str("MALUM_API_BASE_URL", MALUM_DEFAULT_API_BASE_URL).rstrip("/")


def get_malum_public_base_url() -> str:
    return (
        _setting_str("MALUM_PUBLIC_BASE_URL")
        or _setting_str("FRONTEND_HOST")
        or _setting_str("SITE_URL")
    ).rstrip("/")


def get_malum_payment_ttl_seconds() -> int:
    try:
        value = int(getattr(settings, "MALUM_PAYMENT_TTL_SECONDS", MALUM_DEFAULT_PAYMENT_TTL_SECONDS))
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured("MALUM_PAYMENT_TTL_SECONDS must be an integer") from exc
    return max(300, value)


def get_malum_merchant_id() -> str:
    value = _setting_str("MALUM_MERCHANT_ID")
    if not value:
        raise ImproperlyConfigured("MALUM_MERCHANT_ID is not configured")
    return value


def get_malum_private_key() -> str:
    value = _setting_str("MALUM_PRIVATE_KEY")
    if not value:
        raise ImproperlyConfigured("MALUM_PRIVATE_KEY is not configured")
    return value


def malum_private_key_is_sandbox() -> bool:
    private_key = get_malum_private_key().lower()
    return private_key.startswith(("sbx_", "sec_sandbox_"))


def get_malum_webhook_key(*, sandbox: bool) -> str:
    if sandbox:
        value = _setting_str("MALUM_SANDBOX_WEBHOOK_KEY")
        if not value:
            raise ImproperlyConfigured("MALUM_SANDBOX_WEBHOOK_KEY is not configured")
        return value

    value = _setting_str("MALUM_WEBHOOK_KEY")
    if not value:
        raise ImproperlyConfigured("MALUM_WEBHOOK_KEY is not configured")
    return value


def malum_auth_header() -> str:
    configured = _setting_str("MALUM_AUTH_HEADER")
    if configured:
        return configured
    return f"{get_malum_merchant_id()}:{get_malum_private_key()}"


def malum_enabled() -> bool:
    if not _setting_bool("MALUM_ENABLED", False):
        return False
    try:
        get_malum_merchant_id()
        get_malum_private_key()
        get_malum_webhook_key(sandbox=malum_private_key_is_sandbox())
        get_malum_public_base_url()
    except ImproperlyConfigured:
        return False
    return True


def malum_buyer_pays_fees() -> bool:
    return _setting_bool("MALUM_BUYER_PAYS_FEES", False)


def malum_merchant_pays_gateway_fees() -> bool:
    return _setting_bool("MALUM_MERCHANT_PAYS_GW_FEES", False)


def malum_route_key(currency: str | None = None) -> str:
    return f"{MALUM_ROUTE_KEY_PREFIX}:{(currency or get_malum_currency()).lower()}:{MALUM_ROUTE_SLUG}"


def is_malum_deposit_option_key(option_key: str) -> bool:
    return (option_key or "").strip() == malum_route_key()


def build_malum_metadata_value(*, session_public_id, token_pack_code: str) -> str:
    payload = {
        "deposit_session_public_id": str(session_public_id),
        "token_pack_code": (token_pack_code or "").strip(),
        "provider": MALUM_PROVIDER_KEY,
    }
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(value) > 254:
        raise ValidationError("Malum metadata payload is too large")
    return value


def parse_malum_metadata_value(value) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def call_malum_json(*, method: str, path: str, payload: dict | None = None, timeout: int = 20) -> dict:
    body = b""
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    request = urllib_request.Request(
        f"{get_malum_api_base_url()}{path}",
        data=body if method.upper() in {"POST", "PUT", "PATCH"} else None,
        method=method.upper(),
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "MALUM": malum_auth_header(),
        }
    )

    try:
        with urllib_request.urlopen(request, timeout=timeout) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise ValidationError(f"Malum API error {exc.code}: {error_body[:500]}") from exc
    except URLError as exc:
        raise ValidationError(f"Malum API request failed: {exc.reason}") from exc

    try:
        parsed = json.loads(raw_body or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError("Malum API returned invalid JSON") from exc

    if not isinstance(parsed, dict):
        raise ValidationError("Malum API returned an invalid response")
    return parsed


def create_malum_payment(
    *,
    amount,
    currency: str,
    customer_email: str,
    webhook_url: str,
    success_url: str,
    cancel_url: str,
    metadata: str,
    product_title: str,
) -> dict:
    payload = {
        "amount": amount,
        "currency": currency,
        "customer_email": customer_email,
        "webhook_url": webhook_url,
        "success_url": success_url,
        "cancel_url": cancel_url,
        "metadata": metadata,
        "product_title": product_title[:120],
    }

    if malum_buyer_pays_fees():
        payload["buyer_pays_fees"] = True
    if malum_merchant_pays_gateway_fees():
        payload["merchant_pays_gw_fees"] = True

    response = call_malum_json(
        method="POST",
        path=MALUM_CREATE_PAYMENT_PATH,
        payload=payload,
    )

    if str(response.get("status") or "").lower() != "success":
        message = response.get("message") or response.get("error") or "Malum payment creation failed"
        raise ValidationError(str(message))

    transaction_id = str(response.get("transaction_id") or "").strip()
    checkout_url = str(response.get("link") or "").strip()
    if not transaction_id or not checkout_url:
        raise ValidationError("Malum payment response is missing transaction_id or link")

    return response


def lookup_malum_payment(*, transaction_id: str) -> dict:
    normalized_txn = (transaction_id or "").strip()
    if not normalized_txn:
        raise ValidationError("Malum transaction id is required")
    return call_malum_json(
        method="GET",
        path=f"{MALUM_LOOKUP_PATH}?txid={normalized_txn}",
        payload=None,
    )


def verify_malum_webhook_signature(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise PermissionDenied("Invalid Malum webhook payload")

    txn = str(payload.get("txn") or "").strip()
    timestamp = str(payload.get("timestamp") or "").strip()
    provided_signature = str(payload.get("signature") or "").strip().lower()
    sandbox = bool(payload.get("sandbox"))

    if not txn or not timestamp or not provided_signature:
        raise PermissionDenied("Missing Malum webhook signature fields")

    webhook_key = get_malum_webhook_key(sandbox=sandbox)
    expected_signature = hashlib.md5(f"{txn}|{timestamp}|{webhook_key}".encode("utf-8")).hexdigest()

    if not hmac.compare_digest(provided_signature, expected_signature):
        raise PermissionDenied("Invalid Malum webhook signature")
