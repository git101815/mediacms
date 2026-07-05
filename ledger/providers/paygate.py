from __future__ import annotations

import json
import os
from decimal import Decimal, ROUND_HALF_UP
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError

PAYGATE_PROVIDER_KEY = "paygate"
PAYGATE_CHAIN = "paygate"
PAYGATE_ROUTE_SLUG = "hosted_checkout"
PAYGATE_PAYMENT_METHOD_KEY = "paygate:hosted_checkout"
PAYGATE_PAYMENT_METHOD_TYPE = "provider"
PAYGATE_PAYMENT_METHOD_LABEL = "PayGate"
PAYGATE_NETWORK_DISPLAY = "Hosted checkout"
PAYGATE_ROUTE_KEY_PREFIX = "paygate"

PAYGATE_DEFAULT_API_BASE_URL = "https://api.paygate.to"
PAYGATE_DEFAULT_CHECKOUT_BASE_URL = "https://checkout.paygate.to"
PAYGATE_DEFAULT_PAYMENT_TTL_SECONDS = 60 * 60
PAYGATE_DEFAULT_MIN_CANONICAL_STABLE_AMOUNT = 1_000_000  # $1.00, canonical stable units with 6 decimals.

PAYGATE_WALLET_PATH = "/control/wallet.php"
PAYGATE_PROVIDER_STATUS_PATH = "/control/provider-status"
PAYGATE_PAYMENT_STATUS_PATH = "/control/payment-status.php"
PAYGATE_PROCESS_PAYMENT_PATH = "/process-payment.php"
PAYGATE_MULTI_PROVIDER_PAYMENT_PATH = "/pay.php"

PAYGATE_STATUS_PAID = "paid"
PAYGATE_STATUS_UNPAID = "unpaid"


def _setting_bool(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _setting_str(name: str, default: str = "") -> str:
    return str(getattr(settings, name, os.environ.get(name, default)) or "").strip()

def get_paygate_currency() -> str:
    value = _setting_str("PAYGATE_CURRENCY", "USD").upper()
    if value not in {"USD", "EUR", "CAD"}:
        raise ImproperlyConfigured("PAYGATE_CURRENCY must be USD, EUR, or CAD")
    return value


def get_paygate_api_base_url() -> str:
    return _setting_str("PAYGATE_API_BASE_URL", PAYGATE_DEFAULT_API_BASE_URL).rstrip("/")


def get_paygate_checkout_base_url() -> str:
    return _setting_str("PAYGATE_CHECKOUT_BASE_URL", PAYGATE_DEFAULT_CHECKOUT_BASE_URL).rstrip("/")


def get_paygate_public_base_url() -> str:
    return (
        _setting_str("PAYGATE_PUBLIC_BASE_URL")
        or _setting_str("FRONTEND_HOST")
        or _setting_str("SITE_URL")
    ).rstrip("/")


def get_paygate_usdc_polygon_wallet() -> str:
    value = _setting_str("PAYGATE_USDC_POLYGON_WALLET")
    if not value:
        raise ImproperlyConfigured("PAYGATE_USDC_POLYGON_WALLET is not configured")
    if not value.startswith("0x") or len(value) < 20:
        raise ImproperlyConfigured("PAYGATE_USDC_POLYGON_WALLET must be a Polygon wallet address")
    return value


def get_paygate_provider_ids() -> list[str]:
    configured = getattr(settings, "PAYGATE_PROVIDER_IDS", ())

    if isinstance(configured, str):
        values = configured.split(",")
    else:
        values = list(configured or [])

    normalized = []
    for value in values:
        provider_id = str(value or "").strip().lower()
        if provider_id and provider_id not in normalized:
            normalized.append(provider_id)

    return normalized


def get_paygate_provider_labels() -> dict:
    configured = getattr(settings, "PAYGATE_PROVIDER_LABELS", {}) or {}
    return {
        str(key).strip().lower(): str(value).strip()
        for key, value in configured.items()
        if str(key).strip() and str(value).strip()
    }


def get_paygate_provider_label(provider_id: str) -> str:
    normalized = str(provider_id or "").strip().lower()
    labels = get_paygate_provider_labels()
    return labels.get(normalized) or normalized.replace("_", " ").replace("-", " ").title()


def get_paygate_provider_id() -> str:
    provider_ids = get_paygate_provider_ids()
    return provider_ids[0] if provider_ids else ""


def get_paygate_payment_ttl_seconds() -> int:
    value = getattr(settings, "PAYGATE_PAYMENT_TTL_SECONDS", PAYGATE_DEFAULT_PAYMENT_TTL_SECONDS)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured("PAYGATE_PAYMENT_TTL_SECONDS must be an integer") from exc
    return max(300, parsed)


def get_paygate_min_canonical_stable_amount() -> int:
    value = getattr(settings, "PAYGATE_MIN_CANONICAL_STABLE_AMOUNT", PAYGATE_DEFAULT_MIN_CANONICAL_STABLE_AMOUNT)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured("PAYGATE_MIN_CANONICAL_STABLE_AMOUNT must be an integer") from exc
    return max(1, parsed)


def get_paygate_user_agent() -> str:
    return _setting_str(
        "PAYGATE_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )


def paygate_enabled() -> bool:
    if not _setting_bool("PAYGATE_ENABLED", False):
        return False
    try:
        get_paygate_usdc_polygon_wallet()
        get_paygate_public_base_url()
        get_paygate_currency()
    except ImproperlyConfigured:
        return False
    return True


def paygate_route_key(currency: str | None = None, provider_id: str | None = None) -> str:
    currency_part = (currency or get_paygate_currency()).lower()
    provider_part = (provider_id or get_paygate_provider_id() or "multi").strip().lower()
    return f"{PAYGATE_ROUTE_KEY_PREFIX}:{currency_part}:{provider_part}:{PAYGATE_ROUTE_SLUG}"


def canonical_stable_to_paygate_amount(value: int) -> str:
    amount = Decimal(int(value)) / Decimal(1_000_000)
    return format(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def paygate_amount_to_canonical_stable_units(value) -> int:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise ValidationError("Invalid PayGate amount") from exc

    if parsed <= 0:
        raise ValidationError("PayGate amount must be positive")

    scaled = parsed * Decimal(1_000_000)
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def call_paygate_json(*, path: str, params: dict | None = None, timeout: int = 20) -> dict:
    query = urlencode(params or {}, doseq=True)
    url = f"{get_paygate_api_base_url()}{path}"
    if query:
        url = f"{url}?{query}"

    req = urllib_request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": get_paygate_user_agent(),
        },
    )

    original_getaddrinfo = socket.getaddrinfo

    def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)

    try:
        socket.getaddrinfo = ipv4_only_getaddrinfo
        with urllib_request.urlopen(req, timeout=timeout) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise ValidationError(f"PayGate API error {exc.code}: {error_body[:500]}") from exc
    except URLError as exc:
        raise ValidationError(f"PayGate API request failed: {exc.reason}") from exc
    finally:
        socket.getaddrinfo = original_getaddrinfo

    try:
        parsed = json.loads(raw_body or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError(f"PayGate API returned invalid JSON: {raw_body[:300]}") from exc

    if not isinstance(parsed, dict):
        raise ValidationError("PayGate API returned an invalid response")

    return parsed


def create_paygate_wallet(*, payout_wallet: str, callback_url: str) -> dict:
    response = call_paygate_json(
        path=PAYGATE_WALLET_PATH,
        params={
            "address": payout_wallet,
            "callback": callback_url,
        },
    )

    address_in = str(response.get("address_in") or "").strip()
    polygon_address_in = str(response.get("polygon_address_in") or "").strip()
    ipn_token = str(response.get("ipn_token") or "").strip()

    if not address_in:
        raise ValidationError("PayGate wallet response is missing address_in")
    if not polygon_address_in:
        raise ValidationError("PayGate wallet response is missing polygon_address_in")
    if not ipn_token:
        raise ValidationError("PayGate wallet response is missing ipn_token")

    return response


def get_paygate_provider_status() -> dict:
    return call_paygate_json(path=PAYGATE_PROVIDER_STATUS_PATH)


def check_paygate_payment(*, ipn_token: str) -> dict:
    normalized_token = (ipn_token or "").strip()
    if not normalized_token:
        raise ValidationError("PayGate ipn_token is required")

    return call_paygate_json(
        path=PAYGATE_PAYMENT_STATUS_PATH,
        params={"ipn_token": normalized_token},
    )


def build_paygate_checkout_url(
    *,
    address_in: str,
    amount: str,
    customer_email: str,
    currency: str,
    provider_id: str = "",
) -> str:
    normalized_address = unquote((address_in or "").strip())
    if not normalized_address:
        raise ValidationError("PayGate address_in is required")

    normalized_amount = str(amount or "").strip()
    if not normalized_amount:
        raise ValidationError("PayGate amount is required")

    normalized_email = (customer_email or "").strip()
    if not normalized_email:
        raise ValidationError("PayGate customer email is required")

    normalized_currency = (currency or get_paygate_currency()).strip().upper()
    if normalized_currency not in {"USD", "EUR", "CAD"}:
        raise ValidationError("PayGate currency must be USD, EUR, or CAD")

    params = {
        "address": normalized_address,
        "amount": normalized_amount,
        "email": normalized_email,
        "currency": normalized_currency,
    }

    normalized_provider_id = (provider_id or "").strip()
    if normalized_provider_id:
        path = PAYGATE_PROCESS_PAYMENT_PATH
        params["provider"] = normalized_provider_id
    else:
        path = PAYGATE_MULTI_PROVIDER_PAYMENT_PATH

    optional_mapping = {
        "PAYGATE_DOMAIN": "domain",
        "PAYGATE_LOGO_URL": "logo",
        "PAYGATE_BACKGROUND": "background",
        "PAYGATE_THEME": "theme",
        "PAYGATE_BUTTON": "button",
    }
    for setting_name, param_name in optional_mapping.items():
        value = _setting_str(setting_name)
        if value:
            params[param_name] = value

    return f"{get_paygate_checkout_base_url()}{path}?{urlencode(params)}"


__all__ = [
    "PAYGATE_CHAIN",
    "PAYGATE_NETWORK_DISPLAY",
    "PAYGATE_PAYMENT_METHOD_KEY",
    "PAYGATE_PAYMENT_METHOD_LABEL",
    "PAYGATE_PAYMENT_METHOD_TYPE",
    "PAYGATE_PROVIDER_KEY",
    "PAYGATE_STATUS_PAID",
    "PAYGATE_STATUS_UNPAID",
    "build_paygate_checkout_url",
    "canonical_stable_to_paygate_amount",
    "check_paygate_payment",
    "create_paygate_wallet",
    "get_paygate_currency",
    "get_paygate_min_canonical_stable_amount",
    "get_paygate_payment_ttl_seconds",
    "get_paygate_provider_id",
    "get_paygate_provider_ids",
    "get_paygate_provider_label",
    "get_paygate_provider_labels",
    "get_paygate_provider_status",
    "get_paygate_public_base_url",
    "get_paygate_usdc_polygon_wallet",
    "paygate_amount_to_canonical_stable_units",
    "paygate_enabled",
    "paygate_route_key",
]