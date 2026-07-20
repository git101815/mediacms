from __future__ import annotations

import json
import os
from decimal import Decimal, InvalidOperation
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ImproperlyConfigured, ValidationError

from ledger.providers.paygate import get_paygate_user_agent


MTPERELIN_PROVIDER_KEY = "mtpelerin"
MTPERELIN_PAYMENT_METHOD_TYPE = "provider"
MTPERELIN_PAYMENT_METHOD_LABEL = "Bank transfer (Mt Pelerin)"
MTPERELIN_DEFAULT_API_BASE_URL = "https://api.mtpelerin.com"
MTPERELIN_DEFAULT_WIDGET_BASE_URL = "https://widget.mtpelerin.com"
# Public direct-link activation key published by Mt Pelerin in its web-integration docs.
MTPERELIN_DEFAULT_DIRECT_LINK_CTKN = "954139b2-ef3e-4914-82ea-33192d3f43d3"
MTPERELIN_DEFAULT_FIAT_CURRENCIES = ("EUR", "USD")
MTPERELIN_DEFAULT_SETTLEMENT_ROUTE_PREFERENCES = (
    "base:USDC",
    "bsc:USDC",
    "arbitrum:USDC",
    "ethereum:USDC",
)
MTPERELIN_DEFAULT_CACHE_SECONDS = 300
MTPERELIN_DEFAULT_QUOTE_CACHE_SECONDS = 60
MTPERELIN_DEFAULT_API_TIMEOUT_SECONDS = 15
MTPERELIN_DEFAULT_PAYMENT_TTL_SECONDS = 7 * 24 * 60 * 60
MTPERELIN_CANONICAL_DECIMALS = 6
MTPERELIN_NETWORK_BY_MEDIACMS_CHAIN = {
    "ethereum": "mainnet",
    "arbitrum": "arbitrum_mainnet",
    "base": "base_mainnet",
    "bsc": "bsc_mainnet",
}


def _setting_bool(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _setting_str(name: str, default: str = "") -> str:
    return str(getattr(settings, name, os.environ.get(name, default)) or "").strip()


def _setting_int(name: str, default: int, *, minimum: int = 1) -> int:
    value = getattr(settings, name, os.environ.get(name, default))
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(f"{name} must be an integer") from exc
    if parsed < minimum:
        raise ImproperlyConfigured(f"{name} must be >= {minimum}")
    return parsed


def _setting_tuple(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        rows = [item.strip() for item in value.split(",")]
    else:
        try:
            rows = [str(item).strip() for item in value]
        except TypeError as exc:
            raise ImproperlyConfigured(f"{name} must be a list or comma-separated string") from exc
    return tuple(item for item in rows if item)


def mtpelerin_enabled() -> bool:
    return _setting_bool("MTPERELIN_ENABLED", False)


def get_mtpelerin_api_base_url() -> str:
    return _setting_str(
        "MTPERELIN_API_BASE_URL",
        MTPERELIN_DEFAULT_API_BASE_URL,
    ).rstrip("/")


def get_mtpelerin_widget_base_url() -> str:
    return _setting_str(
        "MTPERELIN_WIDGET_BASE_URL",
        MTPERELIN_DEFAULT_WIDGET_BASE_URL,
    ).rstrip("/")


def get_mtpelerin_direct_link_ctkn() -> str:
    value = _setting_str(
        "MTPERELIN_DIRECT_LINK_CTKN",
        MTPERELIN_DEFAULT_DIRECT_LINK_CTKN,
    )
    if not value:
        raise ImproperlyConfigured("MTPERELIN_DIRECT_LINK_CTKN is not configured")
    return value


def get_mtpelerin_fiat_currencies() -> tuple[str, ...]:
    rows = _setting_tuple(
        "MTPERELIN_FIAT_CURRENCIES",
        MTPERELIN_DEFAULT_FIAT_CURRENCIES,
    )
    normalized = []
    for row in rows:
        code = row.upper()
        if code not in normalized:
            normalized.append(code)
    if not normalized:
        raise ImproperlyConfigured("MTPERELIN_FIAT_CURRENCIES cannot be empty")
    return tuple(normalized)


def get_mtpelerin_settlement_route_preferences() -> tuple[str, ...]:
    rows = _setting_tuple(
        "MTPERELIN_SETTLEMENT_ROUTE_PREFERENCES",
        MTPERELIN_DEFAULT_SETTLEMENT_ROUTE_PREFERENCES,
    )
    if not rows:
        raise ImproperlyConfigured(
            "MTPERELIN_SETTLEMENT_ROUTE_PREFERENCES cannot be empty"
        )
    return rows


def get_mtpelerin_payment_ttl_seconds() -> int:
    return _setting_int(
        "MTPERELIN_PAYMENT_TTL_SECONDS",
        MTPERELIN_DEFAULT_PAYMENT_TTL_SECONDS,
    )


def get_mtpelerin_quote_max_age_seconds() -> int:
    return _setting_int("MTPERELIN_QUOTE_MAX_AGE_SECONDS", 30 * 60)


def get_mtpelerin_language() -> str:
    value = _setting_str("MTPERELIN_LANGUAGE", "en").lower()
    return value if value in {"en", "fr", "de", "it", "es", "pt"} else "en"


def get_mtpelerin_network(chain: str) -> str:
    normalized = str(chain or "").strip().lower()
    network = MTPERELIN_NETWORK_BY_MEDIACMS_CHAIN.get(normalized)
    if not network:
        raise ValidationError(f"Unsupported Mt Pelerin settlement chain: {chain}")
    return network


def _http_json(
    *,
    method: str,
    path: str,
    payload: dict | None = None,
) -> dict:
    url = f"{get_mtpelerin_api_base_url()}/{path.lstrip('/')}"
    body = None
    headers = {
        "Accept": "application/json",
        "User-Agent": get_paygate_user_agent(),
    }
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib_request.Request(
        url,
        data=body,
        method=method.upper(),
        headers=headers,
    )
    try:
        with urllib_request.urlopen(
            request,
            timeout=_setting_int(
                "MTPERELIN_API_TIMEOUT_SECONDS",
                MTPERELIN_DEFAULT_API_TIMEOUT_SECONDS,
            ),
        ) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise ValidationError(
            f"Mt Pelerin API error {exc.code}: {error_body[:500]}"
        ) from exc
    except URLError as exc:
        raise ValidationError(
            f"Mt Pelerin API request failed: {exc.reason}"
        ) from exc

    try:
        result = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError("Mt Pelerin API returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise ValidationError("Mt Pelerin API response must be an object")
    return result


def get_mtpelerin_tokens(*, force_refresh: bool = False) -> list[dict]:
    cache_key = "mtpelerin:currencies:tokens:v1"
    if not force_refresh:
        cached = cache.get(cache_key)
        if isinstance(cached, list):
            return [dict(item) for item in cached]

    payload = _http_json(method="GET", path="currencies/tokens")
    tokens = []
    for unique_name, row in payload.items():
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        network = str(row.get("network") or "").strip()
        address = str(row.get("address") or "").strip()
        if not symbol or not network:
            continue
        tokens.append(
            {
                "unique_name": str(unique_name),
                "symbol": symbol,
                "network": network,
                "address": address,
            }
        )

    if not tokens:
        raise ValidationError("Mt Pelerin did not return any supported token")
    cache.set(
        cache_key,
        tokens,
        timeout=_setting_int(
            "MTPERELIN_CACHE_SECONDS",
            MTPERELIN_DEFAULT_CACHE_SECONDS,
        ),
    )
    return [dict(item) for item in tokens]


def mtpelerin_route_available(*, chain: str, asset_code: str) -> bool:
    network = get_mtpelerin_network(chain)
    asset = str(asset_code or "").strip().upper()
    return any(
        item.get("symbol") == asset and item.get("network") == network
        for item in get_mtpelerin_tokens()
    )


def _canonical_amount_to_decimal(target_canonical_amount: int) -> Decimal:
    try:
        normalized = int(target_canonical_amount)
    except (TypeError, ValueError) as exc:
        raise ValidationError("Mt Pelerin target amount must be an integer") from exc
    if normalized <= 0:
        raise ValidationError("Mt Pelerin target amount must be positive")
    return Decimal(normalized) / (Decimal(10) ** MTPERELIN_CANONICAL_DECIMALS)


def format_mtpelerin_target_amount(target_canonical_amount: int) -> str:
    amount = _canonical_amount_to_decimal(target_canonical_amount)
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def get_mtpelerin_quote(
    *,
    fiat_currency: str,
    chain: str,
    asset_code: str,
    target_canonical_amount: int,
    force_refresh: bool = False,
) -> dict:
    fiat = str(fiat_currency or "").strip().upper()
    asset = str(asset_code or "").strip().upper()
    if fiat not in get_mtpelerin_fiat_currencies():
        raise ValidationError(f"Unsupported Mt Pelerin fiat currency: {fiat}")
    if not mtpelerin_route_available(chain=chain, asset_code=asset):
        raise ValidationError(
            f"Mt Pelerin does not publish {asset} on {get_mtpelerin_network(chain)}"
        )

    target_amount = format_mtpelerin_target_amount(target_canonical_amount)
    network = get_mtpelerin_network(chain)
    cache_key = (
        "mtpelerin:quote:v2:"
        f"{fiat}:{network}:{asset}:{target_amount}"
    )
    if not force_refresh:
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            return dict(cached)

    payload = _http_json(
        method="POST",
        path="currency_rates/convert",
        payload={
            "sourceCurrency": fiat,
            "sourceNetwork": "fiat",
            "destAmount": float(Decimal(target_amount)),
            "destCurrency": asset,
            "destNetwork": network,
            "isCardPayment": False,
        },
    )

    try:
        source_amount = Decimal(str(payload["sourceAmount"]))
        destination_amount = Decimal(str(payload["destAmount"]))
    except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValidationError("Mt Pelerin quote is missing valid amounts") from exc
    if source_amount <= 0 or destination_amount <= 0:
        raise ValidationError("Mt Pelerin quote returned non-positive amounts")

    source_currency = str(payload.get("sourceCurrency") or fiat).strip().upper()
    destination_currency = str(payload.get("destCurrency") or asset).strip().upper()
    source_network = str(payload.get("sourceNetwork") or "fiat").strip()
    destination_network = str(payload.get("destNetwork") or network).strip()
    if source_currency != fiat or destination_currency != asset:
        raise ValidationError("Mt Pelerin quote returned different currencies")
    if source_network != "fiat" or destination_network != network:
        raise ValidationError("Mt Pelerin quote returned different networks")

    result = {
        **payload,
        "sourceAmount": format(source_amount, "f"),
        "destAmount": format(destination_amount, "f"),
        "sourceCurrency": source_currency,
        "destCurrency": destination_currency,
        "sourceNetwork": source_network,
        "destNetwork": destination_network,
        "requestedTargetAmount": target_amount,
    }
    cache.set(
        cache_key,
        result,
        timeout=_setting_int(
            "MTPERELIN_QUOTE_CACHE_SECONDS",
            MTPERELIN_DEFAULT_QUOTE_CACHE_SECONDS,
        ),
    )
    return dict(result)


def build_mtpelerin_checkout_url(
    *,
    fiat_currency: str,
    chain: str,
    asset_code: str,
    target_canonical_amount: int,
    address: str,
    validation_code: str,
    validation_signature_b64: str,
) -> str:
    fiat = str(fiat_currency or "").strip().upper()
    asset = str(asset_code or "").strip().upper()
    normalized_address = str(address or "").strip()
    code = str(validation_code or "").strip()
    signature = str(validation_signature_b64 or "").strip()
    if not normalized_address:
        raise ValidationError("Mt Pelerin destination address is required")
    if len(code) != 4 or not code.isdigit() or not (1000 <= int(code) <= 9999):
        raise ValidationError("Mt Pelerin validation code must be between 1000 and 9999")
    if not signature:
        raise ValidationError("Mt Pelerin validation signature is required")

    network = get_mtpelerin_network(chain)
    target_amount = format_mtpelerin_target_amount(target_canonical_amount)
    params = {
        "_ctkn": get_mtpelerin_direct_link_ctkn(),
        "type": "direct-link",
        "tabs": "buy",
        "tab": "buy",
        "lang": get_mtpelerin_language(),
        "bsc": fiat,
        "bdc": asset,
        "bda": target_amount,
        "curs": fiat,
        "crys": asset,
        "net": network,
        "nets": network,
        "dnet": network,
        "addr": normalized_address,
        "code": code,
        "hash": signature,
    }
    return f"{get_mtpelerin_widget_base_url()}/?{urlencode(params)}"


__all__ = [
    "MTPERELIN_PAYMENT_METHOD_LABEL",
    "MTPERELIN_PAYMENT_METHOD_TYPE",
    "MTPERELIN_PROVIDER_KEY",
    "build_mtpelerin_checkout_url",
    "format_mtpelerin_target_amount",
    "get_mtpelerin_fiat_currencies",
    "get_mtpelerin_payment_ttl_seconds",
    "get_mtpelerin_quote",
    "get_mtpelerin_quote_max_age_seconds",
    "get_mtpelerin_settlement_route_preferences",
    "get_mtpelerin_tokens",
    "mtpelerin_enabled",
    "mtpelerin_route_available",
]
