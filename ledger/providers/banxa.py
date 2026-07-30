from __future__ import annotations

import os
import re
from decimal import Decimal
from urllib.parse import urlencode, urlparse

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError

from ledger.fiat import get_fiat_usd_rate, normalize_fiat_currency


BANXA_PROVIDER_KEY = "banxa"
BANXA_PAYMENT_METHOD_KEY = "banxa:card"
BANXA_PAYMENT_METHOD_TYPE = "provider"
BANXA_PAYMENT_METHOD_LABEL = "Card / Apple Pay / Google Pay (Banxa)"
BANXA_REQUIRED_SETTLEMENT_ASSET_CODE = "USDC"

BANXA_DEFAULT_CHECKOUT_BASE_URL = "https://checkout.banxa.com"
BANXA_DEFAULT_FIAT_CURRENCY = "EUR"
BANXA_DEFAULT_PAYMENT_TTL_SECONDS = 7 * 24 * 60 * 60
BANXA_DEFAULT_SETTLEMENT_ROUTE_PREFERENCES = ("base:USDC",)

BANXA_NETWORK_BY_MEDIACMS_CHAIN = {
    "base": "BASE",
}

_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def _setting_bool(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _setting_str(name: str, default: str = "") -> str:
    return str(
        getattr(settings, name, os.environ.get(name, default)) or ""
    ).strip()


def _setting_int(
    name: str,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    value = getattr(settings, name, os.environ.get(name, default))
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(
            f"{name} must be an integer"
        ) from exc
    if parsed < minimum:
        raise ImproperlyConfigured(
            f"{name} must be >= {minimum}"
        )
    return parsed


def _setting_tuple(
    name: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        rows = [item.strip() for item in value.split(",")]
    else:
        try:
            rows = [str(item).strip() for item in value]
        except TypeError as exc:
            raise ImproperlyConfigured(
                f"{name} must be a sequence or comma-separated string"
            ) from exc
    return tuple(item for item in rows if item)


def _validated_https_base_url(
    value: str,
    *,
    setting_name: str,
) -> str:
    normalized = str(value or "").strip().rstrip("/")
    parsed = urlparse(normalized)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ImproperlyConfigured(
            f"{setting_name} must be a valid HTTPS base URL "
            "without credentials, query, or fragment"
        )
    return normalized


def get_banxa_checkout_base_url() -> str:
    return _validated_https_base_url(
        _setting_str(
            "BANXA_CHECKOUT_BASE_URL",
            BANXA_DEFAULT_CHECKOUT_BASE_URL,
        ),
        setting_name="BANXA_CHECKOUT_BASE_URL",
    )


def get_banxa_public_base_url() -> str:
    value = (
        _setting_str("BANXA_PUBLIC_BASE_URL")
        or _setting_str("FRONTEND_HOST")
        or _setting_str("SITE_URL")
    )
    if not value:
        raise ImproperlyConfigured(
            "BANXA_PUBLIC_BASE_URL, FRONTEND_HOST, "
            "or SITE_URL must be configured"
        )
    return _validated_https_base_url(
        value,
        setting_name="BANXA_PUBLIC_BASE_URL",
    )


def get_banxa_fiat_currency() -> str:
    currency = normalize_fiat_currency(
        _setting_str(
            "BANXA_FIAT_CURRENCY",
            BANXA_DEFAULT_FIAT_CURRENCY,
        )
    )
    get_fiat_usd_rate(currency)
    return currency


def get_banxa_payment_ttl_seconds() -> int:
    return _setting_int(
        "BANXA_PAYMENT_TTL_SECONDS",
        BANXA_DEFAULT_PAYMENT_TTL_SECONDS,
        minimum=300,
    )


def get_banxa_settlement_route_preferences() -> tuple[str, ...]:
    preferences = _setting_tuple(
        "BANXA_SETTLEMENT_ROUTE_PREFERENCES",
        BANXA_DEFAULT_SETTLEMENT_ROUTE_PREFERENCES,
    )
    if not preferences:
        raise ImproperlyConfigured(
            "BANXA_SETTLEMENT_ROUTE_PREFERENCES cannot be empty"
        )
    return preferences


def get_banxa_network(chain: str) -> str:
    normalized = str(chain or "").strip().lower()
    network = BANXA_NETWORK_BY_MEDIACMS_CHAIN.get(normalized)
    if not network:
        raise ValidationError(
            f"Banxa does not support MediaCMS chain: {normalized}"
        )
    return network


def format_banxa_coin_amount(canonical_amount: int) -> str:
    try:
        normalized = int(canonical_amount)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            "Banxa coin amount must be an integer"
        ) from exc
    if normalized <= 0:
        raise ValidationError(
            "Banxa coin amount must be positive"
        )

    scaled = Decimal(normalized) / Decimal(1_000_000)
    text = format(scaled, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def build_banxa_checkout_url(
    *,
    chain: str,
    asset_code: str,
    target_canonical_amount: int,
    wallet_address: str,
    return_url: str,
    fiat_currency: str | None = None,
) -> str:
    normalized_asset = str(asset_code or "").strip().upper()
    if normalized_asset != BANXA_REQUIRED_SETTLEMENT_ASSET_CODE:
        raise ValidationError(
            "Banxa settlement asset must be USDC"
        )

    normalized_wallet = str(wallet_address or "").strip()
    if not _EVM_ADDRESS_RE.fullmatch(normalized_wallet):
        raise ValidationError(
            "Banxa wallet address must be a valid EVM address"
        )

    normalized_return_url = str(return_url or "").strip()
    parsed_return_url = urlparse(normalized_return_url)
    if (
        parsed_return_url.scheme != "https"
        or not parsed_return_url.hostname
        or parsed_return_url.username
        or parsed_return_url.password
    ):
        raise ValidationError(
            "Banxa return URL must use HTTPS"
        )

    params = {
        "coinType": normalized_asset,
        "blockchain": get_banxa_network(chain),
        "coinAmount": format_banxa_coin_amount(
            target_canonical_amount
        ),
        "fiatType": normalize_fiat_currency(
            fiat_currency or get_banxa_fiat_currency()
        ),
        "walletAddress": normalized_wallet,
        "returnUrl": normalized_return_url,
    }
    return (
        f"{get_banxa_checkout_base_url()}/?"
        f"{urlencode(params)}"
    )


def banxa_enabled() -> bool:
    if not _setting_bool("BANXA_ENABLED", False):
        return False
    try:
        get_banxa_checkout_base_url()
        get_banxa_public_base_url()
        get_banxa_fiat_currency()
        get_banxa_payment_ttl_seconds()
        get_banxa_settlement_route_preferences()
    except ImproperlyConfigured:
        return False
    return True


__all__ = [
    "BANXA_PAYMENT_METHOD_KEY",
    "BANXA_PAYMENT_METHOD_LABEL",
    "BANXA_PAYMENT_METHOD_TYPE",
    "BANXA_PROVIDER_KEY",
    "BANXA_REQUIRED_SETTLEMENT_ASSET_CODE",
    "banxa_enabled",
    "build_banxa_checkout_url",
    "format_banxa_coin_amount",
    "get_banxa_checkout_base_url",
    "get_banxa_fiat_currency",
    "get_banxa_network",
    "get_banxa_payment_ttl_seconds",
    "get_banxa_public_base_url",
    "get_banxa_settlement_route_preferences",
]
