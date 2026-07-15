from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError

CANONICAL_STABLE_CURRENCY = "USD"
CANONICAL_STABLE_DECIMALS = 6
FIAT_CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "CAD": "CA$",
}


def normalize_fiat_currency(value, *, default: str = CANONICAL_STABLE_CURRENCY) -> str:
    normalized = str(value or default).strip().upper()
    if not normalized:
        raise ImproperlyConfigured("Fiat currency cannot be empty")
    return normalized


def get_fiat_currency_symbol(currency: str) -> str:
    normalized = normalize_fiat_currency(currency)
    return FIAT_CURRENCY_SYMBOLS.get(normalized, f"{normalized} ")


def get_fiat_usd_rate(currency: str) -> Decimal:
    """Return the USD value of one unit of ``currency``.

    Example: EUR/USD = 1.12 means one EUR is worth 1.12 USD, so the
    configured value is ``{"EUR": "1.12"}``.
    """

    normalized = normalize_fiat_currency(currency)
    if normalized == CANONICAL_STABLE_CURRENCY:
        return Decimal("1")

    configured = getattr(settings, "WALLET_FIAT_USD_RATES", {}) or {}
    raw_rate = configured.get(normalized)
    if raw_rate in (None, ""):
        raise ImproperlyConfigured(
            f"WALLET_FIAT_USD_RATES[{normalized!r}] must be configured"
        )

    try:
        rate = Decimal(str(raw_rate))
    except Exception as exc:
        raise ImproperlyConfigured(
            f"WALLET_FIAT_USD_RATES[{normalized!r}] must be a decimal number"
        ) from exc

    if not rate.is_finite() or rate <= 0:
        raise ImproperlyConfigured(
            f"WALLET_FIAT_USD_RATES[{normalized!r}] must be greater than zero"
        )

    return rate


def canonical_stable_to_fiat_decimal(
    value: int,
    *,
    currency: str,
    decimal_places: int = 2,
    rounding=ROUND_HALF_UP,
) -> Decimal:
    try:
        canonical_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError("Canonical stable amount must be an integer") from exc

    if canonical_value < 0:
        raise ValidationError("Canonical stable amount cannot be negative")

    try:
        places = int(decimal_places)
    except (TypeError, ValueError) as exc:
        raise ValidationError("Fiat decimal places must be an integer") from exc

    if places < 0:
        raise ValidationError("Fiat decimal places cannot be negative")

    usd_amount = Decimal(canonical_value) / (
        Decimal(10) ** CANONICAL_STABLE_DECIMALS
    )
    fiat_amount = usd_amount / get_fiat_usd_rate(currency)
    quantum = Decimal(1).scaleb(-places)
    return fiat_amount.quantize(quantum, rounding=rounding)


def canonical_stable_to_fiat_amount(
    value: int,
    *,
    currency: str,
    decimal_places: int = 2,
    rounding=ROUND_HALF_UP,
) -> str:
    amount = canonical_stable_to_fiat_decimal(
        value,
        currency=currency,
        decimal_places=decimal_places,
        rounding=rounding,
    )
    return format(amount, f".{int(decimal_places)}f")


def fiat_amount_to_canonical_stable_units(
    value,
    *,
    currency: str,
    rounding=ROUND_CEILING,
) -> int:
    try:
        fiat_amount = Decimal(str(value))
    except Exception as exc:
        raise ValidationError("Fiat amount must be a decimal number") from exc
    if not fiat_amount.is_finite() or fiat_amount < 0:
        raise ValidationError("Fiat amount must be finite and non-negative")

    canonical = (
        fiat_amount
        * get_fiat_usd_rate(currency)
        * (Decimal(10) ** CANONICAL_STABLE_DECIMALS)
    )
    return int(canonical.to_integral_value(rounding=rounding))


__all__ = [
    "CANONICAL_STABLE_CURRENCY",
    "CANONICAL_STABLE_DECIMALS",
    "canonical_stable_to_fiat_amount",
    "canonical_stable_to_fiat_decimal",
    "fiat_amount_to_canonical_stable_units",
    "get_fiat_currency_symbol",
    "get_fiat_usd_rate",
    "normalize_fiat_currency",
]
