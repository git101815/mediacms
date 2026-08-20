from __future__ import annotations

import json
from datetime import datetime, timezone as dt_timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from functools import lru_cache
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.utils import timezone


NATIVE_QUOTED_AMOUNT_SEMANTICS = "native_quoted"


@lru_cache(maxsize=1)
def get_native_quoted_policy() -> dict:
    path = Path(__file__).resolve().parent / "config" / "native-quoted.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ImproperlyConfigured("Missing ledger/config/native-quoted.json") from exc
    except json.JSONDecodeError as exc:
        raise ImproperlyConfigured("Invalid ledger/config/native-quoted.json") from exc

    if not isinstance(payload, dict):
        raise ImproperlyConfigured("Native quoted policy must be a JSON object")

    try:
        canonical_decimals = int(payload["canonical_stable_decimals"])
        max_age = int(payload["quote_max_age_seconds"])
        future_skew = int(payload["quote_future_skew_seconds"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ImproperlyConfigured("Invalid native quoted timing/decimal policy") from exc

    if canonical_decimals <= 0:
        raise ImproperlyConfigured("canonical_stable_decimals must be positive")
    if max_age <= 0:
        raise ImproperlyConfigured("quote_max_age_seconds must be positive")
    if future_skew < 0:
        raise ImproperlyConfigured("quote_future_skew_seconds cannot be negative")

    raw_assets = payload.get("assets")
    if not isinstance(raw_assets, dict) or not raw_assets:
        raise ImproperlyConfigured("Native quoted policy must define assets")

    assets = {}
    for raw_asset, raw_policy in raw_assets.items():
        asset = str(raw_asset or "").strip().upper()
        if not asset or not isinstance(raw_policy, dict):
            raise ImproperlyConfigured("Invalid native quoted asset policy")
        try:
            native_decimals = int(raw_policy["native_decimals"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f"native_decimals must be an integer for {asset}"
            ) from exc
        if native_decimals <= 0:
            raise ImproperlyConfigured(
                f"native_decimals must be positive for {asset}"
            )
        assets[asset] = {"native_decimals": native_decimals}

    return {
        "canonical_stable_decimals": canonical_decimals,
        "quote_max_age_seconds": max_age,
        "quote_future_skew_seconds": future_skew,
        "assets": assets,
    }


def is_supported_native_asset(asset_code: str) -> bool:
    asset = str(asset_code or "").strip().upper()
    return asset in get_native_quoted_policy()["assets"]


def get_native_asset_decimals(asset_code: str) -> int:
    asset = str(asset_code or "").strip().upper()
    try:
        return int(get_native_quoted_policy()["assets"][asset]["native_decimals"])
    except KeyError as exc:
        raise ValidationError(f"Unsupported native quoted asset: {asset}") from exc


def is_native_quoted_metadata(
    *,
    chain: str,
    asset_code: str,
    token_contract_address: str,
    metadata,
) -> bool:
    del chain
    if str(token_contract_address or "").strip():
        return False
    if not is_supported_native_asset(asset_code):
        return False
    if not isinstance(metadata, dict):
        return False
    return (
        str(metadata.get("amount_semantics") or "").strip().lower()
        == NATIVE_QUOTED_AMOUNT_SEMANTICS
    )


def is_native_quoted_session(session) -> bool:
    return is_native_quoted_metadata(
        chain=getattr(session, "chain", ""),
        asset_code=getattr(session, "asset_code", ""),
        token_contract_address=getattr(session, "token_contract_address", ""),
        metadata=getattr(session, "metadata", None) or {},
    )


def _parse_iso8601(value, *, field_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValidationError(f"{field_name} is required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"{field_name} must include a timezone")
    return parsed.astimezone(dt_timezone.utc)


def normalize_native_usd_quote(
    payload: dict,
    *,
    asset_code: str,
    require_current: bool = True,
) -> dict:
    asset = str(asset_code or "").strip().upper()
    if not is_supported_native_asset(asset):
        raise ValidationError(f"Unsupported native quoted asset: {asset}")
    if not isinstance(payload, dict):
        raise ValidationError(f"{asset}/USD quote payload must be an object")
    if str(payload.get("asset") or "").strip().upper() != asset:
        raise ValidationError(f"Runtime price asset must be {asset}")
    if str(payload.get("currency") or "").strip().upper() != "USD":
        raise ValidationError("Runtime price currency must be USD")

    try:
        price = Decimal(str(payload.get("price")))
    except Exception as exc:
        raise ValidationError(f"{asset}/USD price is invalid") from exc
    if not price.is_finite() or price <= 0:
        raise ValidationError(f"{asset}/USD price must be positive")

    source = str(payload.get("source") or "").strip()
    if not source:
        raise ValidationError(f"{asset}/USD quote source is required")

    observed_at = _parse_iso8601(payload.get("observed_at"), field_name="observed_at")
    expires_at = _parse_iso8601(payload.get("expires_at"), field_name="expires_at")
    if expires_at <= observed_at:
        raise ValidationError(f"{asset}/USD expires_at must be after observed_at")

    now = timezone.now().astimezone(dt_timezone.utc)
    policy = get_native_quoted_policy()
    age_seconds = (now - observed_at).total_seconds()
    if (
        observed_at > now
        and abs(age_seconds) > policy["quote_future_skew_seconds"]
    ):
        raise ValidationError(
            f"{asset}/USD quote observed_at is too far in the future"
        )
    if require_current:
        if expires_at <= now:
            raise ValidationError(f"{asset}/USD quote is expired")
        if age_seconds > policy["quote_max_age_seconds"]:
            raise ValidationError(f"{asset}/USD quote is stale")

    return {
        "asset": asset,
        "currency": "USD",
        "price": format(price, "f"),
        "source": source,
        "observed_at": observed_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }


def native_units_to_canonical_usd(
    raw_amount: int,
    quote: dict,
    *,
    asset_code: str,
) -> int:
    raw_amount = int(raw_amount)
    asset = str(asset_code or "").strip().upper()
    if raw_amount <= 0:
        raise ValidationError(f"{asset} raw amount must be positive")

    normalized_quote = normalize_native_usd_quote(
        quote,
        asset_code=asset,
        require_current=True,
    )
    price = Decimal(normalized_quote["price"])
    native_base = Decimal(10) ** get_native_asset_decimals(asset)
    canonical_base = Decimal(10) ** int(
        get_native_quoted_policy()["canonical_stable_decimals"]
    )
    value = Decimal(raw_amount) * price * canonical_base / native_base
    canonical = int(value.to_integral_value(rounding=ROUND_FLOOR))
    if canonical <= 0:
        raise ValidationError(
            f"{asset} payment value rounds to zero canonical USD"
        )
    return canonical


def canonical_usd_to_required_native_units(
    canonical_amount: int,
    quote: dict,
    *,
    asset_code: str,
) -> int:
    canonical_amount = int(canonical_amount)
    asset = str(asset_code or "").strip().upper()
    if canonical_amount <= 0:
        raise ValidationError("Canonical coverage amount must be positive")

    normalized_quote = normalize_native_usd_quote(
        quote,
        asset_code=asset,
        require_current=True,
    )
    price = Decimal(normalized_quote["price"])
    native_base = Decimal(10) ** get_native_asset_decimals(asset)
    canonical_base = Decimal(10) ** int(
        get_native_quoted_policy()["canonical_stable_decimals"]
    )
    raw = Decimal(canonical_amount) * native_base / (price * canonical_base)
    return max(1, int(raw.to_integral_value(rounding=ROUND_CEILING)))


def build_native_valuation_metadata(
    *,
    raw_amount: int,
    canonical_amount: int,
    quote: dict,
    asset_code: str,
) -> dict:
    asset = str(asset_code or "").strip().upper()
    normalized_quote = normalize_native_usd_quote(
        quote,
        asset_code=asset,
        require_current=True,
    )
    return {
        "amount_semantics": NATIVE_QUOTED_AMOUNT_SEMANTICS,
        "asset": asset,
        "raw_amount": str(int(raw_amount)),
        "native_decimals": get_native_asset_decimals(asset),
        "canonical_stable_amount": int(canonical_amount),
        "quote": normalized_quote,
        "valued_at": timezone.now().isoformat(),
    }
