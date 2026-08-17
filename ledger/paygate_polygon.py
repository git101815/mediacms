from __future__ import annotations

import hmac
import json
import os
from functools import lru_cache
from pathlib import Path
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime, timezone as dt_timezone

from django.core.cache import cache
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.utils import timezone


PAYGATE_POLYGON_CHAIN = "polygon"
PAYGATE_POLYGON_ASSET = "POL"
PAYGATE_POLYGON_TOKEN_CONTRACT = ""
PAYGATE_POLYGON_AMOUNT_SEMANTICS = "native_quoted"
PAYGATE_POLYGON_QUOTE_CACHE_KEY = "ledger:runtime-price:POL:USD:v1"
CANONICAL_STABLE_DECIMALS = 6


@lru_cache(maxsize=1)
def get_paygate_polygon_policy() -> dict:
    path = Path(__file__).resolve().parent / "config" / "paygate-polygon.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ImproperlyConfigured("Missing ledger/config/paygate-polygon.json") from exc
    except json.JSONDecodeError as exc:
        raise ImproperlyConfigured("Invalid ledger/config/paygate-polygon.json") from exc

    if not isinstance(payload, dict):
        raise ImproperlyConfigured("PayGate Polygon policy must be a JSON object")

    expected = {
        "chain": PAYGATE_POLYGON_CHAIN,
        "asset_code": PAYGATE_POLYGON_ASSET,
        "native_decimals": 18,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ImproperlyConfigured(f"Invalid PayGate Polygon policy {key}")

    for key in (
        "required_confirmations",
        "quote_max_age_seconds",
        "quote_future_skew_seconds",
        "underpayment_tolerance_bps",
    ):
        try:
            payload[key] = int(payload[key])
        except (KeyError, TypeError, ValueError) as exc:
            raise ImproperlyConfigured(f"PayGate Polygon policy {key} must be an integer") from exc

    if payload["required_confirmations"] <= 0:
        raise ImproperlyConfigured("PayGate Polygon required_confirmations must be positive")
    if payload["quote_max_age_seconds"] <= 0:
        raise ImproperlyConfigured("PayGate Polygon quote_max_age_seconds must be positive")
    if payload["quote_future_skew_seconds"] < 0:
        raise ImproperlyConfigured("PayGate Polygon quote_future_skew_seconds cannot be negative")
    if not 0 <= payload["underpayment_tolerance_bps"] < 10000:
        raise ImproperlyConfigured("PayGate Polygon underpayment_tolerance_bps must be in [0, 10000)")

    coverage_basis = str(payload.get("coverage_basis") or "").strip()
    if coverage_basis not in {"net_stable_amount", "gross_stable_amount"}:
        raise ImproperlyConfigured(
            "PayGate Polygon coverage_basis must be net_stable_amount or gross_stable_amount"
        )
    payload["coverage_basis"] = coverage_basis

    try:
        observation_floor_wei = int(payload["observation_floor_wei"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ImproperlyConfigured("PayGate Polygon observation_floor_wei must be an integer") from exc
    if observation_floor_wei <= 0:
        raise ImproperlyConfigured("PayGate Polygon observation_floor_wei must be positive")
    payload["observation_floor_wei"] = observation_floor_wei
    return payload


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


def _normalize_quote_payload(payload: dict, *, require_current: bool = True) -> dict:
    if not isinstance(payload, dict):
        raise ValidationError("POL/USD quote payload must be an object")
    if str(payload.get("asset") or "").strip().upper() != PAYGATE_POLYGON_ASSET:
        raise ValidationError("Runtime price asset must be POL")
    if str(payload.get("currency") or "").strip().upper() != "USD":
        raise ValidationError("Runtime price currency must be USD")

    try:
        price = Decimal(str(payload.get("price")))
    except Exception as exc:
        raise ValidationError("POL/USD price is invalid") from exc
    if not price.is_finite() or price <= 0:
        raise ValidationError("POL/USD price must be positive")

    source = str(payload.get("source") or "").strip()
    if not source:
        raise ValidationError("POL/USD quote source is required")

    observed_at = _parse_iso8601(payload.get("observed_at"), field_name="observed_at")
    expires_at = _parse_iso8601(payload.get("expires_at"), field_name="expires_at")
    if expires_at <= observed_at:
        raise ValidationError("POL/USD expires_at must be after observed_at")

    now = timezone.now().astimezone(dt_timezone.utc)
    policy = get_paygate_polygon_policy()
    age_seconds = (now - observed_at).total_seconds()
    if observed_at > now and abs(age_seconds) > policy["quote_future_skew_seconds"]:
        raise ValidationError("POL/USD quote observed_at is too far in the future")
    if require_current:
        if expires_at <= now:
            raise ValidationError("POL/USD quote is expired")
        if age_seconds > policy["quote_max_age_seconds"]:
            raise ValidationError("POL/USD quote is stale")

    return {
        "asset": PAYGATE_POLYGON_ASSET,
        "currency": "USD",
        "price": format(price, "f"),
        "source": source,
        "observed_at": observed_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }


def validate_pol_price_push_secret(provided_secret: str) -> None:
    expected = os.environ.get("PAYGATE_POL_PRICE_PUSH_SECRET", "").strip()
    if not expected:
        raise ImproperlyConfigured("PAYGATE_POL_PRICE_PUSH_SECRET is not configured")
    if not hmac.compare_digest(str(provided_secret or "").strip(), expected):
        raise PermissionDenied("Invalid POL/USD price push secret")


def store_pol_usd_quote(payload: dict) -> dict:
    quote = _normalize_quote_payload(payload, require_current=True)
    now = timezone.now().astimezone(dt_timezone.utc)
    expires_at = _parse_iso8601(quote["expires_at"], field_name="expires_at")
    observed_at = _parse_iso8601(quote["observed_at"], field_name="observed_at")
    policy = get_paygate_polygon_policy()
    ttl_by_expiry = int((expires_at - now).total_seconds())
    ttl_by_age = int(policy["quote_max_age_seconds"] - max(0, (now - observed_at).total_seconds()))
    ttl = max(1, min(ttl_by_expiry, ttl_by_age))
    cache.set(PAYGATE_POLYGON_QUOTE_CACHE_KEY, quote, timeout=ttl)
    return quote


def get_fresh_pol_usd_quote(*, required: bool = True) -> dict | None:
    payload = cache.get(PAYGATE_POLYGON_QUOTE_CACHE_KEY)
    if payload is None:
        if required:
            raise ValidationError("POL/USD runtime quote is unavailable")
        return None
    try:
        return _normalize_quote_payload(payload, require_current=True)
    except ValidationError:
        cache.delete(PAYGATE_POLYGON_QUOTE_CACHE_KEY)
        if required:
            raise
        return None


def is_paygate_polygon_route(*, chain: str, asset_code: str, token_contract_address: str = "") -> bool:
    return (
        str(chain or "").strip().lower() == PAYGATE_POLYGON_CHAIN
        and str(asset_code or "").strip().upper() == PAYGATE_POLYGON_ASSET
        and not str(token_contract_address or "").strip()
    )


def is_paygate_polygon_metadata(*, chain: str, asset_code: str, token_contract_address: str, metadata) -> bool:
    if not is_paygate_polygon_route(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
    ):
        return False
    if not isinstance(metadata, dict):
        return False
    provider = metadata.get("payment_provider") or {}
    return (
        isinstance(provider, dict)
        and str(provider.get("key") or "").strip().lower() == "paygate"
        and str(metadata.get("amount_semantics") or "").strip().lower()
        == PAYGATE_POLYGON_AMOUNT_SEMANTICS
    )


def is_paygate_polygon_session(session) -> bool:
    return is_paygate_polygon_metadata(
        chain=getattr(session, "chain", ""),
        asset_code=getattr(session, "asset_code", ""),
        token_contract_address=getattr(session, "token_contract_address", ""),
        metadata=getattr(session, "metadata", None) or {},
    )


def get_paygate_polygon_credit_minimum_canonical(token_pack_snapshot: dict) -> int:
    if not isinstance(token_pack_snapshot, dict) or not token_pack_snapshot:
        raise ValidationError("PayGate Polygon session is missing token pack snapshot")
    policy = get_paygate_polygon_policy()
    try:
        basis = int(token_pack_snapshot.get(policy["coverage_basis"]) or 0)
        net_amount = int(token_pack_snapshot.get("net_stable_amount") or 0)
    except (TypeError, ValueError) as exc:
        raise ValidationError("Invalid PayGate Polygon token pack coverage amount") from exc
    if basis <= 0 or net_amount <= 0:
        raise ValidationError("PayGate Polygon token pack coverage amount must be positive")

    tolerated = (
        Decimal(basis)
        * Decimal(10000 - policy["underpayment_tolerance_bps"])
        / Decimal(10000)
    )
    tolerated_amount = max(1, int(tolerated.to_integral_value(rounding=ROUND_CEILING)))
    # Never credit below the immutable token value. The tolerance only absorbs
    # provider surcharge / price drift above the net token value.
    return max(net_amount, tolerated_amount)


def pol_wei_to_canonical_usd(raw_amount_wei: int, quote: dict) -> int:
    raw_amount_wei = int(raw_amount_wei)
    if raw_amount_wei <= 0:
        raise ValidationError("POL raw amount must be positive")
    normalized_quote = _normalize_quote_payload(quote, require_current=True)
    price = Decimal(normalized_quote["price"])
    policy = get_paygate_polygon_policy()
    native_base = Decimal(10) ** int(policy["native_decimals"])
    canonical_base = Decimal(10) ** CANONICAL_STABLE_DECIMALS
    value = Decimal(raw_amount_wei) * price * canonical_base / native_base
    canonical = int(value.to_integral_value(rounding=ROUND_FLOOR))
    if canonical <= 0:
        raise ValidationError("POL payment value rounds to zero canonical USD")
    return canonical


def canonical_usd_to_required_pol_wei(canonical_amount: int, quote: dict) -> int:
    canonical_amount = int(canonical_amount)
    if canonical_amount <= 0:
        raise ValidationError("Canonical coverage amount must be positive")
    normalized_quote = _normalize_quote_payload(quote, require_current=True)
    price = Decimal(normalized_quote["price"])
    policy = get_paygate_polygon_policy()
    native_base = Decimal(10) ** int(policy["native_decimals"])
    canonical_base = Decimal(10) ** CANONICAL_STABLE_DECIMALS
    raw = Decimal(canonical_amount) * native_base / (price * canonical_base)
    return max(1, int(raw.to_integral_value(rounding=ROUND_CEILING)))


def build_pol_valuation_metadata(*, raw_amount_wei: int, canonical_amount: int, quote: dict) -> dict:
    normalized_quote = _normalize_quote_payload(quote, require_current=True)
    return {
        "amount_semantics": PAYGATE_POLYGON_AMOUNT_SEMANTICS,
        "asset": PAYGATE_POLYGON_ASSET,
        "raw_amount_wei": str(int(raw_amount_wei)),
        "canonical_stable_amount": int(canonical_amount),
        "quote": normalized_quote,
        "valued_at": timezone.now().isoformat(),
    }
