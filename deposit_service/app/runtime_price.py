from __future__ import annotations

import datetime as dt
from decimal import Decimal, ROUND_CEILING

import httpx


POL_NATIVE_DECIMALS = 18
CANONICAL_STABLE_DECIMALS = 6


def _parse_iso8601(value, *, field_name: str) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise RuntimeError(f"{field_name} is required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise RuntimeError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise RuntimeError(f"{field_name} must include timezone")
    return parsed.astimezone(dt.timezone.utc)


def normalize_pol_usd_quote(
    payload: dict,
    *,
    max_age_seconds: int,
    future_skew_seconds: int,
) -> dict:
    if not isinstance(payload, dict):
        raise RuntimeError("POL/USD runtime price payload must be an object")
    if str(payload.get("asset") or "").strip().upper() != "POL":
        raise RuntimeError("Runtime price asset must be POL")
    if str(payload.get("currency") or "").strip().upper() != "USD":
        raise RuntimeError("Runtime price currency must be USD")

    try:
        price = Decimal(str(payload.get("price")))
    except Exception as exc:
        raise RuntimeError("POL/USD price is invalid") from exc
    if not price.is_finite() or price <= 0:
        raise RuntimeError("POL/USD price must be positive")

    source = str(payload.get("source") or "").strip()
    if not source:
        raise RuntimeError("POL/USD quote source is required")

    observed_at = _parse_iso8601(payload.get("observed_at"), field_name="observed_at")
    expires_at = _parse_iso8601(payload.get("expires_at"), field_name="expires_at")
    if expires_at <= observed_at:
        raise RuntimeError("POL/USD expires_at must be after observed_at")

    now = dt.datetime.now(dt.timezone.utc)
    age_seconds = (now - observed_at).total_seconds()
    if observed_at > now and abs(age_seconds) > int(future_skew_seconds):
        raise RuntimeError("POL/USD quote observed_at is too far in the future")
    if expires_at <= now:
        raise RuntimeError("POL/USD quote is expired")
    if age_seconds > int(max_age_seconds):
        raise RuntimeError("POL/USD quote is stale")

    return {
        "asset": "POL",
        "currency": "USD",
        "price": format(price, "f"),
        "source": source,
        "observed_at": observed_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }


def fetch_pol_usd_quote(
    *,
    base_url: str,
    shared_secret: str,
    timeout_seconds: float,
    max_age_seconds: int,
    future_skew_seconds: int,
) -> dict:
    url = f"{base_url.rstrip('/')}/ledger/runtime-prices/pol-usd"
    response = httpx.get(
        url,
        headers={"X-Internal-Shared-Secret": shared_secret},
        timeout=float(timeout_seconds),
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("POL/USD runtime price response is not valid JSON") from exc
    return normalize_pol_usd_quote(
        payload,
        max_age_seconds=max_age_seconds,
        future_skew_seconds=future_skew_seconds,
    )


def canonical_usd_to_required_pol_wei(canonical_amount: int, quote: dict) -> int:
    canonical_amount = int(canonical_amount)
    if canonical_amount <= 0:
        raise RuntimeError("Canonical coverage amount must be positive")
    price = Decimal(str(quote["price"]))
    native_base = Decimal(10) ** POL_NATIVE_DECIMALS
    canonical_base = Decimal(10) ** CANONICAL_STABLE_DECIMALS
    raw = Decimal(canonical_amount) * native_base / (price * canonical_base)
    return max(1, int(raw.to_integral_value(rounding=ROUND_CEILING)))
