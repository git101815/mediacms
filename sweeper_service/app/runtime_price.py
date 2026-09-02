from __future__ import annotations

import datetime as dt
from decimal import Decimal, ROUND_CEILING

import httpx


CANONICAL_STABLE_DECIMALS = 6
NATIVE_ASSET_DECIMALS = {
    "ETH": 18,
    "BNB": 18,
    "POL": 18,
}


def _native_decimals(asset_code: str) -> int:
    asset = str(asset_code or "").strip().upper()
    try:
        return int(NATIVE_ASSET_DECIMALS[asset])
    except KeyError as exc:
        raise RuntimeError(f"Unsupported native runtime-price asset: {asset}") from exc


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


def normalize_native_usd_quote(
    payload: dict,
    *,
    asset_code: str,
    max_age_seconds: int,
    future_skew_seconds: int,
) -> dict:
    asset = str(asset_code or "").strip().upper()
    _native_decimals(asset)

    if not isinstance(payload, dict):
        raise RuntimeError(f"{asset}/USD runtime price payload must be an object")
    if str(payload.get("asset") or "").strip().upper() != asset:
        raise RuntimeError(f"Runtime price asset must be {asset}")
    if str(payload.get("currency") or "").strip().upper() != "USD":
        raise RuntimeError("Runtime price currency must be USD")

    try:
        price = Decimal(str(payload.get("price")))
    except Exception as exc:
        raise RuntimeError(f"{asset}/USD price is invalid") from exc
    if not price.is_finite() or price <= 0:
        raise RuntimeError(f"{asset}/USD price must be positive")

    source = str(payload.get("source") or "").strip()
    if not source:
        raise RuntimeError(f"{asset}/USD quote source is required")

    observed_at = _parse_iso8601(
        payload.get("observed_at"),
        field_name="observed_at",
    )
    expires_at = _parse_iso8601(
        payload.get("expires_at"),
        field_name="expires_at",
    )
    if expires_at <= observed_at:
        raise RuntimeError(
            f"{asset}/USD expires_at must be after observed_at"
        )

    now = dt.datetime.now(dt.timezone.utc)
    age_seconds = (now - observed_at).total_seconds()
    if (
        observed_at > now
        and abs(age_seconds) > int(future_skew_seconds)
    ):
        raise RuntimeError(
            f"{asset}/USD quote observed_at is too far in the future"
        )
    if expires_at <= now:
        raise RuntimeError(f"{asset}/USD quote is expired")
    if age_seconds > int(max_age_seconds):
        raise RuntimeError(f"{asset}/USD quote is stale")

    return {
        "asset": asset,
        "currency": "USD",
        "price": format(price, "f"),
        "source": source,
        "observed_at": observed_at.isoformat(),
        "expires_at": expires_at.isoformat(),
    }


def fetch_native_usd_quote(
    *,
    asset_code: str,
    base_url: str,
    shared_secret: str,
    timeout_seconds: float,
    max_age_seconds: int,
    future_skew_seconds: int,
) -> dict:
    asset = str(asset_code or "").strip().upper()
    _native_decimals(asset)
    url = (
        f"{base_url.rstrip('/')}/ledger/runtime-prices/"
        f"{asset.lower()}-usd"
    )
    response = httpx.get(
        url,
        headers={"X-Internal-Shared-Secret": shared_secret},
        timeout=float(timeout_seconds),
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{asset}/USD runtime price response is not valid JSON"
        ) from exc

    return normalize_native_usd_quote(
        payload,
        asset_code=asset,
        max_age_seconds=max_age_seconds,
        future_skew_seconds=future_skew_seconds,
    )


def canonical_usd_to_required_native_units(
    canonical_amount: int,
    quote: dict,
    *,
    asset_code: str,
) -> int:
    canonical_amount = int(canonical_amount)
    asset = str(asset_code or "").strip().upper()
    if canonical_amount <= 0:
        raise RuntimeError("Canonical coverage amount must be positive")

    price = Decimal(str(quote["price"]))
    native_base = Decimal(10) ** _native_decimals(asset)
    canonical_base = Decimal(10) ** CANONICAL_STABLE_DECIMALS
    raw = Decimal(canonical_amount) * native_base / (
        price * canonical_base
    )
    return max(
        1,
        int(raw.to_integral_value(rounding=ROUND_CEILING)),
    )


# Compatibility wrappers retained for the existing PayGate-specific tests and
# any local tooling that imported the old POL names.
def normalize_pol_usd_quote(
    payload: dict,
    *,
    max_age_seconds: int,
    future_skew_seconds: int,
) -> dict:
    return normalize_native_usd_quote(
        payload,
        asset_code="POL",
        max_age_seconds=max_age_seconds,
        future_skew_seconds=future_skew_seconds,
    )


def fetch_pol_usd_quote(
    *,
    base_url: str,
    shared_secret: str,
    timeout_seconds: float,
    max_age_seconds: int,
    future_skew_seconds: int,
) -> dict:
    return fetch_native_usd_quote(
        asset_code="POL",
        base_url=base_url,
        shared_secret=shared_secret,
        timeout_seconds=timeout_seconds,
        max_age_seconds=max_age_seconds,
        future_skew_seconds=future_skew_seconds,
    )


def canonical_usd_to_required_pol_wei(
    canonical_amount: int,
    quote: dict,
) -> int:
    return canonical_usd_to_required_native_units(
        canonical_amount,
        quote,
        asset_code="POL",
    )
