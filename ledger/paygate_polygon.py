from __future__ import annotations

import json
from decimal import Decimal, ROUND_CEILING
from functools import lru_cache
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured, ValidationError

from ledger.native_quoted import (
    NATIVE_QUOTED_AMOUNT_SEMANTICS,
    build_native_valuation_metadata,
    canonical_usd_to_required_native_units,
    native_units_to_canonical_usd,
    normalize_native_usd_quote,
)


PAYGATE_POLYGON_CHAIN = "polygon"
PAYGATE_POLYGON_ASSET = "POL"
PAYGATE_POLYGON_TOKEN_CONTRACT = ""
PAYGATE_POLYGON_AMOUNT_SEMANTICS = NATIVE_QUOTED_AMOUNT_SEMANTICS
CANONICAL_STABLE_DECIMALS = 6


@lru_cache(maxsize=1)
def get_paygate_polygon_policy() -> dict:
    # This file now contains only PayGate settlement policy. Quote validation
    # and native amount conversion are delegated to ledger.native_quoted.
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
            raise ImproperlyConfigured(
                f"PayGate Polygon policy {key} must be an integer"
            ) from exc

    if payload["required_confirmations"] <= 0:
        raise ImproperlyConfigured(
            "PayGate Polygon required_confirmations must be positive"
        )
    if payload["quote_max_age_seconds"] <= 0:
        raise ImproperlyConfigured(
            "PayGate Polygon quote_max_age_seconds must be positive"
        )
    if payload["quote_future_skew_seconds"] < 0:
        raise ImproperlyConfigured(
            "PayGate Polygon quote_future_skew_seconds cannot be negative"
        )
    if not 0 <= payload["underpayment_tolerance_bps"] < 10000:
        raise ImproperlyConfigured(
            "PayGate Polygon underpayment_tolerance_bps must be in [0, 10000)"
        )

    coverage_basis = str(payload.get("coverage_basis") or "").strip()
    if coverage_basis not in {"net_stable_amount", "gross_stable_amount"}:
        raise ImproperlyConfigured(
            "PayGate Polygon coverage_basis must be net_stable_amount or gross_stable_amount"
        )
    payload["coverage_basis"] = coverage_basis

    try:
        observation_floor_wei = int(payload["observation_floor_wei"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ImproperlyConfigured(
            "PayGate Polygon observation_floor_wei must be an integer"
        ) from exc
    if observation_floor_wei <= 0:
        raise ImproperlyConfigured(
            "PayGate Polygon observation_floor_wei must be positive"
        )
    payload["observation_floor_wei"] = observation_floor_wei
    return payload


def normalize_pol_usd_quote(
    payload: dict,
    *,
    require_current: bool = True,
) -> dict:
    return normalize_native_usd_quote(
        payload,
        asset_code=PAYGATE_POLYGON_ASSET,
        require_current=require_current,
    )


def is_paygate_polygon_route(
    *,
    chain: str,
    asset_code: str,
    token_contract_address: str = "",
) -> bool:
    return (
        str(chain or "").strip().lower() == PAYGATE_POLYGON_CHAIN
        and str(asset_code or "").strip().upper() == PAYGATE_POLYGON_ASSET
        and not str(token_contract_address or "").strip()
    )


def is_paygate_polygon_metadata(
    *,
    chain: str,
    asset_code: str,
    token_contract_address: str,
    metadata,
) -> bool:
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


def get_paygate_polygon_credit_minimum_canonical(
    token_pack_snapshot: dict,
) -> int:
    if not isinstance(token_pack_snapshot, dict) or not token_pack_snapshot:
        raise ValidationError(
            "PayGate Polygon session is missing token pack snapshot"
        )
    policy = get_paygate_polygon_policy()
    try:
        basis = int(token_pack_snapshot.get(policy["coverage_basis"]) or 0)
        net_amount = int(token_pack_snapshot.get("net_stable_amount") or 0)
    except (TypeError, ValueError) as exc:
        raise ValidationError(
            "Invalid PayGate Polygon token pack coverage amount"
        ) from exc
    if basis <= 0 or net_amount <= 0:
        raise ValidationError(
            "PayGate Polygon token pack coverage amount must be positive"
        )

    tolerated = (
        Decimal(basis)
        * Decimal(10000 - policy["underpayment_tolerance_bps"])
        / Decimal(10000)
    )
    tolerated_amount = max(
        1,
        int(tolerated.to_integral_value(rounding=ROUND_CEILING)),
    )
    # PayGate tolerance may absorb provider surcharge / price drift, but it
    # must never credit below the immutable token value.
    return max(net_amount, tolerated_amount)


def pol_wei_to_canonical_usd(raw_amount_wei: int, quote: dict) -> int:
    return native_units_to_canonical_usd(
        raw_amount_wei,
        quote,
        asset_code=PAYGATE_POLYGON_ASSET,
    )


def canonical_usd_to_required_pol_wei(
    canonical_amount: int,
    quote: dict,
) -> int:
    return canonical_usd_to_required_native_units(
        canonical_amount,
        quote,
        asset_code=PAYGATE_POLYGON_ASSET,
    )


def build_pol_valuation_metadata(
    *,
    raw_amount_wei: int,
    canonical_amount: int,
    quote: dict,
) -> dict:
    generic = build_native_valuation_metadata(
        raw_amount=raw_amount_wei,
        canonical_amount=canonical_amount,
        quote=quote,
        asset_code=PAYGATE_POLYGON_ASSET,
    )
    # Compatibility shape for existing PayGate observations/admin tooling.
    return {
        "amount_semantics": generic["amount_semantics"],
        "asset": generic["asset"],
        "raw_amount_wei": str(int(raw_amount_wei)),
        "canonical_stable_amount": generic["canonical_stable_amount"],
        "quote": generic["quote"],
        "valued_at": generic["valued_at"],
    }
