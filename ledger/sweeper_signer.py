from __future__ import annotations

import json
import os
import time
import uuid
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, ValidationError

from ledger.internal_api import build_internal_request_signature


def _setting_str(name: str, default: str = "") -> str:
    return str(getattr(settings, name, os.environ.get(name, default)) or "").strip()


def _setting_float(name: str, default: float) -> float:
    value = getattr(settings, name, os.environ.get(name, default))
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(f"{name} must be numeric") from exc
    if parsed <= 0:
        raise ImproperlyConfigured(f"{name} must be > 0")
    return parsed


def _shared_secret() -> str:
    value = (
        _setting_str("LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET")
        or _setting_str("DFX_SWEEPER_SIGNER_SHARED_SECRET")
    )
    if not value:
        raise ImproperlyConfigured(
            "LEDGER_INTERNAL_SWEEPER_SERVICE_SHARED_SECRET is not configured"
        )
    return value


def _gateway_header() -> tuple[str, str]:
    secret = (
        _setting_str("MEDIACMS_INTERNAL_GATEWAY_SECRET")
        or _setting_str("LEDGER_INTERNAL_GATEWAY_SECRET")
    )
    name = (
        _setting_str("MEDIACMS_INTERNAL_GATEWAY_HEADER")
        or _setting_str("LEDGER_INTERNAL_GATEWAY_HEADER")
        or "X-Ledger-Internal-Gateway"
    )
    return name, secret


def sign_dfx_auth_message(
    *,
    chain: str,
    derivation_index: int,
    address: str,
) -> dict:
    base_url = _setting_str(
        "DFX_SWEEPER_SIGNER_BASE_URL",
        "http://sweeper_service:8080",
    ).rstrip("/")
    if not base_url:
        raise ImproperlyConfigured("DFX_SWEEPER_SIGNER_BASE_URL is not configured")

    service_name = _setting_str(
        "DFX_SWEEPER_SIGNER_SERVICE_NAME",
        "mediacms-web",
    )
    if not service_name:
        raise ImproperlyConfigured("DFX_SWEEPER_SIGNER_SERVICE_NAME is not configured")

    payload = {
        "chain": str(chain or "").strip().lower(),
        "derivation_index": int(derivation_index),
        "address": str(address or "").strip().lower(),
    }
    if payload["derivation_index"] < 0:
        raise ValidationError("DFX derivation index cannot be negative")
    if not payload["address"]:
        raise ValidationError("DFX deposit address is required")

    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timestamp = str(int(time.time()))
    nonce = uuid.uuid4().hex
    signature = build_internal_request_signature(
        service_name=service_name,
        timestamp=timestamp,
        nonce=nonce,
        body_bytes=body,
        shared_secret=_shared_secret(),
    )
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Ledger-Service": service_name,
        "X-Ledger-Timestamp": timestamp,
        "X-Ledger-Nonce": nonce,
        "X-Ledger-Signature": signature,
    }
    gateway_name, gateway_secret = _gateway_header()
    if gateway_secret:
        headers[gateway_name] = gateway_secret

    request = urllib_request.Request(
        f"{base_url}/v1/sign/dfx",
        data=body,
        method="POST",
        headers=headers,
    )
    try:
        with urllib_request.urlopen(
            request,
            timeout=_setting_float("DFX_SWEEPER_SIGNER_TIMEOUT_SECONDS", 10),
        ) as response:
            raw = response.read().decode("utf-8")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise ValidationError(
            f"DFX sweeper signer error {exc.code}: {error_body[:500]}"
        ) from exc
    except URLError as exc:
        raise ValidationError(
            f"DFX sweeper signer request failed: {exc.reason}"
        ) from exc

    try:
        result = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError("DFX sweeper signer returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise ValidationError("DFX sweeper signer response must be an object")

    returned_address = str(result.get("address") or "").strip().lower()
    returned_signature = str(result.get("signature") or "").strip()
    returned_message = str(result.get("message") or "")
    if returned_address != payload["address"]:
        raise ValidationError("DFX sweeper signer returned a different address")
    if not returned_signature.startswith("0x") or len(returned_signature) != 132:
        raise ValidationError("DFX sweeper signer returned an invalid EVM signature")
    if not returned_message.endswith(payload["address"]):
        raise ValidationError("DFX sweeper signer returned an invalid DFX message")

    return {
        "address": returned_address,
        "signature": returned_signature,
        "message": returned_message,
    }


__all__ = ["sign_dfx_auth_message"]
