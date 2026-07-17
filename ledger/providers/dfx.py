from __future__ import annotations

import json
import os
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from django.conf import settings
from django.core.cache import cache
from django.core.exceptions import ImproperlyConfigured, ValidationError

from ledger.fiat import get_fiat_usd_rate, normalize_fiat_currency
from ledger.providers.paygate import get_paygate_user_agent

DFX_PROVIDER_KEY = "dfx"
DFX_PAYMENT_METHOD_KEY = "dfx:bank"
DFX_PAYMENT_METHOD_TYPE = "provider"
DFX_PAYMENT_METHOD_LABEL = "Bank transfer (DFX)"
DFX_DEFAULT_API_BASE_URL = "https://api.dfx.swiss"
DFX_DEFAULT_APP_BASE_URL = "https://app.dfx.swiss"
DFX_DEFAULT_PAYMENT_TTL_SECONDS = 7 * 24 * 60 * 60
DFX_DEFAULT_CACHE_SECONDS = 300
DFX_DEFAULT_API_TIMEOUT_SECONDS = 20
DFX_DEFAULT_SETTLEMENT_ROUTE_PREFERENCES = (
    "arbitrum:USDC",
    "arbitrum:USDT",
    "base:USDC",
    "bsc:USDC",
    "bsc:USDT",
    "ethereum:USDC",
    "ethereum:USDT",
)

DFX_CHAIN_BY_MEDIACMS_CHAIN = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "bsc": "BinanceSmartChain",
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


def get_dfx_api_base_url() -> str:
    return _setting_str("DFX_API_BASE_URL", DFX_DEFAULT_API_BASE_URL).rstrip("/")


def get_dfx_app_base_url() -> str:
    return _setting_str("DFX_APP_BASE_URL", DFX_DEFAULT_APP_BASE_URL).rstrip("/")


def get_dfx_public_base_url() -> str:
    return (
        _setting_str("DFX_PUBLIC_BASE_URL")
        or _setting_str("FRONTEND_HOST")
        or _setting_str("SITE_URL")
    ).rstrip("/")


def get_dfx_fiat_currency() -> str:
    currency = normalize_fiat_currency(_setting_str("DFX_FIAT_CURRENCY", "EUR"))
    if currency not in {"EUR", "CHF"}:
        raise ImproperlyConfigured("DFX_FIAT_CURRENCY must be EUR or CHF")
    get_fiat_usd_rate(currency)
    return currency


def get_dfx_payment_method() -> str:
    value = _setting_str("DFX_PAYMENT_METHOD", "Bank").title()
    if value != "Bank":
        raise ImproperlyConfigured("Only DFX bank transfers are enabled by this integration")
    return value


def get_dfx_settlement_route_preferences() -> tuple[str, ...]:
    configured = getattr(
        settings,
        "DFX_SETTLEMENT_ROUTE_PREFERENCES",
        DFX_DEFAULT_SETTLEMENT_ROUTE_PREFERENCES,
    )

    if isinstance(configured, str):
        raw_values = configured.split(",")
    else:
        try:
            raw_values = list(configured)
        except TypeError as exc:
            raise ImproperlyConfigured(
                "DFX_SETTLEMENT_ROUTE_PREFERENCES must be a sequence or "
                "a comma-separated string"
            ) from exc

    preferences = []
    for raw_value in raw_values:
        value = str(raw_value or "").strip()
        if not value:
            continue

        parts = value.split(":")
        if len(parts) not in {2, 3}:
            raise ImproperlyConfigured(
                "Each DFX settlement route preference must be "
                "'chain:ASSET' or an exact MediaCMS route key"
            )

        chain = parts[0].strip().lower()
        asset_code = parts[1].strip().upper()
        if chain not in DFX_CHAIN_BY_MEDIACMS_CHAIN or not asset_code:
            raise ImproperlyConfigured(
                f"Invalid DFX settlement route preference: {value}"
            )

        normalized = f"{chain}:{asset_code}"
        if len(parts) == 3:
            contract = parts[2].strip().lower()
            if not contract:
                raise ImproperlyConfigured(
                    f"Invalid DFX settlement route preference: {value}"
                )
            normalized = f"{normalized}:{contract}"

        if normalized not in preferences:
            preferences.append(normalized)

    if not preferences:
        raise ImproperlyConfigured(
            "DFX_SETTLEMENT_ROUTE_PREFERENCES must contain at least one route"
        )

    return tuple(preferences)


def get_dfx_payment_ttl_seconds() -> int:
    return _setting_int(
        "DFX_PAYMENT_TTL_SECONDS",
        DFX_DEFAULT_PAYMENT_TTL_SECONDS,
        minimum=3600,
    )


def get_dfx_cache_seconds() -> int:
    return _setting_int("DFX_CACHE_SECONDS", DFX_DEFAULT_CACHE_SECONDS, minimum=1)


def get_dfx_api_timeout_seconds() -> int:
    return _setting_int(
        "DFX_API_TIMEOUT_SECONDS",
        DFX_DEFAULT_API_TIMEOUT_SECONDS,
        minimum=1,
    )


def get_dfx_wallet_name() -> str:
    return _setting_str("DFX_WALLET_NAME")


def get_dfx_language() -> str:
    language = _setting_str("DFX_LANGUAGE", "en").lower()
    if language not in {"en", "de", "fr", "it"}:
        raise ImproperlyConfigured("DFX_LANGUAGE must be en, de, fr, or it")
    return language


def get_dfx_chain_name(chain: str) -> str:
    normalized = str(chain or "").strip().lower()
    value = DFX_CHAIN_BY_MEDIACMS_CHAIN.get(normalized)
    if not value:
        raise ValidationError(f"DFX does not support MediaCMS chain: {normalized}")
    return value


def dfx_enabled() -> bool:
    if not _setting_bool("DFX_ENABLED", False):
        return False
    try:
        if not get_dfx_api_base_url():
            raise ImproperlyConfigured("DFX_API_BASE_URL is not configured")
        if not get_dfx_app_base_url():
            raise ImproperlyConfigured("DFX_APP_BASE_URL is not configured")
        if not get_dfx_public_base_url():
            raise ImproperlyConfigured("DFX_PUBLIC_BASE_URL or FRONTEND_HOST must be configured")
        get_dfx_fiat_currency()
        get_dfx_payment_method()
        get_dfx_settlement_route_preferences()
        get_dfx_payment_ttl_seconds()
        if not _setting_str("DFX_SWEEPER_SIGNER_BASE_URL"):
            raise ImproperlyConfigured("DFX_SWEEPER_SIGNER_BASE_URL is not configured")
    except ImproperlyConfigured:
        return False
    return True


def _decode_error_body(exc: HTTPError) -> str:
    try:
        raw = exc.read().decode("utf-8", errors="replace")
    except Exception:
        return ""
    try:
        payload = json.loads(raw)
    except Exception:
        return raw[:500]
    if isinstance(payload, dict):
        message = payload.get("message") or payload.get("error")
        if isinstance(message, list):
            message = "; ".join(str(item) for item in message)
        if message:
            return str(message)[:500]
    return raw[:500]


def call_dfx_json(
    *,
    method: str,
    path: str,
    payload: dict | None = None,
    params: dict | None = None,
    timeout: int | None = None,
    extra_headers: dict | None = None,
):
    query = urlencode(params or {}, doseq=True)
    normalized_path = path if path.startswith("/") else f"/{path}"
    url = f"{get_dfx_api_base_url()}{normalized_path}"
    if query:
        url = f"{url}?{query}"

    body = None
    headers = {"Accept": "application/json", "User-Agent": get_paygate_user_agent(),}
    if extra_headers:
        headers.update(
            {
                str(key): str(value)
                for key, value in extra_headers.items()
                if str(key).strip() and str(value).strip()
            }
        )
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
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
            timeout=timeout or get_dfx_api_timeout_seconds(),
        ) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = _decode_error_body(exc)
        suffix = f": {detail}" if detail else ""
        raise ValidationError(f"DFX API error {exc.code}{suffix}") from exc
    except URLError as exc:
        raise ValidationError(f"DFX API request failed: {exc.reason}") from exc

    try:
        return json.loads(raw_body or "null")
    except json.JSONDecodeError as exc:
        raise ValidationError(f"DFX API returned invalid JSON: {raw_body[:300]}") from exc


def _cache_key(kind: str, suffix: str = "") -> str:
    return f"dfx:{kind}:{get_dfx_api_base_url()}:{suffix}"


def get_dfx_fiats() -> list[dict]:
    key = _cache_key("fiats")
    cached = cache.get(key)
    if isinstance(cached, list):
        return cached

    payload = call_dfx_json(method="GET", path="/v1/fiat")
    if not isinstance(payload, list):
        raise ValidationError("DFX fiat response must be a list")
    rows = [dict(item) for item in payload if isinstance(item, dict)]
    cache.set(key, rows, get_dfx_cache_seconds())
    return rows


def get_dfx_fiat(currency: str | None = None) -> dict:
    normalized = normalize_fiat_currency(currency or get_dfx_fiat_currency())
    for item in get_dfx_fiats():
        if str(item.get("name") or "").strip().upper() == normalized:
            return item
    raise ValidationError(f"DFX does not expose fiat currency {normalized}")


def get_dfx_assets_for_blockchain(blockchain: str) -> list[dict]:
    normalized = str(blockchain or "").strip()
    if not normalized:
        raise ValidationError("DFX blockchain is required")
    key = _cache_key("assets", normalized)
    cached = cache.get(key)
    if isinstance(cached, list):
        return cached

    payload = call_dfx_json(
        method="GET",
        path="/v1/asset",
        params={"blockchains": normalized},
    )
    if not isinstance(payload, list):
        raise ValidationError("DFX asset response must be a list")
    rows = [dict(item) for item in payload if isinstance(item, dict)]
    cache.set(key, rows, get_dfx_cache_seconds())
    return rows


def _asset_names(asset: dict) -> set[str]:
    names = set()
    for key in ("name", "dexName", "uniqueName"):
        value = str(asset.get(key) or "").strip()
        if not value:
            continue
        names.add(value.upper())
        if "/" in value:
            names.add(value.rsplit("/", 1)[-1].upper())
    return names


def find_dfx_asset_for_route(
    *,
    chain: str,
    asset_code: str,
    token_contract_address: str,
    assets: list[dict] | None = None,
) -> dict | None:
    blockchain = get_dfx_chain_name(chain)
    normalized_asset = str(asset_code or "").strip().upper()
    normalized_contract = str(token_contract_address or "").strip().lower()
    candidates = assets if assets is not None else get_dfx_assets_for_blockchain(blockchain)

    for asset in candidates:
        if not isinstance(asset, dict):
            continue
        if str(asset.get("blockchain") or "").strip().lower() != blockchain.lower():
            continue
        if not bool(asset.get("buyable")) or bool(asset.get("comingSoon")):
            continue

        dfx_contract = str(asset.get("chainId") or "").strip().lower()
        if normalized_contract:
            if dfx_contract != normalized_contract:
                continue
        elif dfx_contract:
            continue

        if normalized_asset not in _asset_names(asset):
            continue
        if asset.get("id") in (None, ""):
            continue
        return dict(asset)

    return None


def get_dfx_bank_limits(fiat: dict | None = None) -> tuple[Decimal, Decimal]:
    fiat = fiat or get_dfx_fiat()
    limits = fiat.get("limits") or {}
    bank = {}
    if isinstance(limits, dict):
        for key, value in limits.items():
            if str(key).strip().lower() == "bank" and isinstance(value, dict):
                bank = value
                break

    try:
        minimum = Decimal(str(bank.get("minVolume") or "0"))
        maximum = Decimal(str(bank.get("maxVolume") or "0"))
    except (InvalidOperation, ValueError) as exc:
        raise ValidationError("DFX returned invalid bank transfer limits") from exc

    if minimum < 0 or maximum < 0:
        raise ValidationError("DFX returned negative bank transfer limits")
    return minimum, maximum


def canonical_stable_to_dfx_target_amount(value: int) -> Decimal:
    try:
        canonical = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError("DFX target amount must be canonical stable units") from exc
    if canonical <= 0:
        raise ValidationError("DFX target amount must be positive")
    return Decimal(canonical) / Decimal(1_000_000)


def get_dfx_buy_quote(
    *,
    asset_id: int,
    target_canonical_amount: int,
    fiat_currency: str | None = None,
) -> dict:
    currency = normalize_fiat_currency(fiat_currency or get_dfx_fiat_currency())
    target_amount = canonical_stable_to_dfx_target_amount(target_canonical_amount)
    payload = {
        "currency": {"name": currency},
        "asset": {"id": int(asset_id)},
        "targetAmount": float(target_amount),
        "paymentMethod": get_dfx_payment_method(),
    }
    wallet_name = get_dfx_wallet_name()
    if wallet_name:
        payload["wallet"] = wallet_name

    response = call_dfx_json(
        method="PUT",
        path="/v1/buy/quote",
        payload=payload,
    )
    if not isinstance(response, dict):
        raise ValidationError("DFX quote response must be an object")
    if response.get("isValid") is not True:
        errors = response.get("errors") or response.get("error") or "Quote unavailable"
        raise ValidationError(f"DFX quote is invalid: {errors}")

    try:
        source_amount = Decimal(str(response.get("amount")))
        estimated_amount = Decimal(str(response.get("estimatedAmount")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("DFX quote is missing valid amounts") from exc

    if source_amount <= 0 or estimated_amount <= 0:
        raise ValidationError("DFX quote returned non-positive amounts")

    normalized = dict(response)
    normalized["requestedTargetAmount"] = format(target_amount, "f")
    normalized["sourceAmount"] = format(source_amount, "f")
    normalized["estimatedTargetAmount"] = format(estimated_amount, "f")
    return normalized


def round_dfx_source_amount(value) -> str:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("Invalid DFX source amount") from exc
    if amount <= 0:
        raise ValidationError("DFX source amount must be positive")
    return format(amount.quantize(Decimal("0.01"), rounding=ROUND_CEILING), "f")


def build_dfx_auth_payload(
    *,
    address: str,
    signature: str,
    chain: str,
) -> dict:
    normalized_address = str(address or "").strip()
    normalized_signature = str(signature or "").strip()
    if not normalized_address or not normalized_signature:
        raise ValidationError("DFX address and signature are required")

    payload = {
        "address": normalized_address,
        "signature": normalized_signature,
        "blockchain": get_dfx_chain_name(chain),
        "language": get_dfx_language().upper(),
    }
    wallet_name = get_dfx_wallet_name()
    if wallet_name:
        payload["wallet"] = wallet_name
    return payload


def get_dfx_auth_url() -> str:
    return f"{get_dfx_api_base_url()}/v1/auth"


def build_dfx_checkout_params(
    *,
    asset: dict,
    chain: str,
    fiat_currency: str,
    source_amount,
    external_transaction_id: str,
    redirect_uri: str,
    customer_email: str = "",
) -> dict:
    asset_reference = asset.get("id") or asset.get("uniqueName")
    if asset_reference in (None, ""):
        raise ValidationError("DFX asset reference is missing")

    params = {
        "lang": get_dfx_language(),
        "asset-out": str(asset_reference),
        "assets": str(asset_reference),
        "blockchain": get_dfx_chain_name(chain),
        "blockchains": get_dfx_chain_name(chain),
        "asset-in": normalize_fiat_currency(fiat_currency),
        "amount-in": round_dfx_source_amount(source_amount),
        "payment-method": "bank",
        "external-transaction-id": str(external_transaction_id),
        "redirect-uri": str(redirect_uri),
        "headless": "true",
        "borderless": "true",
    }
    email = str(customer_email or "").strip()
    if email:
        params["mail"] = email
    wallet_name = get_dfx_wallet_name()
    if wallet_name:
        params["wallet"] = wallet_name
    return params

def build_dfx_checkout_url(
    *,
    access_token: str,
    asset: dict,
    chain: str,
    fiat_currency: str,
    source_amount,
    external_transaction_id: str,
    redirect_uri: str,
    customer_email: str = "",
) -> str:
    token = str(access_token or "").strip()
    if not token:
        raise ValidationError("DFX access token is required")
    params = build_dfx_checkout_params(
        asset=asset,
        chain=chain,
        fiat_currency=fiat_currency,
        source_amount=source_amount,
        external_transaction_id=external_transaction_id,
        redirect_uri=redirect_uri,
        customer_email=customer_email,
    )
    params["session"] = token
    return f"{get_dfx_app_base_url()}/buy?{urlencode(params)}"


__all__ = [
    "DFX_PAYMENT_METHOD_KEY",
    "DFX_PAYMENT_METHOD_LABEL",
    "DFX_PAYMENT_METHOD_TYPE",
    "DFX_PROVIDER_KEY",
    "build_dfx_auth_payload",
    "build_dfx_checkout_params",
    "build_dfx_checkout_url",
    "canonical_stable_to_dfx_target_amount",
    "dfx_enabled",
    "find_dfx_asset_for_route",
    "get_dfx_app_base_url",
    "get_dfx_assets_for_blockchain",
    "get_dfx_auth_url",
    "get_dfx_bank_limits",
    "get_dfx_buy_quote",
    "get_dfx_chain_name",
    "get_dfx_fiat",
    "get_dfx_fiat_currency",
    "get_dfx_payment_ttl_seconds",
    "get_dfx_settlement_route_preferences",
    "get_dfx_public_base_url",
    "round_dfx_source_amount",
]
