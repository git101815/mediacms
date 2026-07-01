from __future__ import annotations

import hashlib
import hmac
import json
import os
import uuid
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from .internal_api import _get_internal_deposit_service_actor
from .models import DepositSession, LEDGER_METADATA_VERSION, TokenPack, TokenWallet
from .services import (
    ACTIVE_DEPOSIT_SESSION_STATUSES,
    LEDGER_OPERATION_FLAG_CREDITING,
    LEDGER_OPERATION_FLAG_DEPOSIT_OPEN,
    PLATFORM_TOKEN_DECIMALS,
    PLATFORM_TOKENS_PER_STABLECOIN,
    STABLECOIN_CANONICAL_DECIMALS,
    _build_token_pack_snapshot,
    _convert_canonical_stable_to_platform_tokens,
    _enforce_deposit_open_cooldown,
    _require_authenticated_actor,
    _require_perm,
    _require_wallet_not_blocked,
    _resolve_created_by,
    apply_ledger_transaction,
    get_external_asset_clearing_wallet,
    get_system_wallet,
    require_ledger_operation_enabled,
)

MALUM_PROVIDER_KEY = "malum"
MALUM_CHAIN = "malum"
MALUM_ROUTE_SLUG = "hosted_checkout"
MALUM_PAYMENT_METHOD_KEY = "malum:hosted_checkout"
MALUM_PAYMENT_METHOD_TYPE = "provider"
MALUM_PAYMENT_METHOD_LABEL = "Card / PayPal"
MALUM_NETWORK_DISPLAY = "Hosted checkout"
MALUM_ROUTE_KEY_PREFIX = "malum"
MALUM_DEFAULT_API_BASE_URL = "https://malum.to"
MALUM_DEFAULT_PAYMENT_TTL_SECONDS = 60 * 60
MALUM_MIN_CANONICAL_STABLE_AMOUNT = 50_000  # $0.50, canonical stable units with 6 decimals.
MALUM_CREATE_PAYMENT_PATH = "/api/v2/payment/create"
MALUM_LOOKUP_PATH = "/api/v2/payment/lookup"

MALUM_STATUS_CREATED = "CREATED"
MALUM_STATUS_PROCESSING = "PROCESSING"
MALUM_STATUS_COMPLETED = "COMPLETED"
MALUM_STATUS_EXPIRED = "EXPIRED"
MALUM_STATUS_CANCELLED = "CANCELLED"
MALUM_STATUS_CANCELED = "CANCELED"

MALUM_ACTIVE_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_CONFIRMING,
}


def _setting_bool(name: str, default: bool = False) -> bool:
    value = getattr(settings, name, os.environ.get(name, default))
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _setting_str(name: str, default: str = "") -> str:
    return str(getattr(settings, name, os.environ.get(name, default)) or "").strip()


def _get_malum_currency() -> str:
    value = _setting_str("MALUM_CURRENCY", "USD").upper()
    if not value:
        raise ImproperlyConfigured("MALUM_CURRENCY is not configured")
    return value


def _get_malum_api_base_url() -> str:
    return _setting_str("MALUM_API_BASE_URL", MALUM_DEFAULT_API_BASE_URL).rstrip("/")


def _get_malum_public_base_url() -> str:
    return (
        _setting_str("MALUM_PUBLIC_BASE_URL")
        or _setting_str("FRONTEND_HOST")
        or _setting_str("SITE_URL")
    ).rstrip("/")


def _get_malum_payment_ttl_seconds() -> int:
    try:
        value = int(getattr(settings, "MALUM_PAYMENT_TTL_SECONDS", MALUM_DEFAULT_PAYMENT_TTL_SECONDS))
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured("MALUM_PAYMENT_TTL_SECONDS must be an integer") from exc
    return max(300, value)


def _get_malum_merchant_id() -> str:
    value = _setting_str("MALUM_MERCHANT_ID")
    if not value:
        raise ImproperlyConfigured("MALUM_MERCHANT_ID is not configured")
    return value


def _get_malum_private_key() -> str:
    value = _setting_str("MALUM_PRIVATE_KEY")
    if not value:
        raise ImproperlyConfigured("MALUM_PRIVATE_KEY is not configured")
    return value


def _malum_private_key_is_sandbox() -> bool:
    return _get_malum_private_key().startswith("sbx_")


def _get_malum_webhook_key(*, sandbox: bool) -> str:
    if sandbox:
        value = _setting_str("MALUM_SANDBOX_WEBHOOK_KEY")
        if not value:
            raise ImproperlyConfigured("MALUM_SANDBOX_WEBHOOK_KEY is not configured")
        return value

    value = _setting_str("MALUM_WEBHOOK_KEY")
    if not value:
        raise ImproperlyConfigured("MALUM_WEBHOOK_KEY is not configured")
    return value


def _malum_auth_header() -> str:
    configured = _setting_str("MALUM_AUTH_HEADER")
    if configured:
        return configured
    return f"{_get_malum_merchant_id()}:{_get_malum_private_key()}"


def malum_enabled() -> bool:
    if not _setting_bool("MALUM_ENABLED", False):
        return False
    try:
        _get_malum_merchant_id()
        private_key = _get_malum_private_key()
        _get_malum_webhook_key(sandbox=private_key.startswith("sbx_"))
        _get_malum_public_base_url()
    except ImproperlyConfigured:
        return False
    return True


def _canonical_stable_to_decimal(value: int) -> Decimal:
    return Decimal(int(value)) / (Decimal(10) ** STABLECOIN_CANONICAL_DECIMALS)


def _canonical_stable_to_malum_amount(value: int) -> float:
    amount = _canonical_stable_to_decimal(value)
    return float(amount)


def _malum_amount_to_canonical_stable_units(value) -> int:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise ValidationError("Invalid Malum amount") from exc

    if parsed <= 0:
        raise ValidationError("Malum amount must be positive")

    scaled = parsed * (Decimal(10) ** STABLECOIN_CANONICAL_DECIMALS)
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def _format_canonical_stable_for_display(value: int) -> str:
    text = format(_canonical_stable_to_decimal(value), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _malum_route_key(currency: str | None = None) -> str:
    return f"{MALUM_ROUTE_KEY_PREFIX}:{(currency or _get_malum_currency()).lower()}:{MALUM_ROUTE_SLUG}"


def get_malum_deposit_option() -> dict | None:
    if not malum_enabled():
        return None

    currency = _get_malum_currency()
    return {
        "key": _malum_route_key(currency),
        "label": MALUM_PAYMENT_METHOD_LABEL,
        "route_label": MALUM_PAYMENT_METHOD_LABEL,
        "network_label": MALUM_NETWORK_DISPLAY,
        "network_display": MALUM_NETWORK_DISPLAY,
        "chain": MALUM_CHAIN,
        "asset_code": currency,
        "token_contract_address": "",
        "required_confirmations": 1,
        "min_amount": MALUM_MIN_CANONICAL_STABLE_AMOUNT,
        "onchain_min_amount": str(MALUM_MIN_CANONICAL_STABLE_AMOUNT),
        "amount_unit": "canonical_stable",
        "onchain_amount_unit": "provider_amount",
        "min_amount_display": _format_canonical_stable_for_display(MALUM_MIN_CANONICAL_STABLE_AMOUNT),
        "session_ttl_seconds": _get_malum_payment_ttl_seconds(),
        "network_slug": MALUM_CHAIN,
        "payment_method_key": MALUM_PAYMENT_METHOD_KEY,
        "payment_method_label": MALUM_PAYMENT_METHOD_LABEL,
        "payment_method_type": MALUM_PAYMENT_METHOD_TYPE,
        "provider_key": MALUM_PROVIDER_KEY,
    }


def is_malum_deposit_option_key(option_key: str) -> bool:
    return (option_key or "").strip() == _malum_route_key()


def _build_absolute_url(path: str) -> str:
    base_url = _get_malum_public_base_url()
    if not base_url:
        raise ImproperlyConfigured("MALUM_PUBLIC_BASE_URL or FRONTEND_HOST must be configured")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def _provider_metadata_for_session(
    *,
    session_public_id,
    reference: str = "",
    checkout_url: str = "",
    status: str = "",
    raw_payload=None,
) -> dict:
    provider = {
        "key": MALUM_PROVIDER_KEY,
        "label": MALUM_PAYMENT_METHOD_LABEL,
        "payment_method_key": MALUM_PAYMENT_METHOD_KEY,
        "payment_method_type": MALUM_PAYMENT_METHOD_TYPE,
        "route_key": _malum_route_key(),
        "reference": (reference or "").strip(),
        "checkout_url": (checkout_url or "").strip(),
        "status": (status or "").strip().upper(),
        "session_public_id": str(session_public_id),
        "sandbox": _malum_private_key_is_sandbox(),
    }
    if raw_payload is not None:
        provider["raw_payload"] = raw_payload
    return provider


def _build_malum_metadata_value(*, session_public_id, token_pack_code: str) -> str:
    payload = {
        "deposit_session_public_id": str(session_public_id),
        "token_pack_code": (token_pack_code or "").strip(),
        "provider": MALUM_PROVIDER_KEY,
    }
    value = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(value) > 254:
        raise ValidationError("Malum metadata payload is too large")
    return value


def _parse_malum_metadata_value(value) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _find_reusable_malum_session(*, wallet: TokenWallet, token_pack_code: str) -> DepositSession | None:
    now = timezone.now()
    candidates = (
        DepositSession.objects.select_for_update()
        .filter(
            wallet=wallet,
            chain=MALUM_CHAIN,
            route_key=_malum_route_key(),
            status__in=MALUM_ACTIVE_STATUSES,
            expires_at__gt=now,
        )
        .order_by("-created_at")
    )

    normalized_pack_code = (token_pack_code or "").strip()
    for session in candidates:
        metadata = session.metadata or {}
        snapshot = metadata.get("token_pack") or {}
        provider = metadata.get("payment_provider") or {}
        if provider.get("key") != MALUM_PROVIDER_KEY:
            continue
        if normalized_pack_code and (snapshot.get("code") or "").strip() != normalized_pack_code:
            continue
        return session
    return None


def _call_malum_json(*, method: str, path: str, payload: dict | None = None, timeout: int = 20) -> dict:
    body = b""
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    request = urllib_request.Request(
        f"{_get_malum_api_base_url()}{path}",
        data=body if method.upper() in {"POST", "PUT", "PATCH"} else None,
        method=method.upper(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "MALUM": _malum_auth_header(),
        },
    )

    try:
        with urllib_request.urlopen(request, timeout=timeout) as response:
            raw_body = response.read().decode("utf-8")
    except HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise ValidationError(f"Malum API error {exc.code}: {error_body[:500]}") from exc
    except URLError as exc:
        raise ValidationError(f"Malum API request failed: {exc.reason}") from exc

    try:
        parsed = json.loads(raw_body or "{}")
    except json.JSONDecodeError as exc:
        raise ValidationError("Malum API returned invalid JSON") from exc

    if not isinstance(parsed, dict):
        raise ValidationError("Malum API returned an invalid response")
    return parsed


def create_malum_payment(
    *,
    amount_canonical_stable: int,
    currency: str,
    customer_email: str,
    webhook_url: str,
    success_url: str,
    cancel_url: str,
    metadata: str,
    product_title: str,
) -> dict:
    payload = {
        "amount": _canonical_stable_to_malum_amount(amount_canonical_stable),
        "currency": currency,
        "customer_email": customer_email,
        "webhook_url": webhook_url,
        "success_url": success_url,
        "cancel_url": cancel_url,
        "metadata": metadata,
        "product_title": product_title[:120],
    }

    if _setting_bool("MALUM_BUYER_PAYS_FEES", False):
        payload["buyer_pays_fees"] = True
    if _setting_bool("MALUM_MERCHANT_PAYS_GW_FEES", False):
        payload["merchant_pays_gw_fees"] = True

    response = _call_malum_json(
        method="POST",
        path=MALUM_CREATE_PAYMENT_PATH,
        payload=payload,
    )

    if str(response.get("status") or "").lower() != "success":
        message = response.get("message") or response.get("error") or "Malum payment creation failed"
        raise ValidationError(str(message))

    transaction_id = str(response.get("transaction_id") or "").strip()
    checkout_url = str(response.get("link") or "").strip()
    if not transaction_id or not checkout_url:
        raise ValidationError("Malum payment response is missing transaction_id or link")

    return response


def lookup_malum_payment(*, transaction_id: str) -> dict:
    normalized_txn = (transaction_id or "").strip()
    if not normalized_txn:
        raise ValidationError("Malum transaction id is required")
    return _call_malum_json(
        method="GET",
        path=f"{MALUM_LOOKUP_PATH}?txid={normalized_txn}",
        payload=None,
    )


@transaction.atomic
def open_malum_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    token_pack: TokenPack,
) -> DepositSession:
    actor = _require_authenticated_actor(actor)
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_DEPOSIT_OPEN)

    if not malum_enabled():
        raise ValidationError("Card payments are temporarily unavailable")

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    _require_wallet_not_blocked(wallet)

    if wallet.wallet_type != TokenWallet.TYPE_USER:
        raise ValidationError("Deposit sessions can only target user wallets")

    if wallet.user_id != actor.id:
        raise PermissionDenied("Cannot open a deposit session for another user's wallet")

    customer_email = (getattr(actor, "email", "") or "").strip()
    if not customer_email:
        raise ValidationError("A verified email address is required for card payments")

    token_pack_snapshot = _build_token_pack_snapshot(token_pack=token_pack)
    expected_canonical_amount = int(token_pack_snapshot["gross_stable_amount"])
    if expected_canonical_amount < MALUM_MIN_CANONICAL_STABLE_AMOUNT:
        raise ValidationError("Selected token pack is below Malum's minimum payment amount")

    existing_session = _find_reusable_malum_session(
        wallet=wallet,
        token_pack_code=token_pack_snapshot["code"],
    )
    if existing_session is not None:
        return existing_session

    _enforce_deposit_open_cooldown(user=wallet.user)

    public_id = uuid.uuid4()
    currency = _get_malum_currency()
    route_key = _malum_route_key(currency)
    synthetic_ref = f"malum:{public_id.hex}"
    now = timezone.now()
    expires_at = now + timedelta(seconds=_get_malum_payment_ttl_seconds())
    session_path = reverse("wallet_deposit_session", kwargs={"public_id": public_id})

    metadata = {
        "display_label": MALUM_PAYMENT_METHOD_LABEL,
        "allocation_source": "provider_checkout",
        "chain_family": "provider",
        "token_pack": token_pack_snapshot,
        "payment_method": {
            "key": MALUM_PAYMENT_METHOD_KEY,
            "type": MALUM_PAYMENT_METHOD_TYPE,
            "label": MALUM_PAYMENT_METHOD_LABEL,
            "show_network_step": False,
        },
        "payment_provider": _provider_metadata_for_session(session_public_id=public_id),
        "amount_unit": "canonical_stable",
        "expected_canonical_stable_amount": int(expected_canonical_amount),
        "stablecoin_canonical_decimals": STABLECOIN_CANONICAL_DECIMALS,
        "platform_token_decimals": PLATFORM_TOKEN_DECIMALS,
        "platform_tokens_per_stablecoin": PLATFORM_TOKENS_PER_STABLECOIN,
    }

    deposit_session = DepositSession.objects.create(
        public_id=public_id,
        user=wallet.user,
        wallet=wallet,
        chain=MALUM_CHAIN,
        asset_code=currency,
        token_contract_address="",
        route_key=route_key,
        display_label=MALUM_PAYMENT_METHOD_LABEL,
        deposit_address=synthetic_ref,
        address_derivation_ref=synthetic_ref,
        derivation_index=None,
        derivation_path="",
        status=DepositSession.STATUS_AWAITING_PAYMENT,
        min_amount=int(expected_canonical_amount),
        expected_onchain_raw_amount=int(expected_canonical_amount),
        required_confirmations=1,
        expires_at=expires_at,
        created_by=actor,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )

    malum_metadata = _build_malum_metadata_value(
        session_public_id=public_id,
        token_pack_code=token_pack_snapshot["code"],
    )

    try:
        payment_response = create_malum_payment(
            amount_canonical_stable=expected_canonical_amount,
            currency=currency,
            customer_email=customer_email,
            webhook_url=_build_absolute_url(reverse("malum_webhook")),
            success_url=_build_absolute_url(session_path),
            cancel_url=_build_absolute_url(session_path),
            metadata=malum_metadata,
            product_title=f"{token_pack_snapshot.get('name') or 'Token pack'} tokens",
        )
    except Exception as exc:
        failed_metadata = dict(deposit_session.metadata or {})
        provider = dict(failed_metadata.get("payment_provider") or {})
        provider.update(
            {
                "status": "CREATE_FAILED",
                "last_error": str(exc)[:1000],
                "last_error_at": timezone.now().isoformat(),
            }
        )
        failed_metadata["payment_provider"] = provider
        deposit_session.status = DepositSession.STATUS_FAILED
        deposit_session.metadata = failed_metadata
        deposit_session.save(update_fields=["status", "metadata", "updated_at"])
        raise

    transaction_id = str(payment_response["transaction_id"]).strip()
    checkout_url = str(payment_response["link"]).strip()

    metadata = dict(deposit_session.metadata or {})
    metadata["payment_provider"] = _provider_metadata_for_session(
        session_public_id=deposit_session.public_id,
        reference=transaction_id,
        checkout_url=checkout_url,
        status=MALUM_STATUS_CREATED,
        raw_payload=payment_response,
    )
    deposit_session.metadata = metadata
    deposit_session.save(update_fields=["metadata", "updated_at"])
    return deposit_session


def verify_malum_webhook_signature(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise PermissionDenied("Invalid Malum webhook payload")

    txn = str(payload.get("txn") or "").strip()
    timestamp = str(payload.get("timestamp") or "").strip()
    provided_signature = str(payload.get("signature") or "").strip().lower()
    sandbox = bool(payload.get("sandbox"))

    if not txn or not timestamp or not provided_signature:
        raise PermissionDenied("Missing Malum webhook signature fields")

    webhook_key = _get_malum_webhook_key(sandbox=sandbox)
    expected_signature = hashlib.md5(f"{txn}|{timestamp}|{webhook_key}".encode("utf-8")).hexdigest()

    if not hmac.compare_digest(provided_signature, expected_signature):
        raise PermissionDenied("Invalid Malum webhook signature")


def _get_malum_session_from_payload(payload: dict) -> DepositSession:
    txn = str(payload.get("txn") or "").strip()
    metadata = _parse_malum_metadata_value(payload.get("metadata"))
    public_id = str(metadata.get("deposit_session_public_id") or "").strip()

    if public_id:
        try:
            return DepositSession.objects.get(public_id=public_id, chain=MALUM_CHAIN)
        except DepositSession.DoesNotExist as exc:
            raise ValidationError("Unknown Malum deposit session") from exc

    if txn:
        session = (
            DepositSession.objects.filter(
                chain=MALUM_CHAIN,
                metadata__payment_provider__key=MALUM_PROVIDER_KEY,
                metadata__payment_provider__reference=txn,
            )
            .order_by("-created_at")
            .first()
        )
        if session is not None:
            return session

    raise ValidationError("Malum webhook cannot be matched to a deposit session")


def _update_malum_provider_metadata(
    *,
    deposit_session: DepositSession,
    status: str,
    raw_payload: dict,
    observed_canonical_amount: int | None = None,
) -> None:
    metadata = dict(deposit_session.metadata or {})
    provider = dict(metadata.get("payment_provider") or {})
    provider.update(
        {
            "key": MALUM_PROVIDER_KEY,
            "label": MALUM_PAYMENT_METHOD_LABEL,
            "reference": str(raw_payload.get("txn") or provider.get("reference") or "").strip(),
            "status": status,
            "last_status": status,
            "last_webhook_at": timezone.now().isoformat(),
            "last_payload": raw_payload,
            "sandbox": bool(raw_payload.get("sandbox")),
        }
    )
    metadata["payment_provider"] = provider
    deposit_session.metadata = metadata
    if observed_canonical_amount is not None:
        deposit_session.observed_amount = int(observed_canonical_amount)


@transaction.atomic
def _mark_malum_session_without_credit(*, deposit_session: DepositSession, status: str, raw_payload: dict) -> DepositSession:
    deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
    paid_canonical_amount = None
    if raw_payload.get("requested_amount") not in (None, ""):
        paid_canonical_amount = _malum_amount_to_canonical_stable_units(raw_payload.get("requested_amount"))

    if status == MALUM_STATUS_PROCESSING:
        desired_status = DepositSession.STATUS_CONFIRMING
    elif status == MALUM_STATUS_EXPIRED:
        desired_status = DepositSession.STATUS_EXPIRED
    elif status in {MALUM_STATUS_CANCELLED, MALUM_STATUS_CANCELED}:
        desired_status = DepositSession.STATUS_CANCELED
    else:
        desired_status = DepositSession.STATUS_AWAITING_PAYMENT

    if deposit_session.status not in {
        DepositSession.STATUS_CREDITED,
        getattr(DepositSession, "STATUS_SWEPT", "swept"),
    }:
        deposit_session.status = desired_status

    deposit_session.observed_txid = str(raw_payload.get("txn") or deposit_session.observed_txid or "").strip()
    _update_malum_provider_metadata(
        deposit_session=deposit_session,
        status=status,
        raw_payload=raw_payload,
        observed_canonical_amount=paid_canonical_amount,
    )
    deposit_session.save(
        update_fields=[
            "status",
            "observed_txid",
            "observed_amount",
            "metadata",
            "updated_at",
        ]
    )
    return deposit_session


@transaction.atomic
def credit_malum_deposit_session(
    *,
    actor,
    deposit_session: DepositSession,
    provider_reference: str,
    paid_canonical_stable_amount: int,
    raw_payload: dict,
    created_by=None,
):
    _require_perm(actor, "ledger.can_credit_confirmed_deposits")
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_CREDITING)
    created_by = _resolve_created_by(actor=actor, created_by=created_by)

    provider_reference = (provider_reference or "").strip()
    if not provider_reference:
        raise ValidationError("Malum provider reference is required")

    paid_canonical_stable_amount = int(paid_canonical_stable_amount)
    if paid_canonical_stable_amount <= 0:
        raise ValidationError("Paid amount must be positive")

    deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
    wallet = TokenWallet.objects.select_for_update().get(id=deposit_session.wallet_id)
    deposit_session.wallet = wallet

    if deposit_session.chain != MALUM_CHAIN:
        raise ValidationError("Deposit session is not a Malum provider session")

    metadata = deposit_session.metadata or {}
    provider = metadata.get("payment_provider") or {}
    if provider.get("key") != MALUM_PROVIDER_KEY:
        raise ValidationError("Deposit session payment provider is not Malum")

    if deposit_session.status == DepositSession.STATUS_CREDITED:
        if not deposit_session.credited_ledger_txn_id:
            raise ValidationError("Credited provider session missing linked ledger transaction")
        return deposit_session.credited_ledger_txn

    if deposit_session.status == getattr(DepositSession, "STATUS_SWEPT", "swept"):
        if deposit_session.credited_ledger_txn_id:
            return deposit_session.credited_ledger_txn
        raise ValidationError("Swept provider sessions cannot be credited again")

    token_pack_snapshot = metadata.get("token_pack") or {}
    clearing_wallet = get_external_asset_clearing_wallet()
    external_id = f"malum-deposit-credit:{provider_reference}"

    if token_pack_snapshot:
        user_credit_amount = int(token_pack_snapshot.get("token_amount") or 0)
        expected_gross_canonical_stable_amount = int(token_pack_snapshot.get("gross_stable_amount") or 0)
        expected_net_canonical_stable_amount = int(token_pack_snapshot.get("net_stable_amount") or 0)

        if user_credit_amount <= 0 or expected_gross_canonical_stable_amount <= 0:
            raise ValidationError("Provider deposit session is missing a valid token pack snapshot")

        if paid_canonical_stable_amount < expected_gross_canonical_stable_amount:
            raise ValidationError("Provider paid amount is below the expected token pack price")

        gross_token_equivalent_amount = _convert_canonical_stable_to_platform_tokens(
            paid_canonical_stable_amount
        )
        platform_fee_credit_amount = gross_token_equivalent_amount - user_credit_amount
        if platform_fee_credit_amount < 0:
            raise ValidationError("Provider paid amount is lower than the token pack token value")

        platform_fees_wallet = get_system_wallet(
            TokenWallet.SYSTEM_PLATFORM_FEES,
            allow_negative=False,
        )
        entries = [
            (clearing_wallet, -int(gross_token_equivalent_amount)),
            (deposit_session.wallet, int(user_credit_amount)),
        ]
        if platform_fee_credit_amount > 0:
            entries.append((platform_fees_wallet, int(platform_fee_credit_amount)))

        txn = apply_ledger_transaction(
            actor=actor,
            kind="deposit",
            entries=entries,
            created_by=created_by,
            external_id=external_id,
            memo=f"Confirmed Malum top-up {provider_reference}",
            metadata={
                "source": "malum_provider_deposit",
                "deposit_session_id": deposit_session.id,
                "deposit_session_public_id": str(deposit_session.public_id),
                "provider": MALUM_PROVIDER_KEY,
                "provider_reference": provider_reference,
                "requested_currency": _get_malum_currency(),
                "observed_canonical_stable_amount": int(paid_canonical_stable_amount),
                "expected_gross_canonical_stable_amount": int(expected_gross_canonical_stable_amount),
                "expected_net_canonical_stable_amount": int(expected_net_canonical_stable_amount),
                "expected_fee_canonical_stable_amount": int(token_pack_snapshot.get("fee_stable_amount") or 0),
                "user_credit_amount": int(user_credit_amount),
                "platform_fee_credit_amount": int(platform_fee_credit_amount),
                "gross_token_equivalent_amount": int(gross_token_equivalent_amount),
                "token_pack": token_pack_snapshot,
                "payment_method": metadata.get("payment_method") or {},
                "payment_provider": provider,
                "raw_payload": raw_payload,
                "amount_unit": "canonical_stable",
                "stablecoin_canonical_decimals": STABLECOIN_CANONICAL_DECIMALS,
                "platform_token_decimals": PLATFORM_TOKEN_DECIMALS,
                "platform_tokens_per_stablecoin": PLATFORM_TOKENS_PER_STABLECOIN,
            },
        )
    else:
        ledger_credit_amount = _convert_canonical_stable_to_platform_tokens(paid_canonical_stable_amount)
        txn = apply_ledger_transaction(
            actor=actor,
            kind="deposit",
            entries=[
                (clearing_wallet, -int(ledger_credit_amount)),
                (deposit_session.wallet, int(ledger_credit_amount)),
            ],
            created_by=created_by,
            external_id=external_id,
            memo=f"Confirmed Malum deposit {provider_reference}",
            metadata={
                "source": "malum_provider_deposit",
                "deposit_session_id": deposit_session.id,
                "deposit_session_public_id": str(deposit_session.public_id),
                "provider": MALUM_PROVIDER_KEY,
                "provider_reference": provider_reference,
                "canonical_stable_amount": int(paid_canonical_stable_amount),
                "ledger_credit_amount": int(ledger_credit_amount),
                "raw_payload": raw_payload,
                "amount_unit": "canonical_stable",
                "stablecoin_canonical_decimals": STABLECOIN_CANONICAL_DECIMALS,
                "platform_token_decimals": PLATFORM_TOKEN_DECIMALS,
                "platform_tokens_per_stablecoin": PLATFORM_TOKENS_PER_STABLECOIN,
            },
        )

    _update_malum_provider_metadata(
        deposit_session=deposit_session,
        status=MALUM_STATUS_COMPLETED,
        raw_payload=raw_payload,
        observed_canonical_amount=paid_canonical_stable_amount,
    )
    provider = dict(deposit_session.metadata.get("payment_provider") or {})
    provider["credited_at"] = timezone.now().isoformat()
    provider["credited_ledger_txn_id"] = txn.id
    deposit_session.metadata["payment_provider"] = provider

    deposit_session.observed_txid = provider_reference
    deposit_session.observed_amount = paid_canonical_stable_amount
    deposit_session.confirmations = 1
    deposit_session.status = DepositSession.STATUS_CREDITED
    deposit_session.credited_ledger_txn = txn
    deposit_session.save(
        update_fields=[
            "observed_txid",
            "observed_amount",
            "confirmations",
            "status",
            "credited_ledger_txn",
            "metadata",
            "updated_at",
        ]
    )
    return txn


def process_malum_webhook(payload: dict) -> dict:
    verify_malum_webhook_signature(payload)

    status = str(payload.get("status") or "").strip().upper()
    if not status:
        raise ValidationError("Malum webhook status is missing")

    provider_reference = str(payload.get("txn") or "").strip()
    if not provider_reference:
        raise ValidationError("Malum webhook transaction id is missing")

    deposit_session = _get_malum_session_from_payload(payload)

    if status == MALUM_STATUS_COMPLETED:
        requested_currency = str(payload.get("requested_currency") or "").strip().upper()
        if requested_currency and requested_currency != _get_malum_currency():
            raise ValidationError("Malum webhook currency does not match configuration")

        paid_canonical_amount = _malum_amount_to_canonical_stable_units(payload.get("requested_amount"))
        actor = _get_internal_deposit_service_actor()
        txn = credit_malum_deposit_session(
            actor=actor,
            deposit_session=deposit_session,
            provider_reference=provider_reference,
            paid_canonical_stable_amount=paid_canonical_amount,
            raw_payload=payload,
        )
        return {
            "provider": MALUM_PROVIDER_KEY,
            "provider_reference": provider_reference,
            "status": status,
            "deposit_session_public_id": str(deposit_session.public_id),
            "ledger_txn_id": txn.id,
            "credited": True,
        }

    updated_session = _mark_malum_session_without_credit(
        deposit_session=deposit_session,
        status=status,
        raw_payload=payload,
    )
    return {
        "provider": MALUM_PROVIDER_KEY,
        "provider_reference": provider_reference,
        "status": status,
        "deposit_session_public_id": str(updated_session.public_id),
        "ledger_txn_id": None,
        "credited": False,
    }
