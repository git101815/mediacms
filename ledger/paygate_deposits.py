from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlencode

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from ledger.internal_api import _get_internal_deposit_service_actor
from ledger.models import DepositSession, LEDGER_METADATA_VERSION, TokenPack, TokenWallet
from ledger.providers.paygate import (
    PAYGATE_CHAIN,
    PAYGATE_NETWORK_DISPLAY,
    PAYGATE_PAYMENT_METHOD_KEY,
    PAYGATE_PAYMENT_METHOD_LABEL,
    PAYGATE_PAYMENT_METHOD_TYPE,
    PAYGATE_PROVIDER_KEY,
    PAYGATE_STATUS_PAID,
    build_paygate_checkout_url,
    canonical_stable_to_paygate_amount,
    check_paygate_payment,
    create_paygate_wallet,
    get_paygate_currency,
    get_paygate_min_canonical_stable_amount,
    get_paygate_payment_ttl_seconds,
    get_paygate_provider_id,
    get_paygate_provider_ids,
    get_paygate_provider_label,
    get_paygate_public_base_url,
    get_paygate_usdc_polygon_wallet,
    paygate_amount_to_canonical_stable_units,
    paygate_enabled,
    paygate_route_key,
)
from ledger.services import (
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

PAYGATE_ACTIVE_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_CONFIRMING,
}


def _canonical_stable_to_decimal(value: int) -> Decimal:
    return Decimal(int(value)) / (Decimal(10) ** STABLECOIN_CANONICAL_DECIMALS)


def _format_canonical_stable_for_display(value: int) -> str:
    text = format(_canonical_stable_to_decimal(value), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _build_absolute_url(path: str) -> str:
    base_url = get_paygate_public_base_url()
    if not base_url:
        raise ImproperlyConfigured("PAYGATE_PUBLIC_BASE_URL or FRONTEND_HOST must be configured")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def get_paygate_deposit_options() -> list[dict]:
    if not paygate_enabled():
        return []

    currency = get_paygate_currency()
    provider_ids = get_paygate_provider_ids()
    min_amount = get_paygate_min_canonical_stable_amount()

    if not provider_ids:
        provider_ids = [""]

    options = []
    for provider_id in provider_ids:
        provider_label = get_paygate_provider_label(provider_id) if provider_id else PAYGATE_PAYMENT_METHOD_LABEL
        label = f"PayGate · {provider_label}" if provider_id else PAYGATE_PAYMENT_METHOD_LABEL

        options.append(
            {
                "key": paygate_route_key(currency, provider_id),
                "label": label,
                "route_label": label,
                "network_label": PAYGATE_NETWORK_DISPLAY,
                "network_display": PAYGATE_NETWORK_DISPLAY,
                "chain": PAYGATE_CHAIN,
                "asset_code": currency,
                "token_contract_address": "",
                "required_confirmations": 1,
                "min_amount": int(min_amount),
                "onchain_min_amount": str(min_amount),
                "amount_unit": "canonical_stable",
                "onchain_amount_unit": "provider_amount",
                "min_amount_display": _format_canonical_stable_for_display(min_amount),
                "session_ttl_seconds": get_paygate_payment_ttl_seconds(),
                "network_slug": PAYGATE_CHAIN,
                "payment_method_key": f"{PAYGATE_PAYMENT_METHOD_KEY}:{provider_id or 'multi'}",
                "payment_method_label": label,
                "payment_method_type": PAYGATE_PAYMENT_METHOD_TYPE,
                "provider_key": PAYGATE_PROVIDER_KEY,
                "paygate_provider_id": provider_id,
                "paygate_provider_label": provider_label,
            }
        )

    return options


def get_paygate_deposit_option() -> dict | None:
    options = get_paygate_deposit_options()
    return options[0] if options else None


def _provider_metadata_for_session(
    *,
    session_public_id,
    address_in: str = "",
    polygon_address_in: str = "",
    ipn_token: str = "",
    checkout_url: str = "",
    status: str = "",
    provider_id: str = "",
    raw_payload=None,
) -> dict:
    normalized_provider_id = (provider_id or get_paygate_provider_id() or "").strip().lower()
    provider_label = (
        get_paygate_provider_label(normalized_provider_id)
        if normalized_provider_id
        else PAYGATE_PAYMENT_METHOD_LABEL
    )
    display_label = (
        f"PayGate · {provider_label}"
        if normalized_provider_id
        else PAYGATE_PAYMENT_METHOD_LABEL
    )

    provider = {
        "key": PAYGATE_PROVIDER_KEY,
        "label": display_label,
        "payment_method_key": PAYGATE_PAYMENT_METHOD_KEY,
        "payment_method_type": PAYGATE_PAYMENT_METHOD_TYPE,
        "route_key": paygate_route_key(provider_id=normalized_provider_id),
        "reference": (ipn_token or "").strip(),
        "address_in": (address_in or "").strip(),
        "polygon_address_in": (polygon_address_in or "").strip(),
        "ipn_token": (ipn_token or "").strip(),
        "checkout_url": (checkout_url or "").strip(),
        "status": (status or "").strip().upper(),
        "session_public_id": str(session_public_id),
        "provider_id": normalized_provider_id,
        "provider_label": provider_label,
    }
    if raw_payload is not None:
        provider["raw_payload"] = raw_payload
    return provider

def _find_reusable_paygate_session(
    *,
    wallet: TokenWallet,
    token_pack_code: str,
    provider_id: str = "",
) -> DepositSession | None:
    now = timezone.now()
    normalized_provider_id = (provider_id or "").strip().lower()

    candidates = (
        DepositSession.objects.select_for_update()
        .filter(
            wallet=wallet,
            chain=PAYGATE_CHAIN,
            route_key=paygate_route_key(provider_id=normalized_provider_id),
            status__in=PAYGATE_ACTIVE_STATUSES,
            expires_at__gt=now,
        )
        .order_by("-created_at")
    )

    normalized_pack_code = (token_pack_code or "").strip()
    for session in candidates:
        metadata = session.metadata or {}
        snapshot = metadata.get("token_pack") or {}
        provider = metadata.get("payment_provider") or {}

        if provider.get("key") != PAYGATE_PROVIDER_KEY:
            continue

        session_provider_id = str(provider.get("provider_id") or "").strip().lower()
        if session_provider_id != normalized_provider_id:
            continue

        if normalized_pack_code and (snapshot.get("code") or "").strip() != normalized_pack_code:
            continue

        return session

    return None

@transaction.atomic
def open_paygate_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    token_pack: TokenPack,
    provider_id: str = "",
    payment_price_bps=0,
) -> DepositSession:
    actor = _require_authenticated_actor(actor)
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_DEPOSIT_OPEN)

    if not paygate_enabled():
        raise ValidationError("PayGate payments are temporarily unavailable")

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    _require_wallet_not_blocked(wallet)

    if wallet.wallet_type != TokenWallet.TYPE_USER:
        raise ValidationError("Deposit sessions can only target user wallets")

    if wallet.user_id != actor.id:
        raise PermissionDenied("Cannot open a deposit session for another user's wallet")

    customer_email = (getattr(actor, "email", "") or "").strip()
    if not customer_email:
        raise ValidationError("A verified email address is required for PayGate payments")

    token_pack_snapshot = _build_token_pack_snapshot(
        token_pack=token_pack,
        payment_price_bps=payment_price_bps,
    )
    expected_canonical_amount = int(token_pack_snapshot["gross_stable_amount"])
    min_amount = get_paygate_min_canonical_stable_amount()

    if expected_canonical_amount < min_amount:
        raise ValidationError("Selected token pack is below PayGate's minimum payment amount")

    provider_id = (provider_id or get_paygate_provider_id() or "").strip().lower()
    provider_display_label = (
        f"PayGate · {get_paygate_provider_label(provider_id)}"
        if provider_id
        else PAYGATE_PAYMENT_METHOD_LABEL
    )

    existing_session = _find_reusable_paygate_session(
        wallet=wallet,
        token_pack_code=token_pack_snapshot["code"],
        provider_id=provider_id,
    )
    if existing_session is not None:
        return existing_session

    _enforce_deposit_open_cooldown(user=wallet.user)

    public_id = uuid.uuid4()
    currency = get_paygate_currency()
    route_key = paygate_route_key(currency, provider_id)
    synthetic_ref = f"paygate:{public_id.hex}"
    now = timezone.now()
    expires_at = now + timedelta(seconds=get_paygate_payment_ttl_seconds())

    callback_path = f"{reverse('paygate_callback')}?{urlencode({'number': str(public_id)})}"
    callback_url = _build_absolute_url(callback_path)

    metadata = {
        "display_label": provider_display_label,
        "allocation_source": "provider_checkout",
        "chain_family": "provider",
        "token_pack": token_pack_snapshot,
        "payment_method": {
            "key": f"{PAYGATE_PAYMENT_METHOD_KEY}:{provider_id}",
            "type": PAYGATE_PAYMENT_METHOD_TYPE,
            "label": provider_display_label,
            "show_network_step": False,
        },
        "payment_provider": _provider_metadata_for_session(
            session_public_id=public_id,
            provider_id=provider_id,
        ),
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
        chain=PAYGATE_CHAIN,
        asset_code=currency,
        token_contract_address="",
        route_key=route_key,
        display_label=provider_display_label,
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

    try:
        wallet_response = create_paygate_wallet(
            payout_wallet=get_paygate_usdc_polygon_wallet(),
            callback_url=callback_url,
        )

        address_in = str(wallet_response["address_in"]).strip()
        polygon_address_in = str(wallet_response["polygon_address_in"]).strip()
        ipn_token = str(wallet_response["ipn_token"]).strip()

        checkout_url = build_paygate_checkout_url(
            address_in=address_in,
            amount=canonical_stable_to_paygate_amount(expected_canonical_amount),
            customer_email=customer_email,
            currency=currency,
            provider_id=provider_id,
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

    metadata = dict(deposit_session.metadata or {})
    metadata["payment_provider"] = _provider_metadata_for_session(
        session_public_id=deposit_session.public_id,
        address_in=address_in,
        polygon_address_in=polygon_address_in,
        ipn_token=ipn_token,
        checkout_url=checkout_url,
        status="CREATED",
        provider_id=provider_id,
        raw_payload=wallet_response,
    )
    metadata["paygate_callback_url"] = callback_url
    deposit_session.metadata = metadata
    deposit_session.save(update_fields=["metadata", "updated_at"])
    return deposit_session


def _get_paygate_session_from_payload(payload: dict) -> DepositSession:
    public_id = str(payload.get("number") or payload.get("deposit_session_public_id") or "").strip()
    address_in = str(payload.get("address_in") or "").strip()
    ipn_token = str(payload.get("ipn_token") or "").strip()

    if public_id:
        try:
            return DepositSession.objects.get(public_id=public_id, chain=PAYGATE_CHAIN)
        except DepositSession.DoesNotExist as exc:
            raise ValidationError("Unknown PayGate deposit session") from exc

    if address_in:
        session = (
            DepositSession.objects.filter(
                chain=PAYGATE_CHAIN,
                metadata__payment_provider__key=PAYGATE_PROVIDER_KEY,
                metadata__payment_provider__address_in=address_in,
            )
            .order_by("-created_at")
            .first()
        )
        if session is not None:
            return session

    if ipn_token:
        session = (
            DepositSession.objects.filter(
                chain=PAYGATE_CHAIN,
                metadata__payment_provider__key=PAYGATE_PROVIDER_KEY,
                metadata__payment_provider__ipn_token=ipn_token,
            )
            .order_by("-created_at")
            .first()
        )
        if session is not None:
            return session

    raise ValidationError("PayGate callback cannot be matched to a deposit session")


def _update_paygate_provider_metadata(
    *,
    deposit_session: DepositSession,
    status: str,
    raw_payload: dict,
    observed_canonical_amount: int | None = None,
) -> None:
    metadata = dict(deposit_session.metadata or {})
    provider = dict(metadata.get("payment_provider") or {})
    provider_id = str(provider.get("provider_id") or "").strip().lower()
    provider_label = provider.get("provider_label") or get_paygate_provider_label(provider_id)

    provider.update(
        {
            "key": PAYGATE_PROVIDER_KEY,
            "label": f"PayGate · {provider_label}" if provider_id else PAYGATE_PAYMENT_METHOD_LABEL,
            "status": status,
            "last_status": status,
            "last_callback_at": timezone.now().isoformat(),
            "last_payload": raw_payload,
        }
    )

    for key in [
        "value_coin",
        "coin",
        "txid_in",
        "txid_out",
        "address_in",
        "value_forwarded_coin",
    ]:
        if raw_payload.get(key) not in (None, ""):
            provider[key] = str(raw_payload.get(key)).strip()

    metadata["payment_provider"] = provider
    deposit_session.metadata = metadata

    if observed_canonical_amount is not None:
        deposit_session.observed_amount = int(observed_canonical_amount)


@transaction.atomic
def credit_paygate_deposit_session(
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
        raise ValidationError("PayGate provider reference is required")

    paid_canonical_stable_amount = int(paid_canonical_stable_amount)
    if paid_canonical_stable_amount <= 0:
        raise ValidationError("Paid amount must be positive")

    deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
    wallet = TokenWallet.objects.select_for_update().get(id=deposit_session.wallet_id)
    deposit_session.wallet = wallet

    if deposit_session.chain != PAYGATE_CHAIN:
        raise ValidationError("Deposit session is not a PayGate provider session")

    metadata = deposit_session.metadata or {}
    provider = metadata.get("payment_provider") or {}
    if provider.get("key") != PAYGATE_PROVIDER_KEY:
        raise ValidationError("Deposit session payment provider is not PayGate")

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
    external_id = f"paygate-deposit-credit:{provider_reference}"

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
            memo=f"Confirmed PayGate top-up {provider_reference}",
            metadata={
                "source": "paygate_provider_deposit",
                "deposit_session_id": deposit_session.id,
                "deposit_session_public_id": str(deposit_session.public_id),
                "provider": PAYGATE_PROVIDER_KEY,
                "provider_reference": provider_reference,
                "requested_currency": get_paygate_currency(),
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
            memo=f"Confirmed PayGate deposit {provider_reference}",
            metadata={
                "source": "paygate_provider_deposit",
                "deposit_session_id": deposit_session.id,
                "deposit_session_public_id": str(deposit_session.public_id),
                "provider": PAYGATE_PROVIDER_KEY,
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

    _update_paygate_provider_metadata(
        deposit_session=deposit_session,
        status=PAYGATE_STATUS_PAID.upper(),
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


def process_paygate_callback(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValidationError("Invalid PayGate callback payload")

    deposit_session = _get_paygate_session_from_payload(payload)

    provider = (deposit_session.metadata or {}).get("payment_provider") or {}
    expected_address_in = str(provider.get("address_in") or "").strip()
    callback_address_in = str(payload.get("address_in") or "").strip()

    if expected_address_in and callback_address_in and expected_address_in != callback_address_in:
        raise ValidationError("PayGate callback address does not match deposit session")

    ipn_token = str(provider.get("ipn_token") or "").strip()
    if not ipn_token:
        raise ValidationError("PayGate deposit session is missing ipn_token")

    status_response = check_paygate_payment(ipn_token=ipn_token)
    status = str(status_response.get("status") or "").strip().lower()

    if status != PAYGATE_STATUS_PAID:
        _update_paygate_provider_metadata(
            deposit_session=deposit_session,
            status=status.upper() or PAYGATE_STATUS_UNPAID.upper(),
            raw_payload={**payload, "status_response": status_response},
        )
        deposit_session.status = DepositSession.STATUS_CONFIRMING
        deposit_session.save(update_fields=["status", "metadata", "updated_at"])
        return {
            "provider": PAYGATE_PROVIDER_KEY,
            "provider_reference": ipn_token,
            "status": status or PAYGATE_STATUS_UNPAID,
            "deposit_session_public_id": str(deposit_session.public_id),
            "ledger_txn_id": None,
            "credited": False,
        }

    paid_value = (
        status_response.get("value_coin")
        or payload.get("value_coin")
        or payload.get("value_forwarded_coin")
    )
    paid_canonical_amount = paygate_amount_to_canonical_stable_units(paid_value)

    provider_reference = (
        str(status_response.get("txid_out") or "").strip()
        or str(payload.get("txid_out") or "").strip()
        or str(payload.get("txid_in") or "").strip()
        or ipn_token
    )

    actor = _get_internal_deposit_service_actor()
    raw_payload = {**payload, "status_response": status_response}

    txn = credit_paygate_deposit_session(
        actor=actor,
        deposit_session=deposit_session,
        provider_reference=provider_reference,
        paid_canonical_stable_amount=paid_canonical_amount,
        raw_payload=raw_payload,
    )

    return {
        "provider": PAYGATE_PROVIDER_KEY,
        "provider_reference": provider_reference,
        "status": PAYGATE_STATUS_PAID,
        "deposit_session_public_id": str(deposit_session.public_id),
        "ledger_txn_id": txn.id,
        "credited": True,
    }


__all__ = [
    "get_paygate_deposit_option",
    "open_paygate_deposit_session",
    "credit_paygate_deposit_session",
    "process_paygate_callback",
    "get_paygate_deposit_options",
]