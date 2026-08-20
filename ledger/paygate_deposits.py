from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlencode

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from ledger.fiat import get_fiat_usd_rate
from ledger.models import DepositSession, LEDGER_METADATA_VERSION, TokenPack, TokenWallet
from ledger.providers.paygate import (
    PAYGATE_CHAIN,
    PAYGATE_NETWORK_DISPLAY,
    PAYGATE_PAYMENT_METHOD_KEY,
    PAYGATE_PAYMENT_METHOD_LABEL,
    PAYGATE_PAYMENT_METHOD_TYPE,
    PAYGATE_PROVIDER_KEY,
    PAYGATE_STATUS_PAID,
    PAYGATE_STATUS_UNPAID,
    build_paygate_checkout_url,
    canonical_stable_to_paygate_amount,
    check_paygate_payment,
    create_paygate_wallet,
    get_paygate_payment_ttl_seconds,
    get_paygate_provider_id,
    get_paygate_provider_currency,
    get_paygate_provider_ids,
    get_paygate_provider_label,
    get_paygate_provider_min_canonical_stable_amount,
    get_paygate_public_base_url,
    paygate_enabled,
    paygate_route_key,
)
from ledger.native_quoted import (
    NATIVE_QUOTED_AMOUNT_SEMANTICS,
    get_native_asset_decimals,
)
from ledger.paygate_polygon import (
    PAYGATE_POLYGON_ASSET,
    PAYGATE_POLYGON_CHAIN,
    get_paygate_polygon_policy,
)
from ledger.services import (
    LEDGER_OPERATION_FLAG_CREDITING,
    LEDGER_OPERATION_FLAG_DEPOSIT_OPEN,
    PLATFORM_TOKEN_DECIMALS,
    PLATFORM_TOKENS_PER_STABLECOIN,
    STABLECOIN_CANONICAL_DECIMALS,
    _allocate_session_address,
    _build_token_pack_snapshot,
    _convert_canonical_stable_to_platform_tokens,
    _token_pack_credit_required_canonical_amount,
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

    provider_ids = get_paygate_provider_ids()

    if not provider_ids:
        provider_ids = [""]

    options = []
    for provider_id in provider_ids:
        currency = get_paygate_provider_currency(provider_id)
        min_amount = get_paygate_provider_min_canonical_stable_amount(provider_id)
        provider_label = get_paygate_provider_label(provider_id) if provider_id else PAYGATE_PAYMENT_METHOD_LABEL
        label = provider_label if provider_id else PAYGATE_PAYMENT_METHOD_LABEL

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
                "payment_currency": currency,
                "payment_currency_usd_rate": format(get_fiat_usd_rate(currency), "f"),
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
    checkout_currency: str = "",
    checkout_amount: str = "",
    checkout_currency_usd_rate: str = "",
    raw_payload=None,
) -> dict:
    normalized_provider_id = (provider_id or get_paygate_provider_id() or "").strip().lower()
    provider_label = (
        get_paygate_provider_label(normalized_provider_id)
        if normalized_provider_id
        else PAYGATE_PAYMENT_METHOD_LABEL
    )
    display_label = provider_label if normalized_provider_id else PAYGATE_PAYMENT_METHOD_LABEL
    normalized_checkout_currency = (
        checkout_currency or get_paygate_provider_currency(normalized_provider_id)
    ).strip().upper()

    provider = {
        "key": PAYGATE_PROVIDER_KEY,
        "label": display_label,
        "payment_method_key": PAYGATE_PAYMENT_METHOD_KEY,
        "payment_method_type": PAYGATE_PAYMENT_METHOD_TYPE,
        "route_key": paygate_route_key(
            currency=normalized_checkout_currency,
            provider_id=normalized_provider_id,
        ),
        "reference": (ipn_token or "").strip(),
        "address_in": (address_in or "").strip(),
        "polygon_address_in": (polygon_address_in or "").strip(),
        "ipn_token": (ipn_token or "").strip(),
        "checkout_url": (checkout_url or "").strip(),
        "status": (status or "").strip().upper(),
        "session_public_id": str(session_public_id),
        "provider_id": normalized_provider_id,
        "provider_label": provider_label,
        "checkout_currency": normalized_checkout_currency,
        "checkout_amount": str(checkout_amount or "").strip(),
        "checkout_currency_usd_rate": str(checkout_currency_usd_rate or "").strip(),
    }
    if raw_payload is not None:
        provider["raw_payload"] = raw_payload
    return provider

def _find_reusable_paygate_session(
    *,
    wallet: TokenWallet,
    token_pack_code: str,
    provider_id: str = "",
    expected_canonical_amount: int | None = None,
    checkout_currency: str = "",
) -> DepositSession | None:
    now = timezone.now()
    normalized_provider_id = (provider_id or "").strip().lower()
    normalized_checkout_currency = get_paygate_provider_currency(normalized_provider_id)

    candidates = (
        DepositSession.objects.select_for_update()
        .filter(
            wallet=wallet,
            chain=PAYGATE_POLYGON_CHAIN,
            route_key=paygate_route_key(
                currency=checkout_currency or normalized_checkout_currency,
                provider_id=normalized_provider_id,
            ),
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
        if expected_canonical_amount is not None and int(session.min_amount) != int(expected_canonical_amount):
            continue
        session_provider_id = str(provider.get("provider_id") or "").strip().lower()
        if session_provider_id != normalized_provider_id:
            continue

        if normalized_pack_code and (snapshot.get("code") or "").strip() != normalized_pack_code:
            continue

        return session

    return None

def open_paygate_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    token_pack: TokenPack,
    provider_id: str = "",
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> DepositSession:
    actor = _require_authenticated_actor(actor)
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_DEPOSIT_OPEN)

    if not paygate_enabled():
        raise ValidationError("PayGate payments are temporarily unavailable")

    customer_email = (getattr(actor, "email", "") or "").strip()
    if not customer_email:
        raise ValidationError("A verified email address is required for PayGate payments")

    with transaction.atomic():
        wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
        _require_wallet_not_blocked(wallet)

        if wallet.wallet_type != TokenWallet.TYPE_USER:
            raise ValidationError("Deposit sessions can only target user wallets")
        if wallet.user_id != actor.id:
            raise PermissionDenied("Cannot open a deposit session for another user's wallet")

        token_pack_snapshot = _build_token_pack_snapshot(
            token_pack=token_pack,
            payment_price_bps=payment_price_bps,
            payment_price_fixed_canonical=payment_price_fixed_canonical,
        )
        expected_canonical_amount = int(token_pack_snapshot["gross_stable_amount"])
        provider_id = (provider_id or get_paygate_provider_id() or "").strip().lower()
        currency = get_paygate_provider_currency(provider_id)
        provider_display_label = (
            get_paygate_provider_label(provider_id)
            if provider_id
            else PAYGATE_PAYMENT_METHOD_LABEL
        )
        min_amount = get_paygate_provider_min_canonical_stable_amount(provider_id)
        if expected_canonical_amount < min_amount:
            raise ValidationError(
                f"Selected token pack is below {provider_display_label}'s minimum payment amount"
            )

        currency_usd_rate = format(get_fiat_usd_rate(currency), "f")
        checkout_amount = canonical_stable_to_paygate_amount(
            expected_canonical_amount,
            currency=currency,
        )

        existing_session = _find_reusable_paygate_session(
            wallet=wallet,
            token_pack_code=token_pack_snapshot["code"],
            provider_id=provider_id,
            expected_canonical_amount=expected_canonical_amount,
            checkout_currency=currency,
        )
        if existing_session is not None:
            return existing_session

        _enforce_deposit_open_cooldown(user=wallet.user)

        public_id = uuid.uuid4()
        route_key = paygate_route_key(currency, provider_id)
        deposit_address, derivation_index, derivation_path = _allocate_session_address(
            chain=PAYGATE_POLYGON_CHAIN,
            asset_code=PAYGATE_POLYGON_ASSET,
            token_contract_address="",
        )
        policy = get_paygate_polygon_policy()
        now = timezone.now()
        expires_at = now + timedelta(seconds=get_paygate_payment_ttl_seconds())

        callback_path = f"{reverse('paygate_callback')}?{urlencode({'number': str(public_id)})}"
        callback_url = _build_absolute_url(callback_path)

        metadata = {
            "display_label": provider_display_label,
            "allocation_source": "session_derivation",
            "chain_family": "evm",
            "amount_semantics": NATIVE_QUOTED_AMOUNT_SEMANTICS,
            "settlement": {
                "provider": PAYGATE_PROVIDER_KEY,
                "chain": PAYGATE_POLYGON_CHAIN,
                "asset_code": PAYGATE_POLYGON_ASSET,
                "token_contract_address": "",
                "native_decimals": get_native_asset_decimals(
                    PAYGATE_POLYGON_ASSET
                ),
                "required_confirmations": int(policy["required_confirmations"]),
            },
            "token_pack": token_pack_snapshot,
            "payment_method": {
                "key": f"{PAYGATE_PAYMENT_METHOD_KEY}:{provider_id}",
                "type": PAYGATE_PAYMENT_METHOD_TYPE,
                "label": provider_display_label,
                "show_network_step": False,
            },
            "payment_provider": _provider_metadata_for_session(
                session_public_id=public_id,
                status="CREATING",
                provider_id=provider_id,
                checkout_currency=currency,
                checkout_amount=checkout_amount,
                checkout_currency_usd_rate=currency_usd_rate,
            ),
            "amount_unit": "canonical_stable",
            "expected_canonical_stable_amount": int(expected_canonical_amount),
            "canonical_currency": "USD",
            "checkout_currency": currency,
            "checkout_amount": checkout_amount,
            "checkout_currency_usd_rate": currency_usd_rate,
            "stablecoin_canonical_decimals": STABLECOIN_CANONICAL_DECIMALS,
            "platform_token_decimals": PLATFORM_TOKEN_DECIMALS,
            "platform_tokens_per_stablecoin": PLATFORM_TOKENS_PER_STABLECOIN,
            "paygate_callback_url": callback_url,
        }

        deposit_session = DepositSession.objects.create(
            public_id=public_id,
            user=wallet.user,
            wallet=wallet,
            chain=PAYGATE_POLYGON_CHAIN,
            asset_code=PAYGATE_POLYGON_ASSET,
            token_contract_address="",
            route_key=route_key,
            display_label=provider_display_label,
            deposit_address=deposit_address,
            address_derivation_ref=derivation_path,
            derivation_index=derivation_index,
            derivation_path=derivation_path,
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            min_amount=int(expected_canonical_amount),
            expected_onchain_raw_amount=None,
            required_confirmations=int(policy["required_confirmations"]),
            expires_at=expires_at,
            created_by=actor,
            metadata=metadata,
            metadata_version=LEDGER_METADATA_VERSION,
        )

    try:
        wallet_response = create_paygate_wallet(
            payout_wallet=deposit_address,
            callback_url=callback_url,
        )
        address_in = str(wallet_response["address_in"]).strip()
        polygon_address_in = str(wallet_response["polygon_address_in"]).strip()
        ipn_token = str(wallet_response["ipn_token"]).strip()
        checkout_url = build_paygate_checkout_url(
            address_in=address_in,
            amount=checkout_amount,
            customer_email=customer_email,
            currency=currency,
            provider_id=provider_id,
        )
    except Exception as exc:
        with transaction.atomic():
            failed_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
            failed_metadata = dict(failed_session.metadata or {})
            provider = dict(failed_metadata.get("payment_provider") or {})
            provider.update(
                {
                    "status": "CREATE_FAILED",
                    "last_error": str(exc)[:1000],
                    "last_error_at": timezone.now().isoformat(),
                }
            )
            failed_metadata["payment_provider"] = provider
            failed_session.status = DepositSession.STATUS_FAILED
            failed_session.metadata = failed_metadata
            failed_session.save(update_fields=["status", "metadata", "updated_at"])
        raise

    with transaction.atomic():
        deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
        metadata = dict(deposit_session.metadata or {})
        metadata["payment_provider"] = _provider_metadata_for_session(
            session_public_id=deposit_session.public_id,
            address_in=address_in,
            polygon_address_in=polygon_address_in,
            ipn_token=ipn_token,
            checkout_url=checkout_url,
            status="CREATED",
            provider_id=provider_id,
            checkout_currency=currency,
            checkout_amount=checkout_amount,
            checkout_currency_usd_rate=currency_usd_rate,
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

    base_qs = DepositSession.objects.filter(
        metadata__payment_provider__key=PAYGATE_PROVIDER_KEY,
    )

    if public_id:
        session = base_qs.filter(public_id=public_id).first()
        if session is None:
            raise ValidationError("Unknown PayGate deposit session")
        return session

    if address_in:
        session = (
            base_qs.filter(metadata__payment_provider__address_in=address_in)
            .order_by("-created_at")
            .first()
        )
        if session is not None:
            return session

    if ipn_token:
        session = (
            base_qs.filter(metadata__payment_provider__ipn_token=ipn_token)
            .order_by("-created_at")
            .first()
        )
        if session is not None:
            return session

    raise ValidationError("Unable to resolve PayGate deposit session")


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
        expected_net_canonical_stable_amount = (
            _token_pack_credit_required_canonical_amount(
                token_pack_snapshot
            )
        )

        if user_credit_amount <= 0 or expected_net_canonical_stable_amount is None:
            raise ValidationError("Provider deposit session is missing a valid token pack snapshot")

        if paid_canonical_stable_amount < expected_net_canonical_stable_amount:
            raise ValidationError("Provider paid amount is below the token pack net amount")

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
                "requested_currency": (
                    provider.get("checkout_currency")
                    or metadata.get("checkout_currency")
                    or deposit_session.asset_code
                ),
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
    raw_payload = {**payload, "status_response": status_response}

    _update_paygate_provider_metadata(
        deposit_session=deposit_session,
        status=status.upper() or PAYGATE_STATUS_UNPAID.upper(),
        raw_payload=raw_payload,
    )

    # Never let a provider callback downgrade an already credited/swept on-chain session.
    if deposit_session.status in PAYGATE_ACTIVE_STATUSES:
        deposit_session.status = DepositSession.STATUS_CONFIRMING
        deposit_session.save(update_fields=["status", "metadata", "updated_at"])
    else:
        deposit_session.save(update_fields=["metadata", "updated_at"])

    provider_reference = (
        str(status_response.get("txid_out") or "").strip()
        or str(payload.get("txid_out") or "").strip()
        or str(payload.get("txid_in") or "").strip()
        or ipn_token
    )
    credited = deposit_session.status in {
        DepositSession.STATUS_CREDITED,
        getattr(DepositSession, "STATUS_SWEPT", "swept"),
    }
    return {
        "provider": PAYGATE_PROVIDER_KEY,
        "provider_reference": provider_reference,
        "status": status or PAYGATE_STATUS_UNPAID,
        "deposit_session_public_id": str(deposit_session.public_id),
        "ledger_txn_id": deposit_session.credited_ledger_txn_id,
        "credited": bool(credited),
        "source_of_truth": "polygon_onchain",
    }


__all__ = [
    "get_paygate_deposit_option",
    "open_paygate_deposit_session",
    "credit_paygate_deposit_session",
    "process_paygate_callback",
    "get_paygate_deposit_options",
]