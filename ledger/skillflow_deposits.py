from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ledger.fiat import get_fiat_usd_rate
from ledger.internal_api import _get_internal_deposit_service_actor
from ledger.models import DepositSession, LEDGER_METADATA_VERSION, TokenPack, TokenWallet
from ledger.providers.skillflow import (
    SKILLFLOW_CHAIN,
    SKILLFLOW_CURRENCY,
    SKILLFLOW_MIN_EUR_AMOUNT,
    SKILLFLOW_NETWORK_DISPLAY,
    SKILLFLOW_PAYMENT_METHOD_KEY,
    SKILLFLOW_PAYMENT_METHOD_LABEL,
    SKILLFLOW_PAYMENT_METHOD_TYPE,
    SKILLFLOW_PROVIDER_KEY,
    canonical_stable_to_skillflow_amount,
    create_skillflow_checkout,
    get_skillflow_min_canonical_stable_amount,
    get_skillflow_payment_ttl_seconds,
    get_skillflow_public_base_url_or_error,
    skillflow_amount_to_canonical_stable_units,
    skillflow_enabled,
    skillflow_route_key,
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
    _token_pack_credit_required_canonical_amount,
    apply_ledger_transaction,
    get_external_asset_clearing_wallet,
    get_system_wallet,
    require_ledger_operation_enabled,
)


SKILLFLOW_ACTIVE_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_CONFIRMING,
}


def _format_canonical_stable_for_display(value: int) -> str:
    amount = Decimal(int(value)) / (Decimal(10) ** STABLECOIN_CANONICAL_DECIMALS)
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _build_absolute_url(path: str) -> str:
    base_url = get_skillflow_public_base_url_or_error()
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def _skillflow_request_metadata(
    *,
    session_public_id,
    token_pack_snapshot: dict,
    expected_canonical_amount: int,
    checkout_amount: str,
) -> dict:
    return {
        "provider": SKILLFLOW_PROVIDER_KEY,
        "depositSessionPublicId": str(session_public_id),
        "tokenPackCode": str(token_pack_snapshot.get("code") or ""),
        "expectedCanonicalStableAmount": int(expected_canonical_amount),
        "checkoutAmount": str(checkout_amount),
        "checkoutCurrency": SKILLFLOW_CURRENCY,
    }


def _provider_metadata_for_session(
    *,
    session_public_id,
    user_id: str,
    checkout_amount: str,
    checkout_currency_usd_rate: str,
    request_metadata: dict,
    reference: str = "",
    mollie_id: str = "",
    checkout_url: str = "",
    description: str = "",
    status: str = "",
    raw_response=None,
) -> dict:
    provider = {
        "key": SKILLFLOW_PROVIDER_KEY,
        "label": SKILLFLOW_PAYMENT_METHOD_LABEL,
        "network_display": SKILLFLOW_NETWORK_DISPLAY,
        "payment_method_key": SKILLFLOW_PAYMENT_METHOD_KEY,
        "payment_method_type": SKILLFLOW_PAYMENT_METHOD_TYPE,
        "route_key": skillflow_route_key(),
        "reference": str(reference or "").strip(),
        "payment_id": str(reference or "").strip(),
        "mollie_id": str(mollie_id or "").strip(),
        "checkout_url": str(checkout_url or "").strip(),
        "checkout_currency": SKILLFLOW_CURRENCY,
        "checkout_amount": str(checkout_amount or "").strip(),
        "checkout_currency_usd_rate": str(checkout_currency_usd_rate or "").strip(),
        "description": str(description or "").strip(),
        "status": str(status or "").strip().upper(),
        "session_public_id": str(session_public_id),
        "user_id": str(user_id),
        "request_metadata": request_metadata,
    }
    if raw_response is not None:
        provider["raw_response"] = raw_response
    return provider


def get_skillflow_deposit_option() -> dict | None:
    if not skillflow_enabled():
        return None

    minimum_canonical_amount = get_skillflow_min_canonical_stable_amount()
    currency_usd_rate = format(get_fiat_usd_rate(SKILLFLOW_CURRENCY), "f")
    return {
        "key": skillflow_route_key(),
        "label": SKILLFLOW_PAYMENT_METHOD_LABEL,
        "route_label": SKILLFLOW_PAYMENT_METHOD_LABEL,
        "network_label": SKILLFLOW_NETWORK_DISPLAY,
        "network_display": SKILLFLOW_NETWORK_DISPLAY,
        "chain": SKILLFLOW_CHAIN,
        "asset_code": SKILLFLOW_CURRENCY,
        "token_contract_address": "",
        "required_confirmations": 1,
        "min_amount": int(minimum_canonical_amount),
        "onchain_min_amount": format(SKILLFLOW_MIN_EUR_AMOUNT, ".2f"),
        "amount_unit": "canonical_stable",
        "onchain_amount_unit": "provider_amount",
        "min_amount_display": _format_canonical_stable_for_display(minimum_canonical_amount),
        "session_ttl_seconds": get_skillflow_payment_ttl_seconds(),
        "network_slug": SKILLFLOW_CHAIN,
        "payment_method_key": SKILLFLOW_PAYMENT_METHOD_KEY,
        "payment_method_label": SKILLFLOW_PAYMENT_METHOD_LABEL,
        "payment_method_type": SKILLFLOW_PAYMENT_METHOD_TYPE,
        "provider_key": SKILLFLOW_PROVIDER_KEY,
        "payment_currency": SKILLFLOW_CURRENCY,
        "payment_currency_usd_rate": currency_usd_rate,
        "payment_requires_route_selection": False,
        "payment_price_mode": "fixed",
    }


def _find_reusable_skillflow_session(
    *,
    wallet: TokenWallet,
    token_pack_code: str,
    expected_canonical_amount: int,
    checkout_amount: str,
) -> DepositSession | None:
    candidates = (
        DepositSession.objects.select_for_update()
        .filter(
            wallet=wallet,
            chain=SKILLFLOW_CHAIN,
            route_key=skillflow_route_key(),
            status__in=SKILLFLOW_ACTIVE_STATUSES,
            expires_at__gt=timezone.now(),
        )
        .order_by("-created_at")
    )

    normalized_pack_code = str(token_pack_code or "").strip()
    normalized_checkout_amount = Decimal(str(checkout_amount))
    for session in candidates:
        metadata = session.metadata or {}
        snapshot = metadata.get("token_pack") or {}
        provider = metadata.get("payment_provider") or {}
        if provider.get("key") != SKILLFLOW_PROVIDER_KEY:
            continue
        if normalized_pack_code and str(snapshot.get("code") or "").strip() != normalized_pack_code:
            continue
        if int(metadata.get("expected_canonical_stable_amount") or 0) != int(expected_canonical_amount):
            continue
        try:
            existing_checkout_amount = Decimal(
                str(provider.get("checkout_amount") or metadata.get("checkout_amount"))
            )
        except (InvalidOperation, TypeError, ValueError):
            continue
        if existing_checkout_amount != normalized_checkout_amount:
            continue
        return session
    return None


def open_skillflow_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    token_pack: TokenPack,
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> DepositSession:
    actor = _require_authenticated_actor(actor)
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_DEPOSIT_OPEN)

    if not skillflow_enabled():
        raise ValidationError("Skillflow card payments are temporarily unavailable")

    customer_email = str(getattr(actor, "email", "") or "").strip()

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
        checkout_amount = canonical_stable_to_skillflow_amount(expected_canonical_amount)
        if Decimal(checkout_amount) < SKILLFLOW_MIN_EUR_AMOUNT:
            raise ValidationError("Selected token pack is below Skillflow's minimum payment amount")

        existing_session = _find_reusable_skillflow_session(
            wallet=wallet,
            token_pack_code=token_pack_snapshot["code"],
            expected_canonical_amount=expected_canonical_amount,
            checkout_amount=checkout_amount,
        )
        if existing_session is not None:
            return existing_session

        _enforce_deposit_open_cooldown(user=wallet.user)

        public_id = uuid.uuid4()
        user_id = str(wallet.user_id)
        currency_usd_rate = format(get_fiat_usd_rate(SKILLFLOW_CURRENCY), "f")
        request_metadata = _skillflow_request_metadata(
            session_public_id=public_id,
            token_pack_snapshot=token_pack_snapshot,
            expected_canonical_amount=expected_canonical_amount,
            checkout_amount=checkout_amount,
        )
        synthetic_reference = f"skillflow:{public_id.hex}"
        expires_at = timezone.now() + timedelta(seconds=get_skillflow_payment_ttl_seconds())

        metadata = {
            "display_label": SKILLFLOW_PAYMENT_METHOD_LABEL,
            "allocation_source": "provider_checkout",
            "chain_family": "provider",
            "token_pack": token_pack_snapshot,
            "payment_method": {
                "key": SKILLFLOW_PAYMENT_METHOD_KEY,
                "type": SKILLFLOW_PAYMENT_METHOD_TYPE,
                "label": SKILLFLOW_PAYMENT_METHOD_LABEL,
                "show_network_step": False,
            },
            "payment_provider": _provider_metadata_for_session(
                session_public_id=public_id,
                user_id=user_id,
                checkout_amount=checkout_amount,
                checkout_currency_usd_rate=currency_usd_rate,
                request_metadata=request_metadata,
                status="CREATING",
            ),
            "amount_unit": "canonical_stable",
            "expected_canonical_stable_amount": expected_canonical_amount,
            "canonical_currency": "USD",
            "checkout_currency": SKILLFLOW_CURRENCY,
            "checkout_amount": checkout_amount,
            "checkout_currency_usd_rate": currency_usd_rate,
            "stablecoin_canonical_decimals": STABLECOIN_CANONICAL_DECIMALS,
            "platform_token_decimals": PLATFORM_TOKEN_DECIMALS,
            "platform_tokens_per_stablecoin": PLATFORM_TOKENS_PER_STABLECOIN,
        }

        deposit_session = DepositSession.objects.create(
            public_id=public_id,
            user=wallet.user,
            wallet=wallet,
            chain=SKILLFLOW_CHAIN,
            asset_code=SKILLFLOW_CURRENCY,
            token_contract_address="",
            route_key=skillflow_route_key(),
            display_label=SKILLFLOW_PAYMENT_METHOD_LABEL,
            deposit_address=synthetic_reference,
            address_derivation_ref=synthetic_reference,
            derivation_index=None,
            derivation_path="",
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            min_amount=expected_canonical_amount,
            expected_onchain_raw_amount=None,
            required_confirmations=1,
            expires_at=expires_at,
            created_by=actor,
            metadata=metadata,
            metadata_version=LEDGER_METADATA_VERSION,
        )

    session_path = reverse(
        "wallet_deposit_session",
        kwargs={"public_id": deposit_session.public_id},
    )
    return_url = _build_absolute_url(session_path)
    try:
        checkout_response = create_skillflow_checkout(
            user_id=user_id,
            amount_eur=checkout_amount,
            email=customer_email,
            redirect_url=return_url,
            cancel_url=return_url,
            metadata=request_metadata,
        )
    except Exception as exc:
        with transaction.atomic():
            failed_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
            if failed_session.status not in {
                DepositSession.STATUS_CREDITED,
                getattr(DepositSession, "STATUS_SWEPT", "swept"),
            }:
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

    payment_id = str(checkout_response["paymentId"])
    with transaction.atomic():
        deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
        metadata = dict(deposit_session.metadata or {})
        current_provider = dict(metadata.get("payment_provider") or {})
        current_reference = str(current_provider.get("reference") or "").strip()
        if current_reference and current_reference != payment_id:
            raise ValidationError("Skillflow response paymentId conflicts with the webhook paymentId")

        provider = _provider_metadata_for_session(
            session_public_id=deposit_session.public_id,
            user_id=user_id,
            checkout_amount=checkout_amount,
            checkout_currency_usd_rate=currency_usd_rate,
            request_metadata=request_metadata,
            reference=payment_id,
            mollie_id=str(current_provider.get("mollie_id") or ""),
            checkout_url=str(checkout_response["url"]),
            description=str(checkout_response["description"]),
            status=(
                str(current_provider.get("status") or "PAID")
                if deposit_session.status == DepositSession.STATUS_CREDITED
                else "CREATED"
            ),
            raw_response=checkout_response,
        )
        for key in (
            "last_event",
            "last_status",
            "last_webhook_at",
            "last_payload",
            "provider_timestamp",
            "payer_email",
            "credited_at",
            "credited_ledger_txn_id",
        ):
            if key in current_provider:
                provider[key] = current_provider[key]
        metadata["payment_provider"] = provider
        deposit_session.metadata = metadata
        deposit_session.save(update_fields=["metadata", "updated_at"])

    return deposit_session


def _get_skillflow_session_from_payload(payload: dict) -> DepositSession:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise ValidationError("Skillflow webhook metadata is missing")
    public_id = str(metadata.get("depositSessionPublicId") or "").strip()
    if not public_id:
        raise ValidationError("Skillflow webhook depositSessionPublicId is missing")
    try:
        parsed_public_id = uuid.UUID(public_id)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValidationError("Skillflow webhook depositSessionPublicId is invalid") from exc

    try:
        return DepositSession.objects.get(
            public_id=parsed_public_id,
            chain=SKILLFLOW_CHAIN,
            metadata__payment_provider__key=SKILLFLOW_PROVIDER_KEY,
        )
    except DepositSession.DoesNotExist as exc:
        raise ValidationError("Unknown Skillflow deposit session") from exc


def _parse_skillflow_webhook_amount(value) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("Skillflow webhook amount is invalid") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValidationError("Skillflow webhook amount must be finite and positive")
    if amount != amount.quantize(Decimal("0.01")):
        raise ValidationError("Skillflow webhook amount has more than two decimal places")
    return amount


def _validate_skillflow_webhook_against_session(
    *,
    payload: dict,
    deposit_session: DepositSession,
) -> tuple[str, str, Decimal, str]:
    if not isinstance(payload, dict):
        raise ValidationError("Skillflow webhook payload must be an object")
    if str(payload.get("event") or "").strip() != "payment.succeeded":
        raise ValidationError("Unsupported Skillflow webhook event")
    if str(payload.get("status") or "").strip().lower() != "paid":
        raise ValidationError("Skillflow webhook payment is not paid")

    payment_id = str(payload.get("paymentId") or "").strip()
    mollie_id = str(payload.get("mollieId") or "").strip()
    user_id = str(payload.get("userId") or "").strip()
    currency = str(payload.get("currency") or "").strip().upper()
    if not payment_id or len(payment_id) > 128:
        raise ValidationError("Skillflow webhook paymentId is invalid")
    if not mollie_id or len(mollie_id) > 128:
        raise ValidationError("Skillflow webhook mollieId is invalid")
    if not user_id:
        raise ValidationError("Skillflow webhook userId is missing")
    if currency != SKILLFLOW_CURRENCY:
        raise ValidationError("Skillflow webhook currency must be EUR")

    provider_timestamp = str(payload.get("timestamp") or "").strip()
    parsed_provider_timestamp = parse_datetime(provider_timestamp)
    if parsed_provider_timestamp is None or timezone.is_naive(parsed_provider_timestamp):
        raise ValidationError("Skillflow webhook timestamp must be an ISO 8601 timezone-aware value")

    paid_amount = _parse_skillflow_webhook_amount(payload.get("amount"))
    metadata = deposit_session.metadata or {}
    provider = metadata.get("payment_provider") or {}
    if deposit_session.chain != SKILLFLOW_CHAIN or provider.get("key") != SKILLFLOW_PROVIDER_KEY:
        raise ValidationError("Deposit session is not a Skillflow provider session")

    expected_user_id = str(provider.get("user_id") or deposit_session.user_id)
    if user_id != expected_user_id:
        raise ValidationError("Skillflow webhook userId does not match the deposit session")

    expected_payment_id = str(provider.get("reference") or provider.get("payment_id") or "").strip()
    if expected_payment_id and payment_id != expected_payment_id:
        raise ValidationError("Skillflow webhook paymentId does not match the deposit session")

    expected_currency = str(
        provider.get("checkout_currency") or metadata.get("checkout_currency") or ""
    ).strip().upper()
    if expected_currency != SKILLFLOW_CURRENCY:
        raise ValidationError("Skillflow deposit session has an invalid checkout currency")
    try:
        expected_amount = Decimal(
            str(provider.get("checkout_amount") or metadata.get("checkout_amount"))
        )
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("Skillflow deposit session has an invalid checkout amount") from exc
    if paid_amount != expected_amount:
        raise ValidationError("Skillflow webhook amount does not match the frozen checkout amount")

    request_metadata = provider.get("request_metadata")
    if not isinstance(request_metadata, dict) or payload.get("metadata") != request_metadata:
        raise ValidationError("Skillflow webhook metadata does not match the checkout request")

    return payment_id, mollie_id, paid_amount, provider_timestamp


@transaction.atomic
def credit_skillflow_deposit_session(
    *,
    actor,
    deposit_session: DepositSession,
    payload: dict,
    created_by=None,
):
    _require_perm(actor, "ledger.can_credit_confirmed_deposits")
    require_ledger_operation_enabled(LEDGER_OPERATION_FLAG_CREDITING)
    created_by = _resolve_created_by(actor=actor, created_by=created_by)

    deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
    wallet = TokenWallet.objects.select_for_update().get(id=deposit_session.wallet_id)
    deposit_session.wallet = wallet

    payment_id, mollie_id, paid_amount, provider_timestamp = (
        _validate_skillflow_webhook_against_session(
            payload=payload,
            deposit_session=deposit_session,
        )
    )
    metadata = dict(deposit_session.metadata or {})
    provider = dict(metadata.get("payment_provider") or {})

    duplicate_payment = (
        DepositSession.objects.filter(
            metadata__payment_provider__key=SKILLFLOW_PROVIDER_KEY,
            metadata__payment_provider__reference=payment_id,
        )
        .exclude(id=deposit_session.id)
        .exists()
    )
    duplicate_mollie = (
        DepositSession.objects.filter(
            metadata__payment_provider__key=SKILLFLOW_PROVIDER_KEY,
            metadata__payment_provider__mollie_id=mollie_id,
        )
        .exclude(id=deposit_session.id)
        .exists()
    )
    if duplicate_payment or duplicate_mollie:
        raise ValidationError("Skillflow payment identifiers are already bound to another session")

    if deposit_session.status == DepositSession.STATUS_CREDITED:
        if not deposit_session.credited_ledger_txn_id:
            raise ValidationError("Credited Skillflow session is missing its ledger transaction")
        if str(provider.get("reference") or "") != payment_id:
            raise ValidationError("Credited Skillflow session has a different paymentId")
        if str(provider.get("mollie_id") or "") != mollie_id:
            raise ValidationError("Credited Skillflow session has a different mollieId")
        return deposit_session.credited_ledger_txn

    if deposit_session.status == getattr(DepositSession, "STATUS_SWEPT", "swept"):
        if deposit_session.credited_ledger_txn_id:
            return deposit_session.credited_ledger_txn
        raise ValidationError("Swept Skillflow sessions cannot be credited")

    token_pack_snapshot = metadata.get("token_pack") or {}
    user_credit_amount = int(token_pack_snapshot.get("token_amount") or 0)
    expected_gross_canonical_amount = int(token_pack_snapshot.get("gross_stable_amount") or 0)
    expected_net_canonical_amount = _token_pack_credit_required_canonical_amount(
        token_pack_snapshot
    )
    if user_credit_amount <= 0 or expected_net_canonical_amount is None:
        raise ValidationError("Skillflow session is missing a valid token pack snapshot")

    frozen_currency_usd_rate = str(
        provider.get("checkout_currency_usd_rate")
        or metadata.get("checkout_currency_usd_rate")
        or ""
    ).strip()
    paid_canonical_amount = skillflow_amount_to_canonical_stable_units(
        paid_amount,
        currency_usd_rate=frozen_currency_usd_rate,
    )
    if paid_canonical_amount < expected_gross_canonical_amount:
        raise ValidationError("Skillflow paid amount is below the frozen token pack price")

    gross_token_equivalent_amount = _convert_canonical_stable_to_platform_tokens(
        paid_canonical_amount
    )
    platform_fee_credit_amount = gross_token_equivalent_amount - user_credit_amount
    if platform_fee_credit_amount < 0:
        raise ValidationError("Skillflow paid amount is lower than the token pack token value")

    clearing_wallet = get_external_asset_clearing_wallet()
    platform_fees_wallet = get_system_wallet(
        TokenWallet.SYSTEM_PLATFORM_FEES,
        allow_negative=False,
    )
    entries = [
        (clearing_wallet, -int(gross_token_equivalent_amount)),
        (wallet, int(user_credit_amount)),
    ]
    if platform_fee_credit_amount > 0:
        entries.append((platform_fees_wallet, int(platform_fee_credit_amount)))

    txn = apply_ledger_transaction(
        actor=actor,
        kind="deposit",
        entries=entries,
        created_by=created_by,
        external_id=f"skillflow-deposit-credit:{payment_id}",
        memo=f"Confirmed Skillflow top-up {payment_id}",
        metadata={
            "source": "skillflow_provider_deposit",
            "deposit_session_id": deposit_session.id,
            "deposit_session_public_id": str(deposit_session.public_id),
            "provider": SKILLFLOW_PROVIDER_KEY,
            "provider_reference": payment_id,
            "skillflow_payment_id": payment_id,
            "mollie_id": mollie_id,
            "event": "payment.succeeded",
            "provider_status": "paid",
            "provider_timestamp": provider_timestamp,
            "checkout_currency": SKILLFLOW_CURRENCY,
            "checkout_amount": format(paid_amount, ".2f"),
            "checkout_currency_usd_rate": frozen_currency_usd_rate,
            "observed_canonical_stable_amount": int(paid_canonical_amount),
            "expected_gross_canonical_stable_amount": int(expected_gross_canonical_amount),
            "expected_net_canonical_stable_amount": int(expected_net_canonical_amount),
            "expected_fee_canonical_stable_amount": int(
                token_pack_snapshot.get("fee_stable_amount") or 0
            ),
            "user_credit_amount": int(user_credit_amount),
            "platform_fee_credit_amount": int(platform_fee_credit_amount),
            "gross_token_equivalent_amount": int(gross_token_equivalent_amount),
            "token_pack": token_pack_snapshot,
            "payment_method": metadata.get("payment_method") or {},
            "payment_provider": provider,
            "raw_payload": payload,
            "amount_unit": "canonical_stable",
            "stablecoin_canonical_decimals": STABLECOIN_CANONICAL_DECIMALS,
            "platform_token_decimals": PLATFORM_TOKEN_DECIMALS,
            "platform_tokens_per_stablecoin": PLATFORM_TOKENS_PER_STABLECOIN,
        },
    )

    provider.update(
        {
            "key": SKILLFLOW_PROVIDER_KEY,
            "reference": payment_id,
            "payment_id": payment_id,
            "mollie_id": mollie_id,
            "status": "PAID",
            "last_status": "paid",
            "last_event": "payment.succeeded",
            "last_webhook_at": timezone.now().isoformat(),
            "provider_timestamp": provider_timestamp,
            "payer_email": str(payload.get("email") or "").strip(),
            "last_payload": payload,
            "credited_at": timezone.now().isoformat(),
            "credited_ledger_txn_id": txn.id,
        }
    )
    metadata["payment_provider"] = provider

    deposit_session.metadata = metadata
    deposit_session.observed_txid = payment_id
    deposit_session.observed_amount = paid_canonical_amount
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


def process_skillflow_webhook(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValidationError("Skillflow webhook payload must be an object")
    deposit_session = _get_skillflow_session_from_payload(payload)
    actor = _get_internal_deposit_service_actor()
    txn = credit_skillflow_deposit_session(
        actor=actor,
        deposit_session=deposit_session,
        payload=payload,
    )
    return {
        "provider": SKILLFLOW_PROVIDER_KEY,
        "provider_reference": str(payload.get("paymentId") or ""),
        "deposit_session_public_id": str(deposit_session.public_id),
        "ledger_txn_id": txn.id,
        "credited": True,
    }


__all__ = [
    "credit_skillflow_deposit_session",
    "get_skillflow_deposit_option",
    "open_skillflow_deposit_session",
    "process_skillflow_webhook",
]
