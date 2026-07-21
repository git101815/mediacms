from __future__ import annotations

import secrets
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qsl, urlparse

from django.core.exceptions import ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from ledger.fiat import get_fiat_usd_rate
from ledger.internal_api import _get_internal_deposit_service_actor
from ledger.models import (
    DepositSession,
    LEDGER_METADATA_VERSION,
    LedgerHold,
    LedgerTransaction,
    TokenPack,
    TokenWallet,
)
from ledger.providers.mtpelerin import (
    MTPERELIN_PAYMENT_METHOD_TYPE,
    MTPERELIN_PROVIDER_KEY,
    build_mtpelerin_checkout_url,
    format_mtpelerin_target_amount,
    get_mtpelerin_fiat_currencies,
    get_mtpelerin_payment_ttl_seconds,
    get_mtpelerin_quote,
    get_mtpelerin_quote_max_age_seconds,
    get_mtpelerin_settlement_route_preferences,
    mtpelerin_enabled,
    mtpelerin_route_available,
)
from ledger.services import (
    _build_token_pack_snapshot,
    apply_ledger_transaction,
    create_wallet_hold,
    get_external_asset_clearing_wallet,
    list_available_deposit_options,
    open_user_deposit_session,
)
from ledger.sweeper_signer import sign_mtpelerin_address_validation


def _network_label(chain: str) -> str:
    labels = {
        "ethereum": "Ethereum",
        "arbitrum": "Arbitrum One",
        "base": "Base",
        "bsc": "BNB Chain",
    }
    normalized = str(chain or "").strip().lower()
    return labels.get(normalized, normalized)


def _payment_method_key(fiat_currency: str) -> str:
    return f"mtpelerin:{str(fiat_currency or '').strip().lower()}"


def _payment_method_label(fiat_currency: str) -> str:
    fiat = str(fiat_currency or "").strip().upper()
    return f"Bank transfer (Mt Pelerin · {fiat})"


def _ordered_mtpelerin_settlement_routes() -> list[dict]:
    routes = list_available_deposit_options()
    preferences = get_mtpelerin_settlement_route_preferences()
    ordered = []
    seen_route_keys = set()

    for preference in preferences:
        normalized_preference = str(preference).strip().lower()
        for route in routes:
            route_key = str(route.get("key") or "").strip()
            if not route_key or route_key in seen_route_keys:
                continue
            chain_asset_key = (
                f"{str(route.get('chain') or '').strip().lower()}:"
                f"{str(route.get('asset_code') or '').strip().lower()}"
            )
            if normalized_preference not in {
                route_key.lower(),
                chain_asset_key,
            }:
                continue
            ordered.append(route)
            seen_route_keys.add(route_key)

    return ordered


def _find_mtpelerin_route(option_key: str) -> dict:
    route = next(
        (
            item
            for item in list_available_deposit_options()
            if str(item.get("key") or "") == str(option_key or "")
        ),
        None,
    )
    if route is None:
        raise ValidationError("Invalid Mt Pelerin deposit route")
    return route


def _mtpelerin_launch_url(session_public_id) -> str:
    return reverse(
        "wallet_mtpelerin_launch",
        kwargs={"public_id": session_public_id},
    )


MTPERELIN_EVENT_ORDER_CREATED = "orderCreated"
MTPERELIN_EVENT_PAYMENT_SUBMITTED = "paymentSubmitted"
MTPERELIN_PENDING_CREDIT_KEY = "mtpelerin_pending_credit"
MTPERELIN_PENDING_CREDIT_STATUS_HELD = "held"
MTPERELIN_PENDING_CREDIT_STATUS_SETTLED = "settled"
MTPERELIN_PENDING_CREDIT_STATUS_EXPIRED = "expired"
MTPERELIN_PENDING_MEMO = (
    "Waiting for transaction to complete (can take several days)"
)

MTPERELIN_EVENT_ALLOWED_FIELDS = {
    MTPERELIN_EVENT_ORDER_CREATED: {
        "id",
        "expirationDate",
        "cryptoAddress",
        "currencyIn",
        "currencyOut",
        "marketRate",
        "network",
        "paymentMode",
        "type",
        "valueIn",
        "valueOut",
    },
    MTPERELIN_EVENT_PAYMENT_SUBMITTED: {
        "paymentType",
        "paymentId",
    },
}


def _sanitize_mtpelerin_event_data(
    event_type: str,
    event_data,
) -> dict:
    if not isinstance(event_data, dict):
        raise ValidationError("Mt Pelerin event data must be an object")

    allowed_fields = MTPERELIN_EVENT_ALLOWED_FIELDS.get(event_type)
    if allowed_fields is None:
        raise ValidationError("Unsupported Mt Pelerin browser event")

    sanitized = {}
    for key in allowed_fields:
        value = event_data.get(key)
        if value in (None, ""):
            continue
        if isinstance(value, (str, int, float, bool)):
            sanitized[key] = str(value).strip()[:512]

    return sanitized


def _mtpelerin_widget_options(checkout_url: str) -> dict:
    parsed = urlparse(str(checkout_url or "").strip())
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValidationError("Mt Pelerin checkout URL must use HTTPS")

    options = dict(parse_qsl(parsed.query, keep_blank_values=True))
    options.pop("type", None)
    if not options.get("_ctkn"):
        raise ValidationError("Mt Pelerin widget options are missing _ctkn")
    return options


def _update_provider_metadata(
    *,
    session: DepositSession,
    provider: dict,
    extra_metadata: dict | None = None,
) -> DepositSession:
    metadata = dict(session.metadata or {})
    display_label = str(provider.get("label") or "Bank transfer (Mt Pelerin)")
    metadata["display_label"] = display_label
    metadata["payment_provider"] = provider
    if extra_metadata:
        metadata.update(extra_metadata)
    session.metadata = metadata
    session.display_label = display_label
    session.save(update_fields=["metadata", "display_label", "updated_at"])
    return session


def _preflight_mtpelerin_purchase(
    *,
    option_key: str,
    fiat_currency: str,
    token_pack: TokenPack,
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> dict:
    route = _find_mtpelerin_route(option_key)
    fiat = str(fiat_currency or "").strip().upper()
    asset_code = str(route.get("asset_code") or "").strip().upper()
    chain = str(route.get("chain") or "").strip().lower()

    if fiat not in get_mtpelerin_fiat_currencies():
        raise ValidationError("Unsupported Mt Pelerin fiat currency")
    if asset_code != "USDC":
        raise ValidationError("Mt Pelerin settlement asset must be USDC")
    if not mtpelerin_route_available(chain=chain, asset_code=asset_code):
        raise ValidationError(
            "The selected Mt Pelerin settlement route is currently unavailable"
        )

    # Keep the TokenPack row lock local. The provider request must not run
    # while the database transaction is open.
    with transaction.atomic():
        token_pack_snapshot = _build_token_pack_snapshot(
            token_pack=token_pack,
            payment_price_bps=payment_price_bps,
            payment_price_fixed_canonical=payment_price_fixed_canonical,
        )

    expected_canonical_amount = int(token_pack_snapshot["gross_stable_amount"])
    quote = get_mtpelerin_quote(
        fiat_currency=fiat,
        chain=chain,
        asset_code=asset_code,
        target_canonical_amount=expected_canonical_amount,
    )

    try:
        source_amount = Decimal(str(quote["sourceAmount"]))
        destination_amount = Decimal(str(quote["destAmount"]))
    except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValidationError("Mt Pelerin quote is missing valid amounts") from exc
    if source_amount <= 0 or destination_amount <= 0:
        raise ValidationError("Mt Pelerin quote returned invalid amounts")

    return {
        "route": route,
        "fiat_currency": fiat,
        "source_amount": format(source_amount, "f"),
        "target_asset_amount": format_mtpelerin_target_amount(
            expected_canonical_amount
        ),
        "target_canonical_amount": expected_canonical_amount,
        "quote": quote,
        "prepared_at": timezone.now().isoformat(),
    }


def get_mtpelerin_deposit_options() -> list[dict]:
    if not mtpelerin_enabled():
        return []

    try:
        fiat_currencies = get_mtpelerin_fiat_currencies()
        settlement_routes = _ordered_mtpelerin_settlement_routes()
    except Exception:
        return []

    options = []
    for fiat_currency in fiat_currencies:
        chosen = None
        for route in settlement_routes:
            chain = str(route.get("chain") or "").strip().lower()
            asset_code = str(route.get("asset_code") or "").strip().upper()
            if asset_code != "USDC":
                continue
            # Do not make Mt Pelerin network requests while rendering the wallet.
            # Availability and the exact quote are checked when the user opens
            # the provider session, so a transient provider outage cannot remove
            # the payment method from the UI.
            chosen = route
            break

        if chosen is None:
            continue

        route_key = str(chosen["key"])
        chain = str(chosen.get("chain") or "").strip().lower()
        asset_code = str(chosen.get("asset_code") or "").strip().upper()
        label = _payment_method_label(fiat_currency)
        options.append(
            {
                **chosen,
                "key": f"mtpelerin:{fiat_currency.lower()}:{route_key}",
                "deposit_route_key": route_key,
                "label": label,
                "route_label": label,
                "network_display": _network_label(chain),
                "payment_method_key": _payment_method_key(fiat_currency),
                "payment_method_label": label,
                "payment_method_type": MTPERELIN_PAYMENT_METHOD_TYPE,
                "provider_key": MTPERELIN_PROVIDER_KEY,
                "payment_currency": fiat_currency,
                "payment_currency_usd_rate": format(
                    get_fiat_usd_rate(fiat_currency),
                    "f",
                ),
                "payment_requires_route_selection": False,
                "payment_open_new_tab": True,
                "payment_price_mode": "fixed",
                "mtpelerin_settlement_asset_code": asset_code,
                "mtpelerin_settlement_network": _network_label(chain),
            }
        )

    return options


def open_mtpelerin_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    option_key: str,
    fiat_currency: str,
    token_pack: TokenPack,
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> DepositSession:
    if not mtpelerin_enabled():
        raise ValidationError(
            "Mt Pelerin bank transfers are temporarily unavailable"
        )

    preflight = _preflight_mtpelerin_purchase(
        option_key=option_key,
        fiat_currency=fiat_currency,
        token_pack=token_pack,
        payment_price_bps=payment_price_bps,
        payment_price_fixed_canonical=payment_price_fixed_canonical,
    )
    label = _payment_method_label(preflight["fiat_currency"])
    method_key = _payment_method_key(preflight["fiat_currency"])

    session = open_user_deposit_session(
        actor=actor,
        wallet=wallet,
        option_key=option_key,
        token_pack=token_pack,
        payment_method_key=method_key,
        payment_method_type=MTPERELIN_PAYMENT_METHOD_TYPE,
        payment_method_label=label,
        show_network_step=False,
        payment_price_bps=payment_price_bps,
        payment_price_fixed_canonical=payment_price_fixed_canonical,
        session_ttl_seconds=get_mtpelerin_payment_ttl_seconds(),
    )

    if session.status != DepositSession.STATUS_AWAITING_PAYMENT:
        return session
    if session.derivation_index is None:
        raise ValidationError(
            "Mt Pelerin session is missing its derivation index"
        )

    provider = {
        "key": MTPERELIN_PROVIDER_KEY,
        "label": label,
        "payment_method_key": method_key,
        "payment_method_type": MTPERELIN_PAYMENT_METHOD_TYPE,
        "route_key": session.route_key,
        "session_public_id": str(session.public_id),
        "network_display": _network_label(session.chain),
        "chain": session.chain,
        "asset_code": session.asset_code,
        "token_contract_address": session.token_contract_address,
        "checkout_url": _mtpelerin_launch_url(session.public_id),
        "checkout_currency": preflight["fiat_currency"],
        "checkout_amount": preflight["source_amount"],
        "target_asset_amount": preflight["target_asset_amount"],
        "quote": preflight["quote"],
        "quote_prepared_at": preflight["prepared_at"],
        "status": "READY_TO_LAUNCH",
    }
    return _update_provider_metadata(
        session=session,
        provider=provider,
        extra_metadata={
            "allocation_source": "mtpelerin_bank_checkout",
            "metadata_version": LEDGER_METADATA_VERSION,
            "mtpelerin_preflight": preflight,
            "checkout_currency": preflight["fiat_currency"],
            "checkout_amount": preflight["source_amount"],
            "checkout_currency_usd_rate": format(
                get_fiat_usd_rate(preflight["fiat_currency"]),
                "f",
            ),
            "mtpelerin_target_asset_amount": preflight[
                "target_asset_amount"
            ],
        },
    )


def _load_or_refresh_preflight(session: DepositSession) -> dict:
    metadata = dict(session.metadata or {})
    preflight = metadata.get("mtpelerin_preflight")
    provider = dict(metadata.get("payment_provider") or {})
    if isinstance(preflight, dict):
        prepared = parse_datetime(str(preflight.get("prepared_at") or ""))
        if prepared is not None:
            if timezone.is_naive(prepared):
                prepared = timezone.make_aware(prepared)
            age = (timezone.now() - prepared).total_seconds()
            route = preflight.get("route")
            if (
                0 <= age <= get_mtpelerin_quote_max_age_seconds()
                and isinstance(route, dict)
                and str(route.get("key") or "") == str(session.route_key or "")
                and int(preflight.get("target_canonical_amount") or 0)
                == int(session.min_amount)
            ):
                return dict(preflight)

    route = _find_mtpelerin_route(session.route_key)
    fiat_currency = str(
        provider.get("checkout_currency")
        or metadata.get("checkout_currency")
        or ""
    ).strip().upper()
    quote = get_mtpelerin_quote(
        fiat_currency=fiat_currency,
        chain=session.chain,
        asset_code=session.asset_code,
        target_canonical_amount=int(session.min_amount),
        force_refresh=True,
    )
    return {
        "route": route,
        "fiat_currency": fiat_currency,
        "source_amount": quote["sourceAmount"],
        "target_asset_amount": format_mtpelerin_target_amount(
            int(session.min_amount)
        ),
        "target_canonical_amount": int(session.min_amount),
        "quote": quote,
        "prepared_at": timezone.now().isoformat(),
    }


def prepare_mtpelerin_browser_launch(
    *,
    session: DepositSession,
    actor,
) -> dict:
    if session.status != DepositSession.STATUS_AWAITING_PAYMENT:
        raise ValidationError(
            "Mt Pelerin session is no longer awaiting payment"
        )
    if session.derivation_index is None:
        raise ValidationError(
            "Mt Pelerin session is missing its derivation index"
        )
    if session.user_id != getattr(actor, "id", None):
        raise ValidationError("Mt Pelerin session does not belong to this user")

    metadata = dict(session.metadata or {})
    provider = dict(metadata.get("payment_provider") or {})
    if provider.get("key") != MTPERELIN_PROVIDER_KEY:
        raise ValidationError("Deposit session is not a Mt Pelerin session")
    if str(session.asset_code or "").strip().upper() != "USDC":
        raise ValidationError("Mt Pelerin settlement asset must be USDC")

    preflight = _load_or_refresh_preflight(session)
    validation_code = str(1000 + secrets.randbelow(9000))
    signer_result = sign_mtpelerin_address_validation(
        chain=session.chain,
        derivation_index=int(session.derivation_index),
        address=session.deposit_address,
        code=validation_code,
    )
    checkout_url = build_mtpelerin_checkout_url(
        fiat_currency=preflight["fiat_currency"],
        chain=session.chain,
        asset_code=session.asset_code,
        source_amount=preflight["source_amount"],
        target_canonical_amount=int(session.min_amount),
        address=signer_result["address"],
        validation_code=validation_code,
        validation_signature_b64=signer_result["signature"],
    )

    provider.update(
        {
            "status": "LAUNCH_READY",
            "checkout_url": _mtpelerin_launch_url(session.public_id),
            "checkout_currency": preflight["fiat_currency"],
            "checkout_amount": preflight["source_amount"],
            "target_asset_amount": preflight["target_asset_amount"],
            "quote": preflight["quote"],
            "quote_prepared_at": preflight["prepared_at"],
            "launch_prepared_at": timezone.now().isoformat(),
        }
    )
    _update_provider_metadata(
        session=session,
        provider=provider,
        extra_metadata={
            "mtpelerin_preflight": preflight,
            "checkout_currency": preflight["fiat_currency"],
            "checkout_amount": preflight["source_amount"],
            "checkout_currency_usd_rate": format(
                get_fiat_usd_rate(preflight["fiat_currency"]),
                "f",
            ),
            "mtpelerin_target_asset_amount": preflight[
                "target_asset_amount"
            ],
        },
    )

    # The signature is returned only in the short-lived redirect URL and is
    # never persisted in DepositSession metadata.
    return {
        "checkout_url": checkout_url,
        "widget_options": _mtpelerin_widget_options(checkout_url),
        "event_url": reverse(
            "wallet_mtpelerin_event",
            kwargs={"public_id": session.public_id},
        ),
        "wallet_url": reverse("wallet"),
        "session_url": reverse(
            "wallet_deposit_session",
            kwargs={"public_id": session.public_id},
        ),
    }


@transaction.atomic
def record_mtpelerin_browser_event(
    *,
    session: DepositSession,
    actor,
    event_type: str,
    event_data,
) -> dict:
    event_type = str(event_type or "").strip()
    sanitized = _sanitize_mtpelerin_event_data(
        event_type,
        event_data,
    )

    session = (
        DepositSession.objects.select_for_update()
        .select_related("wallet")
        .get(id=session.id)
    )
    if session.user_id != getattr(actor, "id", None):
        raise ValidationError(
            "Mt Pelerin session does not belong to this user"
        )

    metadata = dict(session.metadata or {})
    provider = dict(metadata.get("payment_provider") or {})
    if provider.get("key") != MTPERELIN_PROVIDER_KEY:
        raise ValidationError("Deposit session is not a Mt Pelerin session")

    if session.status in {
        DepositSession.STATUS_CREDITED,
        DepositSession.STATUS_SWEPT,
        DepositSession.STATUS_EXPIRED,
        DepositSession.STATUS_FAILED,
        DepositSession.STATUS_CANCELED,
    }:
        raise ValidationError("Mt Pelerin session is no longer active")

    now = timezone.now()
    if session.expires_at <= now:
        raise ValidationError("Mt Pelerin session has expired")

    events = dict(metadata.get("mtpelerin_browser_events") or {})

    if event_type == MTPERELIN_EVENT_ORDER_CREATED:
        events["order_created"] = {
            **sanitized,
            "recorded_at": now.isoformat(),
        }
        provider["status"] = "ORDER_CREATED"
        order_id = str(sanitized.get("id") or "").strip()
        if order_id:
            provider["order_id"] = order_id
            provider["reference"] = order_id

        metadata["mtpelerin_browser_events"] = events
        metadata["payment_provider"] = provider
        session.metadata = metadata
        session.save(update_fields=["metadata", "updated_at"])

        return {
            "event_type": event_type,
            "provider_status": provider["status"],
            "pending_credit_status": "",
        }

    payment_type = str(
        sanitized.get("paymentType") or ""
    ).strip()
    if payment_type != "bankTransfer":
        raise ValidationError(
            "Mt Pelerin paymentSubmitted must be a bank transfer"
        )

    payment_id = str(
        sanitized.get("paymentId") or ""
    ).strip()
    if not payment_id:
        raise ValidationError(
            "Mt Pelerin paymentSubmitted is missing paymentId"
        )

    pending_credit = dict(
        metadata.get(MTPERELIN_PENDING_CREDIT_KEY) or {}
    )
    pending_status = str(
        pending_credit.get("status") or ""
    ).strip().lower()

    if pending_status in {
        MTPERELIN_PENDING_CREDIT_STATUS_HELD,
        MTPERELIN_PENDING_CREDIT_STATUS_SETTLED,
    }:
        return {
            "event_type": event_type,
            "provider_status": provider.get("status") or "",
            "pending_credit_status": pending_status,
        }
    if pending_status == MTPERELIN_PENDING_CREDIT_STATUS_EXPIRED:
        raise ValidationError(
            "Mt Pelerin pending credit has already expired"
        )

    token_pack = dict(metadata.get("token_pack") or {})
    token_amount = int(token_pack.get("token_amount") or 0)
    if token_amount <= 0:
        raise ValidationError(
            "Mt Pelerin session is missing a valid token pack snapshot"
        )

    service_actor = _get_internal_deposit_service_actor()
    clearing_wallet = get_external_asset_clearing_wallet()
    pending_txn = apply_ledger_transaction(
        actor=service_actor,
        kind="mtpelerin_deposit_pending",
        entries=[
            (clearing_wallet, -token_amount),
            (session.wallet, token_amount),
        ],
        external_id=(
            "mtpelerin-pending-credit:"
            f"{session.public_id}"
        ),
        memo=MTPERELIN_PENDING_MEMO,
        metadata={
            "source": "mtpelerin_payment_submitted",
            "deposit_session_id": session.id,
            "deposit_session_public_id": str(session.public_id),
            "user_id": session.user_id,
            "token_amount": token_amount,
            "payment_id": payment_id,
            "payment_type": payment_type,
            "expires_at": session.expires_at.isoformat(),
            "token_pack": token_pack,
        },
    )
    if pending_txn.status != LedgerTransaction.STATUS_POSTED:
        raise ValidationError(
            "Mt Pelerin pending credit transaction was not posted"
        )

    hold = create_wallet_hold(
        actor=service_actor,
        wallet=session.wallet,
        amount=token_amount,
        reason=MTPERELIN_PENDING_MEMO,
        metadata={
            "source": "mtpelerin_payment_submitted",
            "deposit_session_id": session.id,
            "deposit_session_public_id": str(session.public_id),
            "pending_ledger_txn_id": pending_txn.id,
            "payment_id": payment_id,
            "expires_at": session.expires_at.isoformat(),
        },
    )

    events["payment_submitted"] = {
        **sanitized,
        "recorded_at": now.isoformat(),
    }
    provider.update(
        {
            "status": "PAYMENT_SUBMITTED",
            "payment_id": payment_id,
            "payment_type": payment_type,
            "payment_submitted_at": now.isoformat(),
        }
    )
    pending_credit = {
        "status": MTPERELIN_PENDING_CREDIT_STATUS_HELD,
        "amount": token_amount,
        "ledger_txn_id": pending_txn.id,
        "hold_id": hold.id,
        "payment_id": payment_id,
        "payment_type": payment_type,
        "submitted_at": now.isoformat(),
        "expires_at": session.expires_at.isoformat(),
        "memo": MTPERELIN_PENDING_MEMO,
    }
    metadata["mtpelerin_browser_events"] = events
    metadata["payment_provider"] = provider
    metadata[MTPERELIN_PENDING_CREDIT_KEY] = pending_credit
    session.metadata = metadata
    session.save(update_fields=["metadata", "updated_at"])

    return {
        "event_type": event_type,
        "provider_status": provider["status"],
        "pending_credit_status": pending_credit["status"],
        "pending_token_amount": token_amount,
    }


__all__ = [
    "get_mtpelerin_deposit_options",
    "open_mtpelerin_deposit_session",
    "prepare_mtpelerin_browser_launch",
    "record_mtpelerin_browser_event",
]
