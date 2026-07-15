from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_CEILING

from django.core.exceptions import ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from ledger.fiat import fiat_amount_to_canonical_stable_units, get_fiat_usd_rate
from ledger.models import DepositSession, LEDGER_METADATA_VERSION, TokenPack, TokenWallet
from ledger.providers.dfx import (
    DFX_PAYMENT_METHOD_KEY,
    DFX_PAYMENT_METHOD_LABEL,
    DFX_PAYMENT_METHOD_TYPE,
    DFX_PROVIDER_KEY,
    build_dfx_auth_payload,
    build_dfx_checkout_params,
    dfx_enabled,
    find_dfx_asset_for_route,
    get_dfx_app_base_url,
    get_dfx_assets_for_blockchain,
    get_dfx_auth_url,
    get_dfx_bank_limits,
    get_dfx_buy_quote,
    get_dfx_chain_name,
    get_dfx_fiat,
    get_dfx_fiat_currency,
    get_dfx_payment_ttl_seconds,
    get_dfx_public_base_url,
    round_dfx_source_amount,
)
from ledger.services import (
    _build_token_pack_snapshot,
    list_available_deposit_options,
    open_user_deposit_session,
)
from ledger.sweeper_signer import sign_dfx_auth_message


def _network_label(chain: str) -> str:
    labels = {
        "ethereum": "Ethereum",
        "arbitrum": "Arbitrum One",
        "base": "Base",
        "bsc": "BNB Chain",
    }
    normalized = str(chain or "").strip().lower()
    return labels.get(normalized, normalized)


def _absolute_dfx_return_url(session_public_id) -> str:
    path = reverse(
        "wallet_dfx_return",
        kwargs={"public_id": session_public_id},
    )
    return f"{get_dfx_public_base_url()}{path}"


def _dfx_launch_url(session_public_id) -> str:
    return reverse(
        "wallet_dfx_launch",
        kwargs={"public_id": session_public_id},
    )


def _preflight_dfx_purchase(
    *,
    option_key: str,
    token_pack: TokenPack,
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> dict:
    route = next(
        (
            item
            for item in list_available_deposit_options()
            if str(item.get("key") or "") == str(option_key or "")
        ),
        None,
    )
    if route is None:
        raise ValidationError("Invalid DFX deposit route")

    asset = find_dfx_asset_for_route(
        chain=route.get("chain") or "",
        asset_code=route.get("asset_code") or "",
        token_contract_address=route.get("token_contract_address") or "",
    )
    if asset is None:
        raise ValidationError("Selected MediaCMS route is not buyable through DFX")

    # _build_token_pack_snapshot() locks the TokenPack row with
    # select_for_update(). Keep that lock in a short local transaction instead
    # of holding a database transaction open during the DFX network request.
    with transaction.atomic():
        token_pack_snapshot = _build_token_pack_snapshot(
            token_pack=token_pack,
            payment_price_bps=payment_price_bps,
            payment_price_fixed_canonical=payment_price_fixed_canonical,
        )
    expected_canonical_amount = int(token_pack_snapshot["gross_stable_amount"])
    quote = get_dfx_buy_quote(
        asset_id=int(asset["id"]),
        target_canonical_amount=expected_canonical_amount,
        fiat_currency=get_dfx_fiat_currency(),
    )

    try:
        source_amount = Decimal(str(quote["sourceAmount"]))
    except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
        raise ValidationError("DFX quote is missing a valid fiat amount") from exc

    fiat = get_dfx_fiat(get_dfx_fiat_currency())
    minimum_fiat, maximum_fiat = get_dfx_bank_limits(fiat)
    if maximum_fiat <= 0:
        raise ValidationError("DFX bank transfers are currently unavailable")
    if source_amount < minimum_fiat:
        raise ValidationError(
            "Selected token pack is below DFX's minimum bank transfer amount"
        )
    if source_amount > maximum_fiat:
        raise ValidationError(
            "Selected token pack is above DFX's maximum bank transfer amount"
        )

    return {
        "asset": asset,
        "quote": quote,
        "source_amount": format(source_amount, "f"),
        "minimum_fiat": format(minimum_fiat, "f"),
        "maximum_fiat": format(maximum_fiat, "f"),
    }


def get_dfx_deposit_options() -> list[dict]:
    if not dfx_enabled():
        return []

    try:
        currency = get_dfx_fiat_currency()
        fiat = get_dfx_fiat(currency)
        minimum_fiat, maximum_fiat = get_dfx_bank_limits(fiat)
    except Exception:
        return []

    if maximum_fiat <= 0:
        return []

    dfx_min_canonical = (
        fiat_amount_to_canonical_stable_units(
            minimum_fiat,
            currency=currency,
            rounding=ROUND_CEILING,
        )
        if minimum_fiat > 0
        else 1
    )
    currency_rate = format(get_fiat_usd_rate(currency), "f")
    assets_by_blockchain: dict[str, list[dict]] = {}
    options = []

    for route in list_available_deposit_options():
        chain = str(route.get("chain") or "").strip().lower()
        try:
            blockchain = get_dfx_chain_name(chain)
            if blockchain not in assets_by_blockchain:
                assets_by_blockchain[blockchain] = get_dfx_assets_for_blockchain(
                    blockchain
                )
            asset = find_dfx_asset_for_route(
                chain=chain,
                asset_code=route.get("asset_code") or "",
                token_contract_address=route.get("token_contract_address") or "",
                assets=assets_by_blockchain[blockchain],
            )
        except Exception:
            continue

        if asset is None:
            continue

        route_key = str(route["key"])
        network_display = _network_label(chain)
        asset_code = str(route["asset_code"]).upper()
        min_amount = max(int(route["min_amount"]), int(dfx_min_canonical))
        options.append(
            {
                **route,
                "key": f"dfx:{route_key}",
                "deposit_route_key": route_key,
                "label": f"{DFX_PAYMENT_METHOD_LABEL} · {network_display} · {asset_code}",
                "route_label": f"{network_display} · {asset_code}",
                "network_display": network_display,
                "payment_method_key": DFX_PAYMENT_METHOD_KEY,
                "payment_method_label": DFX_PAYMENT_METHOD_LABEL,
                "payment_method_type": DFX_PAYMENT_METHOD_TYPE,
                "provider_key": DFX_PROVIDER_KEY,
                "payment_currency": currency,
                "payment_currency_usd_rate": currency_rate,
                "payment_requires_route_selection": True,
                "payment_price_mode": "provider_quote",
                "min_amount": min_amount,
                "dfx_asset_id": int(asset["id"]),
                "dfx_asset_unique_name": str(asset.get("uniqueName") or ""),
                "dfx_blockchain": blockchain,
                "dfx_bank_min_fiat": format(minimum_fiat, "f"),
                "dfx_bank_max_fiat": format(maximum_fiat, "f"),
            }
        )

    return options


def _update_provider_metadata(
    *,
    session: DepositSession,
    provider: dict,
    display_label: str,
    extra_metadata: dict | None = None,
) -> DepositSession:
    metadata = dict(session.metadata or {})
    metadata["display_label"] = display_label
    metadata["payment_provider"] = provider
    if extra_metadata:
        metadata.update(extra_metadata)
    session.metadata = metadata
    session.display_label = display_label
    session.save(update_fields=["metadata", "display_label", "updated_at"])
    return session


def open_dfx_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    option_key: str,
    token_pack: TokenPack,
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> DepositSession:
    if not dfx_enabled():
        raise ValidationError("DFX bank transfers are temporarily unavailable")

    # Validate the live DFX quote and bank limits before allocating an address.
    # The launch page obtains a fresh quote because the user may open it later.
    _preflight_dfx_purchase(
        option_key=option_key,
        token_pack=token_pack,
        payment_price_bps=payment_price_bps,
        payment_price_fixed_canonical=payment_price_fixed_canonical,
    )

    session = open_user_deposit_session(
        actor=actor,
        wallet=wallet,
        option_key=option_key,
        token_pack=token_pack,
        payment_method_key=DFX_PAYMENT_METHOD_KEY,
        payment_method_type=DFX_PAYMENT_METHOD_TYPE,
        payment_method_label=DFX_PAYMENT_METHOD_LABEL,
        show_network_step=False,
        payment_price_bps=payment_price_bps,
        payment_price_fixed_canonical=payment_price_fixed_canonical,
        session_ttl_seconds=get_dfx_payment_ttl_seconds(),
    )

    if session.status != DepositSession.STATUS_AWAITING_PAYMENT:
        return session
    if session.derivation_index is None:
        raise ValidationError("DFX session is missing its derivation index")

    display_label = (
        f"{DFX_PAYMENT_METHOD_LABEL} · "
        f"{_network_label(session.chain)} · {session.asset_code}"
    )
    provider = {
        "key": DFX_PROVIDER_KEY,
        "label": DFX_PAYMENT_METHOD_LABEL,
        "payment_method_key": DFX_PAYMENT_METHOD_KEY,
        "payment_method_type": DFX_PAYMENT_METHOD_TYPE,
        "route_key": session.route_key,
        "session_public_id": str(session.public_id),
        "network_display": _network_label(session.chain),
        "chain": session.chain,
        "asset_code": session.asset_code,
        "token_contract_address": session.token_contract_address,
        "external_transaction_id": str(session.public_id),
        "checkout_url": _dfx_launch_url(session.public_id),
        "status": "READY_TO_LAUNCH",
    }
    return _update_provider_metadata(
        session=session,
        provider=provider,
        display_label=display_label,
        extra_metadata={
            "allocation_source": "dfx_bank_checkout",
            "metadata_version": LEDGER_METADATA_VERSION,
        },
    )


def prepare_dfx_browser_launch(
    *,
    session: DepositSession,
    actor,
) -> dict:
    if session.status != DepositSession.STATUS_AWAITING_PAYMENT:
        raise ValidationError("DFX session is no longer awaiting payment")
    if session.derivation_index is None:
        raise ValidationError("DFX session is missing its derivation index")

    metadata = dict(session.metadata or {})
    current_provider = dict(metadata.get("payment_provider") or {})
    if current_provider.get("key") != DFX_PROVIDER_KEY:
        raise ValidationError("Deposit session is not a DFX session")

    display_label = (
        f"{DFX_PAYMENT_METHOD_LABEL} · "
        f"{_network_label(session.chain)} · {session.asset_code}"
    )

    try:
        asset = find_dfx_asset_for_route(
            chain=session.chain,
            asset_code=session.asset_code,
            token_contract_address=session.token_contract_address,
        )
        if asset is None:
            raise ValidationError("Selected MediaCMS route is not buyable through DFX")

        quote = get_dfx_buy_quote(
            asset_id=int(asset["id"]),
            target_canonical_amount=int(session.min_amount),
            fiat_currency=get_dfx_fiat_currency(),
        )
        checkout_amount = round_dfx_source_amount(quote["sourceAmount"])
        signer_result = sign_dfx_auth_message(
            chain=session.chain,
            derivation_index=int(session.derivation_index),
            address=session.deposit_address,
        )
        auth_payload = build_dfx_auth_payload(
            # The signed message contains the normalized lowercase address.
            # Submit that exact address to DFX or EIP-191 verification would
            # fail for checksum-cased MediaCMS addresses.
            address=signer_result["address"],
            signature=signer_result["signature"],
            chain=session.chain,
        )
        checkout_params = build_dfx_checkout_params(
            asset=asset,
            chain=session.chain,
            fiat_currency=get_dfx_fiat_currency(),
            source_amount=checkout_amount,
            external_transaction_id=str(session.public_id),
            redirect_uri=_absolute_dfx_return_url(session.public_id),
            customer_email=getattr(actor, "email", "") or "",
        )
    except Exception as exc:
        current_provider.update(
            {
                "status": "LAUNCH_FAILED",
                "last_error": str(exc)[:1000],
                "last_error_at": timezone.now().isoformat(),
            }
        )
        _update_provider_metadata(
            session=session,
            provider=current_provider,
            display_label=display_label,
        )
        raise

    currency = get_dfx_fiat_currency()
    current_provider.update(
        {
            "status": "LAUNCH_READY",
            "checkout_url": _dfx_launch_url(session.public_id),
            "checkout_currency": currency,
            "checkout_amount": checkout_amount,
            "checkout_currency_usd_rate": format(
                get_fiat_usd_rate(currency),
                "f",
            ),
            "target_asset_amount": quote["requestedTargetAmount"],
            "estimated_target_asset_amount": quote["estimatedTargetAmount"],
            "dfx_asset_id": int(asset["id"]),
            "dfx_asset_unique_name": str(asset.get("uniqueName") or ""),
            "dfx_blockchain": get_dfx_chain_name(session.chain),
            "quote": quote,
            "launch_prepared_at": timezone.now().isoformat(),
        }
    )
    _update_provider_metadata(
        session=session,
        provider=current_provider,
        display_label=display_label,
        extra_metadata={
            "checkout_currency": currency,
            "checkout_amount": checkout_amount,
            "checkout_currency_usd_rate": current_provider[
                "checkout_currency_usd_rate"
            ],
            "dfx_target_asset_amount": quote["requestedTargetAmount"],
        },
    )

    # The signature is returned only to the authenticated user's browser and is
    # never stored in the database. The browser calls DFX directly, so DFX sees
    # the end-user IP rather than the MediaCMS server IP.
    return {
        "auth_url": get_dfx_auth_url(),
        "auth_payload": auth_payload,
        "checkout_url": f"{get_dfx_app_base_url()}/buy",
        "checkout_params": checkout_params,
        "wallet_url": reverse("wallet"),
        "session_url": reverse(
            "wallet_deposit_session",
            kwargs={"public_id": session.public_id},
        ),
    }


__all__ = [
    "get_dfx_deposit_options",
    "open_dfx_deposit_session",
    "prepare_dfx_browser_launch",
]
