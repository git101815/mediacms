from __future__ import annotations

from django.core.exceptions import ValidationError
from django.urls import reverse
from django.utils import timezone

from ledger.models import (
    DepositSession,
    LEDGER_METADATA_VERSION,
    TokenPack,
    TokenWallet,
)
from ledger.providers.banxa import (
    BANXA_PAYMENT_METHOD_KEY,
    BANXA_PAYMENT_METHOD_LABEL,
    BANXA_PAYMENT_METHOD_TYPE,
    BANXA_PROVIDER_KEY,
    BANXA_REQUIRED_SETTLEMENT_ASSET_CODE,
    banxa_enabled,
    build_banxa_checkout_url,
    format_banxa_coin_amount,
    get_banxa_fiat_currency,
    get_banxa_network,
    get_banxa_payment_ttl_seconds,
    get_banxa_public_base_url,
    get_banxa_settlement_route_preferences,
)
from ledger.services import (
    list_available_deposit_options,
    open_user_deposit_session,
)


def _network_label(chain: str) -> str:
    labels = {
        "ethereum": "Ethereum",
        "arbitrum": "Arbitrum One",
        "base": "Base",
        "bsc": "BNB Chain",
    }
    normalized = str(chain or "").strip().lower()
    return labels.get(normalized, normalized)


def _banxa_launch_url(session_public_id) -> str:
    return reverse(
        "wallet_banxa_launch",
        kwargs={"public_id": session_public_id},
    )


def _absolute_banxa_return_url(session_public_id) -> str:
    path = reverse(
        "wallet_deposit_session",
        kwargs={"public_id": session_public_id},
    )
    return f"{get_banxa_public_base_url()}{path}"


def _ordered_banxa_settlement_routes() -> list[dict]:
    routes = list_available_deposit_options()
    preferences = get_banxa_settlement_route_preferences()
    ordered = []
    seen_route_keys = set()

    for preference in preferences:
        normalized_preference = str(
            preference or ""
        ).strip().lower()
        for route in routes:
            route_key = str(route.get("key") or "").strip()
            if not route_key or route_key in seen_route_keys:
                continue

            chain = str(
                route.get("chain") or ""
            ).strip().lower()
            asset_code = str(
                route.get("asset_code") or ""
            ).strip().upper()
            chain_asset_key = f"{chain}:{asset_code}".lower()

            if normalized_preference not in {
                route_key.lower(),
                chain_asset_key,
            }:
                continue
            if (
                asset_code
                != BANXA_REQUIRED_SETTLEMENT_ASSET_CODE
            ):
                continue

            try:
                get_banxa_network(chain)
            except ValidationError:
                continue

            ordered.append(route)
            seen_route_keys.add(route_key)

    return ordered


def _find_banxa_route(option_key: str) -> dict:
    route = next(
        (
            item
            for item in list_available_deposit_options()
            if str(item.get("key") or "")
            == str(option_key or "")
        ),
        None,
    )
    if route is None:
        raise ValidationError(
            "Invalid Banxa deposit route"
        )

    asset_code = str(
        route.get("asset_code") or ""
    ).strip().upper()
    if asset_code != BANXA_REQUIRED_SETTLEMENT_ASSET_CODE:
        raise ValidationError(
            "Banxa settlement asset must be USDC"
        )

    get_banxa_network(route.get("chain") or "")
    return route


def _update_provider_metadata(
    *,
    session: DepositSession,
    provider: dict,
    extra_metadata: dict | None = None,
) -> DepositSession:
    metadata = dict(session.metadata or {})
    display_label = str(
        provider.get("label")
        or BANXA_PAYMENT_METHOD_LABEL
    )
    metadata["display_label"] = display_label
    metadata["payment_provider"] = provider
    if extra_metadata:
        metadata.update(extra_metadata)

    session.metadata = metadata
    session.display_label = display_label
    session.save(
        update_fields=[
            "metadata",
            "display_label",
            "updated_at",
        ]
    )
    return session


def get_banxa_deposit_options() -> list[dict]:
    if not banxa_enabled():
        return []

    try:
        fiat_currency = get_banxa_fiat_currency()
        settlement_routes = _ordered_banxa_settlement_routes()
    except Exception:
        return []

    chosen = settlement_routes[0] if settlement_routes else None
    if chosen is None:
        return []

    route_key = str(chosen.get("key") or "")
    chain = str(
        chosen.get("chain") or ""
    ).strip().lower()
    asset_code = str(
        chosen.get("asset_code") or ""
    ).strip().upper()

    return [
        {
            **chosen,
            "key": f"banxa:{route_key}",
            "deposit_route_key": route_key,
            "label": BANXA_PAYMENT_METHOD_LABEL,
            "route_label": BANXA_PAYMENT_METHOD_LABEL,
            "network_display": _network_label(chain),
            "payment_method_key": BANXA_PAYMENT_METHOD_KEY,
            "payment_method_label": (
                BANXA_PAYMENT_METHOD_LABEL
            ),
            "payment_method_type": (
                BANXA_PAYMENT_METHOD_TYPE
            ),
            "provider_key": BANXA_PROVIDER_KEY,
            # Pack pricing remains canonical USD/USDC.
            # fiatType only preselects the Banxa checkout fiat.
            "payment_currency": "USD",
            "payment_currency_usd_rate": "1",
            "payment_requires_route_selection": False,
            "payment_open_new_tab": True,
            "payment_price_mode": "fixed",
            "banxa_checkout_fiat_currency": fiat_currency,
            "banxa_settlement_asset_code": asset_code,
            "banxa_settlement_network": (
                _network_label(chain)
            ),
        }
    ]


def open_banxa_deposit_session(
    *,
    actor,
    wallet: TokenWallet,
    option_key: str,
    token_pack: TokenPack,
    payment_price_bps=0,
    payment_price_fixed_canonical=0,
) -> DepositSession:
    if not banxa_enabled():
        raise ValidationError(
            "Banxa card payments are temporarily unavailable"
        )

    _find_banxa_route(option_key)

    session = open_user_deposit_session(
        actor=actor,
        wallet=wallet,
        option_key=option_key,
        token_pack=token_pack,
        payment_method_key=BANXA_PAYMENT_METHOD_KEY,
        payment_method_type=BANXA_PAYMENT_METHOD_TYPE,
        payment_method_label=BANXA_PAYMENT_METHOD_LABEL,
        show_network_step=False,
        payment_price_bps=payment_price_bps,
        payment_price_fixed_canonical=(
            payment_price_fixed_canonical
        ),
        session_ttl_seconds=(
            get_banxa_payment_ttl_seconds()
        ),
    )

    if session.status != DepositSession.STATUS_AWAITING_PAYMENT:
        return session
    if session.derivation_index is None:
        raise ValidationError(
            "Banxa session is missing its derivation index"
        )

    target_asset_amount = format_banxa_coin_amount(
        int(session.min_amount)
    )
    provider = {
        "key": BANXA_PROVIDER_KEY,
        "label": BANXA_PAYMENT_METHOD_LABEL,
        "payment_method_key": BANXA_PAYMENT_METHOD_KEY,
        "payment_method_type": BANXA_PAYMENT_METHOD_TYPE,
        "route_key": session.route_key,
        "session_public_id": str(session.public_id),
        "network_display": _network_label(session.chain),
        "chain": session.chain,
        "asset_code": session.asset_code,
        "token_contract_address": (
            session.token_contract_address
        ),
        "checkout_url": _banxa_launch_url(
            session.public_id
        ),
        "checkout_currency": "USD",
        "checkout_amount": target_asset_amount,
        "target_asset_amount": target_asset_amount,
        "checkout_fiat_currency": (
            get_banxa_fiat_currency()
        ),
        "status": "READY_TO_LAUNCH",
    }
    return _update_provider_metadata(
        session=session,
        provider=provider,
        extra_metadata={
            "allocation_source": (
                "banxa_public_checkout"
            ),
            "metadata_version": LEDGER_METADATA_VERSION,
            "checkout_currency": "USD",
            "checkout_amount": target_asset_amount,
            "banxa_target_asset_amount": (
                target_asset_amount
            ),
            "banxa_checkout_fiat_currency": (
                get_banxa_fiat_currency()
            ),
        },
    )


def prepare_banxa_browser_launch(
    *,
    session: DepositSession,
    actor,
) -> dict:
    if not banxa_enabled():
        raise ValidationError(
            "Banxa card payments are temporarily unavailable"
        )
    if session.status != DepositSession.STATUS_AWAITING_PAYMENT:
        raise ValidationError(
            "Banxa session is no longer awaiting payment"
        )
    if session.derivation_index is None:
        raise ValidationError(
            "Banxa session is missing its derivation index"
        )
    if session.user_id != getattr(actor, "id", None):
        raise ValidationError(
            "Banxa session does not belong to this user"
        )

    metadata = dict(session.metadata or {})
    provider = dict(
        metadata.get("payment_provider") or {}
    )
    if provider.get("key") != BANXA_PROVIDER_KEY:
        raise ValidationError(
            "Deposit session is not a Banxa session"
        )
    if (
        str(session.asset_code or "").strip().upper()
        != BANXA_REQUIRED_SETTLEMENT_ASSET_CODE
    ):
        raise ValidationError(
            "Banxa settlement asset must be USDC"
        )

    route = _find_banxa_route(session.route_key)
    if (
        str(route.get("chain") or "").strip().lower()
        != str(session.chain or "").strip().lower()
        or str(
            route.get("asset_code") or ""
        ).strip().upper()
        != str(session.asset_code or "").strip().upper()
        or str(
            route.get("token_contract_address") or ""
        ).strip().lower()
        != str(
            session.token_contract_address or ""
        ).strip().lower()
    ):
        raise ValidationError(
            "Banxa session no longer matches "
            "its MediaCMS deposit route"
        )

    target_asset_amount = format_banxa_coin_amount(
        int(session.min_amount)
    )
    checkout_url = build_banxa_checkout_url(
        chain=session.chain,
        asset_code=session.asset_code,
        target_canonical_amount=int(session.min_amount),
        wallet_address=session.deposit_address,
        return_url=_absolute_banxa_return_url(
            session.public_id
        ),
        fiat_currency=get_banxa_fiat_currency(),
    )

    provider.update(
        {
            "status": "LAUNCH_READY",
            "checkout_url": _banxa_launch_url(
                session.public_id
            ),
            "checkout_currency": "USD",
            "checkout_amount": target_asset_amount,
            "target_asset_amount": target_asset_amount,
            "checkout_fiat_currency": (
                get_banxa_fiat_currency()
            ),
            "launch_prepared_at": (
                timezone.now().isoformat()
            ),
        }
    )
    _update_provider_metadata(
        session=session,
        provider=provider,
        extra_metadata={
            "checkout_currency": "USD",
            "checkout_amount": target_asset_amount,
            "banxa_target_asset_amount": (
                target_asset_amount
            ),
            "banxa_checkout_fiat_currency": (
                get_banxa_fiat_currency()
            ),
        },
    )

    return {
        "checkout_url": checkout_url,
    }


__all__ = [
    "get_banxa_deposit_options",
    "open_banxa_deposit_session",
    "prepare_banxa_browser_launch",
]
