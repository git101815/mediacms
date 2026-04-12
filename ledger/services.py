from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Max, Q
from .models import (
    LEDGER_METADATA_VERSION,
    LEDGER_OUTBOX_MAX_RETRIES,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
    LedgerSaga,
    LedgerSagaStep,
    LEDGER_OUTBOX_RETRY_DELAY_SECONDS,
    LedgerHold,
    LedgerVelocityWindow,
    LEDGER_ACTION_DEPOSIT,
    LEDGER_ACTION_PURCHASE,
    LEDGER_ACTION_TRANSFER,
    LEDGER_ACTION_WITHDRAWAL,
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    WalletRequest,
    DepositSession,
    ObservedOnchainTransfer,
    SYSTEM_WALLET_EXTERNAL_ASSET_CLEARING,
    DepositAddress,
    DepositSweepJob,
    DepositRouteCounter,
)
import uuid
from django.utils import timezone
from datetime import timedelta
import os
from bip_utils import Bip44, Bip44Changes, Bip44Coins
from django.conf import settings
ACTIVE_DEPOSIT_SESSION_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_SEEN_ONCHAIN,
    DepositSession.STATUS_CONFIRMING,
}
ACTIVE_DEPOSIT_WATCH_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_SEEN_ONCHAIN,
    DepositSession.STATUS_CONFIRMING,
}

def _compute_next_retry_at() -> timezone.datetime:
    return timezone.now() + timedelta(seconds=LEDGER_OUTBOX_RETRY_DELAY_SECONDS)

def _require_authenticated_actor(actor):
    if actor is None:
        raise PermissionDenied("Actor is required")
    if not getattr(actor, "is_authenticated", False):
        raise PermissionDenied("Authenticated actor required")
    if not getattr(actor, "is_active", False):
        raise PermissionDenied("Active actor required")
    return actor


def _require_perm(actor, perm_codename: str):
    actor = _require_authenticated_actor(actor)
    if getattr(actor, "is_superuser", False):
        return actor
    if not actor.has_perm(perm_codename):
        raise PermissionDenied(f"Missing permission: {perm_codename}")
    return actor


def _resolve_created_by(*, actor, created_by):
    actor = _require_authenticated_actor(actor)

    if created_by is None:
        return actor

    if created_by.id == actor.id:
        return created_by

    if getattr(actor, "is_superuser", False):
        return created_by

    if actor.has_perm("ledger.can_impersonate_ledger_creator"):
        return created_by

    raise PermissionDenied("Cannot set created_by for another user")

def _create_outbox_event(*, txn: LedgerTransaction, topic: str, payload: dict, metadata_version: int) -> LedgerOutbox:
    return LedgerOutbox.objects.create(
        txn=txn,
        topic=topic,
        aggregate_type="ledger_transaction",
        aggregate_id=txn.id,
        payload=payload,
        metadata_version=metadata_version,
    )

def get_failed_outbox_events(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")
    return LedgerOutbox.objects.filter(
        status=LedgerOutbox.STATUS_FAILED
    ).order_by("created_at")[:limit]


def get_dead_lettered_outbox_events(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")
    return LedgerOutbox.objects.filter(
        status=LedgerOutbox.STATUS_DEAD_LETTERED
    ).order_by("dead_lettered_at", "created_at")[:limit]


def get_stale_pending_outbox_events(*, actor, older_than_seconds: int = 900, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_outbox")

    threshold = timezone.now() - timedelta(seconds=older_than_seconds)
    return LedgerOutbox.objects.filter(
        status=LedgerOutbox.STATUS_PENDING,
        created_at__lte=threshold,
    ).order_by("created_at")[:limit]

def get_failed_sagas(*, actor, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_sagas")
    return LedgerSaga.objects.filter(
        status=LedgerSaga.STATUS_FAILED
    ).order_by("failed_at", "created_at")[:limit]

def get_wallet_available_balance(wallet: TokenWallet) -> int:
    return int(wallet.balance) - int(wallet.held_balance)

def _require_wallet_not_blocked(wallet: TokenWallet):
    if wallet.risk_status == LEDGER_RISK_STATUS_BLOCKED:
        raise ValidationError("Wallet is blocked")
    if wallet.review_required or wallet.risk_status == LEDGER_RISK_STATUS_REVIEW:
        raise ValidationError("Wallet requires review")

def _require_wallet_owner(actor, wallet: TokenWallet):
    actor = _require_authenticated_actor(actor)
    if getattr(actor, "is_superuser", False):
        return actor
    if wallet.user_id != actor.id:
        raise PermissionDenied("Cannot manage another user's wallet")
    return actor


def _normalize_wallet_request_amount(amount) -> int:
    try:
        normalized = int(amount)
    except (TypeError, ValueError):
        raise ValidationError("Amount must be a whole number")
    if normalized <= 0:
        raise ValidationError("Amount must be greater than zero")
    return normalized


def _build_wallet_request_reference(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"

def _floor_window_start(now, window_seconds: int):
    ts = int(now.timestamp())
    floored = ts - (ts % window_seconds)
    return timezone.datetime.fromtimestamp(floored, tz=now.tzinfo)

def record_wallet_velocity(*, wallet: TokenWallet, action: str, amount: int):
    now = timezone.now()

    for window_seconds in [3600, 86400]:
        window_start = _floor_window_start(now, window_seconds)
        obj, _ = LedgerVelocityWindow.objects.select_for_update().get_or_create(
            wallet=wallet,
            action=action,
            window_seconds=window_seconds,
            window_start=window_start,
            defaults={"amount": 0, "count": 0},
        )
        obj.amount += int(amount)
        obj.count += 1
        obj.save(update_fields=["amount", "count", "updated_at"])


def get_wallet_velocity_amount(*, wallet: TokenWallet, action: str, window_seconds: int) -> int:
    now = timezone.now()
    window_start = _floor_window_start(now, window_seconds)
    obj = LedgerVelocityWindow.objects.filter(
        wallet=wallet,
        action=action,
        window_seconds=window_seconds,
        window_start=window_start,
    ).first()
    return int(obj.amount) if obj else 0


def enforce_wallet_velocity_limits(*, wallet: TokenWallet, action: str, amount: int):
    if amount <= 0:
        return

    hourly_current = get_wallet_velocity_amount(wallet=wallet, action=action, window_seconds=3600)
    daily_current = get_wallet_velocity_amount(wallet=wallet, action=action, window_seconds=86400)

    if action in [LEDGER_ACTION_WITHDRAWAL, LEDGER_ACTION_TRANSFER, LEDGER_ACTION_PURCHASE]:
        if wallet.hourly_outflow_limit is not None and hourly_current + amount > wallet.hourly_outflow_limit:
            raise ValidationError("Hourly outflow limit exceeded")
        if wallet.daily_outflow_limit is not None and daily_current + amount > wallet.daily_outflow_limit:
            raise ValidationError("Daily outflow limit exceeded")

    if action == LEDGER_ACTION_DEPOSIT:
        if wallet.hourly_inflow_limit is not None and hourly_current + amount > wallet.hourly_inflow_limit:
            raise ValidationError("Hourly inflow limit exceeded")
        if wallet.daily_inflow_limit is not None and daily_current + amount > wallet.daily_inflow_limit:
            raise ValidationError("Daily inflow limit exceeded")

def get_stale_compensating_sagas(*, actor, older_than_seconds: int = 900, limit: int = 100):
    _require_perm(actor, "ledger.can_manage_ledger_sagas")

    threshold = timezone.now() - timedelta(seconds=older_than_seconds)
    return LedgerSaga.objects.filter(
        status=LedgerSaga.STATUS_COMPENSATING,
        created_at__lte=threshold,
    ).order_by("created_at")[:limit]

def get_system_wallet(system_key: str, *, allow_negative: bool) -> TokenWallet:
    wallet, created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=system_key,
        defaults={
            "allow_negative": allow_negative,
        },
    )
    if not created and wallet.allow_negative != allow_negative:
        raise ValidationError(
            f"System wallet '{system_key}' has allow_negative={wallet.allow_negative}, "
            f"expected {allow_negative}"
        )
    return wallet

def _infer_action_from_kind(kind: str) -> str:
    lowered = kind.lower()
    if "withdraw" in lowered:
        return LEDGER_ACTION_WITHDRAWAL
    if "purchase" in lowered or "buy" in lowered:
        return LEDGER_ACTION_PURCHASE
    if "deposit" in lowered or "mint" in lowered:
        return LEDGER_ACTION_DEPOSIT
    return LEDGER_ACTION_TRANSFER

def _normalize_chain(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_evm_address(value: str) -> str:
    return (value or "").strip().lower()

def _parse_derivation_index_from_ref(address_derivation_ref: str):
    raw_value = (address_derivation_ref or "").strip()
    if not raw_value:
        return None

    tail = raw_value.rsplit("/", 1)[-1].rsplit(":", 1)[-1]
    try:
        parsed = int(tail)
    except (TypeError, ValueError):
        return None

    if parsed < 0:
        return None

    return parsed

def _get_claimed_sweep_job_for_update(*, actor, public_id, service_name: str) -> DepositSweepJob:
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")

    job = DepositSweepJob.objects.select_for_update().get(public_id=public_id)

    if job.claimed_by_service != service_name:
        raise ValidationError("Sweep job is not claimed by this service")

    if job.claim_expires_at is None or job.claim_expires_at <= timezone.now():
        raise ValidationError("Sweep job claim has expired")

    return job

def _clear_sweep_job_claim(job: DepositSweepJob) -> None:
    job.claimed_by_service = ""
    job.claim_expires_at = None

from decimal import Decimal

DEPOSIT_SESSION_STATUS_CANCELED = getattr(DepositSession, "STATUS_CANCELED", "canceled")

NETWORK_DISPLAY_LABELS = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum One",
    "base": "Base",
    "bsc": "BNB Chain",
}

DISPLAY_DECIMALS_BY_ROUTE = {
    ("ethereum", "USDT"): 6,
    ("ethereum", "USDC"): 6,
    ("arbitrum", "USDT"): 6,
    ("arbitrum", "USDC"): 6,
    ("base", "USDT"): 6,
    ("base", "USDC"): 6,
    ("bsc", "USDT"): 18,
    ("bsc", "USDC"): 18,
}

SUPPORTED_EVM_DEPOSIT_CHAINS = {
    "ethereum",
    "bsc",
    "arbitrum",
    "base",
}

def _get_network_display_label(chain: str) -> str:
    normalized_chain = _normalize_chain(chain)
    return NETWORK_DISPLAY_LABELS.get(normalized_chain, (chain or "").strip() or "Unknown")


def _get_deposit_display_decimals(*, chain: str, asset_code: str) -> int | None:
    normalized_chain = _normalize_chain(chain)
    normalized_asset = (asset_code or "").strip().upper()
    return DISPLAY_DECIMALS_BY_ROUTE.get((normalized_chain, normalized_asset))


def _format_deposit_display_amount(*, raw_amount: int, chain: str, asset_code: str) -> str:
    normalized_raw = int(raw_amount)
    decimals = _get_deposit_display_decimals(chain=chain, asset_code=asset_code)
    if decimals is None:
        return str(normalized_raw)

    scaled = Decimal(normalized_raw) / (Decimal(10) ** int(decimals))
    text = format(scaled, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _build_route_label(*, chain: str, asset_code: str, display_label: str = "") -> str:
    explicit = (display_label or "").strip()
    if explicit:
        return explicit
    return f"{_get_network_display_label(chain)} · {(asset_code or '').strip().upper()}"


def _build_deposit_route_key(*, chain: str, asset_code: str, token_contract_address: str) -> str:
    normalized_chain = _normalize_chain(chain)
    normalized_asset_code = (asset_code or "").strip().upper()
    normalized_token_contract_address = _normalize_evm_address(token_contract_address)
    return f"{normalized_chain}:{normalized_asset_code}:{normalized_token_contract_address}"


def build_deposit_option_key(*, chain: str, asset_code: str, token_contract_address: str) -> str:
    normalized_chain = _normalize_chain(chain)
    normalized_asset = (asset_code or "").strip().upper()
    normalized_contract = _normalize_evm_address(token_contract_address)
    contract_part = normalized_contract or "native"
    return f"{normalized_chain}:{normalized_asset}:{contract_part}"


def parse_deposit_option_key(option_key: str) -> tuple[str, str, str]:
    raw_value = (option_key or "").strip()
    parts = raw_value.split(":", 2)
    if len(parts) != 3:
        raise ValidationError("Invalid deposit option")
    chain, asset_code, contract_part = parts
    token_contract_address = "" if contract_part == "native" else contract_part
    return _normalize_chain(chain), asset_code.strip().upper(), _normalize_evm_address(token_contract_address)


def _get_deposit_evm_account_xpub() -> str:
    env_value = (os.environ.get("DEPOSIT_EVM_ACCOUNT_XPUB") or "").strip()
    if env_value:
        return env_value

    settings_value = (getattr(settings, "DEPOSIT_EVM_ACCOUNT_XPUB", "") or "").strip()
    if settings_value:
        return settings_value

    raise ValidationError("DEPOSIT_EVM_ACCOUNT_XPUB is not configured")


def _derive_session_deposit_address(*, chain: str, derivation_index: int) -> tuple[str, str]:
    normalized_chain = _normalize_chain(chain)
    if normalized_chain not in SUPPORTED_EVM_DEPOSIT_CHAINS:
        raise ValidationError(f"Unsupported deposit derivation chain: {chain}")

    account_xpub = _get_deposit_evm_account_xpub()
    ctx = Bip44.FromExtendedKey(account_xpub, Bip44Coins.ETHEREUM)
    addr_ctx = ctx.Change(Bip44Changes.CHAIN_EXT).AddressIndex(int(derivation_index))

    address = addr_ctx.PublicKey().ToAddress().lower()
    derivation_path = f"m/44'/60'/0'/0/{int(derivation_index)}"
    return address, derivation_path


def _get_route_template(*, chain: str, asset_code: str, token_contract_address: str) -> DepositAddress:
    normalized_chain = _normalize_chain(chain)
    normalized_asset_code = (asset_code or "").strip().upper()
    normalized_token_contract_address = _normalize_evm_address(token_contract_address)

    template = (
        DepositAddress.objects.filter(
            chain=normalized_chain,
            asset_code=normalized_asset_code,
            token_contract_address=normalized_token_contract_address,
        )
        .order_by("id")
        .first()
    )
    if template is None:
        raise ValidationError(
            f"No deposit route template found for {normalized_chain}/{normalized_asset_code}"
        )
    return template


def _find_reusable_active_session(
    *,
    wallet: TokenWallet,
    chain: str,
    asset_code: str,
    token_contract_address: str,
) -> DepositSession | None:
    return (
        DepositSession.objects.select_for_update()
        .filter(
            wallet=wallet,
            chain=_normalize_chain(chain),
            asset_code=(asset_code or "").strip().upper(),
            token_contract_address=_normalize_evm_address(token_contract_address),
            status__in=ACTIVE_DEPOSIT_SESSION_STATUSES,
            expires_at__gt=timezone.now(),
        )
        .order_by("-created_at")
        .first()
    )


def _get_or_create_route_counter_for_update(
    *,
    chain: str,
    asset_code: str,
    token_contract_address: str,
) -> DepositRouteCounter:
    normalized_chain = _normalize_chain(chain)
    normalized_asset_code = (asset_code or "").strip().upper()
    normalized_token_contract_address = _normalize_evm_address(token_contract_address)
    route_key = _build_deposit_route_key(
        chain=normalized_chain,
        asset_code=normalized_asset_code,
        token_contract_address=normalized_token_contract_address,
    )

    existing = (
        DepositRouteCounter.objects.select_for_update()
        .filter(route_key=route_key)
        .first()
    )
    if existing is not None:
        return existing

    max_session_index = (
        DepositSession.objects.filter(route_key=route_key)
        .aggregate(value=Max("derivation_index"))
        .get("value")
    )
    max_legacy_index = (
        DepositAddress.objects.filter(
            chain=normalized_chain,
            asset_code=normalized_asset_code,
            token_contract_address=normalized_token_contract_address,
        )
        .aggregate(value=Max("derivation_index"))
        .get("value")
    )

    max_index = max(
        -1 if max_session_index is None else int(max_session_index),
        -1 if max_legacy_index is None else int(max_legacy_index),
    )

    try:
        created = DepositRouteCounter.objects.create(
            route_key=route_key,
            chain=normalized_chain,
            asset_code=normalized_asset_code,
            token_contract_address=normalized_token_contract_address,
            next_derivation_index=max_index + 1,
            metadata={
                "seeded_from": "legacy_deposit_addresses",
            },
            metadata_version=LEDGER_METADATA_VERSION,
        )
        return DepositRouteCounter.objects.select_for_update().get(id=created.id)
    except IntegrityError:
        return DepositRouteCounter.objects.select_for_update().get(route_key=route_key)


def _allocate_session_address(
    *,
    chain: str,
    asset_code: str,
    token_contract_address: str,
) -> tuple[str, int, str]:
    counter = _get_or_create_route_counter_for_update(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
    )
    derivation_index = int(counter.next_derivation_index)
    address, derivation_path = _derive_session_deposit_address(
        chain=chain,
        derivation_index=derivation_index,
    )

    counter.next_derivation_index = derivation_index + 1
    counter.save(update_fields=["next_derivation_index", "updated_at"])

    return address, derivation_index, derivation_path

def _abandon_open_sweep_jobs_for_session(*, deposit_session: DepositSession) -> int:
    return DepositSweepJob.objects.filter(
        deposit_session=deposit_session,
        status__in=[
            DepositSweepJob.STATUS_PENDING,
            DepositSweepJob.STATUS_READY_TO_SWEEP,
            DepositSweepJob.STATUS_FUNDING_BROADCASTED,
            DepositSweepJob.STATUS_SWEEP_BROADCASTED,
        ],
    ).update(
        status=DepositSweepJob.STATUS_ABANDONED,
        claimed_by_service="",
        claim_expires_at=None,
        updated_at=timezone.now(),
    )

def build_evm_event_key(*, chain: str, txid: str, log_index: int) -> str:
    normalized_chain = _normalize_chain(chain)
    normalized_txid = (txid or "").strip().lower()
    return f"{normalized_chain}:{normalized_txid}:{int(log_index)}"

def get_external_asset_clearing_wallet() -> TokenWallet:
    return get_system_wallet(
        TokenWallet.SYSTEM_EXTERNAL_ASSET_CLEARING,
        allow_negative=True,
    )


@transaction.atomic
def create_wallet_deposit_request(
    *,
    actor,
    wallet: TokenWallet,
    amount: int,
    notes: str = "",
    metadata=None,
) -> WalletRequest:
    actor = _require_wallet_owner(actor, wallet)

    if metadata is None:
        metadata = {}

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    _require_wallet_not_blocked(wallet)

    normalized_amount = _normalize_wallet_request_amount(amount)
    enforce_wallet_velocity_limits(
        wallet=wallet,
        action=LEDGER_ACTION_DEPOSIT,
        amount=normalized_amount,
    )

    reference = _build_wallet_request_reference("dep")

    wallet_request = WalletRequest.objects.create(
        wallet=wallet,
        request_type=WalletRequest.REQUEST_TYPE_DEPOSIT,
        status=WalletRequest.STATUS_PENDING,
        amount=normalized_amount,
        asset_code="TOKENS",
        reference=reference,
        notes=notes,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
        created_by=actor,
    )
    return wallet_request


@transaction.atomic
def create_wallet_withdrawal_request(
    *,
    actor,
    wallet: TokenWallet,
    amount: int,
    destination_address: str,
    notes: str = "",
    metadata=None,
) -> WalletRequest:
    actor = _require_wallet_owner(actor, wallet)

    if metadata is None:
        metadata = {}

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    _require_wallet_not_blocked(wallet)

    normalized_amount = _normalize_wallet_request_amount(amount)
    destination_address = (destination_address or "").strip()
    if not destination_address:
        raise ValidationError("Destination address is required")

    enforce_wallet_velocity_limits(
        wallet=wallet,
        action=LEDGER_ACTION_WITHDRAWAL,
        amount=normalized_amount,
    )

    available_balance = get_wallet_available_balance(wallet)
    if not wallet.allow_negative and normalized_amount > available_balance:
        raise ValidationError("Insufficient available balance")

    reference = _build_wallet_request_reference("wdr")

    wallet.held_balance += normalized_amount
    wallet.save(update_fields=["held_balance", "updated_at"])

    hold = LedgerHold.objects.create(
        wallet=wallet,
        amount=normalized_amount,
        reason=f"Reserved for withdrawal request {reference}",
        created_by=actor,
        metadata={
            **metadata,
            "reference": reference,
            "destination_address": destination_address,
            "source": "wallet_ui",
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    wallet_request = WalletRequest.objects.create(
        wallet=wallet,
        request_type=WalletRequest.REQUEST_TYPE_WITHDRAWAL,
        status=WalletRequest.STATUS_PENDING,
        amount=normalized_amount,
        asset_code="TOKENS",
        destination_address=destination_address,
        reference=reference,
        notes=notes,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
        hold=hold,
        created_by=actor,
    )
    return wallet_request

@transaction.atomic
def provision_deposit_address(
    *,
    actor,
    chain: str,
    asset_code: str,
    token_contract_address: str,
    display_label: str,
    address: str,
    address_derivation_ref: str,
    required_confirmations,
    min_amount,
    session_ttl_seconds,
    metadata=None,
    derivation_index=None,
):
    _require_perm(actor, "ledger.can_manage_deposit_addresses")

    if metadata is None:
        metadata = {}

    chain = _normalize_chain(chain)
    asset_code = (asset_code or "").strip().upper()
    token_contract_address = _normalize_evm_address(token_contract_address)
    address = _normalize_evm_address(address)
    address_derivation_ref = (address_derivation_ref or "").strip()
    display_label = (display_label or "").strip()

    try:
        required_confirmations = int(required_confirmations)
        min_amount = int(min_amount)
        session_ttl_seconds = int(session_ttl_seconds)
        derivation_index = int(derivation_index)
    except (TypeError, ValueError):
        raise ValidationError("Invalid deposit address configuration")

    if not chain:
        raise ValidationError("Chain is required")
    if not asset_code:
        raise ValidationError("Asset code is required")
    if not address:
        raise ValidationError("Address is required")
    if not address_derivation_ref:
        raise ValidationError("Address derivation ref is required")
    if derivation_index < 0:
        raise ValidationError("Derivation index cannot be negative")
    if required_confirmations < 1:
        raise ValidationError("Required confirmations must be at least 1")
    if min_amount <= 0:
        raise ValidationError("Minimum amount must be positive")
    if session_ttl_seconds <= 0:
        raise ValidationError("Session TTL must be positive")

    existing_by_address = (
        DepositAddress.objects.select_for_update()
        .filter(address=address)
        .first()
    )
    existing_by_ref = (
        DepositAddress.objects.select_for_update()
        .filter(address_derivation_ref=address_derivation_ref)
        .first()
    )
    existing_by_index = (
        DepositAddress.objects.select_for_update()
        .filter(
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract_address,
            derivation_index=derivation_index,
        )
        .first()
    )

    candidate_ids = {
        obj.id
        for obj in (existing_by_address, existing_by_ref, existing_by_index)
        if obj is not None
    }

    if len(candidate_ids) > 1:
        raise ValidationError(
            "Address, derivation reference, and derivation index already belong to different rows"
        )

    existing = None
    if existing_by_address is not None:
        existing = existing_by_address
    elif existing_by_ref is not None:
        existing = existing_by_ref
    else:
        existing = existing_by_index

    if existing is not None:
        immutable_mismatch = (
            existing.chain != chain
            or existing.asset_code != asset_code
            or existing.token_contract_address != token_contract_address
            or existing.display_label != display_label
            or existing.address != address
            or existing.address_derivation_ref != address_derivation_ref
            or existing.derivation_index != derivation_index
            or int(existing.required_confirmations) != required_confirmations
            or int(existing.min_amount) != min_amount
            or int(existing.session_ttl_seconds) != session_ttl_seconds
        )
        if immutable_mismatch:
            raise ValidationError("Deposit address already exists with different immutable fields")

        return existing, False

    created = DepositAddress.objects.create(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
        display_label=display_label,
        address=address,
        address_derivation_ref=address_derivation_ref,
        derivation_index=derivation_index,
        required_confirmations=required_confirmations,
        min_amount=min_amount,
        session_ttl_seconds=session_ttl_seconds,
        status=DepositAddress.STATUS_AVAILABLE,
        metadata=metadata,
        metadata_version=LEDGER_METADATA_VERSION,
    )
    return created, True


@transaction.atomic
def provision_deposit_addresses_batch(*, actor, address_rows):
    _require_perm(actor, "ledger.can_manage_deposit_addresses")

    if not isinstance(address_rows, list) or not address_rows:
        raise ValidationError("Addresses payload must be a non-empty list")

    created_count = 0
    existing_count = 0
    rows = []

    for row in address_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each address row must be an object")

        address_obj, created = provision_deposit_address(
            actor=actor,
            chain=row.get("chain", ""),
            asset_code=row.get("asset_code", ""),
            token_contract_address=row.get("token_contract_address", ""),
            display_label=row.get("display_label", ""),
            address=row.get("address", ""),
            address_derivation_ref=row.get("address_derivation_ref", ""),
            required_confirmations=row.get("required_confirmations"),
            min_amount=row.get("min_amount"),
            session_ttl_seconds=row.get("session_ttl_seconds"),
            metadata=row.get("metadata") or {},
            derivation_index=row.get("derivation_index"),
        )

        if created:
            created_count += 1
        else:
            existing_count += 1

        rows.append(
            {
                "id": address_obj.id,
                "chain": address_obj.chain,
                "asset_code": address_obj.asset_code,
                "token_contract_address": address_obj.token_contract_address,
                "address": address_obj.address,
                "address_derivation_ref": address_obj.address_derivation_ref,
                "status": address_obj.status,
                "created": created,
                "derivation_index": address_obj.derivation_index,
            }
        )

    return {
        "created_count": created_count,
        "existing_count": existing_count,
        "rows": rows,
    }

def list_available_deposit_options() -> list[dict]:
    rows = (
        DepositAddress.objects.values(
            "display_label",
            "chain",
            "asset_code",
            "token_contract_address",
            "required_confirmations",
            "min_amount",
            "session_ttl_seconds",
        )
        .order_by("display_label", "chain", "asset_code", "token_contract_address")
        .distinct()
    )

    options = []
    for row in rows:
        option_key = build_deposit_option_key(
            chain=row["chain"],
            asset_code=row["asset_code"],
            token_contract_address=row["token_contract_address"],
        )
        route_label = _build_route_label(
            chain=row["chain"],
            asset_code=row["asset_code"],
            display_label=row["display_label"],
        )
        network_label = _get_network_display_label(row["chain"])
        min_amount_display = _format_deposit_display_amount(
            raw_amount=row["min_amount"],
            chain=row["chain"],
            asset_code=row["asset_code"],
        )

        options.append(
            {
                "key": option_key,
                "label": row["display_label"],
                "route_label": route_label,
                "network_label": network_label,
                "chain": row["chain"],
                "asset_code": row["asset_code"],
                "token_contract_address": row["token_contract_address"],
                "required_confirmations": row["required_confirmations"],
                "min_amount": row["min_amount"],
                "min_amount_display": min_amount_display,
                "session_ttl_seconds": row["session_ttl_seconds"],
                "network_slug": _normalize_chain(row["chain"]),
            }
        )
    return options

@transaction.atomic
def open_user_deposit_session(*, actor, wallet: TokenWallet, option_key: str) -> DepositSession:
    actor = _require_authenticated_actor(actor)

    wallet = TokenWallet.objects.select_for_update().get(id=wallet.id)
    if wallet.wallet_type != TokenWallet.TYPE_USER:
        raise ValidationError("Deposit sessions can only target user wallets")
    if wallet.user_id != actor.id:
        raise PermissionDenied("Cannot open a deposit session for another user's wallet")

    chain, asset_code, token_contract_address = parse_deposit_option_key(option_key)

    existing_session = _find_reusable_active_session(
        wallet=wallet,
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
    )
    if existing_session is not None:
        return existing_session

    template = _get_route_template(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
    )

    deposit_address, derivation_index, derivation_path = _allocate_session_address(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
    )

    expires_at = timezone.now() + timedelta(seconds=int(template.session_ttl_seconds))
    route_key = _build_deposit_route_key(
        chain=chain,
        asset_code=asset_code,
        token_contract_address=token_contract_address,
    )
    display_label = _build_route_label(
        chain=template.chain,
        asset_code=template.asset_code,
        display_label=template.display_label,
    )

    return DepositSession.objects.create(
        user=wallet.user,
        wallet=wallet,
        chain=template.chain,
        asset_code=template.asset_code,
        token_contract_address=template.token_contract_address,
        route_key=route_key,
        display_label=display_label,
        deposit_address=deposit_address,
        address_derivation_ref=derivation_path,
        derivation_index=derivation_index,
        derivation_path=derivation_path,
        status=DepositSession.STATUS_AWAITING_PAYMENT,
        min_amount=int(template.min_amount),
        required_confirmations=int(template.required_confirmations),
        expires_at=expires_at,
        created_by=actor,
        metadata={
            "display_label": template.display_label,
            "allocation_source": "session_derivation",
            "route_template_id": template.id,
            "chain_family": "evm",
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

@transaction.atomic
def ingest_deposit_observation_event(
    *,
    actor,
    session_public_id,
    chain: str,
    txid: str,
    log_index: int,
    block_number: int | None,
    from_address: str,
    to_address: str,
    token_contract_address: str,
    asset_code: str,
    amount: int,
    confirmations: int,
    raw_payload=None,
):
    if raw_payload is None:
        raw_payload = {}

    deposit_session = (
        DepositSession.objects.select_for_update()
        .select_related("wallet")
        .get(public_id=session_public_id)
    )

    observed_transfer = record_onchain_observation(
        actor=actor,
        deposit_session=deposit_session,
        chain=chain,
        txid=txid,
        log_index=log_index,
        block_number=block_number,
        from_address=from_address,
        to_address=to_address,
        token_contract_address=token_contract_address,
        asset_code=asset_code,
        amount=amount,
        confirmations=confirmations,
        raw_payload=raw_payload,
    )

    deposit_session.refresh_from_db()
    observed_transfer.refresh_from_db()

    ledger_txn = None
    if observed_transfer.confirmations >= deposit_session.required_confirmations:
        ledger_txn = credit_confirmed_deposit_session(
            actor=actor,
            deposit_session=deposit_session,
            observed_transfer=observed_transfer,
        )
        deposit_session.refresh_from_db()
        observed_transfer.refresh_from_db()

    return {
        "deposit_session": deposit_session,
        "observed_transfer": observed_transfer,
        "ledger_txn": ledger_txn,
    }


def get_deposit_stats(*, actor, option_rows):
    _require_perm(actor, "ledger.can_manage_deposit_addresses")

    if not isinstance(option_rows, list) or not option_rows:
        raise ValidationError("Options payload must be a non-empty list")

    results = []
    for row in option_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each option row must be an object")

        chain = _normalize_chain(row.get("chain", ""))
        asset_code = (row.get("asset_code", "") or "").strip().upper()
        token_contract_address = _normalize_evm_address(row.get("token_contract_address", ""))

        if not chain or not asset_code:
            raise ValidationError("Each option requires chain and asset_code")

        route_key = _build_deposit_route_key(
            chain=chain,
            asset_code=asset_code,
            token_contract_address=token_contract_address,
        )

        counter = DepositRouteCounter.objects.filter(route_key=route_key).first()
        active_sessions = DepositSession.objects.filter(
            route_key=route_key,
            status__in=ACTIVE_DEPOSIT_SESSION_STATUSES,
        ).count()
        total_sessions = DepositSession.objects.filter(route_key=route_key).count()

        results.append(
            {
                "chain": chain,
                "asset_code": asset_code,
                "token_contract_address": token_contract_address,
                "route_key": route_key,
                "next_derivation_index": int(counter.next_derivation_index) if counter is not None else 0,
                "active_session_count": active_sessions,
                "total_session_count": total_sessions,
            }
        )

    return results

@transaction.atomic
def list_active_deposit_watch_targets(*, actor, option_rows):
    _require_perm(actor, "ledger.can_view_deposit_sessions")

    if not isinstance(option_rows, list) or not option_rows:
        raise ValidationError("Options payload must be a non-empty list")

    results = []
    now = timezone.now()

    for row in option_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each option row must be an object")

        chain = _normalize_chain(row.get("chain", ""))
        asset_code = (row.get("asset_code", "") or "").strip().upper()
        token_contract_address = _normalize_evm_address(row.get("token_contract_address", ""))

        if not chain or not asset_code:
            raise ValidationError("Each option requires chain and asset_code")

        sessions = (
            DepositSession.objects.filter(
                chain=chain,
                asset_code=asset_code,
                token_contract_address=token_contract_address,
                status__in=ACTIVE_DEPOSIT_WATCH_STATUSES,
                expires_at__gt=now,
            )
            .order_by("id")
            .values(
                "public_id",
                "deposit_address",
                "required_confirmations",
                "min_amount",
                "status",
                "expires_at",
            )
        )

        targets = []
        for session in sessions:
            targets.append(
                {
                    "session_public_id": str(session["public_id"]),
                    "deposit_address": session["deposit_address"],
                    "required_confirmations": session["required_confirmations"],
                    "min_amount": session["min_amount"],
                    "status": session["status"],
                    "expires_at": session["expires_at"].isoformat(),
                }
            )

        results.append(
            {
                "chain": chain,
                "asset_code": asset_code,
                "token_contract_address": token_contract_address,
                "targets": targets,
            }
        )

    return results

@transaction.atomic
def enqueue_deposit_sweep_job(*, actor, deposit_session: DepositSession, observed_transfer: ObservedOnchainTransfer):
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")

    deposit_session = DepositSession.objects.select_for_update().get(id=deposit_session.id)
    observed_transfer = ObservedOnchainTransfer.objects.select_for_update().get(id=observed_transfer.id)

    if deposit_session.status != DepositSession.STATUS_CREDITED:
        raise ValidationError("Cannot enqueue sweep for a non-credited deposit session")

    if observed_transfer.status != ObservedOnchainTransfer.STATUS_CREDITED:
        raise ValidationError("Cannot enqueue sweep for a non-credited observed transfer")

    derivation_index = deposit_session.derivation_index
    if derivation_index is None:
        derivation_index = _parse_derivation_index_from_ref(deposit_session.address_derivation_ref)

    job, created = DepositSweepJob.objects.get_or_create(
        observed_transfer=observed_transfer,
        defaults={
            "deposit_session": deposit_session,
            "chain": deposit_session.chain,
            "asset_code": deposit_session.asset_code,
            "token_contract_address": deposit_session.token_contract_address,
            "source_address": deposit_session.deposit_address,
            "address_derivation_ref": deposit_session.address_derivation_ref,
            "derivation_index": derivation_index,
            "amount": observed_transfer.amount,
            "status": DepositSweepJob.STATUS_PENDING,
            "metadata": {
                "source": "credited_deposit",
                "deposit_session_public_id": str(deposit_session.public_id),
                "observed_transfer_event_key": observed_transfer.event_key,
            },
            "metadata_version": LEDGER_METADATA_VERSION,
        },
    )

    if not created:
        immutable_mismatch = (
            job.deposit_session_id != deposit_session.id
            or job.chain != deposit_session.chain
            or job.asset_code != deposit_session.asset_code
            or job.token_contract_address != deposit_session.token_contract_address
            or job.source_address != deposit_session.deposit_address
            or job.address_derivation_ref != deposit_session.address_derivation_ref
            or int(job.amount) != int(observed_transfer.amount)
        )
        if immutable_mismatch:
            raise ValidationError("Sweep job already exists with different immutable fields")

    return job

@transaction.atomic
def claim_deposit_sweep_jobs(*, actor, service_name: str, option_rows, limit: int, lease_seconds: int):
    if not isinstance(option_rows, list) or not option_rows:
        raise ValidationError("Options payload must be a non-empty list")
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")

    limit = int(limit)
    lease_seconds = int(lease_seconds)

    if limit <= 0:
        raise ValidationError("Claim limit must be positive")
    if lease_seconds <= 0:
        raise ValidationError("Claim lease must be positive")

    now = timezone.now()
    claim_until = now + timezone.timedelta(seconds=lease_seconds)

    claimed = []

    for row in option_rows:
        if not isinstance(row, dict):
            raise ValidationError("Each option row must be an object")
        chain = _normalize_chain(row.get("chain", ""))
        asset_code = (row.get("asset_code", "") or "").strip().upper()
        token_contract_address = _normalize_evm_address(row.get("token_contract_address", ""))
        if not chain or not asset_code:
            raise ValidationError("Each option requires chain and asset_code")

        qs = (
            DepositSweepJob.objects.select_for_update(skip_locked=True)
            .filter(
                chain=chain,
                asset_code=asset_code,
                token_contract_address=token_contract_address,
                status__in=[
                    DepositSweepJob.STATUS_PENDING,
                    DepositSweepJob.STATUS_READY_TO_SWEEP,
                    DepositSweepJob.STATUS_FUNDING_BROADCASTED,
                    DepositSweepJob.STATUS_SWEEP_BROADCASTED,
                ],
            )
            .filter(Q(claim_expires_at__isnull=True) | Q(claim_expires_at__lt=now))
            .order_by("id")
        )

        for job in qs[: limit - len(claimed)]:
            job.claimed_by_service = service_name
            job.claim_expires_at = claim_until
            job.attempt_count += 1
            job.save(update_fields=["claimed_by_service", "claim_expires_at", "attempt_count", "updated_at"])

            claimed.append(
                {
                    "public_id": str(job.public_id),
                    "status": job.status,
                    "chain": job.chain,
                    "asset_code": job.asset_code,
                    "token_contract_address": job.token_contract_address,
                    "source_address": job.source_address,
                    "address_derivation_ref": job.address_derivation_ref,
                    "derivation_index": job.derivation_index,
                    "amount": job.amount,
                    "gas_funding_txid": job.gas_funding_txid,
                    "sweep_txid": job.sweep_txid,
                }
            )

            if len(claimed) >= limit:
                break

        if len(claimed) >= limit:
            break

    return claimed

@transaction.atomic
def mark_sweep_job_funding_broadcasted(
    *,
    actor,
    public_id,
    service_name: str,
    gas_funding_txid: str,
    destination_address: str,
) -> DepositSweepJob:
    gas_funding_txid = (gas_funding_txid or "").strip().lower()
    destination_address = _normalize_evm_address(destination_address)

    if not gas_funding_txid:
        raise ValidationError("Gas funding txid is required")
    if not destination_address:
        raise ValidationError("Destination address is required")

    job = _get_claimed_sweep_job_for_update(
        actor=actor,
        public_id=public_id,
        service_name=service_name,
    )

    if job.status == DepositSweepJob.STATUS_FUNDING_BROADCASTED:
        if job.gas_funding_txid == gas_funding_txid and job.destination_address == destination_address:
            return job
        raise ValidationError("Sweep job already has a different gas funding txid")

    if job.status != DepositSweepJob.STATUS_PENDING:
        raise ValidationError("Sweep job cannot transition to funding_broadcasted from its current status")

    job.status = DepositSweepJob.STATUS_FUNDING_BROADCASTED
    job.gas_funding_txid = gas_funding_txid
    job.destination_address = destination_address
    job.last_error = ""
    job.save(
        update_fields=[
            "status",
            "gas_funding_txid",
            "destination_address",
            "last_error",
            "updated_at",
        ]
    )
    return job


@transaction.atomic
def mark_sweep_job_ready_to_sweep(
    *,
    actor,
    public_id,
    service_name: str,
) -> DepositSweepJob:
    job = _get_claimed_sweep_job_for_update(
        actor=actor,
        public_id=public_id,
        service_name=service_name,
    )

    if job.status == DepositSweepJob.STATUS_READY_TO_SWEEP:
        return job

    if job.status != DepositSweepJob.STATUS_FUNDING_BROADCASTED:
        raise ValidationError("Sweep job cannot transition to ready_to_sweep from its current status")

    if not job.gas_funding_txid:
        raise ValidationError("Sweep job is missing gas funding txid")

    job.status = DepositSweepJob.STATUS_READY_TO_SWEEP
    job.last_error = ""
    job.save(update_fields=["status", "last_error", "updated_at"])
    return job


@transaction.atomic
def mark_sweep_job_sweep_broadcasted(
    *,
    actor,
    public_id,
    service_name: str,
    sweep_txid: str,
    destination_address: str,
) -> DepositSweepJob:
    sweep_txid = (sweep_txid or "").strip().lower()
    destination_address = _normalize_evm_address(destination_address)

    if not sweep_txid:
        raise ValidationError("Sweep txid is required")
    if not destination_address:
        raise ValidationError("Destination address is required")

    job = _get_claimed_sweep_job_for_update(
        actor=actor,
        public_id=public_id,
        service_name=service_name,
    )

    if job.status == DepositSweepJob.STATUS_SWEEP_BROADCASTED:
        if job.sweep_txid == sweep_txid and job.destination_address == destination_address:
            return job
        raise ValidationError("Sweep job already has a different sweep txid")

    if job.status != DepositSweepJob.STATUS_READY_TO_SWEEP:
        raise ValidationError("Sweep job cannot transition to sweep_broadcasted from its current status")

    job.status = DepositSweepJob.STATUS_SWEEP_BROADCASTED
    job.sweep_txid = sweep_txid
    job.destination_address = destination_address
    job.last_error = ""
    job.save(
        update_fields=[
            "status",
            "sweep_txid",
            "destination_address",
            "last_error",
            "updated_at",
        ]
    )
    return job


@transaction.atomic
def mark_sweep_job_confirmed(
    *,
    actor,
    public_id,
    service_name: str,
) -> DepositSweepJob:
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")

    job = DepositSweepJob.objects.select_for_update().get(public_id=public_id)

    if job.status == DepositSweepJob.STATUS_CONFIRMED:
        return job

    if job.claimed_by_service != service_name:
        raise ValidationError("Sweep job is not claimed by this service")

    if job.claim_expires_at is None or job.claim_expires_at <= timezone.now():
        raise ValidationError("Sweep job claim has expired")

    if job.status != DepositSweepJob.STATUS_SWEEP_BROADCASTED:
        raise ValidationError("Sweep job cannot transition to confirmed from its current status")

    if not job.sweep_txid:
        raise ValidationError("Sweep job is missing sweep txid")

    job.status = DepositSweepJob.STATUS_CONFIRMED
    job.confirmed_at = timezone.now()
    job.last_error = ""
    _clear_sweep_job_claim(job)
    job.save(
        update_fields=[
            "status",
            "confirmed_at",
            "last_error",
            "claimed_by_service",
            "claim_expires_at",
            "updated_at",
        ]
    )

    session = DepositSession.objects.select_for_update().get(id=job.deposit_session_id)
    if session.status != DepositSession.STATUS_SWEPT:
        session.status = DepositSession.STATUS_SWEPT
        session.swept_at = timezone.now()
        session.save(update_fields=["status", "swept_at", "updated_at"])

    return job


@transaction.atomic
def mark_sweep_job_failed(
    *,
    actor,
    public_id,
    service_name: str,
    error: str,
) -> DepositSweepJob:
    _require_perm(actor, "ledger.can_manage_deposit_sweep_jobs")
    error = (error or "").strip()
    if not error:
        raise ValidationError("Failure reason is required")

    job = DepositSweepJob.objects.select_for_update().get(public_id=public_id)

    if job.status == DepositSweepJob.STATUS_FAILED:
        if job.last_error == error:
            return job
        raise ValidationError("Sweep job is already failed with a different error")

    if job.claimed_by_service != service_name:
        raise ValidationError("Sweep job is not claimed by this service")

    if job.claim_expires_at is None or job.claim_expires_at <= timezone.now():
        raise ValidationError("Sweep job claim has expired")

    if job.status in {
        DepositSweepJob.STATUS_CONFIRMED,
        DepositSweepJob.STATUS_ABANDONED,
    }:
        raise ValidationError("Terminal sweep job cannot transition to failed")

    job.status = DepositSweepJob.STATUS_FAILED
    job.last_error = error
    _clear_sweep_job_claim(job)
    job.save(
        update_fields=[
            "status",
            "last_error",
            "claimed_by_service",
            "claim_expires_at",
            "updated_at",
        ]
    )
    return job

@transaction.atomic
def cancel_user_deposit_session(*, actor, deposit_session: DepositSession) -> DepositSession:
    actor = _require_authenticated_actor(actor)

    deposit_session = (
        DepositSession.objects.select_for_update()
        .select_related("wallet")
        .get(id=deposit_session.id)
    )

    if deposit_session.wallet.user_id != actor.id and not actor.has_perm("ledger.can_manage_deposit_sessions"):
        raise PermissionDenied("Cannot cancel another user's deposit session")

    terminal_statuses = {
        DepositSession.STATUS_EXPIRED,
        DepositSession.STATUS_FAILED,
        DEPOSIT_SESSION_STATUS_CANCELED,
        getattr(DepositSession, "STATUS_SWEPT", "swept"),
    }
    if deposit_session.status in terminal_statuses:
        return deposit_session

    if deposit_session.status == DepositSession.STATUS_CREDITED:
        raise ValidationError("Cannot cancel a credited deposit session")

    if deposit_session.observed_txid:
        raise ValidationError("Cannot cancel a deposit session that already has an observed transaction")

    deposit_session.status = DEPOSIT_SESSION_STATUS_CANCELED
    deposit_session.save(update_fields=["status", "updated_at"])

    _abandon_open_sweep_jobs_for_session(deposit_session=deposit_session)
    return deposit_session


@transaction.atomic
def expire_stale_deposit_sessions(*, actor, limit: int = 500) -> int:
    _require_perm(actor, "ledger.can_manage_deposit_sessions")

    now = timezone.now()
    expired_count = 0

    sessions = (
        DepositSession.objects.select_for_update(skip_locked=True)
        .filter(
            status__in=ACTIVE_DEPOSIT_SESSION_STATUSES,
            expires_at__lte=now,
        )
        .order_by("id")[: int(limit)]
    )

    for session in sessions:
        session.status = DepositSession.STATUS_EXPIRED
        session.save(update_fields=["status", "updated_at"])

        _abandon_open_sweep_jobs_for_session(deposit_session=session)
        expired_count += 1

    return expired_count

@transaction.atomic
def delete_user_deposit_session(*, actor, deposit_session: DepositSession) -> None:
    actor = _require_authenticated_actor(actor)

    deposit_session = (
        DepositSession.objects.select_for_update()
        .select_related("wallet")
        .get(id=deposit_session.id)
    )

    if deposit_session.wallet.user_id != actor.id and not actor.has_perm("ledger.can_manage_deposit_sessions"):
        raise PermissionDenied("Cannot delete another user's deposit session")

    allowed_statuses = {
        DepositSession.STATUS_AWAITING_PAYMENT,
        DEPOSIT_SESSION_STATUS_CANCELED,
    }
    if deposit_session.status not in allowed_statuses:
        raise ValidationError("Only pending or canceled deposit sessions can be deleted")

    if deposit_session.observed_txid:
        raise ValidationError("Cannot delete a deposit session that already observed a transaction")

    deposit_session.delete()