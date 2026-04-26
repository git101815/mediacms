from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Exists, OuterRef
from django.utils import timezone

from ledger.models import (
    DepositSession,
    DepositSweepJob,
    OrphanDepositRecoveryAudit,
)
from ledger.services import (
    _get_route_onchain_decimals,
    _normalize_chain,
    _normalize_evm_address,
    _parse_derivation_index_from_ref,
)
from sweeper_service.app.claim_once import (
    _compute_effective_gas_price_wei,
    _compute_native_transfer_fee_wei,
    _estimate_erc20_transfer_gas,
)
from sweeper_service.app.config import (
    SweepOptionConfig,
    _normalize_private_key,
    _read_secret_file,
    _resolve_env_placeholder,
    _resolve_rpc_urls,
)
from sweeper_service.app.derivation import EvmDeriver
from sweeper_service.app.evm import (
    NonceAllocator,
    address_from_private_key,
    build_web3,
    get_erc20_balance,
    get_native_balance,
    send_erc20_transfer,
    send_native_transfer,
    wait_for_confirmations,
)
from sweeper_service.app.reference_head import get_reference_head
from sweeper_service.app.rpc_pool import choose_best_rpc_url


ACTIVE_DEPOSIT_SESSION_STATUSES = {
    DepositSession.STATUS_AWAITING_PAYMENT,
    DepositSession.STATUS_SEEN_ONCHAIN,
    DepositSession.STATUS_CONFIRMING,
}

ACTIVE_SWEEP_JOB_STATUSES = {
    DepositSweepJob.STATUS_PENDING,
    DepositSweepJob.STATUS_READY_TO_SWEEP,
    DepositSweepJob.STATUS_FUNDING_BROADCASTED,
    DepositSweepJob.STATUS_SWEEP_BROADCASTED,
}

TERMINAL_AUDIT_STATUSES = {
    OrphanDepositRecoveryAudit.STATUS_EMPTY_FINAL,
    OrphanDepositRecoveryAudit.STATUS_DUST_FINAL,
    OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL,
    OrphanDepositRecoveryAudit.STATUS_SWEPT_TOKEN_FINAL,
    OrphanDepositRecoveryAudit.STATUS_SWEPT_NATIVE_FINAL,
    OrphanDepositRecoveryAudit.STATUS_SWEPT_BOTH_FINAL,
}

SUPPORTED_STABLECOIN_ASSETS = {
    "USDT",
    "USDC",
}

USD_QUANT = Decimal("0.00000001")
NATIVE_WEI_DECIMALS = Decimal("1000000000000000000")


@dataclass(frozen=True)
class RecoveryRuntimeConfig:
    deriver: EvmDeriver
    option_index: dict[tuple[str, str, str], SweepOptionConfig]
    request_timeout_seconds: float
    rpc_max_lag_blocks: int
    rpc_max_reference_lag_blocks: int
    reference_heads_base_url: str
    reference_heads_shared_secret: str
    reference_heads_timeout_seconds: float
    reference_heads_max_age_seconds: int


def _parse_decimal(value: str, *, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise CommandError(f"{field_name} must be a valid decimal number") from exc

    if parsed < 0:
        raise CommandError(f"{field_name} must be >= 0")

    return parsed


def _quantize_usd(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(USD_QUANT)


def _decimal_to_json(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _option_key(*, chain: str, asset_code: str, token_contract_address: str) -> tuple[str, str, str]:
    return (
        _normalize_chain(chain),
        (asset_code or "").strip().upper(),
        _normalize_evm_address(token_contract_address),
    )


def _build_option_index(options: list[SweepOptionConfig]) -> dict[tuple[str, str, str], SweepOptionConfig]:
    indexed: dict[tuple[str, str, str], SweepOptionConfig] = {}
    for option in options:
        indexed[_option_key(
            chain=option.chain,
            asset_code=option.asset_code,
            token_contract_address=option.token_contract_address,
        )] = option
    return indexed


def _load_runtime_config_from_path(
    *,
    config_path: str,
    request_timeout_seconds: float,
    rpc_max_lag_blocks: int,
    rpc_max_reference_lag_blocks: int,
    reference_heads_base_url: str,
    reference_heads_shared_secret: str,
    reference_heads_timeout_seconds: float,
    reference_heads_max_age_seconds: int,
) -> RecoveryRuntimeConfig:
    raw = json.loads(Path(config_path).read_text(encoding="utf-8"))

    raw_options = raw.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        raise CommandError("Config file must contain a non-empty 'options' list")

    mnemonic = _read_secret_file(raw["mnemonic_file"])
    mnemonic_passphrase_file = str(raw.get("mnemonic_passphrase_file", "") or "").strip()
    mnemonic_passphrase = _read_secret_file(mnemonic_passphrase_file) if mnemonic_passphrase_file else ""
    account_index = int(raw.get("account_index", 0))
    if account_index < 0:
        raise CommandError("account_index must be >= 0")

    deriver = EvmDeriver(
        mnemonic=mnemonic,
        passphrase=mnemonic_passphrase,
        account_index=account_index,
    )

    options: list[SweepOptionConfig] = []
    for item in raw_options:
        funding_private_key = _normalize_private_key(
            _read_secret_file(item["funding_private_key_file"])
        )

        destination_address = str(
            _resolve_env_placeholder(item.get("destination_address", ""))
        ).strip().lower()
        if not destination_address:
            raise CommandError(f"Option {item.get('key', '')} must define destination_address")

        raw_max_gas_funding_amount_wei = item.get("max_gas_funding_amount_wei")
        if raw_max_gas_funding_amount_wei is None:
            raw_max_gas_funding_amount_wei = item["gas_funding_amount_wei"]

        option = SweepOptionConfig(
            key=str(item["key"]).strip(),
            chain=str(item["chain"]).strip().lower(),
            asset_code=str(item["asset_code"]).strip().upper(),
            token_contract_address=str(item.get("token_contract_address", "")).strip().lower(),
            rpc_urls=_resolve_rpc_urls(item),
            funding_private_key=funding_private_key,
            destination_address=destination_address,
            funding_confirmations=int(item.get("funding_confirmations", 1)),
            sweep_confirmations=int(item.get("sweep_confirmations", 1)),
            max_gas_funding_amount_wei=int(raw_max_gas_funding_amount_wei),
            erc20_transfer_gas_limit=int(item.get("erc20_transfer_gas_limit", 100000)),
            gas_limit_multiplier_bps=int(item.get("gas_limit_multiplier_bps", 12000)),
            gas_limit_retry_multiplier_bps=int(item.get("gas_limit_retry_multiplier_bps", 15000)),
            tx_timeout_seconds=int(item.get("tx_timeout_seconds", 300)),
            gas_price_multiplier_bps=int(item.get("gas_price_multiplier_bps", 12000)),
            poa_compatible=bool(item.get("poa_compatible", False)),
        )
        options.append(option)

    return RecoveryRuntimeConfig(
        deriver=deriver,
        option_index=_build_option_index(options),
        request_timeout_seconds=float(request_timeout_seconds),
        rpc_max_lag_blocks=int(rpc_max_lag_blocks),
        rpc_max_reference_lag_blocks=int(rpc_max_reference_lag_blocks),
        reference_heads_base_url=str(reference_heads_base_url or "").strip(),
        reference_heads_shared_secret=str(reference_heads_shared_secret or "").strip(),
        reference_heads_timeout_seconds=float(reference_heads_timeout_seconds),
        reference_heads_max_age_seconds=int(reference_heads_max_age_seconds),
    )


def _human_token_amount(*, chain: str, asset_code: str, raw_amount: int) -> Decimal:
    decimals = _get_route_onchain_decimals(chain=chain, asset_code=asset_code)
    return Decimal(int(raw_amount)) / (Decimal(10) ** int(decimals))


def _native_wei_to_human(amount_wei: int) -> Decimal:
    return Decimal(int(amount_wei)) / NATIVE_WEI_DECIMALS


def _native_wei_to_usd(*, amount_wei: int, native_price_usd: Decimal) -> Decimal:
    return _quantize_usd(_native_wei_to_human(amount_wei) * native_price_usd)


def _stablecoin_value_usd(*, asset_code: str, raw_amount: int, chain: str) -> Decimal | None:
    normalized_asset = (asset_code or "").strip().upper()
    if normalized_asset not in SUPPORTED_STABLECOIN_ASSETS:
        return None
    return _quantize_usd(_human_token_amount(chain=chain, asset_code=asset_code, raw_amount=raw_amount))


def _get_candidate_queryset(*, chain: str, asset_code: str, addresses: list[str], session_public_ids: list[str], older_than_hours: int, force_recheck: bool):
    cutoff = timezone.now() - timezone.timedelta(hours=int(older_than_hours))

    qs = DepositSession.objects.filter(chain=_normalize_chain(chain)).exclude(
        status__in=ACTIVE_DEPOSIT_SESSION_STATUSES
    ).filter(updated_at__lte=cutoff)

    if asset_code:
        qs = qs.filter(asset_code=(asset_code or "").strip().upper())

    if addresses:
        qs = qs.filter(deposit_address__in=[_normalize_evm_address(value) for value in addresses])

    if session_public_ids:
        qs = qs.filter(public_id__in=session_public_ids)

    active_sweep_jobs = DepositSweepJob.objects.filter(
        deposit_session_id=OuterRef("pk"),
        status__in=ACTIVE_SWEEP_JOB_STATUSES,
    )
    qs = qs.annotate(has_active_sweep_job=Exists(active_sweep_jobs)).filter(has_active_sweep_job=False)

    if not force_recheck:
        terminal_audit = OrphanDepositRecoveryAudit.objects.filter(
            chain=OuterRef("chain"),
            deposit_address=OuterRef("deposit_address"),
            status__in=TERMINAL_AUDIT_STATUSES,
        )
        qs = qs.annotate(has_terminal_audit=Exists(terminal_audit)).filter(has_terminal_audit=False)

    return qs.order_by("updated_at", "id")


def _get_or_create_audit(*, session: DepositSession) -> OrphanDepositRecoveryAudit:
    audit, _ = OrphanDepositRecoveryAudit.objects.get_or_create(
        chain=_normalize_chain(session.chain),
        deposit_address=_normalize_evm_address(session.deposit_address),
        defaults={
            "deposit_session": session,
            "asset_code": (session.asset_code or "").strip().upper(),
            "token_contract_address": _normalize_evm_address(session.token_contract_address),
            "address_derivation_ref": session.address_derivation_ref or "",
            "derivation_index": session.derivation_index,
        },
    )

    audit.deposit_session = session
    audit.asset_code = (session.asset_code or "").strip().upper()
    audit.token_contract_address = _normalize_evm_address(session.token_contract_address)
    audit.address_derivation_ref = session.address_derivation_ref or ""
    audit.derivation_index = session.derivation_index
    return audit


def _save_audit(
    *,
    audit: OrphanDepositRecoveryAudit,
    status: str,
    decision_reason: str,
    token_balance: int,
    native_balance: int,
    token_value_usd: Decimal | None,
    native_value_usd: Decimal | None,
    token_recovery_cost_usd: Decimal | None,
    native_recovery_cost_usd: Decimal | None,
    funding_txid: str,
    token_sweep_txid: str,
    native_sweep_txid: str,
    error_message: str,
    metadata: dict,
) -> None:
    audit.status = status
    audit.decision_reason = (decision_reason or "").strip()
    audit.last_token_balance = int(token_balance)
    audit.last_native_balance = int(native_balance)
    audit.last_token_value_usd = _quantize_usd(token_value_usd)
    audit.last_native_value_usd = _quantize_usd(native_value_usd)
    audit.last_estimated_token_recovery_cost_usd = _quantize_usd(token_recovery_cost_usd)
    audit.last_estimated_native_recovery_cost_usd = _quantize_usd(native_recovery_cost_usd)
    audit.funding_txid = (funding_txid or "").strip()
    audit.token_sweep_txid = (token_sweep_txid or "").strip()
    audit.native_sweep_txid = (native_sweep_txid or "").strip()
    audit.last_error = (error_message or "").strip()
    audit.metadata = metadata
    audit.last_checked_at = timezone.now()

    if status in TERMINAL_AUDIT_STATUSES:
        audit.finalized_at = timezone.now()
    else:
        audit.finalized_at = None

    audit.save()


class Command(BaseCommand):
    help = (
        "Recover profitable residual token/native balances from finalized deposit-session addresses. "
        "Terminal empty/dust/ignored decisions are persisted even without --commit to avoid rescanning."
    )

    def add_arguments(self, parser):
        parser.add_argument("--config-path", required=True, help="Sweeper JSON config path.")
        parser.add_argument("--chain", required=True, help="Single chain to process.")
        parser.add_argument("--asset-code", default="", help="Optional asset filter, such as USDT.")
        parser.add_argument("--address", nargs="*", default=[], help="Optional deposit address filter.")
        parser.add_argument("--session-public-id", nargs="*", default=[], help="Optional session public_id filter.")
        parser.add_argument("--older-than-hours", type=int, default=72, help="Only process sessions older than this age.")
        parser.add_argument("--max-addresses", type=int, default=100, help="Maximum number of candidate addresses to inspect.")
        parser.add_argument("--native-price-usd", required=True, help="Native asset USD price for the selected chain.")
        parser.add_argument("--min-token-value-usd", default="5", help="Minimum token residual value in USD.")
        parser.add_argument("--min-native-value-usd", default="2", help="Minimum native recoverable value in USD.")
        parser.add_argument("--profit-multiplier", default="2", help="Token value must be at least cost * multiplier.")
        parser.add_argument("--rpc-max-lag-blocks", type=int, default=64)
        parser.add_argument("--rpc-max-reference-lag-blocks", type=int, default=64)
        parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
        parser.add_argument("--reference-heads-base-url", default="")
        parser.add_argument("--reference-heads-shared-secret", default="")
        parser.add_argument("--reference-heads-timeout-seconds", type=float, default=5.0)
        parser.add_argument("--reference-heads-max-age-seconds", type=int, default=60)
        parser.add_argument("--force-recheck", action="store_true")
        parser.add_argument("--commit", action="store_true")

    def handle(self, *args, **options):
        chain = _normalize_chain(options["chain"])
        asset_code = (options["asset_code"] or "").strip().upper()
        addresses = [str(value).strip().lower() for value in options["address"] if str(value).strip()]
        session_public_ids = [str(value).strip() for value in options["session_public_id"] if str(value).strip()]

        if options["older_than_hours"] < 0:
            raise CommandError("--older-than-hours must be >= 0")
        if options["max_addresses"] <= 0:
            raise CommandError("--max-addresses must be > 0")

        reference_heads_base_url = str(options["reference_heads_base_url"] or "").strip()
        reference_heads_shared_secret = str(options["reference_heads_shared_secret"] or "").strip()
        if bool(reference_heads_base_url) != bool(reference_heads_shared_secret):
            raise CommandError(
                "Provide both --reference-heads-base-url and --reference-heads-shared-secret, or neither."
            )

        native_price_usd = _parse_decimal(options["native_price_usd"], field_name="--native-price-usd")
        min_token_value_usd = _parse_decimal(options["min_token_value_usd"], field_name="--min-token-value-usd")
        min_native_value_usd = _parse_decimal(options["min_native_value_usd"], field_name="--min-native-value-usd")
        profit_multiplier = _parse_decimal(options["profit_multiplier"], field_name="--profit-multiplier")

        if native_price_usd <= 0:
            raise CommandError("--native-price-usd must be > 0")
        if profit_multiplier < 1:
            raise CommandError("--profit-multiplier must be >= 1")

        runtime = _load_runtime_config_from_path(
            config_path=options["config_path"],
            request_timeout_seconds=options["request_timeout_seconds"],
            rpc_max_lag_blocks=options["rpc_max_lag_blocks"],
            rpc_max_reference_lag_blocks=options["rpc_max_reference_lag_blocks"],
            reference_heads_base_url=reference_heads_base_url,
            reference_heads_shared_secret=reference_heads_shared_secret,
            reference_heads_timeout_seconds=options["reference_heads_timeout_seconds"],
            reference_heads_max_age_seconds=options["reference_heads_max_age_seconds"],
        )

        candidates = list(
            _get_candidate_queryset(
                chain=chain,
                asset_code=asset_code,
                addresses=addresses,
                session_public_ids=session_public_ids,
                older_than_hours=options["older_than_hours"],
                force_recheck=bool(options["force_recheck"]),
            )[: int(options["max_addresses"])]
        )

        if not candidates:
            self.stdout.write("No orphan deposit candidates matched the filters.")
            return

        summary: dict[str, int] = {}
        for session in candidates:
            status = self._process_candidate(
                runtime=runtime,
                session=session,
                native_price_usd=native_price_usd,
                min_token_value_usd=min_token_value_usd,
                min_native_value_usd=min_native_value_usd,
                profit_multiplier=profit_multiplier,
                commit=bool(options["commit"]),
            )
            summary[status] = summary.get(status, 0) + 1

        self.stdout.write("")
        self.stdout.write("Summary:")
        self.stdout.write(f"candidates={len(candidates)}")
        for key in sorted(summary):
            self.stdout.write(f"{key}={summary[key]}")

    def _process_candidate(
        self,
        *,
        runtime: RecoveryRuntimeConfig,
        session: DepositSession,
        native_price_usd: Decimal,
        min_token_value_usd: Decimal,
        min_native_value_usd: Decimal,
        profit_multiplier: Decimal,
        commit: bool,
    ) -> str:
        source_address = _normalize_evm_address(session.deposit_address)
        audit = _get_or_create_audit(session=session)

        funding_txid = ""
        token_sweep_txid = ""
        native_sweep_txid = ""
        token_balance = 0
        native_balance = 0
        token_value_usd: Decimal | None = None
        native_value_usd: Decimal | None = None
        token_recovery_cost_usd: Decimal | None = None
        native_recovery_cost_usd: Decimal | None = None

        try:
            option = runtime.option_index.get(
                _option_key(
                    chain=session.chain,
                    asset_code=session.asset_code,
                    token_contract_address=session.token_contract_address,
                )
            )
            if option is None:
                status = OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="missing_route_option",
                    token_balance=0,
                    native_balance=0,
                    token_value_usd=None,
                    native_value_usd=None,
                    token_recovery_cost_usd=None,
                    native_recovery_cost_usd=None,
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata={
                        "commit": commit,
                        "planned_actions": [],
                        "session_public_id": str(session.public_id),
                    },
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"IGNORED address={source_address} reason=missing_route_option session={session.public_id}"
                    )
                )
                return status

            derivation_index = session.derivation_index
            if derivation_index is None:
                derivation_index = _parse_derivation_index_from_ref(session.address_derivation_ref)

            if derivation_index is None:
                status = OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="missing_derivation_index",
                    token_balance=0,
                    native_balance=0,
                    token_value_usd=None,
                    native_value_usd=None,
                    token_recovery_cost_usd=None,
                    native_recovery_cost_usd=None,
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata={
                        "commit": commit,
                        "planned_actions": [],
                        "session_public_id": str(session.public_id),
                    },
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"IGNORED address={source_address} reason=missing_derivation_index session={session.public_id}"
                    )
                )
                return status

            derived_address = runtime.deriver.derive_address(
                chain=option.chain,
                address_index=int(derivation_index),
            ).lower()
            if derived_address != source_address:
                status = OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="derivation_mismatch",
                    token_balance=0,
                    native_balance=0,
                    token_value_usd=None,
                    native_value_usd=None,
                    token_recovery_cost_usd=None,
                    native_recovery_cost_usd=None,
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata={
                        "commit": commit,
                        "planned_actions": [],
                        "session_public_id": str(session.public_id),
                        "derived_address": derived_address,
                        "expected_address": source_address,
                    },
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"IGNORED address={source_address} reason=derivation_mismatch session={session.public_id}"
                    )
                )
                return status

            source_private_key = runtime.deriver.derive_private_key(
                chain=option.chain,
                address_index=int(derivation_index),
            )

            if option.destination_address == source_address:
                status = OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="destination_equals_source",
                    token_balance=0,
                    native_balance=0,
                    token_value_usd=None,
                    native_value_usd=None,
                    token_recovery_cost_usd=None,
                    native_recovery_cost_usd=None,
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata={
                        "commit": commit,
                        "planned_actions": [],
                        "session_public_id": str(session.public_id),
                    },
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"IGNORED address={source_address} reason=destination_equals_source session={session.public_id}"
                    )
                )
                return status

            reference_head = None
            if runtime.reference_heads_base_url and runtime.reference_heads_shared_secret:
                reference_head = get_reference_head(
                    chain=option.chain,
                    base_url=runtime.reference_heads_base_url,
                    shared_secret=runtime.reference_heads_shared_secret,
                    timeout_seconds=runtime.reference_heads_timeout_seconds,
                    max_age_seconds=runtime.reference_heads_max_age_seconds,
                )

            selected_rpc_url = choose_best_rpc_url(
                option_key=option.key,
                rpc_urls=option.rpc_urls,
                poa_compatible=option.poa_compatible,
                request_timeout_seconds=runtime.request_timeout_seconds,
                max_lag_blocks=runtime.rpc_max_lag_blocks,
                reference_head=reference_head,
                max_reference_lag_blocks=runtime.rpc_max_reference_lag_blocks,
            )
            w3 = build_web3(
                rpc_url=selected_rpc_url,
                poa_compatible=option.poa_compatible,
                request_timeout_seconds=runtime.request_timeout_seconds,
            )
            nonce_allocator = NonceAllocator()

            token_balance = int(
                get_erc20_balance(
                    w3=w3,
                    token_contract_address=option.token_contract_address,
                    owner_address=source_address,
                )
            )
            native_balance = int(get_native_balance(w3=w3, address=source_address))

            if token_balance == 0 and native_balance == 0:
                status = OrphanDepositRecoveryAudit.STATUS_EMPTY_FINAL
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="empty_wallet",
                    token_balance=token_balance,
                    native_balance=native_balance,
                    token_value_usd=None,
                    native_value_usd=Decimal("0"),
                    token_recovery_cost_usd=None,
                    native_recovery_cost_usd=Decimal("0"),
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata={
                        "commit": commit,
                        "planned_actions": [],
                        "selected_rpc_url": selected_rpc_url,
                        "reference_head": reference_head,
                        "session_public_id": str(session.public_id),
                    },
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"EMPTY FINAL address={source_address} session={session.public_id}"
                    )
                )
                return status

            effective_gas_price_wei = int(_compute_effective_gas_price_wei(w3=w3, option=option))
            native_transfer_fee_wei = int(_compute_native_transfer_fee_wei(w3=w3, option=option))

            token_transfer_gas_limit = 0
            token_transfer_fee_wei = 0
            token_topup_needed_wei = 0
            funding_tx_fee_wei = 0
            token_profitable = False

            if token_balance > 0:
                token_value_usd = _stablecoin_value_usd(
                    asset_code=session.asset_code,
                    raw_amount=token_balance,
                    chain=session.chain,
                )
                if token_value_usd is None:
                    status = OrphanDepositRecoveryAudit.STATUS_IGNORED_FINAL
                    _save_audit(
                        audit=audit,
                        status=status,
                        decision_reason="unsupported_token_pricing",
                        token_balance=token_balance,
                        native_balance=native_balance,
                        token_value_usd=None,
                        native_value_usd=_native_wei_to_usd(amount_wei=native_balance, native_price_usd=native_price_usd),
                        token_recovery_cost_usd=None,
                        native_recovery_cost_usd=None,
                        funding_txid="",
                        token_sweep_txid="",
                        native_sweep_txid="",
                        error_message="",
                        metadata={
                            "commit": commit,
                            "planned_actions": [],
                            "selected_rpc_url": selected_rpc_url,
                            "reference_head": reference_head,
                            "session_public_id": str(session.public_id),
                        },
                    )
                    self.stdout.write(
                        self.style.WARNING(
                            f"IGNORED address={source_address} reason=unsupported_token_pricing session={session.public_id}"
                        )
                    )
                    return status

                token_transfer_gas_limit = int(
                    _estimate_erc20_transfer_gas(
                        w3=w3,
                        option=option,
                        source_address=source_address,
                        amount=token_balance,
                    )
                )
                token_transfer_fee_wei = int(token_transfer_gas_limit) * int(effective_gas_price_wei)
                token_topup_needed_wei = max(0, token_transfer_fee_wei - native_balance)
                funding_tx_fee_wei = int(native_transfer_fee_wei) if token_topup_needed_wei > 0 else 0
                token_recovery_cost_usd = _native_wei_to_usd(
                    amount_wei=token_transfer_fee_wei + funding_tx_fee_wei,
                    native_price_usd=native_price_usd,
                )

                if token_value_usd >= min_token_value_usd and token_value_usd >= (token_recovery_cost_usd * profit_multiplier):
                    token_profitable = True

            estimated_native_after_token = native_balance
            if token_profitable:
                if token_topup_needed_wei > 0:
                    estimated_native_after_token = 0
                else:
                    estimated_native_after_token = max(0, native_balance - token_transfer_fee_wei)

            native_recoverable_wei = max(0, estimated_native_after_token - native_transfer_fee_wei)
            native_value_usd = _native_wei_to_usd(
                amount_wei=native_recoverable_wei,
                native_price_usd=native_price_usd,
            )
            native_recovery_cost_usd = _native_wei_to_usd(
                amount_wei=native_transfer_fee_wei,
                native_price_usd=native_price_usd,
            )
            native_profitable = native_recoverable_wei > 0 and native_value_usd >= min_native_value_usd

            planned_actions: list[str] = []
            if token_profitable:
                planned_actions.append("recover_token")
            if native_profitable:
                planned_actions.append("recover_native")

            metadata = {
                "commit": commit,
                "planned_actions": planned_actions,
                "selected_rpc_url": selected_rpc_url,
                "reference_head": reference_head,
                "session_public_id": str(session.public_id),
                "token_transfer_gas_limit": int(token_transfer_gas_limit),
                "token_transfer_fee_wei": int(token_transfer_fee_wei),
                "token_topup_needed_wei": int(token_topup_needed_wei),
                "funding_tx_fee_wei": int(funding_tx_fee_wei),
                "native_transfer_fee_wei": int(native_transfer_fee_wei),
                "effective_gas_price_wei": int(effective_gas_price_wei),
                "native_price_usd": _decimal_to_json(native_price_usd),
                "min_token_value_usd": _decimal_to_json(min_token_value_usd),
                "min_native_value_usd": _decimal_to_json(min_native_value_usd),
                "profit_multiplier": _decimal_to_json(profit_multiplier),
            }

            if not planned_actions:
                status = OrphanDepositRecoveryAudit.STATUS_DUST_FINAL
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="below_profit_threshold",
                    token_balance=token_balance,
                    native_balance=native_balance,
                    token_value_usd=token_value_usd,
                    native_value_usd=native_value_usd,
                    token_recovery_cost_usd=token_recovery_cost_usd,
                    native_recovery_cost_usd=native_recovery_cost_usd,
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata=metadata,
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"DUST FINAL address={source_address} session={session.public_id}"
                    )
                )
                return status

            if not commit:
                status = OrphanDepositRecoveryAudit.STATUS_PENDING_CHECK
                _save_audit(
                    audit=audit,
                    status=status,
                    decision_reason="|".join(planned_actions),
                    token_balance=token_balance,
                    native_balance=native_balance,
                    token_value_usd=token_value_usd,
                    native_value_usd=native_value_usd,
                    token_recovery_cost_usd=token_recovery_cost_usd,
                    native_recovery_cost_usd=native_recovery_cost_usd,
                    funding_txid="",
                    token_sweep_txid="",
                    native_sweep_txid="",
                    error_message="",
                    metadata=metadata,
                )
                self.stdout.write(
                    f"DRY RUN address={source_address} session={session.public_id} actions={','.join(planned_actions)}"
                )
                return status

            if token_profitable:
                if token_topup_needed_wei > 0:
                    funding_address = address_from_private_key(option.funding_private_key)
                    funding_wallet_balance = int(get_native_balance(w3=w3, address=funding_address))
                    required_funding_wallet_budget_wei = int(token_topup_needed_wei) + int(funding_tx_fee_wei)
                    if funding_wallet_balance < required_funding_wallet_budget_wei:
                        raise RuntimeError(
                            "Funding wallet does not have enough native balance for orphan recovery"
                        )

                    funding_txid = send_native_transfer(
                        chain=option.chain,
                        w3=w3,
                        nonce_allocator=nonce_allocator,
                        funding_private_key=option.funding_private_key,
                        to_address=source_address,
                        amount_wei=int(token_topup_needed_wei),
                        gas_price_multiplier_bps=option.gas_price_multiplier_bps,
                    )
                    wait_for_confirmations(
                        w3=w3,
                        txid=funding_txid,
                        required_confirmations=option.funding_confirmations,
                        timeout_seconds=option.tx_timeout_seconds,
                    )

                token_sweep_txid = send_erc20_transfer(
                    chain=option.chain,
                    w3=w3,
                    nonce_allocator=nonce_allocator,
                    token_contract_address=option.token_contract_address,
                    source_private_key=source_private_key,
                    destination_address=option.destination_address,
                    amount=token_balance,
                    gas_limit=token_transfer_gas_limit,
                    gas_price_multiplier_bps=option.gas_price_multiplier_bps,
                )
                wait_for_confirmations(
                    w3=w3,
                    txid=token_sweep_txid,
                    required_confirmations=option.sweep_confirmations,
                    timeout_seconds=option.tx_timeout_seconds,
                )
                native_balance = int(get_native_balance(w3=w3, address=source_address))

            native_recoverable_after_exec_wei = max(0, native_balance - native_transfer_fee_wei)
            native_value_after_exec_usd = _native_wei_to_usd(
                amount_wei=native_recoverable_after_exec_wei,
                native_price_usd=native_price_usd,
            )

            if native_recoverable_after_exec_wei > 0 and native_value_after_exec_usd >= min_native_value_usd:
                native_sweep_txid = send_native_transfer(
                    chain=option.chain,
                    w3=w3,
                    nonce_allocator=nonce_allocator,
                    funding_private_key=source_private_key,
                    to_address=option.destination_address,
                    amount_wei=int(native_recoverable_after_exec_wei),
                    gas_price_multiplier_bps=option.gas_price_multiplier_bps,
                )
                wait_for_confirmations(
                    w3=w3,
                    txid=native_sweep_txid,
                    required_confirmations=option.sweep_confirmations,
                    timeout_seconds=option.tx_timeout_seconds,
                )
                native_balance = int(get_native_balance(w3=w3, address=source_address))

            if token_sweep_txid and native_sweep_txid:
                status = OrphanDepositRecoveryAudit.STATUS_SWEPT_BOTH_FINAL
                decision_reason = "recovered_token_and_native"
            elif token_sweep_txid:
                status = OrphanDepositRecoveryAudit.STATUS_SWEPT_TOKEN_FINAL
                decision_reason = "recovered_token_only"
            elif native_sweep_txid:
                status = OrphanDepositRecoveryAudit.STATUS_SWEPT_NATIVE_FINAL
                decision_reason = "recovered_native_only"
            else:
                status = OrphanDepositRecoveryAudit.STATUS_DUST_FINAL
                decision_reason = "below_profit_threshold_after_recheck"

            _save_audit(
                audit=audit,
                status=status,
                decision_reason=decision_reason,
                token_balance=int(get_erc20_balance(
                    w3=w3,
                    token_contract_address=option.token_contract_address,
                    owner_address=source_address,
                )),
                native_balance=native_balance,
                token_value_usd=token_value_usd,
                native_value_usd=_native_wei_to_usd(amount_wei=native_balance, native_price_usd=native_price_usd),
                token_recovery_cost_usd=token_recovery_cost_usd,
                native_recovery_cost_usd=native_recovery_cost_usd,
                funding_txid=funding_txid,
                token_sweep_txid=token_sweep_txid,
                native_sweep_txid=native_sweep_txid,
                error_message="",
                metadata=metadata,
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"COMMIT address={source_address} session={session.public_id} status={status}"
                )
            )
            return status

        except Exception as exc:
            status = OrphanDepositRecoveryAudit.STATUS_RETRYABLE_ERROR
            _save_audit(
                audit=audit,
                status=status,
                decision_reason="retryable_error",
                token_balance=token_balance,
                native_balance=native_balance,
                token_value_usd=token_value_usd,
                native_value_usd=native_value_usd,
                token_recovery_cost_usd=token_recovery_cost_usd,
                native_recovery_cost_usd=native_recovery_cost_usd,
                funding_txid=funding_txid,
                token_sweep_txid=token_sweep_txid,
                native_sweep_txid=native_sweep_txid,
                error_message=str(exc),
                metadata={
                    "commit": commit,
                    "session_public_id": str(session.public_id),
                },
            )
            self.stdout.write(
                self.style.ERROR(
                    f"RETRYABLE ERROR address={source_address} session={session.public_id} error={exc}"
                )
            )
            return status