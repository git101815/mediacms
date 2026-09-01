from __future__ import annotations

import logging
import os
from decimal import Decimal, InvalidOperation

from .claim_once import (
    _build_option_index,
    _build_option_selector,
    _build_option_web3,
    _compute_effective_gas_price_wei,
    _compute_native_transfer_fee_wei,
    _estimate_erc20_transfer_gas,
    _find_option_for_job,
    _release_sender_lock_safely,
    _reserve_sender_nonce,
    _resolve_derivation_index,
)
from .client import MediaCMSInternalClient
from .derivation import EvmDeriver
from .evm import (
    address_from_private_key,
    build_erc20_transfer_transaction,
    build_native_transfer_transaction,
    get_erc20_balance,
    get_native_balance,
    send_signed_transaction,
    sign_transaction,
    wait_for_confirmations,
)
from .runtime_price import fetch_native_usd_quote


STATUS_PENDING_CHECK = "pending_check"
STATUS_RETRYABLE_ERROR = "retryable_error"
STATUS_EMPTY_FINAL = "empty_final"
STATUS_DUST_FINAL = "dust_final"
STATUS_IGNORED_FINAL = "ignored_final"
STATUS_SWEPT_TOKEN_FINAL = "swept_token_final"
STATUS_SWEPT_NATIVE_FINAL = "swept_native_final"
STATUS_SWEPT_BOTH_FINAL = "swept_both_final"
SUPPORTED_STABLECOINS = {"USDT", "USDC"}
NATIVE_ASSET_BY_CHAIN = {
    "ethereum": "ETH",
    "arbitrum": "ETH",
    "base": "ETH",
    "bsc": "BNB",
    "polygon": "POL",
}
WEI = Decimal("1000000000000000000")
USD_QUANT = Decimal("0.00000001")


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def orphan_recovery_enabled() -> bool:
    return _env_bool("SWEEPER_ORPHAN_RECOVERY_ENABLED", True)


def _env_int(name: str, default: int, *, minimum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}")
    return value


def _env_decimal(name: str, default: str, *, minimum: Decimal) -> Decimal:
    try:
        value = Decimal(os.environ.get(name, default).strip())
    except (InvalidOperation, AttributeError) as exc:
        raise RuntimeError(f"{name} must be a decimal") from exc
    if not value.is_finite() or value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}")
    return value


def _usd(value: Decimal) -> Decimal:
    return value.quantize(USD_QUANT)


def _native_usd(*, amount_wei: int, price_usd: Decimal) -> Decimal:
    return _usd((Decimal(int(amount_wei)) / WEI) * price_usd)


def _stablecoin_usd(*, raw_amount: int, decimals: int) -> Decimal:
    return _usd(Decimal(int(raw_amount)) / (Decimal(10) ** int(decimals)))


def _runtime_price_for_chain(*, chain: str, cache: dict[str, tuple[Decimal, dict]]) -> tuple[Decimal, dict]:
    normalized_chain = str(chain or "").strip().lower()
    cached = cache.get(normalized_chain)
    if cached is not None:
        return cached

    native_asset = NATIVE_ASSET_BY_CHAIN.get(normalized_chain)
    if not native_asset:
        raise RuntimeError(f"Unsupported orphan-recovery native asset for chain={normalized_chain}")

    base_url = os.environ.get("RUNTIME_PRICES_BASE_URL", "").strip()
    shared_secret = os.environ.get("RUNTIME_PRICES_SHARED_SECRET", "").strip()
    if not base_url or not shared_secret:
        raise RuntimeError("RUNTIME_PRICES_BASE_URL and RUNTIME_PRICES_SHARED_SECRET are required for orphan recovery")

    quote = fetch_native_usd_quote(
        asset_code=native_asset,
        base_url=base_url,
        shared_secret=shared_secret,
        timeout_seconds=float(os.environ.get("SWEEPER_RUNTIME_PRICES_TIMEOUT_SECONDS", "5")),
        max_age_seconds=_env_int("SWEEPER_RUNTIME_PRICES_MAX_AGE_SECONDS", 360, minimum=1),
        future_skew_seconds=_env_int("SWEEPER_RUNTIME_PRICES_FUTURE_SKEW_SECONDS", 30, minimum=0),
    )
    value = (Decimal(str(quote["price"])), quote)
    cache[normalized_chain] = value
    return value


def _broadcast_with_sender_lock(
    *,
    client,
    config,
    option,
    w3,
    address: str,
    signer_private_key: str,
    tx_builder,
    progress_callback=None,
) -> str:
    sender_lock = None
    txid = ""
    try:
        sender_lock = _reserve_sender_nonce(
            client=client,
            config=config,
            option=option,
            w3=w3,
            address=address,
        )
        tx = tx_builder(int(sender_lock["nonce"]))
        signed = sign_transaction(tx=tx, private_key=signer_private_key)
        txid = signed["txid"]
        send_signed_transaction(
            w3=w3,
            raw_transaction=signed["raw_transaction"],
            expected_txid=txid,
        )
        if progress_callback is not None:
            progress_callback(txid)
        client.confirm_evm_sender_nonce_used(
            chain=option.chain,
            address=sender_lock["address"],
            lock_token=sender_lock["lock_token"],
            nonce=int(sender_lock["nonce"]),
            txid=txid,
        )
        return txid
    except Exception:
        # Before a txid exists it is safe to release immediately. Once a signed
        # tx exists, keep the nonce lease until expiry: an RPC error may be an
        # ambiguous "broadcast succeeded, response lost" case.
        if not txid:
            _release_sender_lock_safely(
                client=client,
                option=option,
                sender_lock=sender_lock,
            )
        raise


def _report(client, candidate: dict, **values) -> dict:
    payload = {
        "status": values.pop("status"),
        "decision_reason": values.pop("decision_reason", ""),
        "token_balance": int(values.pop("token_balance", 0)),
        "native_balance": int(values.pop("native_balance", 0)),
        "token_value_usd": values.pop("token_value_usd", None),
        "native_value_usd": values.pop("native_value_usd", None),
        "token_recovery_cost_usd": values.pop("token_recovery_cost_usd", None),
        "native_recovery_cost_usd": values.pop("native_recovery_cost_usd", None),
        "funding_txid": values.pop(
            "funding_txid",
            str((candidate.get("existing_audit") or {}).get("funding_txid") or ""),
        ),
        "token_sweep_txid": values.pop(
            "token_sweep_txid",
            str((candidate.get("existing_audit") or {}).get("token_sweep_txid") or ""),
        ),
        "native_sweep_txid": values.pop(
            "native_sweep_txid",
            str((candidate.get("existing_audit") or {}).get("native_sweep_txid") or ""),
        ),
        "error_message": values.pop("error_message", ""),
        "metadata": values.pop("metadata", {}),
    }
    if values:
        raise RuntimeError(f"Unexpected orphan-recovery report fields: {sorted(values)}")
    for key in (
        "token_value_usd",
        "native_value_usd",
        "token_recovery_cost_usd",
        "native_recovery_cost_usd",
    ):
        value = payload[key]
        payload[key] = None if value is None else format(Decimal(value), "f")
    return client.record_orphan_recovery_result(
        session_public_id=candidate["session_public_id"],
        claim_token=candidate["claim_token"],
        result=payload,
    )


def _process_candidate(*, client, config, candidate: dict, option_index: dict, price_cache: dict) -> str:
    source_address = str(candidate["source_address"]).strip().lower()
    existing_audit = candidate.get("existing_audit") or {}
    funding_txid = str(existing_audit.get("funding_txid") or "")
    token_sweep_txid = str(existing_audit.get("token_sweep_txid") or "")
    native_sweep_txid = str(existing_audit.get("native_sweep_txid") or "")
    token_balance = native_balance = 0
    token_value_usd = native_value_usd = token_cost_usd = native_cost_usd = None

    try:
        option = _find_option_for_job(option_index=option_index, job=candidate)
        if str(option.asset_code).upper() not in SUPPORTED_STABLECOINS or not option.token_contract_address:
            return _report(
                client,
                candidate,
                status=STATUS_IGNORED_FINAL,
                decision_reason="unsupported_automatic_route",
            )["status"]

        derivation_index = _resolve_derivation_index(candidate)
        deriver = EvmDeriver(
            mnemonic=config.mnemonic,
            passphrase=config.mnemonic_passphrase,
            account_index=config.account_index,
        )
        derived_address = deriver.derive_address(
            chain=option.chain,
            address_index=derivation_index,
        ).lower()
        if derived_address != source_address:
            return _report(
                client,
                candidate,
                status=STATUS_IGNORED_FINAL,
                decision_reason="derivation_mismatch",
            )["status"]

        source_private_key = deriver.derive_private_key(
            chain=option.chain,
            address_index=derivation_index,
        )
        if address_from_private_key(source_private_key).lower() != source_address:
            return _report(
                client,
                candidate,
                status=STATUS_IGNORED_FINAL,
                decision_reason="private_key_mismatch",
            )["status"]
        if str(option.destination_address).strip().lower() == source_address:
            return _report(
                client,
                candidate,
                status=STATUS_IGNORED_FINAL,
                decision_reason="destination_equals_source",
            )["status"]

        w3 = _build_option_web3(config=config, option=option)
        token_balance = int(
            get_erc20_balance(
                w3=w3,
                token_contract_address=option.token_contract_address,
                owner_address=source_address,
            )
        )
        native_balance = int(get_native_balance(w3=w3, address=source_address))
        native_price_usd, runtime_quote = _runtime_price_for_chain(
            chain=option.chain,
            cache=price_cache,
        )

        # A previous worker can have died after a broadcast. Reconcile durable
        # audit txids before making any new funding/sweep decision.
        if funding_txid:
            wait_for_confirmations(
                w3=w3,
                txid=funding_txid,
                required_confirmations=option.funding_confirmations,
                timeout_seconds=option.tx_timeout_seconds,
            )
            native_balance = int(get_native_balance(w3=w3, address=source_address))
        if token_sweep_txid:
            wait_for_confirmations(
                w3=w3,
                txid=token_sweep_txid,
                required_confirmations=option.sweep_confirmations,
                timeout_seconds=option.tx_timeout_seconds,
            )
            token_balance = int(
                get_erc20_balance(
                    w3=w3,
                    token_contract_address=option.token_contract_address,
                    owner_address=source_address,
                )
            )
            native_balance = int(get_native_balance(w3=w3, address=source_address))
        if native_sweep_txid:
            wait_for_confirmations(
                w3=w3,
                txid=native_sweep_txid,
                required_confirmations=option.sweep_confirmations,
                timeout_seconds=option.tx_timeout_seconds,
            )
            native_balance = int(get_native_balance(w3=w3, address=source_address))

        if token_balance == 0 and native_balance == 0:
            if token_sweep_txid and native_sweep_txid:
                status, reason = STATUS_SWEPT_BOTH_FINAL, "reconciled_previous_token_and_native_sweeps"
            elif token_sweep_txid:
                status, reason = STATUS_SWEPT_TOKEN_FINAL, "reconciled_previous_token_sweep"
            elif native_sweep_txid:
                status, reason = STATUS_SWEPT_NATIVE_FINAL, "reconciled_previous_native_sweep"
            else:
                status, reason = STATUS_EMPTY_FINAL, "empty_wallet"
            return _report(
                client,
                candidate,
                status=status,
                decision_reason=reason,
                native_value_usd=Decimal("0"),
                native_recovery_cost_usd=Decimal("0"),
                funding_txid=funding_txid,
                token_sweep_txid=token_sweep_txid,
                native_sweep_txid=native_sweep_txid,
                metadata={"runtime_price_quote": runtime_quote},
            )["status"]

        effective_gas_price_wei = int(_compute_effective_gas_price_wei(w3=w3, option=option))
        native_transfer_fee_wei = int(_compute_native_transfer_fee_wei(w3=w3, option=option))
        token_transfer_gas_limit = 0
        token_transfer_fee_wei = 0
        token_topup_needed_wei = 0
        funding_tx_fee_wei = 0
        token_profitable = False

        min_token_value_usd = _env_decimal(
            "SWEEPER_ORPHAN_RECOVERY_MIN_TOKEN_VALUE_USD", "5", minimum=Decimal("0")
        )
        min_native_value_usd = _env_decimal(
            "SWEEPER_ORPHAN_RECOVERY_MIN_NATIVE_VALUE_USD", "2", minimum=Decimal("0")
        )
        profit_multiplier = _env_decimal(
            "SWEEPER_ORPHAN_RECOVERY_PROFIT_MULTIPLIER", "2", minimum=Decimal("1")
        )

        if token_balance > 0:
            token_value_usd = _stablecoin_usd(
                raw_amount=token_balance,
                decimals=int(candidate["onchain_decimals"]),
            )
            token_transfer_gas_limit = int(
                _estimate_erc20_transfer_gas(
                    w3=w3,
                    option=option,
                    source_address=source_address,
                    amount=token_balance,
                )
            )
            token_transfer_fee_wei = token_transfer_gas_limit * effective_gas_price_wei
            token_topup_needed_wei = max(0, token_transfer_fee_wei - native_balance)
            funding_tx_fee_wei = native_transfer_fee_wei if token_topup_needed_wei > 0 else 0
            token_cost_usd = _native_usd(
                amount_wei=token_transfer_fee_wei + funding_tx_fee_wei,
                price_usd=native_price_usd,
            )
            token_profitable = (
                token_value_usd >= min_token_value_usd
                and token_value_usd >= token_cost_usd * profit_multiplier
            )

        estimated_native_after_token = native_balance
        if token_profitable:
            estimated_native_after_token = (
                0
                if token_topup_needed_wei > 0
                else max(0, native_balance - token_transfer_fee_wei)
            )
        native_recoverable_wei = max(0, estimated_native_after_token - native_transfer_fee_wei)
        native_value_usd = _native_usd(
            amount_wei=native_recoverable_wei,
            price_usd=native_price_usd,
        )
        native_cost_usd = _native_usd(
            amount_wei=native_transfer_fee_wei,
            price_usd=native_price_usd,
        )
        native_profitable = (
            native_recoverable_wei > 0 and native_value_usd >= min_native_value_usd
        )

        planned_actions = []
        if token_profitable:
            planned_actions.append("recover_token")
        if native_profitable:
            planned_actions.append("recover_native")

        metadata = {
            "planned_actions": planned_actions,
            "runtime_price_quote": runtime_quote,
            "token_transfer_gas_limit": token_transfer_gas_limit,
            "token_transfer_fee_wei": token_transfer_fee_wei,
            "token_topup_needed_wei": token_topup_needed_wei,
            "funding_tx_fee_wei": funding_tx_fee_wei,
            "native_transfer_fee_wei": native_transfer_fee_wei,
            "effective_gas_price_wei": effective_gas_price_wei,
        }

        def persist_broadcast(field: str, reason: str):
            def _persist(txid: str) -> None:
                candidate.setdefault("existing_audit", {})[field] = txid
                kwargs = {
                    "funding_txid": funding_txid,
                    "token_sweep_txid": token_sweep_txid,
                    "native_sweep_txid": native_sweep_txid,
                }
                kwargs[field] = txid
                _report(
                    client,
                    candidate,
                    status=STATUS_PENDING_CHECK,
                    decision_reason=reason,
                    token_balance=token_balance,
                    native_balance=native_balance,
                    token_value_usd=token_value_usd,
                    native_value_usd=native_value_usd,
                    token_recovery_cost_usd=token_cost_usd,
                    native_recovery_cost_usd=native_cost_usd,
                    metadata={**metadata, "broadcast_stage": reason},
                    **kwargs,
                )

            return _persist

        if not planned_actions:
            return _report(
                client,
                candidate,
                status=STATUS_DUST_FINAL,
                decision_reason="below_profit_threshold",
                token_balance=token_balance,
                native_balance=native_balance,
                token_value_usd=token_value_usd,
                native_value_usd=native_value_usd,
                token_recovery_cost_usd=token_cost_usd,
                native_recovery_cost_usd=native_cost_usd,
                metadata=metadata,
            )["status"]

        if token_profitable:
            if token_topup_needed_wei > 0:
                funding_address = address_from_private_key(option.funding_private_key).lower()
                funding_wallet_balance = int(get_native_balance(w3=w3, address=funding_address))
                required_budget = token_topup_needed_wei + funding_tx_fee_wei
                if funding_wallet_balance < required_budget:
                    raise RuntimeError(
                        "Funding wallet does not have enough native balance for orphan recovery"
                    )
                funding_txid = _broadcast_with_sender_lock(
                    client=client,
                    config=config,
                    option=option,
                    w3=w3,
                    address=funding_address,
                    signer_private_key=option.funding_private_key,
                    progress_callback=persist_broadcast("funding_txid", "funding_broadcasted"),
                    tx_builder=lambda nonce: build_native_transfer_transaction(
                        w3=w3,
                        nonce=nonce,
                        funding_private_key=option.funding_private_key,
                        to_address=source_address,
                        amount_wei=token_topup_needed_wei,
                        gas_price_multiplier_bps=option.gas_price_multiplier_bps,
                    ),
                )
                wait_for_confirmations(
                    w3=w3,
                    txid=funding_txid,
                    required_confirmations=option.funding_confirmations,
                    timeout_seconds=option.tx_timeout_seconds,
                )

            token_sweep_txid = _broadcast_with_sender_lock(
                client=client,
                config=config,
                option=option,
                w3=w3,
                address=source_address,
                signer_private_key=source_private_key,
                progress_callback=persist_broadcast("token_sweep_txid", "token_sweep_broadcasted"),
                tx_builder=lambda nonce: build_erc20_transfer_transaction(
                    w3=w3,
                    nonce=nonce,
                    token_contract_address=option.token_contract_address,
                    source_private_key=source_private_key,
                    destination_address=option.destination_address,
                    amount=token_balance,
                    gas_limit=token_transfer_gas_limit,
                    gas_price_multiplier_bps=option.gas_price_multiplier_bps,
                ),
            )
            wait_for_confirmations(
                w3=w3,
                txid=token_sweep_txid,
                required_confirmations=option.sweep_confirmations,
                timeout_seconds=option.tx_timeout_seconds,
            )
            native_balance = int(get_native_balance(w3=w3, address=source_address))

        native_recoverable_after_exec_wei = max(0, native_balance - native_transfer_fee_wei)
        native_value_after_exec_usd = _native_usd(
            amount_wei=native_recoverable_after_exec_wei,
            price_usd=native_price_usd,
        )
        if (
            native_recoverable_after_exec_wei > 0
            and native_value_after_exec_usd >= min_native_value_usd
        ):
            native_sweep_txid = _broadcast_with_sender_lock(
                client=client,
                config=config,
                option=option,
                w3=w3,
                address=source_address,
                signer_private_key=source_private_key,
                progress_callback=persist_broadcast("native_sweep_txid", "native_sweep_broadcasted"),
                tx_builder=lambda nonce: build_native_transfer_transaction(
                    w3=w3,
                    nonce=nonce,
                    funding_private_key=source_private_key,
                    to_address=option.destination_address,
                    amount_wei=native_recoverable_after_exec_wei,
                    gas_price_multiplier_bps=option.gas_price_multiplier_bps,
                ),
            )
            wait_for_confirmations(
                w3=w3,
                txid=native_sweep_txid,
                required_confirmations=option.sweep_confirmations,
                timeout_seconds=option.tx_timeout_seconds,
            )
            native_balance = int(get_native_balance(w3=w3, address=source_address))

        final_token_balance = int(
            get_erc20_balance(
                w3=w3,
                token_contract_address=option.token_contract_address,
                owner_address=source_address,
            )
        )
        if token_sweep_txid and native_sweep_txid:
            status = STATUS_SWEPT_BOTH_FINAL
            reason = "recovered_token_and_native"
        elif token_sweep_txid:
            status = STATUS_SWEPT_TOKEN_FINAL
            reason = "recovered_token_only"
        elif native_sweep_txid:
            status = STATUS_SWEPT_NATIVE_FINAL
            reason = "recovered_native_only"
        else:
            status = STATUS_DUST_FINAL
            reason = "below_profit_threshold_after_recheck"

        return _report(
            client,
            candidate,
            status=status,
            decision_reason=reason,
            token_balance=final_token_balance,
            native_balance=native_balance,
            token_value_usd=token_value_usd,
            native_value_usd=_native_usd(amount_wei=native_balance, price_usd=native_price_usd),
            token_recovery_cost_usd=token_cost_usd,
            native_recovery_cost_usd=native_cost_usd,
            funding_txid=funding_txid,
            token_sweep_txid=token_sweep_txid,
            native_sweep_txid=native_sweep_txid,
            metadata=metadata,
        )["status"]
    except Exception as exc:
        logging.exception(
            "sweeper_service action=orphan_recovery_candidate_failed session=%s",
            candidate.get("session_public_id"),
        )
        try:
            return _report(
                client,
                candidate,
                status=STATUS_RETRYABLE_ERROR,
                decision_reason="retryable_error",
                token_balance=token_balance,
                native_balance=native_balance,
                token_value_usd=token_value_usd,
                native_value_usd=native_value_usd,
                token_recovery_cost_usd=token_cost_usd,
                native_recovery_cost_usd=native_cost_usd,
                funding_txid=funding_txid,
                token_sweep_txid=token_sweep_txid,
                native_sweep_txid=native_sweep_txid,
                error_message=str(exc),
            )["status"]
        except Exception:
            logging.exception(
                "sweeper_service action=orphan_recovery_report_failed session=%s",
                candidate.get("session_public_id"),
            )
            raise


def run_orphan_recovery_once(*, config, stop_event=None) -> dict:
    options = [
        _build_option_selector(option)
        for option in config.options
        if option.token_contract_address
        and str(option.asset_code).strip().upper() in SUPPORTED_STABLECOINS
    ]
    if not options:
        return {"claimed": 0, "processed": 0, "retryable": 0}

    client = MediaCMSInternalClient(
        base_url=config.mediacms_base_url,
        service_name=config.service_name,
        shared_secret=config.shared_secret,
        timeout=config.internal_api_timeout_seconds,
    )
    try:
        candidates = client.claim_orphan_recovery_candidates(
            options=options,
            limit=_env_int("SWEEPER_ORPHAN_RECOVERY_BATCH_SIZE", 20, minimum=1),
            older_than_hours=_env_int("SWEEPER_ORPHAN_RECOVERY_OLDER_THAN_HOURS", 72, minimum=0),
            lease_seconds=_env_int("SWEEPER_ORPHAN_RECOVERY_LEASE_SECONDS", 7200, minimum=60),
        )
        option_index = _build_option_index(config.options)
        price_cache = {}
        processed = 0
        retryable = 0
        for candidate in candidates:
            if stop_event is not None and stop_event.is_set():
                break
            status = _process_candidate(
                client=client,
                config=config,
                candidate=candidate,
                option_index=option_index,
                price_cache=price_cache,
            )
            processed += 1
            if status == STATUS_RETRYABLE_ERROR:
                retryable += 1
            logging.info(
                "sweeper_service action=orphan_recovery_complete session=%s status=%s",
                candidate.get("session_public_id"),
                status,
            )
        return {
            "claimed": len(candidates),
            "processed": processed,
            "retryable": retryable,
        }
    finally:
        client.close()


def orphan_recovery_loop(*, config, stop_event) -> None:
    interval = _env_int("SWEEPER_ORPHAN_RECOVERY_INTERVAL_SECONDS", 86400, minimum=60)
    retry_interval = min(interval, _env_int("SWEEPER_ORPHAN_RECOVERY_ERROR_RETRY_SECONDS", 300, minimum=30))
    while not stop_event.is_set():
        delay = interval
        try:
            result = run_orphan_recovery_once(config=config, stop_event=stop_event)
            if result.get("retryable", 0):
                delay = retry_interval
            logging.info(
                "sweeper_service action=orphan_recovery_cycle claimed=%s processed=%s retryable=%s",
                result["claimed"],
                result["processed"],
                result.get("retryable", 0),
            )
        except Exception:
            delay = retry_interval
            logging.exception("sweeper_service action=orphan_recovery_cycle_failed")
        stop_event.wait(delay)
