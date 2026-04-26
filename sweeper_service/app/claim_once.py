import logging
import time
import json
from datetime import datetime, timedelta, timezone as dt_timezone
from .client import MediaCMSInternalClient
from .config import load_config
from .derivation import EvmDeriver
from .evm import (
    address_from_private_key,
    build_erc20_transfer_transaction,
    build_native_transfer_transaction,
    build_web3,
    get_erc20_balance,
    get_native_balance,
    get_receipt_with_confirmations,
    send_signed_transaction,
    sign_transaction,
    transaction_is_known,
    wait_for_confirmations,
)
from typing import NoReturn
from .reference_head import get_reference_head
from .rpc_pool import choose_best_rpc_url
from web3 import Web3
from web3.exceptions import TransactionNotFound

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _build_option_selector(option) -> dict:
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
    }


def _option_key(*, chain: str, asset_code: str, token_contract_address: str) -> tuple[str, str, str]:
    return (
        (chain or "").strip().lower(),
        (asset_code or "").strip().upper(),
        (token_contract_address or "").strip().lower(),
    )


def _build_option_index(options) -> dict:
    indexed = {}
    for option in options:
        key = _option_key(
            chain=option.chain,
            asset_code=option.asset_code,
            token_contract_address=option.token_contract_address,
        )
        indexed[key] = option
    return indexed


def _resolve_derivation_index(job: dict) -> int:
    value = job.get("derivation_index")
    if value is not None:
        return int(value)

    address_derivation_ref = str(job.get("address_derivation_ref", "")).strip()
    tail = address_derivation_ref.rsplit(":", 1)[-1]
    if tail.isdigit():
        return int(tail)

    raise RuntimeError(
        f"Sweep job {job.get('public_id')} is missing derivation_index"
    )


def _find_option_for_job(*, option_index: dict, job: dict):
    key = _option_key(
        chain=job["chain"],
        asset_code=job["asset_code"],
        token_contract_address=job.get("token_contract_address", ""),
    )
    option = option_index.get(key)
    if option is None:
        raise RuntimeError(
            "No sweeper option configured for "
            f"chain={job.get('chain')} asset={job.get('asset_code')} "
            f"token={job.get('token_contract_address', '')}"
        )
    return option


def _truncate_error(message: str, *, max_length: int = 500) -> str:
    normalized = (message or "").strip()
    if len(normalized) <= max_length:
        return normalized

    try:
        payload = json.loads(normalized)
    except Exception:
        return normalized[: max_length - 3] + "..."

    payload["details"] = payload.get("details", {})
    payload["details"]["truncated"] = True
    payload["details"]["original_length"] = len(normalized)

    compact = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(compact) <= max_length:
        return compact

    payload["details"] = {
        "truncated": True,
        "original_length": len(normalized),
    }
    compact = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(compact) <= max_length:
        return compact

    return compact[: max_length - 3] + "..."

def _build_error_payload(*, code: str, message: str, retryable: bool, details: dict | None = None) -> dict:
    return {
        "code": str(code),
        "message": str(message),
        "retryable": bool(retryable),
        "details": details or {},
    }


def _raise_structured_error(*, code: str, message: str, retryable: bool, details: dict | None = None) -> NoReturn:
    payload = _build_error_payload(
        code=code,
        message=message,
        retryable=retryable,
        details=details,
    )
    raise RuntimeError(json.dumps(payload, sort_keys=True, separators=(",", ":")))

def _build_option_web3(*, config, option):
    reference_head = get_reference_head(
        chain=option.chain,
        base_url=config.reference_heads_base_url,
        shared_secret=config.reference_heads_shared_secret,
        timeout_seconds=config.reference_heads_timeout_seconds,
        max_age_seconds=config.reference_heads_max_age_seconds,
    )

    selected_rpc_url = choose_best_rpc_url(
        option_key=option.key,
        rpc_urls=option.rpc_urls,
        poa_compatible=option.poa_compatible,
        max_lag_blocks=config.rpc_max_lag_blocks,
        reference_head=reference_head,
        max_reference_lag_blocks=config.rpc_max_reference_lag_blocks,
        request_timeout_seconds=config.request_timeout_seconds,
    )

    return build_web3(
        rpc_url=selected_rpc_url,
        poa_compatible=option.poa_compatible,
        request_timeout_seconds=config.request_timeout_seconds,
    )

def _estimate_erc20_transfer_gas(*, w3, option, source_address: str, amount: int) -> int:
    token_contract = w3.eth.contract(
        address=Web3.to_checksum_address(option.token_contract_address),
        abi=[
            {
                "constant": False,
                "inputs": [
                    {"name": "_to", "type": "address"},
                    {"name": "_value", "type": "uint256"},
                ],
                "name": "transfer",
                "outputs": [{"name": "", "type": "bool"}],
                "payable": False,
                "stateMutability": "nonpayable",
                "type": "function",
            }
        ],
    )

    try:
        estimated_gas = token_contract.functions.transfer(
            Web3.to_checksum_address(option.destination_address),
            int(amount),
        ).estimate_gas(
            {
                "from": Web3.to_checksum_address(source_address),
            }
        )
    except Exception:
        logging.exception(
            "sweeper_service action=estimate_gas_failed chain=%s asset=%s source=%s fallback_gas_limit=%s",
            option.chain,
            option.asset_code,
            source_address,
            option.erc20_transfer_gas_limit,
        )
        estimated_gas = int(option.erc20_transfer_gas_limit)

    try:
        estimated_gas = int(estimated_gas)
    except (TypeError, ValueError):
        logging.warning(
            "sweeper_service action=estimate_gas_invalid chain=%s asset=%s source=%s fallback_gas_limit=%s raw=%r",
            option.chain,
            option.asset_code,
            source_address,
            option.erc20_transfer_gas_limit,
            estimated_gas,
        )
        estimated_gas = int(option.erc20_transfer_gas_limit)

    gas_limit = (estimated_gas * int(option.gas_limit_multiplier_bps) + 9999) // 10000
    if gas_limit < estimated_gas:
        gas_limit = estimated_gas
    return gas_limit


def _compute_effective_gas_price_wei(*, w3, option) -> int:
    raw_network_gas_price = w3.eth.gas_price
    try:
        network_gas_price = int(raw_network_gas_price)
    except (TypeError, ValueError):
        logging.warning(
            "sweeper_service action=gas_price_invalid chain=%s asset=%s fallback_gas_price=1 raw=%r",
            option.chain,
            option.asset_code,
            raw_network_gas_price,
        )
        network_gas_price = 1

    effective_gas_price = (
        network_gas_price * int(option.gas_price_multiplier_bps) + 9999
    ) // 10000
    if effective_gas_price < network_gas_price:
        effective_gas_price = network_gas_price
    return effective_gas_price


def _compute_required_native_wei(*, w3, option, source_address: str, amount: int) -> tuple[int, int]:
    gas_limit = _estimate_erc20_transfer_gas(
        w3=w3,
        option=option,
        source_address=source_address,
        amount=amount,
    )
    effective_gas_price = _compute_effective_gas_price_wei(w3=w3, option=option)
    required_native = gas_limit * effective_gas_price
    return gas_limit, required_native

def _read_mined_receipt(*, w3, txid: str) -> dict:
    receipt = None
    try:
        receipt = w3.eth.get_transaction_receipt(txid)
    except TransactionNotFound:
        _raise_structured_error(
            code="SWEEP_RECEIPT_MISSING",
            message="Missing receipt for mined transaction",
            retryable=True,
            details={"txid": txid},
        )

    if receipt is None:
        _raise_structured_error(
            code="SWEEP_RECEIPT_MISSING",
            message="Missing receipt for mined transaction",
            retryable=True,
            details={"txid": txid},
        )

    return dict(receipt)

def _looks_like_out_of_gas(*, receipt: dict, attempted_gas_limit: int) -> bool:
    gas_used = int(receipt.get("gasUsed", 0) or 0)
    attempted = int(attempted_gas_limit)
    if attempted <= 0:
        return False
    return gas_used * 100 >= attempted * 80


def _recommended_retry_gas_limit(*, option, attempted_gas_limit: int, gas_used: int) -> int:
    baseline = max(int(attempted_gas_limit), int(gas_used))
    bumped = (
        baseline * int(option.gas_limit_retry_multiplier_bps) + 9999
    ) // 10000
    if bumped <= baseline:
        bumped = baseline + 1
    return bumped


def _compute_retry_budget(
    *,
    w3,
    option,
    source_address: str,
    retry_gas_limit: int,
) -> tuple[int, int]:
    effective_gas_price = _compute_effective_gas_price_wei(w3=w3, option=option)
    required_native = int(retry_gas_limit) * int(effective_gas_price)
    source_native_balance = int(get_native_balance(w3=w3, address=source_address))
    extra_topup_needed = max(0, required_native - source_native_balance)
    return required_native, extra_topup_needed




def _default_retry_delay_seconds(config) -> int:
    return max(15, int(getattr(config, "poll_interval_seconds", 15)))


def _broadcast_prepared_at_from_job(job: dict, key: str):
    value = ((job.get("metadata") or {}).get(key) or {}).get("prepared_at")
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt_timezone.utc)
    return parsed


def _broadcast_missing_deadline_has_passed(*, job: dict, key: str, timeout_seconds: int) -> bool:
    prepared_at = _broadcast_prepared_at_from_job(job, key)
    if prepared_at is None:
        return False
    return datetime.now(dt_timezone.utc) >= prepared_at + timedelta(seconds=int(timeout_seconds))


def _compute_required_native_for_gas_limit(*, w3, option, gas_limit: int) -> int:
    effective_gas_price = _compute_effective_gas_price_wei(w3=w3, option=option)
    return int(gas_limit) * int(effective_gas_price)


def _compute_native_transfer_fee_wei(*, w3, option) -> int:
    effective_gas_price = _compute_effective_gas_price_wei(w3=w3, option=option)
    return 21000 * effective_gas_price


def _ensure_funding_wallet_budget(
    *,
    w3,
    option,
    amount_wei: int,
    public_id: str,
) -> None:
    funding_address = address_from_private_key(option.funding_private_key)
    funding_native_balance = int(get_native_balance(w3=w3, address=funding_address))
    funding_tx_fee_wei = _compute_native_transfer_fee_wei(w3=w3, option=option)
    required_total_wei = int(amount_wei) + int(funding_tx_fee_wei)

    if funding_native_balance >= required_total_wei:
        return

    _raise_structured_error(
        code="SWEEP_FUNDING_WALLET_UNDERFUNDED",
        message="Funding wallet does not have enough native balance for gas top-up",
        retryable=True,
        details={
            "job_public_id": public_id,
            "chain": option.chain,
            "asset_code": option.asset_code,
            "funding_address": funding_address,
            "funding_native_balance_wei": funding_native_balance,
            "topup_amount_wei": int(amount_wei),
            "funding_tx_fee_wei": int(funding_tx_fee_wei),
            "required_total_wei": int(required_total_wei),
        },
    )


def _extract_structured_error(exc: Exception) -> dict | None:
    raw_value = str(exc or "").strip()
    if not raw_value:
        return None

    try:
        payload = json.loads(raw_value)
    except Exception:
        return None

    if not isinstance(payload, dict):
        return None

    if "retryable" not in payload:
        return None

    return payload


def _claim_token_for_job(job: dict) -> str:
    claim_token = str(job.get("claim_token", "")).strip()
    if not claim_token:
        raise RuntimeError(f"Sweep job {job.get('public_id')} is missing claim_token")
    return claim_token


def _sender_lock_seconds(config) -> int:
    value = int(getattr(config, "evm_sender_lock_seconds", 120) or 120)
    return max(30, value)


def _pending_nonce(*, w3, address: str) -> int:
    return int(
        w3.eth.get_transaction_count(
            Web3.to_checksum_address(address),
            block_identifier="pending",
        )
    )


def _reserve_sender_nonce(
    *,
    client: MediaCMSInternalClient,
    config,
    option,
    w3,
    address: str,
) -> dict:
    normalized_address = str(address or "").strip().lower()
    if not normalized_address:
        raise RuntimeError("Cannot reserve nonce for an empty address")

    lock = client.acquire_evm_sender_lock(
        chain=option.chain,
        address=normalized_address,
        lock_seconds=_sender_lock_seconds(config),
    )

    rpc_pending_nonce = _pending_nonce(w3=w3, address=normalized_address)
    backend_next_nonce = lock.get("next_nonce")
    if backend_next_nonce is None:
        selected_nonce = rpc_pending_nonce
    else:
        selected_nonce = max(int(backend_next_nonce), rpc_pending_nonce)

    return {
        **lock,
        "address": normalized_address,
        "nonce": int(selected_nonce),
    }


def _release_sender_lock_safely(
    *,
    client: MediaCMSInternalClient,
    option,
    sender_lock: dict | None,
) -> None:
    if not sender_lock:
        return

    try:
        client.release_evm_sender_lock(
            chain=option.chain,
            address=sender_lock["address"],
            lock_token=sender_lock["lock_token"],
        )
    except Exception:
        logging.exception(
            "sweeper_service action=release_sender_lock_failed chain=%s address=%s",
            option.chain,
            sender_lock.get("address"),
        )


def _send_native_transfer_with_sender_lock(
    *,
    client: MediaCMSInternalClient,
    config,
    w3,
    option,
    public_id: str,
    claim_token: str,
    funding_private_key: str,
    to_address: str,
    amount_wei: int,
    last_sweep_gas_limit: int | None = None,
) -> str:
    funding_address = address_from_private_key(funding_private_key)
    sender_lock = None
    backend_recorded = False

    try:
        sender_lock = _reserve_sender_nonce(
            client=client,
            config=config,
            option=option,
            w3=w3,
            address=funding_address,
        )
        tx = build_native_transfer_transaction(
            w3=w3,
            nonce=sender_lock["nonce"],
            funding_private_key=funding_private_key,
            to_address=to_address,
            amount_wei=amount_wei,
            gas_price_multiplier_bps=option.gas_price_multiplier_bps,
        )
        signed = sign_transaction(tx=tx, private_key=funding_private_key)
        txid = signed["txid"]

        client.mark_funding_broadcasted(
            public_id=public_id,
            claim_token=claim_token,
            gas_funding_txid=txid,
            destination_address=option.destination_address,
            last_sweep_gas_limit=last_sweep_gas_limit,
            sender_address=funding_address,
            nonce=sender_lock["nonce"],
            amount_wei=amount_wei,
        )
        backend_recorded = True

        send_signed_transaction(
            w3=w3,
            raw_transaction=signed["raw_transaction"],
            expected_txid=txid,
        )
        client.confirm_evm_sender_nonce_used(
            chain=option.chain,
            address=funding_address,
            lock_token=sender_lock["lock_token"],
            nonce=sender_lock["nonce"],
            txid=txid,
        )
        return txid
    except Exception:
        if not backend_recorded:
            _release_sender_lock_safely(
                client=client,
                option=option,
                sender_lock=sender_lock,
            )
        raise


def _send_erc20_transfer_with_sender_lock(
    *,
    client: MediaCMSInternalClient,
    config,
    w3,
    option,
    public_id: str,
    claim_token: str,
    token_contract_address: str,
    source_private_key: str,
    destination_address: str,
    amount: int,
    gas_limit: int,
) -> str:
    source_address = address_from_private_key(source_private_key)
    sender_lock = None
    backend_recorded = False

    try:
        sender_lock = _reserve_sender_nonce(
            client=client,
            config=config,
            option=option,
            w3=w3,
            address=source_address,
        )
        tx = build_erc20_transfer_transaction(
            w3=w3,
            nonce=sender_lock["nonce"],
            token_contract_address=token_contract_address,
            source_private_key=source_private_key,
            destination_address=destination_address,
            amount=amount,
            gas_limit=gas_limit,
            gas_price_multiplier_bps=option.gas_price_multiplier_bps,
        )
        signed = sign_transaction(tx=tx, private_key=source_private_key)
        txid = signed["txid"]

        client.mark_sweep_broadcasted(
            public_id=public_id,
            claim_token=claim_token,
            sweep_txid=txid,
            destination_address=option.destination_address,
            last_sweep_gas_limit=gas_limit,
            sender_address=source_address,
            nonce=sender_lock["nonce"],
            amount=amount,
        )
        backend_recorded = True

        send_signed_transaction(
            w3=w3,
            raw_transaction=signed["raw_transaction"],
            expected_txid=txid,
        )
        client.confirm_evm_sender_nonce_used(
            chain=option.chain,
            address=source_address,
            lock_token=sender_lock["lock_token"],
            nonce=sender_lock["nonce"],
            txid=txid,
        )
        return txid
    except Exception:
        if not backend_recorded:
            _release_sender_lock_safely(
                client=client,
                option=option,
                sender_lock=sender_lock,
            )
        raise


def _send_retry_funding_if_needed(
    *,
    client,
    config,
    w3,
    option,
    public_id: str,
    claim_token: str,
    source_address: str,
    extra_topup_needed: int,
) -> None:
    if extra_topup_needed <= 0:
        return

    if extra_topup_needed > int(option.max_gas_funding_amount_wei):
        _raise_structured_error(
            code="SWEEP_RETRY_FUNDING_CAP_EXCEEDED",
            message="Sweep retry funding exceeds configured cap",
            retryable=False,
            details={
                "job_public_id": public_id,
                "required_extra_topup_wei": int(extra_topup_needed),
                "cap_wei": int(option.max_gas_funding_amount_wei),
                "chain": option.chain,
                "asset_code": option.asset_code,
            },
        )

    gas_funding_txid = _send_native_transfer_with_sender_lock(
        client=client,
        config=config,
        w3=w3,
        option=option,
        public_id=public_id,
        claim_token=claim_token,
        funding_private_key=option.funding_private_key,
        to_address=source_address,
        amount_wei=extra_topup_needed,
    )
    wait_for_confirmations(
        w3=w3,
        txid=gas_funding_txid,
        required_confirmations=option.funding_confirmations,
        timeout_seconds=option.tx_timeout_seconds,
    )
    logging.info(
        "sweeper_service action=retry-funding-confirmed public_id=%s txid=%s amount=%s",
        public_id,
        gas_funding_txid,
        extra_topup_needed,
    )


def _run_single_sweep_attempt(
    *,
    client,
    config,
    w3,
    option,
    public_id: str,
    claim_token: str,
    source_private_key: str,
    amount: int,
    gas_limit: int,
) -> tuple[str, dict]:
    sweep_txid = _send_erc20_transfer_with_sender_lock(
        client=client,
        config=config,
        w3=w3,
        option=option,
        public_id=public_id,
        claim_token=claim_token,
        token_contract_address=option.token_contract_address,
        source_private_key=source_private_key,
        destination_address=option.destination_address,
        amount=amount,
        gas_limit=gas_limit,
    )
    wait_for_confirmations(
        w3=w3,
        txid=sweep_txid,
        required_confirmations=option.sweep_confirmations,
        timeout_seconds=option.tx_timeout_seconds,
    )
    receipt = _read_mined_receipt(w3=w3, txid=sweep_txid)
    return sweep_txid, receipt


def _finalize_sweep_with_single_retry(
    *,
    client,
    config,
    w3,
    option,
    public_id: str,
    claim_token: str,
    source_address: str,
    source_private_key: str,
    amount: int,
    initial_gas_limit: int,
) -> str:
    first_sweep_txid, first_receipt = _run_single_sweep_attempt(
        client=client,
        config=config,
        w3=w3,
        option=option,
        public_id=public_id,
        claim_token=claim_token,
        source_private_key=source_private_key,
        amount=amount,
        gas_limit=initial_gas_limit,
    )

    if int(first_receipt.get("status", 0) or 0) == 1:
        return first_sweep_txid

    first_gas_used = int(first_receipt.get("gasUsed", 0) or 0)
    if not _looks_like_out_of_gas(
        receipt=first_receipt,
        attempted_gas_limit=initial_gas_limit,
    ):
        raise RuntimeError(
            f"Sweep reverted without out-of-gas signature for job={public_id} "
            f"txid={first_sweep_txid} gas_used={first_gas_used} "
            f"attempted_gas_limit={initial_gas_limit}"
        )

    retry_gas_limit = _recommended_retry_gas_limit(
        option=option,
        attempted_gas_limit=initial_gas_limit,
        gas_used=first_gas_used,
    )
    required_native, extra_topup_needed = _compute_retry_budget(
        w3=w3,
        option=option,
        source_address=source_address,
        retry_gas_limit=retry_gas_limit,
    )

    logging.warning(
        "sweeper_service action=sweep-retry-planned public_id=%s first_txid=%s "
        "initial_gas_limit=%s gas_used=%s retry_gas_limit=%s required_native=%s extra_topup_needed=%s",
        public_id,
        first_sweep_txid,
        initial_gas_limit,
        first_gas_used,
        retry_gas_limit,
        required_native,
        extra_topup_needed,
    )

    _send_retry_funding_if_needed(
        client=client,
        config=config,
        w3=w3,
        option=option,
        public_id=public_id,
        claim_token=claim_token,
        source_address=source_address,
        extra_topup_needed=extra_topup_needed,
    )

    second_sweep_txid, second_receipt = _run_single_sweep_attempt(
        client=client,
        config=config,
        w3=w3,
        option=option,
        public_id=public_id,
        claim_token=claim_token,
        source_private_key=source_private_key,
        amount=amount,
        gas_limit=retry_gas_limit,
    )

    if int(second_receipt.get("status", 0) or 0) == 1:
        logging.info(
            "sweeper_service action=sweep-retry-succeeded public_id=%s first_txid=%s second_txid=%s retry_gas_limit=%s",
            public_id,
            first_sweep_txid,
            second_sweep_txid,
            retry_gas_limit,
        )
        return second_sweep_txid

    second_gas_used = int(second_receipt.get("gasUsed", 0) or 0)
    _raise_structured_error(
        code="SWEEP_GAS_RETRY_FAILED",
        message="Sweep retry failed after out-of-gas recovery attempt",
        retryable=False,
        details={
            "job_public_id": public_id,
            "first_sweep_txid": first_sweep_txid,
            "second_sweep_txid": second_sweep_txid,
            "initial_gas_limit": int(initial_gas_limit),
            "first_gas_used": int(first_gas_used),
            "retry_gas_limit": int(retry_gas_limit),
            "second_gas_used": int(second_gas_used),
            "extra_topup_needed": int(extra_topup_needed),
            "chain": option.chain,
            "asset_code": option.asset_code,
        },
    )


def _process_claimed_job(
    *,
    client: MediaCMSInternalClient,
    deriver: EvmDeriver,
    config,
    option,
    job: dict,
    w3,
) -> None:
    public_id = str(job["public_id"])
    claim_token = _claim_token_for_job(job)
    status = str(job.get("status", "")).strip().lower()
    source_address = str(job["source_address"]).strip().lower()
    amount = int(job["amount"])
    retry_delay_seconds = _default_retry_delay_seconds(config)
    explicit_gas_limit_override = None

    derivation_index = _resolve_derivation_index(job)
    source_private_key = deriver.derive_private_key(
        chain=option.chain,
        address_index=derivation_index,
    )

    if status == "sweep_broadcasted":
        sweep_txid = str(job.get("sweep_txid", "")).strip()
        if not sweep_txid:
            raise RuntimeError(f"Missing sweep_txid for job={public_id}")

        receipt, confirmations = get_receipt_with_confirmations(
            w3=w3,
            txid=sweep_txid,
        )
        if receipt is None:
            if not transaction_is_known(w3=w3, txid=sweep_txid) and _broadcast_missing_deadline_has_passed(
                job=job,
                key="sweep_broadcast",
                timeout_seconds=option.tx_timeout_seconds,
            ):
                client.mark_sweep_broadcast_missing(
                    public_id=public_id,
                    claim_token=claim_token,
                    sweep_txid=sweep_txid,
                    next_retry_in_seconds=retry_delay_seconds,
                    error="Sweep transaction was not found after broadcast timeout",
                )
                logging.warning(
                    "sweeper_service action=sweep-broadcast-missing public_id=%s txid=%s",
                    public_id,
                    sweep_txid,
                )
                return

            client.mark_rescheduled(
                public_id=public_id,
                claim_token=claim_token,
                next_retry_in_seconds=retry_delay_seconds,
            )
            logging.info(
                "sweeper_service action=sweep-awaiting-confirmations public_id=%s txid=%s confirmations=%s required=%s",
                public_id,
                sweep_txid,
                confirmations,
                option.sweep_confirmations,
            )
            return

        if confirmations < int(option.sweep_confirmations):
            client.mark_rescheduled(
                public_id=public_id,
                claim_token=claim_token,
                next_retry_in_seconds=retry_delay_seconds,
            )
            logging.info(
                "sweeper_service action=sweep-awaiting-confirmations public_id=%s txid=%s confirmations=%s required=%s",
                public_id,
                sweep_txid,
                confirmations,
                option.sweep_confirmations,
            )
            return

        if int(receipt.get("status", 0) or 0) == 1:
            client.mark_confirmed(public_id=public_id, claim_token=claim_token)
            logging.info(
                "sweeper_service action=confirmed-existing public_id=%s sweep_txid=%s confirmations=%s",
                public_id,
                sweep_txid,
                confirmations,
            )
            return

        attempted_gas_limit = int(
            job.get("last_sweep_gas_limit")
            or receipt.get("gasUsed")
            or option.erc20_transfer_gas_limit
        )

        if not _looks_like_out_of_gas(
            receipt=receipt,
            attempted_gas_limit=attempted_gas_limit,
        ):
            _raise_structured_error(
                code="SWEEP_TOKEN_REVERTED",
                message="Sweep reverted without out-of-gas signature",
                retryable=False,
                details={
                    "job_public_id": public_id,
                    "sweep_txid": sweep_txid,
                    "gas_used": int(receipt.get("gasUsed", 0) or 0),
                    "attempted_gas_limit": attempted_gas_limit,
                    "receipt_status": int(receipt.get("status", 0) or 0),
                },
            )

        retry_gas_limit = _recommended_retry_gas_limit(
            option=option,
            attempted_gas_limit=attempted_gas_limit,
            gas_used=int(receipt.get("gasUsed", 0) or 0),
        )
        required_native, extra_topup_needed = _compute_retry_budget(
            w3=w3,
            option=option,
            source_address=source_address,
            retry_gas_limit=retry_gas_limit,
        )

        logging.warning(
            "sweeper_service action=sweep-retry-planned public_id=%s sweep_txid=%s retry_gas_limit=%s required_native=%s extra_topup_needed=%s",
            public_id,
            sweep_txid,
            retry_gas_limit,
            required_native,
            extra_topup_needed,
        )

        if extra_topup_needed > 0:
            if extra_topup_needed > int(option.max_gas_funding_amount_wei):
                _raise_structured_error(
                    code="SWEEP_RETRY_FUNDING_CAP_EXCEEDED",
                    message="Sweep retry funding exceeds configured cap",
                    retryable=False,
                    details={
                        "job_public_id": public_id,
                        "required_extra_topup_wei": int(extra_topup_needed),
                        "cap_wei": int(option.max_gas_funding_amount_wei),
                        "chain": option.chain,
                        "asset_code": option.asset_code,
                    },
                )

            _ensure_funding_wallet_budget(
                w3=w3,
                option=option,
                amount_wei=extra_topup_needed,
                public_id=public_id,
            )

            gas_funding_txid = _send_native_transfer_with_sender_lock(
                client=client,
                config=config,
                w3=w3,
                option=option,
                public_id=public_id,
                claim_token=claim_token,
                funding_private_key=option.funding_private_key,
                to_address=source_address,
                amount_wei=extra_topup_needed,
                last_sweep_gas_limit=retry_gas_limit,
            )
            client.mark_rescheduled(
                public_id=public_id,
                claim_token=claim_token,
                next_retry_in_seconds=retry_delay_seconds,
            )
            logging.info(
                "sweeper_service action=retry-funding-broadcasted public_id=%s txid=%s amount=%s next_gas_limit=%s",
                public_id,
                gas_funding_txid,
                extra_topup_needed,
                retry_gas_limit,
            )
            return

        client.mark_ready_to_sweep(public_id=public_id, claim_token=claim_token)
        status = "ready_to_sweep"
        explicit_gas_limit_override = retry_gas_limit

    if status == "funding_broadcasted":
        gas_funding_txid = str(job.get("gas_funding_txid", "")).strip()
        if not gas_funding_txid:
            raise RuntimeError(f"Missing gas_funding_txid for job={public_id}")

        receipt, confirmations = get_receipt_with_confirmations(
            w3=w3,
            txid=gas_funding_txid,
        )
        if receipt is None:
            if not transaction_is_known(w3=w3, txid=gas_funding_txid) and _broadcast_missing_deadline_has_passed(
                job=job,
                key="gas_funding_broadcast",
                timeout_seconds=option.tx_timeout_seconds,
            ):
                client.mark_funding_broadcast_missing(
                    public_id=public_id,
                    claim_token=claim_token,
                    gas_funding_txid=gas_funding_txid,
                    next_retry_in_seconds=retry_delay_seconds,
                    error="Gas funding transaction was not found after broadcast timeout",
                )
                logging.warning(
                    "sweeper_service action=funding-broadcast-missing public_id=%s txid=%s",
                    public_id,
                    gas_funding_txid,
                )
                return

            client.mark_rescheduled(
                public_id=public_id,
                claim_token=claim_token,
                next_retry_in_seconds=retry_delay_seconds,
            )
            logging.info(
                "sweeper_service action=funding-awaiting-confirmations public_id=%s txid=%s confirmations=%s required=%s",
                public_id,
                gas_funding_txid,
                confirmations,
                option.funding_confirmations,
            )
            return

        if confirmations < int(option.funding_confirmations):
            client.mark_rescheduled(
                public_id=public_id,
                claim_token=claim_token,
                next_retry_in_seconds=retry_delay_seconds,
            )
            logging.info(
                "sweeper_service action=funding-awaiting-confirmations public_id=%s txid=%s confirmations=%s required=%s",
                public_id,
                gas_funding_txid,
                confirmations,
                option.funding_confirmations,
            )
            return

        if int(receipt.get("status", 0) or 0) != 1:
            _raise_structured_error(
                code="SWEEP_GAS_FUNDING_REVERTED",
                message="Gas funding transaction reverted",
                retryable=False,
                details={
                    "job_public_id": public_id,
                    "gas_funding_txid": gas_funding_txid,
                    "receipt_status": int(receipt.get("status", 0) or 0),
                },
            )

    elif status not in {"pending", "ready_to_sweep"}:
        raise RuntimeError(f"Unsupported claimed job status for job={public_id}: {status}")

    source_native_balance = int(get_native_balance(w3=w3, address=source_address))
    estimated_gas_limit, estimated_required_native = _compute_required_native_wei(
        w3=w3,
        option=option,
        source_address=source_address,
        amount=amount,
    )

    chosen_gas_limit = int(
        explicit_gas_limit_override
        or job.get("last_sweep_gas_limit")
        or estimated_gas_limit
    )
    if chosen_gas_limit == int(estimated_gas_limit):
        required_native = int(estimated_required_native)
    else:
        required_native = _compute_required_native_for_gas_limit(
            w3=w3,
            option=option,
            gas_limit=chosen_gas_limit,
        )

    topup_needed = max(0, required_native - source_native_balance)

    logging.info(
        "sweeper_service action=native-budget public_id=%s source=%s native_balance=%s required_native=%s topup_needed=%s gas_limit=%s",
        public_id,
        source_address,
        source_native_balance,
        required_native,
        topup_needed,
        chosen_gas_limit,
    )

    if topup_needed > 0:
        if topup_needed > int(option.max_gas_funding_amount_wei):
            _raise_structured_error(
                code="SWEEP_FUNDING_CAP_EXCEEDED",
                message="Required gas funding exceeds configured cap",
                retryable=False,
                details={
                    "job_public_id": public_id,
                    "required_topup_wei": int(topup_needed),
                    "cap_wei": int(option.max_gas_funding_amount_wei),
                    "chain": option.chain,
                    "asset_code": option.asset_code,
                },
            )

        _ensure_funding_wallet_budget(
            w3=w3,
            option=option,
            amount_wei=topup_needed,
            public_id=public_id,
        )

        gas_funding_txid = _send_native_transfer_with_sender_lock(
            client=client,
            config=config,
            w3=w3,
            option=option,
            public_id=public_id,
            claim_token=claim_token,
            funding_private_key=option.funding_private_key,
            to_address=source_address,
            amount_wei=topup_needed,
            last_sweep_gas_limit=chosen_gas_limit,
        )
        client.mark_rescheduled(
            public_id=public_id,
            claim_token=claim_token,
            next_retry_in_seconds=retry_delay_seconds,
        )
        logging.info(
            "sweeper_service action=funding-broadcasted public_id=%s txid=%s topup_needed=%s next_gas_limit=%s",
            public_id,
            gas_funding_txid,
            topup_needed,
            chosen_gas_limit,
        )
        return

    token_balance = int(
        get_erc20_balance(
            w3=w3,
            token_contract_address=option.token_contract_address,
            owner_address=source_address,
        )
    )
    if token_balance < amount:
        _raise_structured_error(
            code="SWEEP_TOKEN_BALANCE_NOT_READY",
            message="Source wallet token balance is lower than the expected sweep amount",
            retryable=True,
            details={
                "job_public_id": public_id,
                "required_amount": int(amount),
                "actual_amount": int(token_balance),
                "chain": option.chain,
                "asset_code": option.asset_code,
            },
        )

    client.mark_ready_to_sweep(public_id=public_id, claim_token=claim_token)

    sweep_txid = _send_erc20_transfer_with_sender_lock(
        client=client,
        config=config,
        w3=w3,
        option=option,
        public_id=public_id,
        claim_token=claim_token,
        token_contract_address=option.token_contract_address,
        source_private_key=source_private_key,
        destination_address=option.destination_address,
        amount=amount,
        gas_limit=chosen_gas_limit,
    )
    client.mark_rescheduled(
        public_id=public_id,
        claim_token=claim_token,
        next_retry_in_seconds=retry_delay_seconds,
    )

    logging.info(
        "sweeper_service action=sweep-broadcasted public_id=%s chain=%s asset=%s amount=%s sweep_txid=%s gas_limit=%s",
        public_id,
        option.chain,
        option.asset_code,
        amount,
        sweep_txid,
        chosen_gas_limit,
    )

def _prevalidate_claimed_job(*, deriver: EvmDeriver, option, job: dict) -> None:
    public_id = str(job["public_id"])
    source_address = str(job["source_address"]).strip().lower()
    derivation_index = _resolve_derivation_index(job)

    derived_address = deriver.derive_address(
        chain=option.chain,
        address_index=derivation_index,
    )
    if derived_address != source_address:
        raise RuntimeError(
            "Derived address mismatch for "
            f"job={public_id}: derived={derived_address} source={source_address}"
        )

    source_private_key = deriver.derive_private_key(
        chain=option.chain,
        address_index=derivation_index,
    )
    if address_from_private_key(source_private_key) != source_address:
        raise RuntimeError(f"Derived private key mismatch for job={public_id}")

    if option.destination_address == source_address:
        raise RuntimeError(
            f"Refusing to sweep to the same address for job={public_id}"
        )

def run_once(*, client: MediaCMSInternalClient, config) -> None:
    options = [_build_option_selector(option) for option in config.options]
    jobs = client.claim_jobs(
        options=options,
        limit=config.claim_batch_size,
    )
    if not jobs:
        logging.info("sweeper_service action=noop claimed=0")
        return

    option_index = _build_option_index(config.options)
    deriver = EvmDeriver(
        mnemonic=config.mnemonic,
        passphrase=config.mnemonic_passphrase,
        account_index=config.account_index,
    )
    web3_by_option_key = {}

    for job in jobs:
        public_id = str(job["public_id"])
        claim_token = str(job.get("claim_token", "")).strip()
        try:
            option = _find_option_for_job(option_index=option_index, job=job)

            _prevalidate_claimed_job(
                deriver=deriver,
                option=option,
                job=job,
            )

            option_cache_key = option.key
            w3 = web3_by_option_key.get(option_cache_key)
            if w3 is None:
                w3 = _build_option_web3(config=config, option=option)
                web3_by_option_key[option_cache_key] = w3

            _process_claimed_job(
                client=client,
                deriver=deriver,
                config=config,
                option=option,
                job=job,
                w3=w3,
            )
        except Exception as exc:
            structured_error = _extract_structured_error(exc)

            if structured_error and bool(structured_error.get("retryable", False)):
                error_message = _truncate_error(str(structured_error.get("message") or str(exc)))
                error_code = str(structured_error.get("code") or "").strip()
                try:
                    client.mark_rescheduled(
                        public_id=public_id,
                        claim_token=claim_token,
                        next_retry_in_seconds=_default_retry_delay_seconds(config),
                        error=error_message,
                        error_code=error_code,
                        retryable=True,
                        increment_retry_count=True,
                    )
                except Exception:
                    logging.exception(
                        "sweeper_service action=mark_rescheduled_error public_id=%s",
                        public_id,
                    )

                logging.warning(
                    "sweeper_service action=job_rescheduled public_id=%s error_code=%s error=%s",
                    public_id,
                    error_code,
                    error_message,
                )
                continue

            error_message = _truncate_error(str(exc))
            try:
                client.mark_failed(public_id=public_id, claim_token=claim_token, error=error_message)
            except Exception:
                logging.exception(
                    "sweeper_service action=mark_failed_error public_id=%s",
                    public_id,
                )
            logging.exception(
                "sweeper_service action=job_failed public_id=%s",
                public_id,
            )


def main() -> None:
    config = load_config()
    client = MediaCMSInternalClient(
        base_url=config.mediacms_base_url,
        service_name=config.service_name,
        shared_secret=config.shared_secret,
    )
    try:
        while True:
            try:
                run_once(client=client, config=config)
            except Exception:
                logging.exception("sweeper_service cycle failed")
            time.sleep(config.poll_interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()