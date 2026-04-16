import logging
import time

from .client import MediaCMSInternalClient
from .config import load_config
from .derivation import EvmDeriver
from .evm import (
    NonceAllocator,
    address_from_private_key,
    build_web3,
    get_erc20_balance,
    get_native_balance,
    send_erc20_transfer,
    send_native_transfer,
    wait_for_confirmations,
)
from .reference_head import get_reference_head
from .rpc_pool import choose_best_rpc_url
from web3 import Web3

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
    return normalized[: max_length - 3] + "..."


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
        return int(option.erc20_transfer_gas_limit)

    gas_limit = (int(estimated_gas) * int(option.gas_limit_multiplier_bps) + 9999) // 10000
    if gas_limit < int(estimated_gas):
        gas_limit = int(estimated_gas)
    return gas_limit


def _compute_effective_gas_price_wei(*, w3, option) -> int:
    network_gas_price = int(w3.eth.gas_price)
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

def _process_claimed_job(
    *,
    client: MediaCMSInternalClient,
    deriver: EvmDeriver,
    nonce_allocator: NonceAllocator,
    config,
    option,
    job: dict,
    w3,
) -> None:
    public_id = str(job["public_id"])
    status = str(job.get("status", "")).strip().lower()
    source_address = str(job["source_address"]).strip().lower()
    amount = int(job["amount"])
    derivation_index = _resolve_derivation_index(job)
    source_private_key = deriver.derive_private_key(
        chain=option.chain,
        address_index=derivation_index,
    )


    if status == "sweep_broadcasted":
        sweep_txid = str(job.get("sweep_txid", "")).strip()
        if not sweep_txid:
            raise RuntimeError(f"Missing sweep_txid for job={public_id}")
        wait_for_confirmations(
            w3=w3,
            txid=sweep_txid,
            required_confirmations=option.sweep_confirmations,
            timeout_seconds=option.tx_timeout_seconds,
        )
        client.mark_confirmed(public_id=public_id)
        logging.info(
            "sweeper_service action=confirmed-existing public_id=%s sweep_txid=%s",
            public_id,
            sweep_txid,
        )
        return

    if status == "funding_broadcasted":
        gas_funding_txid = str(job.get("gas_funding_txid", "")).strip()
        if not gas_funding_txid:
            raise RuntimeError(f"Missing gas_funding_txid for job={public_id}")
        wait_for_confirmations(
            w3=w3,
            txid=gas_funding_txid,
            required_confirmations=option.funding_confirmations,
            timeout_seconds=option.tx_timeout_seconds,
        )
    elif status not in {"pending", "ready_to_sweep"}:
        raise RuntimeError(f"Unsupported claimed job status for job={public_id}: {status}")

    source_native_balance = get_native_balance(w3=w3, address=source_address)
    estimated_gas_limit, required_native = _compute_required_native_wei(
        w3=w3,
        option=option,
        source_address=source_address,
        amount=amount,
    )
    topup_needed = max(0, required_native - int(source_native_balance))

    logging.info(
        "sweeper_service action=native-budget public_id=%s source=%s native_balance=%s required_native=%s topup_needed=%s estimated_gas_limit=%s",
        public_id,
        source_address,
        source_native_balance,
        required_native,
        topup_needed,
        estimated_gas_limit,
    )

    if topup_needed > 0 and status != "ready_to_sweep":
        if topup_needed > int(option.max_gas_funding_amount_wei):
            raise RuntimeError(
                f"Required gas funding exceeds configured cap for job={public_id}: "
                f"required={topup_needed} cap={option.max_gas_funding_amount_wei}"
            )

        gas_funding_txid = send_native_transfer(
            chain=option.chain,
            w3=w3,
            nonce_allocator=nonce_allocator,
            funding_private_key=option.funding_private_key,
            to_address=source_address,
            amount_wei=topup_needed,
            gas_price_multiplier_bps=option.gas_price_multiplier_bps,
        )
        client.mark_funding_broadcasted(
            public_id=public_id,
            gas_funding_txid=gas_funding_txid,
            destination_address=option.destination_address,
        )
        wait_for_confirmations(
            w3=w3,
            txid=gas_funding_txid,
            required_confirmations=option.funding_confirmations,
            timeout_seconds=option.tx_timeout_seconds,
        )
        logging.info(
            "sweeper_service action=funding-confirmed public_id=%s txid=%s topup_needed=%s",
            public_id,
            gas_funding_txid,
            topup_needed,
        )

    client.mark_ready_to_sweep(public_id=public_id)

    token_balance = get_erc20_balance(
        w3=w3,
        token_contract_address=option.token_contract_address,
        owner_address=source_address,
    )
    if token_balance < amount:
        raise RuntimeError(
            f"Insufficient token balance for job={public_id}: "
            f"required={amount} actual={token_balance}"
        )

    sweep_txid = send_erc20_transfer(
        chain=option.chain,
        w3=w3,
        nonce_allocator=nonce_allocator,
        token_contract_address=option.token_contract_address,
        source_private_key=source_private_key,
        destination_address=option.destination_address,
        amount=amount,
        gas_limit=estimated_gas_limit,
        gas_price_multiplier_bps=option.gas_price_multiplier_bps,
    )
    client.mark_sweep_broadcasted(
        public_id=public_id,
        sweep_txid=sweep_txid,
        destination_address=option.destination_address,
    )

    wait_for_confirmations(
        w3=w3,
        txid=sweep_txid,
        required_confirmations=option.sweep_confirmations,
        timeout_seconds=option.tx_timeout_seconds,
    )
    client.mark_confirmed(public_id=public_id)

    logging.info(
        "sweeper_service action=confirmed public_id=%s chain=%s asset=%s amount=%s sweep_txid=%s",
        public_id,
        option.chain,
        option.asset_code,
        amount,
        sweep_txid,
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
    nonce_allocator = NonceAllocator()
    web3_by_option_key = {}

    for job in jobs:
        public_id = str(job["public_id"])
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
                nonce_allocator=nonce_allocator,
                config=config,
                option=option,
                job=job,
                w3=w3,
            )
        except Exception as exc:
            error_message = _truncate_error(str(exc))
            try:
                client.mark_failed(public_id=public_id, error=error_message)
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