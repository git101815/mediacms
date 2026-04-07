import logging
import time

from .client import MediaCMSInternalClient
from .config import load_config
from .derivation import derive_evm_address
from .state import StateStore
from .observe_once import observe_once

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _build_option_selector(option):
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
    }

def _build_address_row(option, address: str, index: int) -> dict:
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
        "display_label": option.display_label,
        "address": address,
        "address_derivation_ref": f"evm:{option.chain}:external:{index}",
        "required_confirmations": option.required_confirmations,
        "min_amount": option.min_amount,
        "session_ttl_seconds": option.session_ttl_seconds,
        "derivation_index": index,
        "metadata": {
            "provisioned_by": "deposit-service",
            "option_key": option.key,
            "chain_family": "evm",
        },
    }

def _resolve_next_index(*, option, state: StateStore, stats: dict) -> int:
    state_next_index = state.get_next_index(option.key, option.start_index)
    server_next_index = int(stats.get("next_derivation_index", option.start_index))
    return max(option.start_index, state_next_index, server_next_index)


def _iter_chunks(total_count: int, chunk_size: int):
    remaining = int(total_count)
    while remaining > 0:
        current = min(remaining, chunk_size)
        yield current
        remaining -= current

def run_once(*, client: MediaCMSInternalClient, state: StateStore, options, config) -> None:
    if not options:
        return

    selectors = [_build_option_selector(option) for option in options]
    stats_results = client.get_pool_stats(selectors)

    if len(stats_results) != len(options):
        raise RuntimeError(
            f"Unexpected stats result count: expected {len(options)}, got {len(stats_results)}"
        )

    for option, stats in zip(options, stats_results):
        available_count = int(stats["available_count"])
        needed = max(0, option.target_available - available_count)

        if needed <= 0:
            logging.info(
                "option=%s chain=%s asset=%s available=%s target=%s action=skip",
                option.key,
                option.chain,
                option.asset_code,
                available_count,
                option.target_available,
            )
            continue

        next_index = _resolve_next_index(option=option, state=state, stats=stats)

        if next_index > state.get_next_index(option.key, option.start_index):
            logging.warning(
                "option=%s action=resync old_state_next_index=%s server_next_index=%s",
                option.key,
                state.get_next_index(option.key, option.start_index),
                next_index,
            )

        total_created = 0
        total_existing = 0

        for current_batch_size in _iter_chunks(needed, config.provision_batch_size):
            rows = []
            for _ in range(current_batch_size):
                address = derive_evm_address(
                    chain=option.chain,
                    account_xpub=option.account_xpub,
                    address_index=next_index,
                )
                rows.append(_build_address_row(option, address, next_index))
                next_index += 1

            result = client.provision_addresses(rows)
            total_created += int(result.get("created_count", 0))
            total_existing += int(result.get("existing_count", 0))
            state.set_next_index(option.key, next_index)

        logging.info(
            "option=%s chain=%s asset=%s requested=%s created=%s existing=%s next_index=%s batch_size=%s",
            option.key,
            option.chain,
            option.asset_code,
            needed,
            total_created,
            total_existing,
            next_index,
            config.provision_batch_size,
        )

def main() -> None:
    config = load_config()
    state = StateStore(config.state_path)
    client = MediaCMSInternalClient(
        base_url=config.mediacms_base_url,
        service_name=config.service_name,
        shared_secret=config.shared_secret,
    )

    try:
        while True:
            try:
                run_once(client=client, state=state, options=config.options, config=config)
                observe_once(client=client, state=state, options=config.options)
            except Exception:
                logging.exception("deposit_service cycle failed")
            time.sleep(config.poll_interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()