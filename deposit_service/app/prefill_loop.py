import logging
import time

from .client import MediaCMSInternalClient
from .config import load_config
from .derivation import derive_evm_address
from .state import StateStore


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
        "metadata": {
            "provisioned_by": "deposit-service",
            "option_key": option.key,
            "chain_family": "evm",
        },
    }


def run_once(*, client: MediaCMSInternalClient, state: StateStore, options) -> None:
    for option in options:
        stats = client.get_pool_stats([_build_option_selector(option)])[0]
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

        next_index = state.get_next_index(option.key, option.start_index)
        rows = []

        for _ in range(needed):
            address = derive_evm_address(
                chain=option.chain,
                account_xpub=option.account_xpub,
                address_index=next_index,
            )
            rows.append(_build_address_row(option, address, next_index))
            next_index += 1

        result = client.provision_addresses(rows)
        state.set_next_index(option.key, next_index)

        logging.info(
            "option=%s chain=%s asset=%s requested=%s created=%s existing=%s next_index=%s",
            option.key,
            option.chain,
            option.asset_code,
            needed,
            result.get("created_count"),
            result.get("existing_count"),
            next_index,
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
                run_once(client=client, state=state, options=config.options)
            except Exception:
                logging.exception("deposit_service cycle failed")
            time.sleep(config.poll_interval_seconds)
    finally:
        client.close()


if __name__ == "__main__":
    main()