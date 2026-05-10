import logging
import os
import time

from .client import MediaCMSInternalClient
from .config import load_config
from .derivation import derive_evm_address
from .observe_once import observe_once


def _build_option_selector(option):
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
    }


def _build_derivation_ref(*, address_index: int) -> str:
    return f"m/44'/60'/0'/0/{int(address_index)}"


def _chunk_rows(rows: list[dict], size: int):
    for index in range(0, len(rows), size):
        yield rows[index:index + size]


def _get_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _get_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def provision_deposit_addresses_once(*, client, options, batch_size: int) -> dict:
    selectors = [_build_option_selector(option) for option in options]
    stats_rows = client.get_deposit_address_stats(selectors)

    if len(stats_rows) != len(options):
        raise RuntimeError(
            f"Unexpected deposit address stats count: expected {len(options)}, got {len(stats_rows)}"
        )

    global_cursor = 0
    for stats in stats_rows:
        global_cursor = max(
            global_cursor,
            _get_int(stats.get("next_derivation_index"), 0),
        )

    address_rows = []

    for option, stats in zip(options, stats_rows):
        route_next_index = _get_int(stats.get("next_derivation_index"), 0)
        provisioned_address_count = _get_int(stats.get("provisioned_address_count"), 0)

        if route_next_index > 0 or provisioned_address_count > 0:
            continue

        start_index = global_cursor
        end_index = start_index + int(batch_size)
        global_cursor = end_index

        for address_index in range(start_index, end_index):
            address = derive_evm_address(
                chain=option.chain,
                account_xpub=option.account_xpub,
                address_index=address_index,
            )

            address_rows.append(
                {
                    "chain": option.chain,
                    "asset_code": option.asset_code,
                    "token_contract_address": option.token_contract_address,
                    "display_label": option.display_label,
                    "address": address,
                    "address_derivation_ref": _build_derivation_ref(address_index=address_index),
                    "required_confirmations": option.required_confirmations,
                    "min_amount": option.min_amount,
                    "session_ttl_seconds": option.session_ttl_seconds,
                    "metadata": {
                        "provisioned_by": "deposit-service",
                        "option_key": option.key,
                    },
                    "derivation_index": address_index,
                }
            )

    if not address_rows:
        return {"created_count": 0, "existing_count": 0, "rows": []}

    request_batch_size = _get_int(
        os.environ.get("DEPOSIT_SERVICE_PROVISION_REQUEST_BATCH_SIZE"),
        50,
    )
    if request_batch_size <= 0:
        raise RuntimeError("DEPOSIT_SERVICE_PROVISION_REQUEST_BATCH_SIZE must be > 0")

    created_count = 0
    existing_count = 0
    rows = []

    for chunk in _chunk_rows(address_rows, request_batch_size):
        result = client.provision_deposit_addresses(chunk)
        created_count += _get_int(result.get("created_count"), 0)
        existing_count += _get_int(result.get("existing_count"), 0)
        rows.extend(result.get("rows") or [])

    return {
        "created_count": created_count,
        "existing_count": existing_count,
        "rows": rows,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    while True:
        client = None
        try:
            config = load_config()
            client = MediaCMSInternalClient(
                base_url=config.mediacms_base_url,
                service_name=config.service_name,
                shared_secret=config.shared_secret,
                timeout=_get_float(os.environ.get("DEPOSIT_SERVICE_INTERNAL_API_TIMEOUT_SECONDS"), 30.0),
            )

            provision_result = provision_deposit_addresses_once(
                client=client,
                options=config.options,
                batch_size=config.provision_batch_size,
            )
            logging.info(
                "deposit address provision created=%s existing=%s rows=%s",
                provision_result.get("created_count", 0),
                provision_result.get("existing_count", 0),
                len(provision_result.get("rows", [])),
            )

            observe_once(
                client=client,
                options=config.options,
                reference_heads_base_url=config.reference_heads_base_url,
                reference_heads_shared_secret=config.reference_heads_shared_secret,
                reference_heads_timeout_seconds=config.reference_heads_timeout_seconds,
                reference_heads_max_age_seconds=config.reference_heads_max_age_seconds,
                rpc_max_lag_blocks=config.rpc_max_lag_blocks,
                rpc_max_reference_lag_blocks=config.rpc_max_reference_lag_blocks,
            )
        except Exception:
            logging.exception("deposit_service cycle failed")
        finally:
            if client is not None:
                client.close()

        time.sleep(30)


if __name__ == "__main__":
    main()
