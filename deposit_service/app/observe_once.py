import logging

from .erc20_logs import TRANSFER_TOPIC0, address_to_topic, decode_address_from_topic, decode_uint256
from .evm_rpc import build_web3


def _build_option_selector(option):
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
    }


def observe_once(*, client, state, options):
    if not options:
        return

    selectors = [_build_option_selector(option) for option in options]
    watchlist_rows = client.get_watchlist(selectors)

    if len(watchlist_rows) != len(options):
        raise RuntimeError(
            f"Unexpected watchlist result count: expected {len(options)}, got {len(watchlist_rows)}"
        )

    for option, watch in zip(options, watchlist_rows):
        targets = watch["targets"]
        if not targets:
            continue

        by_topic = {
            address_to_topic(item["deposit_address"]): item
            for item in targets
        }

        w3 = build_web3(rpc_url=option.rpc_url, poa_compatible=option.poa_compatible)
        latest_block = w3.eth.block_number

        from_block = max(
            option.start_block,
            state.get_scan_cursor(option.key, option.start_block) - option.reorg_backtrack_blocks,
        )
        to_block_limit = latest_block

        while from_block <= to_block_limit:
            to_block = min(from_block + option.scan_chunk_size - 1, to_block_limit)
            logs = w3.eth.get_logs(
                {
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "address": option.token_contract_address,
                    "topics": [
                        TRANSFER_TOPIC0,
                        None,
                        list(by_topic.keys()),
                    ],
                }
            )

            for log in logs:
                to_topic = log["topics"][2].hex()
                target = by_topic.get(to_topic)
                if target is None:
                    continue

                confirmations = int(latest_block - int(log["blockNumber"]) + 1)
                payload = {
                    "session_public_id": target["session_public_id"],
                    "chain": option.chain,
                    "txid": log["transactionHash"].hex(),
                    "log_index": int(log["logIndex"]),
                    "block_number": int(log["blockNumber"]),
                    "from_address": decode_address_from_topic(log["topics"][1].hex()),
                    "deposit_address": target["deposit_address"],
                    "token_contract_address": option.token_contract_address,
                    "asset_code": option.asset_code,
                    "amount": decode_uint256(log["data"].hex()),
                    "confirmations": confirmations,
                    "raw_payload": {
                        "address": log["address"],
                        "topics": [topic.hex() for topic in log["topics"]],
                        "data": log["data"].hex(),
                        "block_hash": log["blockHash"].hex(),
                        "transaction_hash": log["transactionHash"].hex(),
                        "transaction_index": int(log["transactionIndex"]),
                        "log_index": int(log["logIndex"]),
                        "removed": bool(getattr(log, "removed", False)),
                    },
                }
                client.post_signed("/api/internal/ledger/deposit-observations", payload)

            state.set_scan_cursor(option.key, to_block + 1)
            from_block = to_block + 1

        logging.info(
            "watch option=%s chain=%s asset=%s latest_block=%s",
            option.key,
            option.chain,
            option.asset_code,
            latest_block,
        )