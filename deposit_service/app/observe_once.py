import logging

from web3 import Web3

from .erc20_logs import TRANSFER_TOPIC0, address_to_topic, decode_address_from_topic
from .evm_rpc import build_web3


ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def _build_option_selector(option):
    return {
        "chain": option.chain,
        "asset_code": option.asset_code,
        "token_contract_address": option.token_contract_address,
    }


def _resolve_lookback_blocks(option) -> int:
    value = int(getattr(option, "tx_lookup_lookback_blocks", 5000) or 5000)
    if value <= 0:
        raise RuntimeError(f"tx_lookup_lookback_blocks must be > 0 for option {option.key}")
    return value


def _find_latest_incoming_transfer(*, w3, contract_address, deposit_address, latest_block, lookback_blocks):
    target_topic = address_to_topic(deposit_address)
    from_block = max(0, int(latest_block) - int(lookback_blocks))

    logs = w3.eth.get_logs(
        {
            "fromBlock": from_block,
            "toBlock": latest_block,
            "address": contract_address,
            "topics": [
                TRANSFER_TOPIC0,
                None,
                [target_topic],
            ],
        }
    )

    if not logs:
        return None

    logs = sorted(
        logs,
        key=lambda item: (int(item["blockNumber"]), int(item["transactionIndex"]), int(item["logIndex"])),
        reverse=True,
    )
    return logs[0]


def _observe_option(*, client, option, watch):
    targets = watch["targets"]
    if not targets:
        logging.info(
            "watch option=%s chain=%s asset=%s targets=0 action=skip",
            option.key,
            option.chain,
            option.asset_code,
        )
        return

    w3 = build_web3(rpc_url=option.rpc_url, poa_compatible=option.poa_compatible)
    latest_block = int(w3.eth.block_number)
    contract_address = Web3.to_checksum_address(option.token_contract_address)
    contract = w3.eth.contract(address=contract_address, abi=ERC20_ABI)
    lookback_blocks = _resolve_lookback_blocks(option)

    seen_events = set()

    for target in targets:
        deposit_address = target["deposit_address"]
        session_public_id = target["session_public_id"]

        try:
            checksum_deposit_address = Web3.to_checksum_address(deposit_address)
        except Exception:
            logging.exception(
                "invalid deposit address option=%s session=%s address=%s",
                option.key,
                session_public_id,
                deposit_address,
            )
            continue

        try:
            current_balance = int(contract.functions.balanceOf(checksum_deposit_address).call())
        except Exception:
            logging.exception(
                "balance lookup failed option=%s session=%s address=%s",
                option.key,
                session_public_id,
                deposit_address,
            )
            continue

        if current_balance <= 0:
            continue

        latest_incoming = _find_latest_incoming_transfer(
            w3=w3,
            contract_address=contract_address,
            deposit_address=deposit_address,
            latest_block=latest_block,
            lookback_blocks=lookback_blocks,
        )

        if latest_incoming is None:
            logging.warning(
                "positive balance but no recent incoming transfer found option=%s session=%s address=%s balance=%s latest_block=%s",
                option.key,
                session_public_id,
                deposit_address,
                current_balance,
                latest_block,
            )
            continue

        txid = latest_incoming["transactionHash"].hex()
        log_index = int(latest_incoming["logIndex"])
        event_key = f"{option.chain}:{txid}:{log_index}"

        if event_key in seen_events:
            continue
        seen_events.add(event_key)

        block_number = int(latest_incoming["blockNumber"])
        confirmations = int(latest_block - block_number + 1)

        payload = {
            "session_public_id": session_public_id,
            "chain": option.chain,
            "txid": txid,
            "log_index": log_index,
            "block_number": block_number,
            "from_address": decode_address_from_topic(latest_incoming["topics"][1].hex()),
            "deposit_address": deposit_address,
            "token_contract_address": option.token_contract_address,
            "asset_code": option.asset_code,
            "amount": current_balance,
            "confirmations": confirmations,
            "raw_payload": {
                "address": latest_incoming["address"],
                "topics": [topic.hex() for topic in latest_incoming["topics"]],
                "data": latest_incoming["data"].hex(),
                "block_hash": latest_incoming["blockHash"].hex(),
                "transaction_hash": txid,
                "transaction_index": int(latest_incoming["transactionIndex"]),
                "log_index": log_index,
                "removed": bool(getattr(latest_incoming, "removed", False)),
                "balance_snapshot": str(current_balance),
            },
        }

        client.post_signed("/api/internal/ledger/deposit-observations", payload)

        logging.info(
            "deposit observed option=%s session=%s address=%s txid=%s confirmations=%s balance=%s",
            option.key,
            session_public_id,
            deposit_address,
            txid,
            confirmations,
            current_balance,
        )


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
        try:
            _observe_option(
                client=client,
                option=option,
                watch=watch,
            )
        except Exception:
            logging.exception(
                "watch option failed option=%s chain=%s asset=%s",
                option.key,
                option.chain,
                option.asset_code,
            )