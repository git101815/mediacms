import logging

from web3 import Web3

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


def _is_native_option(option) -> bool:
    return not str(option.token_contract_address or "").strip()


def _transfer_topic() -> str:
    return "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()


def _address_topic(address: str) -> str:
    checksum = Web3.to_checksum_address(address)
    return "0x" + ("0" * 24) + checksum[2:].lower()


def _decode_topic_address(topic_hex: str) -> str:
    return Web3.to_checksum_address("0x" + topic_hex[-40:])


def _find_latest_incoming_erc20_transfer(*, w3, contract_address, deposit_address, latest_block, lookback_blocks):
    from_block = max(0, int(latest_block) - int(lookback_blocks))
    logs = w3.eth.get_logs(
        {
            "fromBlock": from_block,
            "toBlock": latest_block,
            "address": Web3.to_checksum_address(contract_address),
            "topics": [
                _transfer_topic(),
                None,
                [_address_topic(deposit_address)],
            ],
        }
    )

    if not logs:
        return None

    logs = sorted(
        logs,
        key=lambda item: (
            int(item["blockNumber"]),
            int(item["transactionIndex"]),
            int(item["logIndex"]),
        ),
        reverse=True,
    )
    return logs[0]


def _find_latest_incoming_native_transfer(*, w3, deposit_address, latest_block, lookback_blocks):
    checksum_deposit_address = Web3.to_checksum_address(deposit_address)
    from_block = max(0, int(latest_block) - int(lookback_blocks))

    for block_number in range(int(latest_block), from_block - 1, -1):
        block = w3.eth.get_block(block_number, full_transactions=True)
        transactions = block.get("transactions", [])
        for tx in reversed(transactions):
            to_address = tx.get("to")
            value = int(tx.get("value", 0))
            if not to_address:
                continue
            if Web3.to_checksum_address(to_address) != checksum_deposit_address:
                continue
            if value <= 0:
                continue
            return {
                "txid": tx["hash"].hex(),
                "block_number": int(block_number),
                "from_address": Web3.to_checksum_address(tx["from"]),
                "to_address": checksum_deposit_address,
                "amount": value,
                "raw_payload": {
                    "transaction_hash": tx["hash"].hex(),
                    "block_number": int(block_number),
                    "transaction_index": int(tx.get("transactionIndex", 0)),
                    "value": str(value),
                },
            }

    return None


def _post_observation(
    *,
    client,
    option,
    session_public_id,
    deposit_address,
    txid,
    block_number,
    from_address,
    amount,
    confirmations,
    required_confirmations,
    raw_payload,
    log_index=None,
):
    payload = {
        "session_public_id": session_public_id,
        "chain": option.chain,
        "txid": txid,
        "log_index": 0 if log_index is None else int(log_index),
        "block_number": int(block_number),
        "from_address": from_address,
        "deposit_address": deposit_address,
        "token_contract_address": option.token_contract_address,
        "asset_code": option.asset_code,
        "amount": int(amount),
        "confirmations": int(confirmations),
        "required_confirmations": int(required_confirmations),
        "raw_payload": raw_payload,
    }

    client.post_signed("/api/internal/ledger/deposit-observations", payload)


def _observe_token_option(*, client, option, watch):
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

    if _is_native_option(option):
        for target in targets:
            session_public_id = target["session_public_id"]
            deposit_address = target["deposit_address"]
            required_confirmations = int(target["required_confirmations"])
            min_amount = int(target["min_amount"])

            try:
                checksum_deposit_address = Web3.to_checksum_address(deposit_address)
            except Exception:
                logging.exception(
                    "invalid native deposit address option=%s session=%s address=%s",
                    option.key,
                    session_public_id,
                    deposit_address,
                )
                continue

            try:
                current_balance = int(w3.eth.get_balance(checksum_deposit_address))
            except Exception:
                logging.exception(
                    "get_balance failed option=%s session=%s address=%s",
                    option.key,
                    session_public_id,
                    checksum_deposit_address,
                )
                continue

            if current_balance <= 0 or current_balance < min_amount:
                continue

            latest_incoming = _find_latest_incoming_native_transfer(
                w3=w3,
                deposit_address=checksum_deposit_address,
                latest_block=latest_block,
                lookback_blocks=option.lookback_blocks,
            )
            if latest_incoming is None:
                logging.warning(
                    "positive native balance but no recent incoming transfer option=%s session=%s address=%s balance=%s",
                    option.key,
                    session_public_id,
                    checksum_deposit_address,
                    current_balance,
                )
                continue

            confirmations = int(latest_block - int(latest_incoming["block_number"]) + 1)

            _post_observation(
                client=client,
                option=option,
                session_public_id=session_public_id,
                deposit_address=checksum_deposit_address,
                txid=latest_incoming["txid"],
                block_number=latest_incoming["block_number"],
                from_address=latest_incoming["from_address"],
                amount=latest_incoming["amount"],
                confirmations=confirmations,
                required_confirmations=required_confirmations,
                raw_payload=latest_incoming["raw_payload"],
                log_index=0,
            )

            logging.info(
                "native deposit observed option=%s session=%s txid=%s confirmations=%s amount=%s",
                option.key,
                session_public_id,
                latest_incoming["txid"],
                confirmations,
                latest_incoming["amount"],
            )

        return

    contract = w3.eth.contract(
        address=Web3.to_checksum_address(option.token_contract_address),
        abi=ERC20_ABI,
    )

    for target in targets:
        session_public_id = target["session_public_id"]
        deposit_address = target["deposit_address"]
        required_confirmations = int(target["required_confirmations"])
        min_amount = int(target["min_amount"])

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
                "balanceOf failed option=%s session=%s address=%s",
                option.key,
                session_public_id,
                checksum_deposit_address,
            )
            continue

        if current_balance <= 0 or current_balance < min_amount:
            continue

        latest_incoming = _find_latest_incoming_erc20_transfer(
            w3=w3,
            contract_address=option.token_contract_address,
            deposit_address=checksum_deposit_address,
            latest_block=latest_block,
            lookback_blocks=option.lookback_blocks,
        )

        if latest_incoming is None:
            logging.warning(
                "positive balance but no recent incoming transfer option=%s session=%s address=%s balance=%s",
                option.key,
                session_public_id,
                checksum_deposit_address,
                current_balance,
            )
            continue

        block_number = int(latest_incoming["blockNumber"])
        confirmations = int(latest_block - block_number + 1)
        txid = latest_incoming["transactionHash"].hex()
        log_index = int(latest_incoming["logIndex"])

        _post_observation(
            client=client,
            option=option,
            session_public_id=session_public_id,
            deposit_address=checksum_deposit_address,
            txid=txid,
            block_number=block_number,
            from_address=_decode_topic_address(latest_incoming["topics"][1].hex()),
            amount=current_balance,
            confirmations=confirmations,
            required_confirmations=required_confirmations,
            raw_payload={
                "transaction_hash": txid,
                "block_hash": latest_incoming["blockHash"].hex(),
                "transaction_index": int(latest_incoming["transactionIndex"]),
                "log_index": log_index,
                "balance_snapshot": str(current_balance),
            },
            log_index=log_index,
        )

        logging.info(
            "deposit observed option=%s session=%s txid=%s confirmations=%s amount=%s",
            option.key,
            session_public_id,
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
            _observe_token_option(
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