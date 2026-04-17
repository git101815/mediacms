import time

from eth_account import Account
from web3 import Web3
from web3.exceptions import TransactionNotFound
from web3.middleware import ExtraDataToPOAMiddleware

from .erc20 import ERC20_ABI


class NonceAllocator:
    def __init__(self) -> None:
        self._next_nonce_by_key: dict[tuple[str, str], int] = {}

    def next_nonce(self, *, chain: str, w3: Web3, address: str) -> int:
        key = ((chain or "").strip().lower(), address.lower())
        if key not in self._next_nonce_by_key:
            self._next_nonce_by_key[key] = int(
                w3.eth.get_transaction_count(
                    Web3.to_checksum_address(address),
                    block_identifier="pending",
                )
            )

        nonce = self._next_nonce_by_key[key]
        self._next_nonce_by_key[key] += 1
        return nonce


def build_web3(*, rpc_url: str, poa_compatible: bool, request_timeout_seconds: int) -> Web3:
    provider = Web3.HTTPProvider(
        rpc_url,
        request_kwargs={"timeout": float(request_timeout_seconds)},
    )
    w3 = Web3(provider)
    if poa_compatible:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3


def address_from_private_key(private_key: str) -> str:
    return Account.from_key(private_key).address.lower()


def _build_fee_params(*, w3: Web3, gas_price_multiplier_bps: int) -> dict:
    base_gas_price = int(w3.eth.gas_price)
    adjusted_gas_price = max(
        1,
        (base_gas_price * int(gas_price_multiplier_bps) + 9999) // 10000,
    )
    return {"gasPrice": adjusted_gas_price}


def _sign_and_send(*, w3: Web3, tx: dict, private_key: str) -> str:
    signed = Account.sign_transaction(tx, private_key=private_key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    return tx_hash.hex()


def get_native_balance(*, w3: Web3, address: str) -> int:
    return int(w3.eth.get_balance(Web3.to_checksum_address(address)))


def send_native_transfer(
    *,
    chain: str,
    w3: Web3,
    nonce_allocator: NonceAllocator,
    funding_private_key: str,
    to_address: str,
    amount_wei: int,
    gas_price_multiplier_bps: int,
) -> str:
    funding_address = address_from_private_key(funding_private_key)

    tx = {
        "chainId": int(w3.eth.chain_id),
        "nonce": nonce_allocator.next_nonce(
            chain=chain,
            w3=w3,
            address=funding_address,
        ),
        "from": Web3.to_checksum_address(funding_address),
        "to": Web3.to_checksum_address(to_address),
        "value": int(amount_wei),
        "gas": 21000,
        **_build_fee_params(
            w3=w3,
            gas_price_multiplier_bps=gas_price_multiplier_bps,
        ),
    }
    return _sign_and_send(w3=w3, tx=tx, private_key=funding_private_key)


def get_erc20_balance(
    *,
    w3: Web3,
    token_contract_address: str,
    owner_address: str,
) -> int:
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(token_contract_address),
        abi=ERC20_ABI,
    )
    return int(
        contract.functions.balanceOf(
            Web3.to_checksum_address(owner_address)
        ).call()
    )


def send_erc20_transfer(
    *,
    chain: str,
    w3: Web3,
    nonce_allocator: NonceAllocator,
    token_contract_address: str,
    source_private_key: str,
    destination_address: str,
    amount: int,
    gas_limit: int,
    gas_price_multiplier_bps: int,
) -> str:
    source_address = address_from_private_key(source_private_key)
    contract = w3.eth.contract(
        address=Web3.to_checksum_address(token_contract_address),
        abi=ERC20_ABI,
    )

    function = contract.functions.transfer(
        Web3.to_checksum_address(destination_address),
        int(amount),
    )

    tx = function.build_transaction(
        {
            "chainId": int(w3.eth.chain_id),
            "from": Web3.to_checksum_address(source_address),
            "nonce": nonce_allocator.next_nonce(
                chain=chain,
                w3=w3,
                address=source_address,
            ),
            "gas": int(gas_limit),
            **_build_fee_params(
                w3=w3,
                gas_price_multiplier_bps=gas_price_multiplier_bps,
            ),
        }
    )
    return _sign_and_send(w3=w3, tx=tx, private_key=source_private_key)


def wait_for_confirmations(
    *,
    w3: Web3,
    txid: str,
    required_confirmations: int,
    timeout_seconds: int,
    poll_interval_seconds: float = 3.0,
):
    deadline = time.monotonic() + float(timeout_seconds)

    while time.monotonic() < deadline:
        try:
            receipt = w3.eth.get_transaction_receipt(txid)
        except TransactionNotFound:
            time.sleep(poll_interval_seconds)
            continue

        if int(receipt["status"]) != 1:
            raise RuntimeError(f"Transaction failed on-chain: {txid}")

        latest_block = int(w3.eth.block_number)
        confirmations = latest_block - int(receipt["blockNumber"]) + 1
        if confirmations >= int(required_confirmations):
            return receipt

        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        f"Timed out waiting for {required_confirmations} confirmations for {txid}"
    )