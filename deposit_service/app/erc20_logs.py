from web3 import Web3

TRANSFER_TOPIC0 = Web3.keccak(text="Transfer(address,address,uint256)").hex()


def address_to_topic(address: str) -> str:
    normalized = Web3.to_checksum_address(address)
    return "0x" + normalized.lower().replace("0x", "").rjust(64, "0")


def decode_address_from_topic(topic_hex: str) -> str:
    return Web3.to_checksum_address("0x" + topic_hex[-40:]).lower()


def decode_uint256(data_hex: str) -> int:
    return int(data_hex, 16)