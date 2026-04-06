TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _normalize_hex_address(address: str) -> str:
    raw_value = (address or "").strip().lower()

    if raw_value.startswith("0x"):
        raw_value = raw_value[2:]

    if len(raw_value) != 40:
        raise ValueError("Address must contain exactly 20 bytes")

    try:
        int(raw_value, 16)
    except ValueError as exc:
        raise ValueError("Address must be valid hexadecimal") from exc

    return "0x" + raw_value


def address_to_topic(address: str) -> str:
    normalized = _normalize_hex_address(address)
    return "0x" + normalized[2:].rjust(64, "0")


def decode_address_from_topic(topic_hex: str) -> str:
    raw_value = (topic_hex or "").strip().lower()

    if raw_value.startswith("0x"):
        raw_value = raw_value[2:]

    if len(raw_value) != 64:
        raise ValueError("Topic must contain exactly 32 bytes")

    try:
        int(raw_value, 16)
    except ValueError as exc:
        raise ValueError("Topic must be valid hexadecimal") from exc

    return "0x" + raw_value[-40:]


def decode_uint256(data_hex: str) -> int:
    raw_value = (data_hex or "").strip().lower()

    if raw_value.startswith("0x"):
        raw_value = raw_value[2:]

    if not raw_value:
        raise ValueError("uint256 data cannot be empty")

    try:
        return int(raw_value, 16)
    except ValueError as exc:
        raise ValueError("uint256 data must be valid hexadecimal") from exc