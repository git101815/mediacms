import httpx


_CHAIN_ID_BY_NAME = {
    "ethereum": 1,
    "bsc": 56,
    "arbitrum": 42161,
    "base": 8453,
}


def get_reference_chain_id(*, chain: str) -> int:
    normalized = (chain or "").strip().lower()
    try:
        return int(_CHAIN_ID_BY_NAME[normalized])
    except KeyError as exc:
        raise RuntimeError(f"Unsupported reference chain: {chain}") from exc


def get_reference_head(
    *,
    chain: str,
    api_key: str,
    timeout_seconds: float,
) -> int:
    chain_id = get_reference_chain_id(chain=chain)
    url = "https://api.etherscan.io/v2/api"
    params = {
        "chainid": str(chain_id),
        "module": "proxy",
        "action": "eth_blockNumber",
        "apikey": api_key,
    }

    with httpx.Client(timeout=timeout_seconds) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        payload = response.json()

    result = str(payload.get("result", "")).strip()
    if not result.startswith("0x"):
        raise RuntimeError(
            f"Invalid reference head response for chain={chain}: {payload}"
        )

    return int(result, 16)