import json
import os
import re
from dataclasses import dataclass

from eth_account import Account


_PLACEHOLDER_RE = re.compile(r"^\$\{([A-Z0-9_]+)\}$")


@dataclass(frozen=True)
class SweepOptionConfig:
    key: str
    chain: str
    asset_code: str
    token_contract_address: str
    rpc_urls: list[str]
    funding_private_key: str
    destination_address: str
    funding_confirmations: int
    sweep_confirmations: int
    max_gas_funding_amount_wei: int
    erc20_transfer_gas_limit: int
    gas_limit_multiplier_bps: int
    gas_limit_retry_multiplier_bps: int
    tx_timeout_seconds: int
    gas_price_multiplier_bps: int
    poa_compatible: bool


@dataclass(frozen=True)
class ServiceConfig:
    mediacms_base_url: str
    service_name: str
    shared_secret: str
    poll_interval_seconds: int
    claim_batch_size: int
    mnemonic: str
    mnemonic_passphrase: str
    account_index: int
    rpc_max_lag_blocks: int
    rpc_max_reference_lag_blocks: int
    reference_heads_base_url: str
    reference_heads_shared_secret: str
    reference_heads_timeout_seconds: float
    reference_heads_max_age_seconds: int
    options: list[SweepOptionConfig]
    request_timeout_seconds: float


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _resolve_env_placeholder(value):
    if not isinstance(value, str):
        return value

    match = _PLACEHOLDER_RE.fullmatch(value.strip())
    if not match:
        return value

    return _require_env(match.group(1))


def _read_secret_file(path: str) -> str:
    normalized_path = str(path).strip()
    if not normalized_path:
        raise RuntimeError("Secret file path must not be empty")

    try:
        with open(normalized_path, "r", encoding="utf-8") as handle:
            value = handle.read().strip()
    except FileNotFoundError as exc:
        raise RuntimeError(f"Secret file not found: {normalized_path}") from exc

    if value == "":
        return ""

    return value


def _normalize_private_key(value: str) -> str:
    resolved = str(value).strip()
    if resolved.startswith("0x"):
        resolved = resolved[2:]

    if len(resolved) != 64:
        raise RuntimeError("Funding private key must be 32 bytes hex")

    return f"0x{resolved}"


def _resolve_rpc_urls(item: dict) -> list[str]:
    raw_urls = item.get("rpc_urls")
    raw_url = item.get("rpc_url")

    urls: list[str] = []

    if isinstance(raw_urls, list):
        for value in raw_urls:
            resolved = str(_resolve_env_placeholder(value)).strip()
            if resolved:
                urls.append(resolved)

    if raw_url is not None:
        resolved = str(_resolve_env_placeholder(raw_url)).strip()
        if resolved:
            urls.append(resolved)

    deduped: list[str] = []
    seen = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)

    if not deduped:
        raise RuntimeError(f"Option {item.get('key', '')} must define rpc_url or rpc_urls")

    return deduped


def load_config() -> ServiceConfig:
    config_path = _require_env("SWEEPER_SERVICE_CONFIG_PATH")
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    raw_options = raw.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        raise RuntimeError("Sweeper service config must contain a non-empty 'options' list")

    mnemonic = _read_secret_file(raw["mnemonic_file"])
    mnemonic_passphrase = _read_secret_file(raw["mnemonic_passphrase_file"])
    account_index = int(raw.get("account_index", 0))
    if account_index < 0:
        raise RuntimeError("account_index must be >= 0")
    request_timeout_seconds = float(
        os.environ.get("SWEEPER_RPC_REQUEST_TIMEOUT_SECONDS", "10")
    )
    if request_timeout_seconds <= 0:
        raise RuntimeError("SWEEPER_RPC_REQUEST_TIMEOUT_SECONDS must be > 0")

    options: list[SweepOptionConfig] = []
    for item in raw_options:
        funding_private_key = _normalize_private_key(
            _read_secret_file(item["funding_private_key_file"])
        )

        destination_address = str(
            _resolve_env_placeholder(item.get("destination_address", ""))
        ).strip().lower()
        if not destination_address:
            destination_address = Account.from_key(funding_private_key).address.lower()

        raw_max_gas_funding_amount_wei = item.get("max_gas_funding_amount_wei")
        if raw_max_gas_funding_amount_wei is None:
            raw_max_gas_funding_amount_wei = item["gas_funding_amount_wei"]

        option = SweepOptionConfig(
            key=str(item["key"]).strip(),
            chain=str(item["chain"]).strip().lower(),
            asset_code=str(item["asset_code"]).strip().upper(),
            token_contract_address=str(item.get("token_contract_address", "")).strip().lower(),
            rpc_urls=_resolve_rpc_urls(item),
            funding_private_key=funding_private_key,
            destination_address=destination_address,
            funding_confirmations=int(item.get("funding_confirmations", 1)),
            sweep_confirmations=int(item.get("sweep_confirmations", 1)),
            max_gas_funding_amount_wei=int(raw_max_gas_funding_amount_wei),
            erc20_transfer_gas_limit=int(item.get("erc20_transfer_gas_limit", 100000)),
            gas_limit_multiplier_bps=int(item.get("gas_limit_multiplier_bps", 12000)),
            gas_limit_retry_multiplier_bps=int(item.get("gas_limit_retry_multiplier_bps", 15000)),
            tx_timeout_seconds=int(item.get("tx_timeout_seconds", 300)),
            gas_price_multiplier_bps=int(item.get("gas_price_multiplier_bps", 12000)),
            poa_compatible=bool(item.get("poa_compatible", False)),
        )

        if not option.key:
            raise RuntimeError("Sweep option key must not be empty")
        if option.funding_confirmations <= 0:
            raise RuntimeError(f"funding_confirmations must be > 0 for option {option.key}")
        if option.sweep_confirmations <= 0:
            raise RuntimeError(f"sweep_confirmations must be > 0 for option {option.key}")
        if option.max_gas_funding_amount_wei <= 0:
            raise RuntimeError(f"max_gas_funding_amount_wei must be > 0 for option {option.key}")
        if option.erc20_transfer_gas_limit <= 0:
            raise RuntimeError(f"erc20_transfer_gas_limit must be > 0 for option {option.key}")
        if option.gas_limit_multiplier_bps < 10000:
            raise RuntimeError(f"gas_limit_multiplier_bps must be >= 10000 for option {option.key}")
        if option.tx_timeout_seconds <= 0:
            raise RuntimeError(f"tx_timeout_seconds must be > 0 for option {option.key}")
        if option.gas_price_multiplier_bps < 10000:
            raise RuntimeError(f"gas_price_multiplier_bps must be >= 10000 for option {option.key}")
        if option.gas_limit_retry_multiplier_bps < 10000:
            raise RuntimeError(f"gas_limit_retry_multiplier_bps must be >= 10000 for option {option.key}")

        options.append(option)

    claim_batch_size = int(os.environ.get("SWEEPER_SERVICE_CLAIM_BATCH_SIZE", "20"))
    if claim_batch_size <= 0:
        raise RuntimeError("SWEEPER_SERVICE_CLAIM_BATCH_SIZE must be greater than 0")

    poll_interval_seconds = int(os.environ.get("SWEEPER_SERVICE_POLL_INTERVAL_SECONDS", "30"))
    if poll_interval_seconds <= 0:
        raise RuntimeError("SWEEPER_SERVICE_POLL_INTERVAL_SECONDS must be greater than 0")

    rpc_max_lag_blocks = int(os.environ.get("SWEEPER_RPC_MAX_LAG_BLOCKS", "64"))
    if rpc_max_lag_blocks < 0:
        raise RuntimeError("SWEEPER_RPC_MAX_LAG_BLOCKS must be >= 0")

    rpc_max_reference_lag_blocks = int(
        os.environ.get("SWEEPER_RPC_MAX_REFERENCE_LAG_BLOCKS", "64")
    )
    if rpc_max_reference_lag_blocks < 0:
        raise RuntimeError("SWEEPER_RPC_MAX_REFERENCE_LAG_BLOCKS must be >= 0")

    reference_heads_timeout_seconds = float(
        os.environ.get("SWEEPER_REFERENCE_HEADS_TIMEOUT_SECONDS", "5")
    )
    if reference_heads_timeout_seconds <= 0:
        raise RuntimeError("SWEEPER_REFERENCE_HEADS_TIMEOUT_SECONDS must be > 0")

    reference_heads_max_age_seconds = int(
        os.environ.get("SWEEPER_REFERENCE_HEADS_MAX_AGE_SECONDS", "60")
    )
    if reference_heads_max_age_seconds <= 0:
        raise RuntimeError("SWEEPER_REFERENCE_HEADS_MAX_AGE_SECONDS must be > 0")

    return ServiceConfig(
        mediacms_base_url=_require_env("MEDIACMS_INTERNAL_BASE_URL").rstrip("/"),
        service_name=os.environ.get("MEDIACMS_INTERNAL_SERVICE", "sweeper-service").strip() or "sweeper-service",
        shared_secret=_require_env("MEDIACMS_INTERNAL_SHARED_SECRET"),
        poll_interval_seconds=poll_interval_seconds,
        claim_batch_size=claim_batch_size,
        mnemonic=mnemonic,
        mnemonic_passphrase=mnemonic_passphrase,
        account_index=account_index,
        rpc_max_lag_blocks=rpc_max_lag_blocks,
        rpc_max_reference_lag_blocks=rpc_max_reference_lag_blocks,
        reference_heads_base_url=_require_env("REFERENCE_HEADS_BASE_URL").rstrip("/"),
        reference_heads_shared_secret=_require_env("REFERENCE_HEADS_SHARED_SECRET"),
        reference_heads_timeout_seconds=reference_heads_timeout_seconds,
        reference_heads_max_age_seconds=reference_heads_max_age_seconds,
        options=options,
        request_timeout_seconds=request_timeout_seconds,
    )