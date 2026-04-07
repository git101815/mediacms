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
    rpc_url: str
    funding_private_key: str
    destination_address: str
    funding_confirmations: int
    sweep_confirmations: int
    gas_funding_amount_wei: int
    erc20_transfer_gas_limit: int
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
    options: list[SweepOptionConfig]


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

    options: list[SweepOptionConfig] = []
    for item in raw_options:
        funding_private_key = _normalize_private_key(
            _read_secret_file(item["funding_private_key_file"])
        )

        destination_address = str(item.get("destination_address", "")).strip().lower()
        if not destination_address:
            destination_address = Account.from_key(funding_private_key).address.lower()

        option = SweepOptionConfig(
            key=str(item["key"]).strip(),
            chain=str(item["chain"]).strip().lower(),
            asset_code=str(item["asset_code"]).strip().upper(),
            token_contract_address=str(item.get("token_contract_address", "")).strip().lower(),
            rpc_url=str(_resolve_env_placeholder(item["rpc_url"])).strip(),
            funding_private_key=funding_private_key,
            destination_address=destination_address,
            funding_confirmations=int(item.get("funding_confirmations", 1)),
            sweep_confirmations=int(item.get("sweep_confirmations", 1)),
            gas_funding_amount_wei=int(item["gas_funding_amount_wei"]),
            erc20_transfer_gas_limit=int(item.get("erc20_transfer_gas_limit", 100000)),
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
        if option.gas_funding_amount_wei <= 0:
            raise RuntimeError(f"gas_funding_amount_wei must be > 0 for option {option.key}")
        if option.erc20_transfer_gas_limit <= 0:
            raise RuntimeError(f"erc20_transfer_gas_limit must be > 0 for option {option.key}")
        if option.tx_timeout_seconds <= 0:
            raise RuntimeError(f"tx_timeout_seconds must be > 0 for option {option.key}")
        if option.gas_price_multiplier_bps <= 0:
            raise RuntimeError(f"gas_price_multiplier_bps must be > 0 for option {option.key}")

        options.append(option)

    claim_batch_size = int(os.environ.get("SWEEPER_SERVICE_CLAIM_BATCH_SIZE", "20"))
    if claim_batch_size <= 0:
        raise RuntimeError("SWEEPER_SERVICE_CLAIM_BATCH_SIZE must be greater than 0")

    poll_interval_seconds = int(os.environ.get("SWEEPER_SERVICE_POLL_INTERVAL_SECONDS", "30"))
    if poll_interval_seconds <= 0:
        raise RuntimeError("SWEEPER_SERVICE_POLL_INTERVAL_SECONDS must be greater than 0")

    return ServiceConfig(
        mediacms_base_url=_require_env("MEDIACMS_INTERNAL_BASE_URL").rstrip("/"),
        service_name=os.environ.get("MEDIACMS_INTERNAL_SERVICE", "sweeper-service").strip() or "sweeper-service",
        shared_secret=_require_env("MEDIACMS_INTERNAL_SHARED_SECRET"),
        poll_interval_seconds=poll_interval_seconds,
        claim_batch_size=claim_batch_size,
        mnemonic=mnemonic,
        mnemonic_passphrase=mnemonic_passphrase,
        account_index=account_index,
        options=options,
    )