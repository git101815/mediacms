import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DepositOptionConfig:
    key: str
    chain: str
    asset_code: str
    token_contract_address: str
    display_label: str
    rpc_url: str
    required_confirmations: int
    min_amount: int
    session_ttl_seconds: int
    poll_interval_seconds: int
    lookback_blocks: int
    poa_compatible: bool


@dataclass(frozen=True)
class ServiceConfig:
    mediacms_base_url: str
    service_name: str
    shared_secret: str
    state_path: str
    poll_interval_seconds: int
    options: list[DepositOptionConfig]


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _resolve_env_placeholder(value: str):
    if not isinstance(value, str):
        return value
    if value.startswith("${") and value.endswith("}"):
        env_name = value[2:-1].strip()
        if not env_name:
            raise RuntimeError("Empty environment variable placeholder")
        return _require_env(env_name)
    return value


def _build_display_label(*, chain: str, asset_code: str, raw_value: str | None) -> str:
    explicit = (raw_value or "").strip()
    if explicit:
        return explicit

    chain_labels = {
        "ethereum": "Ethereum",
        "arbitrum": "Arbitrum One",
        "base": "Base",
        "bsc": "BNB Chain",
        "polygon": "Polygon",
    }
    normalized_chain = (chain or "").strip().lower()
    chain_label = chain_labels.get(normalized_chain, (chain or "").strip() or "Unknown")
    asset_label = (asset_code or "").strip().upper() or "UNKNOWN"
    return f"{chain_label} · {asset_label}"


def load_config() -> ServiceConfig:
    config_path = _require_env("DEPOSIT_SERVICE_CONFIG_PATH")

    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    raw_options = raw.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        raise RuntimeError("Deposit service config must contain a non-empty 'options' list")

    options: list[DepositOptionConfig] = []

    for item in raw_options:
        option = DepositOptionConfig(
            key=item["key"],
            chain=item["chain"],
            asset_code=item["asset_code"],
            token_contract_address=item["token_contract_address"],
            display_label=_build_display_label(
                chain=item["chain"],
                asset_code=item["asset_code"],
                raw_value=item.get("display_label"),
            ),
            rpc_url=_resolve_env_placeholder(item["rpc_url"]),
            required_confirmations=int(item["required_confirmations"]),
            min_amount=int(item["min_amount"]),
            session_ttl_seconds=int(item["session_ttl_seconds"]),
            poll_interval_seconds=int(item.get("poll_interval_seconds", 15)),
            lookback_blocks=int(item.get("lookback_blocks", 2000)),
            poa_compatible=bool(item.get("poa_compatible", False)),
        )

        if option.required_confirmations <= 0:
            raise RuntimeError(f"required_confirmations must be > 0 for option {option.key}")
        if option.min_amount <= 0:
            raise RuntimeError(f"min_amount must be > 0 for option {option.key}")
        if option.session_ttl_seconds <= 0:
            raise RuntimeError(f"session_ttl_seconds must be > 0 for option {option.key}")
        if option.poll_interval_seconds <= 0:
            raise RuntimeError(f"poll_interval_seconds must be > 0 for option {option.key}")
        if option.lookback_blocks <= 0:
            raise RuntimeError(f"lookback_blocks must be > 0 for option {option.key}")

        options.append(option)

    global_poll_interval = int(os.environ.get("DEPOSIT_SERVICE_POLL_INTERVAL_SECONDS", "15"))
    if global_poll_interval <= 0:
        raise RuntimeError("DEPOSIT_SERVICE_POLL_INTERVAL_SECONDS must be > 0")

    return ServiceConfig(
        mediacms_base_url=_require_env("MEDIACMS_INTERNAL_BASE_URL").rstrip("/"),
        service_name=os.environ.get("MEDIACMS_INTERNAL_SERVICE", "deposit-service").strip() or "deposit-service",
        shared_secret=_require_env("MEDIACMS_INTERNAL_SHARED_SECRET"),
        state_path=_require_env("DEPOSIT_SERVICE_STATE_PATH"),
        poll_interval_seconds=global_poll_interval,
        options=options,
    )