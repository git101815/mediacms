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
    account_xpub: str
    start_index: int
    target_available: int
    required_confirmations: int
    min_amount: int
    session_ttl_seconds: int


@dataclass(frozen=True)
class ServiceConfig:
    mediacms_base_url: str
    service_name: str
    shared_secret: str
    state_path: str
    poll_interval_seconds: int
    options: list[DepositOptionConfig]
    provision_batch_size: int


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_config() -> ServiceConfig:
    config_path = _require_env("DEPOSIT_SERVICE_CONFIG_PATH")

    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    raw_options = raw.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        raise RuntimeError("Deposit service config must contain a non-empty 'options' list")

    provision_batch_size = int(os.environ.get("DEPOSIT_SERVICE_PROVISION_BATCH_SIZE", "100"))
    if provision_batch_size <= 0:
        raise RuntimeError("DEPOSIT_SERVICE_PROVISION_BATCH_SIZE must be greater than 0")

    options = []
    for item in raw_options:
        options.append(
            DepositOptionConfig(
                key=item["key"],
                chain=item["chain"],
                asset_code=item["asset_code"],
                token_contract_address=item.get("token_contract_address", ""),
                display_label=item["display_label"],
                account_xpub=item["account_xpub"],
                start_index=int(item.get("start_index", 0)),
                target_available=int(item["target_available"]),
                required_confirmations=int(item["required_confirmations"]),
                min_amount=int(item["min_amount"]),
                session_ttl_seconds=int(item["session_ttl_seconds"]),
            )
        )

    return ServiceConfig(
        mediacms_base_url=_require_env("MEDIACMS_INTERNAL_BASE_URL").rstrip("/"),
        service_name=os.environ.get("MEDIACMS_INTERNAL_SERVICE", "deposit-service").strip() or "deposit-service",
        shared_secret=_require_env("MEDIACMS_INTERNAL_SHARED_SECRET"),
        state_path=_require_env("DEPOSIT_SERVICE_STATE_PATH"),
        poll_interval_seconds=int(os.environ.get("DEPOSIT_SERVICE_POLL_INTERVAL_SECONDS", "30")),
        options=options,
        provision_batch_size = provision_batch_size,
    )