import json
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SweepOptionConfig:
    chain: str
    asset_code: str
    token_contract_address: str


@dataclass(frozen=True)
class ServiceConfig:
    mediacms_base_url: str
    service_name: str
    shared_secret: str
    poll_interval_seconds: int
    claim_batch_size: int
    options: list[SweepOptionConfig]


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_config() -> ServiceConfig:
    config_path = _require_env("SWEEPER_SERVICE_CONFIG_PATH")

    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    raw_options = raw.get("options")
    if not isinstance(raw_options, list) or not raw_options:
        raise RuntimeError("Sweeper service config must contain a non-empty 'options' list")

    options = [
        SweepOptionConfig(
            chain=item["chain"],
            asset_code=item["asset_code"],
            token_contract_address=item.get("token_contract_address", ""),
        )
        for item in raw_options
    ]

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
        options=options,
    )