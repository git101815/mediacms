import json

import pytest

from sweeper_service.app.config import load_config


def test_load_config_rejects_non_positive_claim_batch_size(tmp_path, monkeypatch):
    config_path = tmp_path / "sweeper-service.json"
    config_path.write_text(
        json.dumps(
            {
                "options": [
                    {
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("SWEEPER_SERVICE_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("MEDIACMS_INTERNAL_BASE_URL", "http://web")
    monkeypatch.setenv("MEDIACMS_INTERNAL_SHARED_SECRET", "secret")
    monkeypatch.setenv("SWEEPER_SERVICE_CLAIM_BATCH_SIZE", "0")

    with pytest.raises(RuntimeError, match="SWEEPER_SERVICE_CLAIM_BATCH_SIZE"):
        load_config()