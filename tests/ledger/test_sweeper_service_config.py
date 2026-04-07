import json

import pytest

from sweeper_service.app.config import load_config


def test_load_config_rejects_non_positive_claim_batch_size(tmp_path, monkeypatch):
    mnemonic_file = tmp_path / "mnemonic.txt"
    passphrase_file = tmp_path / "passphrase.txt"
    funding_key_file = tmp_path / "funding-key.txt"
    config_path = tmp_path / "sweeper-service.json"

    mnemonic_file.write_text(
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
        encoding="utf-8",
    )
    passphrase_file.write_text("", encoding="utf-8")
    funding_key_file.write_text("0x" + ("11" * 32), encoding="utf-8")

    config_path.write_text(
        json.dumps(
            {
                "mnemonic_file": str(mnemonic_file),
                "mnemonic_passphrase_file": str(passphrase_file),
                "account_index": 0,
                "options": [
                    {
                        "key": "ethereum-usdt",
                        "chain": "ethereum",
                        "asset_code": "USDT",
                        "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
                        "rpc_url": "${ETHEREUM_RPC_URL}",
                        "funding_private_key_file": str(funding_key_file),
                        "destination_address": "0x9999999999999999999999999999999999999999",
                        "gas_funding_amount_wei": "1000000000000000",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("SWEEPER_SERVICE_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("MEDIACMS_INTERNAL_BASE_URL", "http://web")
    monkeypatch.setenv("MEDIACMS_INTERNAL_SHARED_SECRET", "secret")
    monkeypatch.setenv("ETHEREUM_RPC_URL", "https://rpc.example")
    monkeypatch.setenv("SWEEPER_SERVICE_CLAIM_BATCH_SIZE", "0")

    with pytest.raises(RuntimeError, match="SWEEPER_SERVICE_CLAIM_BATCH_SIZE"):
        load_config()