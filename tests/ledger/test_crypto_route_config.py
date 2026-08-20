import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read_json(path):
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def test_direct_crypto_routes_include_native_assets_and_polygon_stables():
    payload = _read_json("deposit_service/config/deposit-service.json")
    options = {item["key"]: item for item in payload["options"]}

    required = {
        "ethereum-eth",
        "ethereum-usdt",
        "ethereum-usdc",
        "bsc-bnb",
        "bsc-usdt",
        "bsc-usdc",
        "arbitrum-eth",
        "arbitrum-usdt",
        "arbitrum-usdc",
        "base-eth",
        "base-usdc",
        "polygon-pol",
        "polygon-usdc",
        "polygon-usdt",
    }
    assert required <= set(options)
    assert "polygon-pol-paygate" not in options

    for key in (
        "ethereum-eth",
        "bsc-bnb",
        "arbitrum-eth",
        "base-eth",
        "polygon-pol",
    ):
        option = options[key]
        assert option["token_contract_address"] == ""
        assert option["amount_semantics"] == "native_quoted"
        assert option["provision_addresses"] is True

    assert (
        options["polygon-usdc"]["token_contract_address"].lower()
        == "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"
    )
    assert (
        options["polygon-usdt"]["token_contract_address"].lower()
        == "0xc2132d05d31c914a87c6611c10748aeb04b58e8f"
    )


def test_sweeper_has_matching_routes_and_polygon_erc20_gas_funding():
    payload = _read_json("sweeper_service/config/sweeper-service.json")
    options = {item["key"]: item for item in payload["options"]}

    required = {
        "ethereum-eth",
        "bsc-bnb",
        "arbitrum-eth",
        "base-eth",
        "polygon-pol",
        "polygon-usdc",
        "polygon-usdt",
    }
    assert required <= set(options)
    assert "polygon-pol-paygate" not in options

    for key in (
        "ethereum-eth",
        "bsc-bnb",
        "arbitrum-eth",
        "base-eth",
        "polygon-pol",
    ):
        assert options[key]["token_contract_address"] == ""
        assert "funding_private_key_file" not in options[key]

    for key in ("polygon-usdc", "polygon-usdt"):
        assert (
            options[key]["funding_private_key_file"]
            == "/run/secrets/polygon_sweeper_funding_private_key"
        )
        assert int(options[key]["max_gas_funding_amount_wei"]) > 0

def test_native_runtime_price_age_covers_five_minute_source_refresh():
    native_policy = _read_json("ledger/config/native-quoted.json")
    paygate_policy = _read_json("ledger/config/paygate-polygon.json")
    deposit_config = _read_json(
        "deposit_service/config/deposit-service.json"
    )

    assert native_policy["quote_max_age_seconds"] == 360
    assert paygate_policy["quote_max_age_seconds"] == 360
    assert deposit_config["runtime_prices"]["max_age_seconds"] == 360

