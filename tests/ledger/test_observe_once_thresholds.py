from types import SimpleNamespace

from deposit_service.app import observe_once


class FakeObservationClient:
    def __init__(self):
        self.payloads = []

    def post_signed(self, path, payload):
        self.payloads.append((path, payload))


class FakeBalanceCall:
    def __init__(self, amount_by_block):
        self.amount_by_block = amount_by_block

    def call(self, block_identifier=None):
        if block_identifier is None:
            return self.amount_by_block["latest"]
        return self.amount_by_block.get(int(block_identifier), 0)


class FakeBalanceFunction:
    def __init__(self, amount_by_block):
        self.amount_by_block = amount_by_block

    def balanceOf(self, address):
        return FakeBalanceCall(self.amount_by_block)


class FakeContract:
    def __init__(self, amount_by_block):
        self.functions = FakeBalanceFunction(amount_by_block)


class FakeEth:
    def __init__(self, amount_by_block):
        self.block_number = 200
        self.amount_by_block = amount_by_block

    def contract(self, address, abi):
        return FakeContract(self.amount_by_block)


class FakeWeb3:
    def __init__(self, amount_by_block):
        self.eth = FakeEth(amount_by_block)


def test_erc20_observer_detects_partial_payment_above_observation_minimum(monkeypatch):
    one_usdt_bsc_raw = 10**18
    five_usdt_bsc_raw = 5 * 10**18
    seven_usdt_bsc_raw = 7 * 10**18

    amount_by_block = {"latest": five_usdt_bsc_raw}
    for block_number in range(0, 120):
        amount_by_block[block_number] = 0
    for block_number in range(120, 201):
        amount_by_block[block_number] = five_usdt_bsc_raw

    monkeypatch.setattr(
        observe_once,
        "_build_option_web3",
        lambda **kwargs: FakeWeb3(amount_by_block),
    )

    client = FakeObservationClient()

    option = SimpleNamespace(
        key="bsc-usdt",
        chain="bsc",
        asset_code="USDT",
        token_contract_address="0x55d398326f99059ff775485246999027b3197955",
        observation_min_amount=one_usdt_bsc_raw,
        lookback_blocks=200,
        rpc_urls=["http://fake-rpc"],
        poa_compatible=True,
    )

    watch = {
        "targets": [
            {
                "session_public_id": "session-partial-bsc",
                "deposit_address": "0x1111111111111111111111111111111111111111",
                "required_confirmations": 12,
                "min_amount": 7_000_000,
                "onchain_min_amount": seven_usdt_bsc_raw,
                "amount_unit": "canonical_stable",
            }
        ]
    }

    observe_once._observe_token_option(
        client=client,
        option=option,
        watch=watch,
        reference_heads_base_url="",
        reference_heads_shared_secret="",
        reference_heads_timeout_seconds=1,
        reference_heads_max_age_seconds=60,
        rpc_max_lag_blocks=64,
        rpc_max_reference_lag_blocks=64,
    )

    assert len(client.payloads) == 1

    path, payload = client.payloads[0]
    assert path == "/api/internal/ledger/deposit-observations"
    assert payload["session_public_id"] == "session-partial-bsc"
    assert payload["amount"] == five_usdt_bsc_raw
    assert payload["detection_method"] == "balance_verification"
    assert payload["raw_payload"]["meets_min_amount"] is False
    assert payload["raw_payload"]["observation_min_amount"] == str(one_usdt_bsc_raw)
    assert payload["raw_payload"]["onchain_min_amount"] == str(seven_usdt_bsc_raw)


def test_erc20_observer_ignores_dust_below_observation_minimum(monkeypatch):
    one_usdt_bsc_raw = 10**18
    half_usdt_bsc_raw = 5 * 10**17
    seven_usdt_bsc_raw = 7 * 10**18

    amount_by_block = {"latest": half_usdt_bsc_raw}
    for block_number in range(0, 201):
        amount_by_block[block_number] = half_usdt_bsc_raw

    monkeypatch.setattr(
        observe_once,
        "_build_option_web3",
        lambda **kwargs: FakeWeb3(amount_by_block),
    )

    client = FakeObservationClient()

    option = SimpleNamespace(
        key="bsc-usdt",
        chain="bsc",
        asset_code="USDT",
        token_contract_address="0x55d398326f99059ff775485246999027b3197955",
        observation_min_amount=one_usdt_bsc_raw,
        lookback_blocks=200,
        rpc_urls=["http://fake-rpc"],
        poa_compatible=True,
    )

    watch = {
        "targets": [
            {
                "session_public_id": "session-dust-bsc",
                "deposit_address": "0x1111111111111111111111111111111111111111",
                "required_confirmations": 12,
                "min_amount": 7_000_000,
                "onchain_min_amount": seven_usdt_bsc_raw,
                "amount_unit": "canonical_stable",
            }
        ]
    }

    observe_once._observe_token_option(
        client=client,
        option=option,
        watch=watch,
        reference_heads_base_url="",
        reference_heads_shared_secret="",
        reference_heads_timeout_seconds=1,
        reference_heads_max_age_seconds=60,
        rpc_max_lag_blocks=64,
        rpc_max_reference_lag_blocks=64,
    )

    assert client.payloads == []