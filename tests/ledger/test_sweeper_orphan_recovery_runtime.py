from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from sweeper_service.app import orphan_recovery as worker
from sweeper_service.app.config import SweepOptionConfig


TOKEN_CONTRACT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
SOURCE_ADDRESS = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
DESTINATION_ADDRESS = "0x9999999999999999999999999999999999999999"
FUNDING_ADDRESS = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
SOURCE_KEY = "0x" + "22" * 32
FUNDING_KEY = "0x" + "11" * 32


class RecordingClient:
    def __init__(self):
        self.results = []
        self.confirm_evm_sender_nonce_used = Mock()

    def record_orphan_recovery_result(self, *, session_public_id, claim_token, result):
        copied = dict(result)
        self.results.append(
            {
                "session_public_id": session_public_id,
                "claim_token": claim_token,
                "result": copied,
            }
        )
        return {"status": copied["status"]}


def _option():
    return SweepOptionConfig(
        key="ethereum-usdt",
        chain="ethereum",
        asset_code="USDT",
        token_contract_address=TOKEN_CONTRACT,
        rpc_urls=["https://rpc.example"],
        funding_private_key=FUNDING_KEY,
        destination_address=DESTINATION_ADDRESS,
        funding_confirmations=1,
        sweep_confirmations=1,
        max_gas_funding_amount_wei=10**18,
        erc20_transfer_gas_limit=100000,
        gas_limit_multiplier_bps=12000,
        gas_limit_retry_multiplier_bps=15000,
        tx_timeout_seconds=300,
        gas_price_multiplier_bps=12000,
        poa_compatible=False,
    )


def _config():
    return SimpleNamespace(
        mnemonic="test mnemonic",
        mnemonic_passphrase="",
        account_index=0,
        evm_sender_lock_seconds=120,
    )


def _candidate(*, existing_audit=None):
    return {
        "session_public_id": "11111111-1111-1111-1111-111111111111",
        "claim_token": "claim-token",
        "chain": "ethereum",
        "asset_code": "USDT",
        "token_contract_address": TOKEN_CONTRACT,
        "source_address": SOURCE_ADDRESS,
        "address_derivation_ref": "m/44'/60'/0'/0/7",
        "derivation_index": 7,
        "onchain_decimals": 6,
        "existing_audit": dict(existing_audit or {}),
    }


def _deriver(*, address=SOURCE_ADDRESS):
    value = Mock()
    value.derive_address.return_value = address
    value.derive_private_key.return_value = SOURCE_KEY
    return value


def _option_index(option):
    return {("ethereum", "USDT", TOKEN_CONTRACT): option}


def _address_for_key(private_key):
    if private_key == SOURCE_KEY:
        return SOURCE_ADDRESS
    if private_key == FUNDING_KEY:
        return FUNDING_ADDRESS
    raise AssertionError(f"unexpected private key in test: {private_key}")


def _runtime_quote():
    return Decimal("3000"), {
        "asset": "ETH",
        "currency": "USD",
        "price": "3000",
        "source": "test",
    }


def test_derivation_mismatch_never_broadcasts():
    option = _option()
    client = RecordingClient()
    candidate = _candidate()

    with patch.object(
        worker,
        "EvmDeriver",
        return_value=_deriver(address="0xcccccccccccccccccccccccccccccccccccccccc"),
    ), patch.object(worker, "_broadcast_with_sender_lock") as broadcast:
        status = worker._process_candidate(
            client=client,
            config=_config(),
            candidate=candidate,
            option_index=_option_index(option),
            price_cache={},
        )

    assert status == worker.STATUS_IGNORED_FINAL
    assert client.results[-1]["result"]["decision_reason"] == "derivation_mismatch"
    broadcast.assert_not_called()


def test_dust_candidate_never_broadcasts():
    option = _option()
    client = RecordingClient()
    candidate = _candidate()

    with patch.object(worker, "EvmDeriver", return_value=_deriver()), patch.object(
        worker, "address_from_private_key", side_effect=_address_for_key
    ), patch.object(worker, "_build_option_web3", return_value=object()), patch.object(
        worker, "get_erc20_balance", return_value=1_000_000
    ), patch.object(worker, "get_native_balance", return_value=0), patch.object(
        worker, "_runtime_price_for_chain", return_value=_runtime_quote()
    ), patch.object(
        worker, "_compute_effective_gas_price_wei", return_value=1_000_000_000
    ), patch.object(
        worker, "_compute_native_transfer_fee_wei", return_value=21_000_000_000_000
    ), patch.object(
        worker, "_estimate_erc20_transfer_gas", return_value=50_000
    ), patch.object(worker, "_broadcast_with_sender_lock") as broadcast:
        status = worker._process_candidate(
            client=client,
            config=_config(),
            candidate=candidate,
            option_index=_option_index(option),
            price_cache={},
        )

    assert status == worker.STATUS_DUST_FINAL
    assert client.results[-1]["result"]["decision_reason"] == "below_profit_threshold"
    broadcast.assert_not_called()


def test_broadcast_persists_progress_before_nonce_confirmation_and_keeps_lock_on_ambiguity():
    client = RecordingClient()
    client.confirm_evm_sender_nonce_used.side_effect = RuntimeError("backend confirm unavailable")
    progress = []

    with patch.object(
        worker,
        "_reserve_sender_nonce",
        return_value={
            "address": SOURCE_ADDRESS,
            "lock_token": "lock-1",
            "nonce": 7,
        },
    ), patch.object(
        worker,
        "sign_transaction",
        return_value={"raw_transaction": b"raw", "txid": "0xabc"},
    ), patch.object(worker, "send_signed_transaction", return_value="0xabc"), patch.object(
        worker, "_release_sender_lock_safely"
    ) as release:
        with pytest.raises(RuntimeError, match="backend confirm unavailable"):
            worker._broadcast_with_sender_lock(
                client=client,
                config=_config(),
                option=_option(),
                w3=object(),
                address=SOURCE_ADDRESS,
                signer_private_key=SOURCE_KEY,
                tx_builder=lambda nonce: {"nonce": nonce},
                progress_callback=progress.append,
            )

    assert progress == ["0xabc"]
    client.confirm_evm_sender_nonce_used.assert_called_once()
    release.assert_not_called()


def test_prebroadcast_failure_releases_sender_lock():
    client = RecordingClient()

    with patch.object(
        worker,
        "_reserve_sender_nonce",
        return_value={
            "address": SOURCE_ADDRESS,
            "lock_token": "lock-2",
            "nonce": 9,
        },
    ), patch.object(worker, "sign_transaction") as sign, patch.object(
        worker, "send_signed_transaction"
    ) as send, patch.object(worker, "_release_sender_lock_safely") as release:
        with pytest.raises(RuntimeError, match="prebroadcast failure"):
            worker._broadcast_with_sender_lock(
                client=client,
                config=_config(),
                option=_option(),
                w3=object(),
                address=SOURCE_ADDRESS,
                signer_private_key=SOURCE_KEY,
                tx_builder=lambda _nonce: (_ for _ in ()).throw(
                    RuntimeError("prebroadcast failure")
                ),
            )

    sign.assert_not_called()
    send.assert_not_called()
    client.confirm_evm_sender_nonce_used.assert_not_called()
    release.assert_called_once()


def test_retryable_report_does_not_erase_txid_persisted_by_broadcast_callback():
    option = _option()
    client = RecordingClient()
    candidate = _candidate()

    def native_balance(*, w3, address):
        del w3
        return 0 if address.lower() == SOURCE_ADDRESS else 10**18

    def broadcast_then_fail(*, progress_callback, **_kwargs):
        progress_callback("0xfunding")
        raise RuntimeError("nonce confirmation failed after broadcast")

    with patch.object(worker, "EvmDeriver", return_value=_deriver()), patch.object(
        worker, "address_from_private_key", side_effect=_address_for_key
    ), patch.object(worker, "_build_option_web3", return_value=object()), patch.object(
        worker, "get_erc20_balance", return_value=10_000_000
    ), patch.object(worker, "get_native_balance", side_effect=native_balance), patch.object(
        worker, "_runtime_price_for_chain", return_value=_runtime_quote()
    ), patch.object(
        worker, "_compute_effective_gas_price_wei", return_value=1_000_000_000
    ), patch.object(
        worker, "_compute_native_transfer_fee_wei", return_value=21_000_000_000_000
    ), patch.object(
        worker, "_estimate_erc20_transfer_gas", return_value=50_000
    ), patch.object(
        worker, "_broadcast_with_sender_lock", side_effect=broadcast_then_fail
    ), patch.object(worker.logging, "exception"):
        status = worker._process_candidate(
            client=client,
            config=_config(),
            candidate=candidate,
            option_index=_option_index(option),
            price_cache={},
        )

    assert status == worker.STATUS_RETRYABLE_ERROR
    assert len(client.results) == 2
    assert client.results[0]["result"]["status"] == worker.STATUS_PENDING_CHECK
    assert client.results[0]["result"]["funding_txid"] == "0xfunding"
    assert client.results[1]["result"]["status"] == worker.STATUS_RETRYABLE_ERROR
    assert client.results[1]["result"]["funding_txid"] == "0xfunding"
    assert candidate["existing_audit"]["funding_txid"] == "0xfunding"


def test_retry_with_existing_token_txid_reconciles_without_second_broadcast():
    option = _option()
    client = RecordingClient()
    candidate = _candidate(existing_audit={"token_sweep_txid": "0xtoken"})

    with patch.object(worker, "EvmDeriver", return_value=_deriver()), patch.object(
        worker, "address_from_private_key", side_effect=_address_for_key
    ), patch.object(worker, "_build_option_web3", return_value=object()), patch.object(
        worker, "get_erc20_balance", side_effect=[10_000_000, 0]
    ), patch.object(worker, "get_native_balance", return_value=0), patch.object(
        worker, "_runtime_price_for_chain", return_value=_runtime_quote()
    ), patch.object(worker, "wait_for_confirmations") as wait, patch.object(
        worker, "_broadcast_with_sender_lock"
    ) as broadcast:
        status = worker._process_candidate(
            client=client,
            config=_config(),
            candidate=candidate,
            option_index=_option_index(option),
            price_cache={},
        )

    assert status == worker.STATUS_SWEPT_TOKEN_FINAL
    wait.assert_called_once()
    assert wait.call_args.kwargs["txid"] == "0xtoken"
    broadcast.assert_not_called()
    assert client.results[-1]["result"]["token_sweep_txid"] == "0xtoken"
    assert (
        client.results[-1]["result"]["decision_reason"]
        == "reconciled_previous_token_sweep"
    )
