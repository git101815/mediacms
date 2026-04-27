from unittest.mock import Mock, patch

from sweeper_service.app import claim_once
from tests.ledger.test_sweeper_service_runtime import _make_config, _make_job, _make_option


def test_residual_balance_snapshot_with_empty_token_balance_does_not_fund_native():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(amount=9999000)
    job["metadata"] = {
        "source": "residual_deposit",
        "observed_transfer_event_key": "ethereum:balance:session:123:9999000",
    }
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "get_erc20_balance", return_value=0), \
         patch.object(claim_once, "get_native_balance") as get_native_balance, \
         patch.object(claim_once, "build_native_transfer_transaction") as build_native_transfer_transaction, \
         patch.object(claim_once, "send_signed_transaction") as send_signed_transaction:
        claim_once.run_once(client=client, config=config)

    get_native_balance.assert_not_called()
    build_native_transfer_transaction.assert_not_called()
    send_signed_transaction.assert_not_called()
    client.mark_failed.assert_called_once()
    assert "SWEEP_RESIDUAL_TOKEN_BALANCE_EMPTY" in client.mark_failed.call_args.kwargs["error"]


def test_non_residual_insufficient_token_balance_reschedules_without_native_funding():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(amount=250)
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "get_erc20_balance", return_value=249), \
         patch.object(claim_once, "get_native_balance") as get_native_balance, \
         patch.object(claim_once, "build_native_transfer_transaction") as build_native_transfer_transaction, \
         patch.object(claim_once, "send_signed_transaction") as send_signed_transaction:
        claim_once.run_once(client=client, config=config)

    get_native_balance.assert_not_called()
    build_native_transfer_transaction.assert_not_called()
    send_signed_transaction.assert_not_called()
    client.mark_rescheduled.assert_called_once()
    assert client.mark_rescheduled.call_args.kwargs["error_code"] == "SWEEP_TOKEN_BALANCE_NOT_READY"
