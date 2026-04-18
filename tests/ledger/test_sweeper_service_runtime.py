from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import sweeper_service.app.claim_once as claim_once


USDT_ETH = "0xdac17f958d2ee523a2206206994597c13d831ec7"


def _make_option():
    return SimpleNamespace(
        key="ethereum-usdt",
        chain="ethereum",
        asset_code="USDT",
        token_contract_address=USDT_ETH,
        rpc_urls=["https://rpc.example"],
        funding_private_key="0x" + "22" * 32,
        destination_address="0x" + "99" * 20,
        funding_confirmations=1,
        sweep_confirmations=1,
        max_gas_funding_amount_wei=100000,
        erc20_transfer_gas_limit=100000,
        gas_limit_multiplier_bps=12000,
        gas_limit_retry_multiplier_bps=15000,
        tx_timeout_seconds=300,
        gas_price_multiplier_bps=12000,
        poa_compatible=False,
    )


def _make_config(option):
    return SimpleNamespace(
        options=[option],
        claim_batch_size=20,
        mnemonic="test mnemonic",
        mnemonic_passphrase="",
        account_index=0,
        rpc_max_lag_blocks=64,
        rpc_max_reference_lag_blocks=64,
        reference_heads_base_url="https://reference-heads.example",
        reference_heads_shared_secret="secret",
        reference_heads_timeout_seconds=5.0,
        reference_heads_max_age_seconds=60,
        request_timeout_seconds=10.0,
    )


def _make_job(
    *,
    status="pending",
    public_id="job-1",
    source_address=None,
    amount=250,
    derivation_index=42,
    gas_funding_txid=None,
    sweep_txid=None,
):
    if source_address is None:
        source_address = "0x" + "11" * 20

    job = {
        "public_id": public_id,
        "chain": "ethereum",
        "asset_code": "USDT",
        "token_contract_address": USDT_ETH,
        "source_address": source_address,
        "amount": amount,
        "status": status,
        "address_derivation_ref": f"evm:ethereum:external:{derivation_index}",
        "derivation_index": derivation_index,
    }

    if gas_funding_txid:
        job["gas_funding_txid"] = gas_funding_txid

    if sweep_txid:
        job["sweep_txid"] = sweep_txid

    return job


def test_run_once_pending_job_funds_sweeps_and_confirms():
    option = _make_option()
    config = _make_config(option)
    job = _make_job()
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    nonce_allocator = Mock()
    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "NonceAllocator", return_value=nonce_allocator), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "_estimate_erc20_transfer_gas", return_value=50000), \
         patch.object(claim_once, "_compute_effective_gas_price_wei", return_value=1), \
         patch.object(claim_once, "_read_mined_receipt", return_value={"status": 1, "gasUsed": 50000}), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "get_native_balance", return_value=0), \
         patch.object(claim_once, "send_native_transfer", return_value="0xgas"), \
         patch.object(claim_once, "wait_for_confirmations") as wait_for_confirmations, \
         patch.object(claim_once, "get_erc20_balance", return_value=job["amount"]), \
         patch.object(claim_once, "send_erc20_transfer", return_value="0xsweep"):
        claim_once.run_once(client=client, config=config)

    client.claim_jobs.assert_called_once_with(
        options=[
            {
                "chain": "ethereum",
                "asset_code": "USDT",
                "token_contract_address": USDT_ETH,
            }
        ],
        limit=20,
    )

    assert client.method_calls == [
        call.claim_jobs(
            options=[
                {
                    "chain": "ethereum",
                    "asset_code": "USDT",
                    "token_contract_address": USDT_ETH,
                }
            ],
            limit=20,
        ),
        call.mark_funding_broadcasted(
            public_id="job-1",
            gas_funding_txid="0xgas",
            destination_address=option.destination_address,
        ),
        call.mark_ready_to_sweep(public_id="job-1"),
        call.mark_sweep_broadcasted(
            public_id="job-1",
            sweep_txid="0xsweep",
            destination_address=option.destination_address,
        ),
        call.mark_confirmed(public_id="job-1"),
    ]

    assert wait_for_confirmations.call_count == 2
    assert wait_for_confirmations.call_args_list[0].kwargs["txid"] == "0xgas"
    assert wait_for_confirmations.call_args_list[1].kwargs["txid"] == "0xsweep"


def test_run_once_resumes_funding_broadcasted_job_without_refunding():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(status="funding_broadcasted", gas_funding_txid="0xgas")
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    nonce_allocator = Mock()
    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "NonceAllocator", return_value=nonce_allocator), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "_estimate_erc20_transfer_gas", return_value=50000), \
         patch.object(claim_once, "_compute_effective_gas_price_wei", return_value=1), \
         patch.object(claim_once, "_read_mined_receipt", return_value={"status": 1, "gasUsed": 50000}), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "get_native_balance", return_value=option.max_gas_funding_amount_wei), \
         patch.object(claim_once, "send_native_transfer") as send_native_transfer, \
         patch.object(claim_once, "wait_for_confirmations") as wait_for_confirmations, \
         patch.object(claim_once, "get_erc20_balance", return_value=job["amount"]), \
         patch.object(claim_once, "send_erc20_transfer", return_value="0xsweep"):
        claim_once.run_once(client=client, config=config)

    send_native_transfer.assert_not_called()
    client.mark_funding_broadcasted.assert_not_called()
    client.mark_ready_to_sweep.assert_called_once_with(public_id="job-1")
    client.mark_sweep_broadcasted.assert_called_once()
    client.mark_confirmed.assert_called_once_with(public_id="job-1")

    assert wait_for_confirmations.call_count == 2
    assert wait_for_confirmations.call_args_list[0].kwargs["txid"] == "0xgas"
    assert wait_for_confirmations.call_args_list[1].kwargs["txid"] == "0xsweep"


def test_run_once_confirms_existing_sweep_broadcasted_job_without_rebroadcast():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(status="sweep_broadcasted", sweep_txid="0xsweep")
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    nonce_allocator = Mock()
    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "NonceAllocator", return_value=nonce_allocator), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "_read_mined_receipt", return_value={"status": 1, "gasUsed": 50000}), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "wait_for_confirmations") as wait_for_confirmations, \
         patch.object(claim_once, "send_native_transfer") as send_native_transfer, \
         patch.object(claim_once, "send_erc20_transfer") as send_erc20_transfer:
        claim_once.run_once(client=client, config=config)

    send_native_transfer.assert_not_called()
    send_erc20_transfer.assert_not_called()
    client.mark_confirmed.assert_called_once_with(public_id="job-1")
    wait_for_confirmations.assert_called_once()
    assert wait_for_confirmations.call_args.kwargs["txid"] == "0xsweep"


def test_run_once_marks_failed_when_derived_address_mismatches_job():
    option = _make_option()
    config = _make_config(option)
    job = _make_job()
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = "0x" + "44" * 20

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "NonceAllocator", return_value=Mock()), \
         patch.object(claim_once, "_build_option_web3") as build_option_web3:
        claim_once.run_once(client=client, config=config)

    build_option_web3.assert_not_called()
    client.mark_failed.assert_called_once()
    assert "Derived address mismatch" in client.mark_failed.call_args.kwargs["error"]


def test_run_once_marks_failed_when_token_balance_is_insufficient():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(amount=250)
    client = Mock()
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    nonce_allocator = Mock()
    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "NonceAllocator", return_value=nonce_allocator), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "_estimate_erc20_transfer_gas", return_value=50000), \
         patch.object(claim_once, "_compute_effective_gas_price_wei", return_value=1), \
         patch.object(claim_once, "_read_mined_receipt", return_value={"status": 1, "gasUsed": 50000}), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "get_native_balance", return_value=50000), \
         patch.object(claim_once, "get_erc20_balance", return_value=249), \
         patch.object(claim_once, "send_native_transfer") as send_native_transfer, \
         patch.object(claim_once, "send_erc20_transfer") as send_erc20_transfer:
        claim_once.run_once(client=client, config=config)

    send_native_transfer.assert_not_called()
    send_erc20_transfer.assert_not_called()
    client.mark_sweep_broadcasted.assert_not_called()
    client.mark_confirmed.assert_not_called()
    client.mark_failed.assert_called_once()
    assert "Insufficient token balance" in client.mark_failed.call_args.kwargs["error"]