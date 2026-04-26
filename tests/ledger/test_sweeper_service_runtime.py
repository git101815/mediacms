from types import SimpleNamespace
from unittest.mock import Mock, patch

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
        poll_interval_seconds=30,
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
        "claim_token": "claim-token-1",
        "address_derivation_ref": f"evm:ethereum:external:{derivation_index}",
        "derivation_index": derivation_index,
    }

    if gas_funding_txid:
        job["gas_funding_txid"] = gas_funding_txid

    if sweep_txid:
        job["sweep_txid"] = sweep_txid

    return job


def test_run_once_pending_job_funds_then_reschedules():
    option = _make_option()
    config = _make_config(option)
    job = _make_job()
    client = Mock()
    client.acquire_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 0,
        "lock_token": "sender-lock-token-1",
        "lock_expires_at": None,
    }
    client.confirm_evm_sender_nonce_used.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 1,
    }
    client.release_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "released": True,
    }
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    w3 = Mock()

    funding_address = "0x" + "22" * 20

    def fake_address_from_private_key(private_key):
        if private_key == option.funding_private_key:
            return funding_address
        return job["source_address"]

    def fake_get_native_balance(*, w3, address):
        if address.lower() == funding_address.lower():
            return 100000
        return 0

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
            patch.object(claim_once, "_pending_nonce", return_value=0), \
            patch.object(claim_once, "_estimate_erc20_transfer_gas", return_value=50000), \
         patch.object(claim_once, "_compute_effective_gas_price_wei", return_value=1), \
         patch.object(claim_once, "address_from_private_key", side_effect=fake_address_from_private_key), \
         patch.object(claim_once, "get_native_balance", side_effect=fake_get_native_balance), \
         patch.object(claim_once, "send_native_transfer", return_value="0xgas"), \
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

    client.acquire_evm_sender_lock.assert_called_once_with(
        chain="ethereum",
        address=funding_address,
        lock_seconds=120,
    )
    client.confirm_evm_sender_nonce_used.assert_called_once_with(
        chain="ethereum",
        address=funding_address,
        lock_token="sender-lock-token-1",
        nonce=0,
        txid="0xgas",
    )
    client.release_evm_sender_lock.assert_not_called()

    client.mark_funding_broadcasted.assert_called_once_with(
        public_id="job-1",
        claim_token=job["claim_token"],
        gas_funding_txid="0xgas",
        destination_address=option.destination_address,
        last_sweep_gas_limit=50000,
    )
    client.mark_rescheduled.assert_called_once_with(
        public_id="job-1",
        claim_token=job["claim_token"],
        next_retry_in_seconds=30,
    )


def test_run_once_resumes_funding_broadcasted_job_without_refunding():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(status="funding_broadcasted", gas_funding_txid="0xgas")
    client = Mock()
    client.acquire_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 0,
        "lock_token": "sender-lock-token-1",
        "lock_expires_at": None,
    }
    client.confirm_evm_sender_nonce_used.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 1,
    }
    client.release_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "released": True,
    }
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    w3 = Mock()

    funding_address = "0x" + "22" * 20

    def fake_address_from_private_key(private_key):
        if private_key == option.funding_private_key:
            return funding_address
        return job["source_address"]

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
            patch.object(claim_once, "_pending_nonce", return_value=0), \
            patch.object(claim_once, "_estimate_erc20_transfer_gas", return_value=50000), \
         patch.object(claim_once, "_compute_effective_gas_price_wei", return_value=1), \
         patch.object(claim_once, "get_receipt_with_confirmations", return_value=({"status": 1, "gasUsed": 21000}, 1)), \
         patch.object(claim_once, "address_from_private_key", side_effect=fake_address_from_private_key), \
         patch.object(claim_once, "get_native_balance", return_value=option.max_gas_funding_amount_wei), \
         patch.object(claim_once, "send_native_transfer") as send_native_transfer, \
         patch.object(claim_once, "get_erc20_balance", return_value=job["amount"]), \
         patch.object(claim_once, "send_erc20_transfer", return_value="0xsweep"):
        claim_once.run_once(client=client, config=config)

    send_native_transfer.assert_not_called()
    client.mark_funding_broadcasted.assert_not_called()
    client.mark_ready_to_sweep.assert_called_once_with(
        public_id="job-1",
        claim_token=job["claim_token"],
    )
    client.mark_sweep_broadcasted.assert_called_once_with(
        public_id="job-1",
        claim_token=job["claim_token"],
        sweep_txid="0xsweep",
        destination_address=option.destination_address,
        last_sweep_gas_limit=50000,
    )
    client.mark_confirmed.assert_not_called()
    client.mark_rescheduled.assert_called_once_with(
        public_id=job["public_id"],
        claim_token=job["claim_token"],
        next_retry_in_seconds=config.poll_interval_seconds,
    )


def test_run_once_confirms_existing_sweep_broadcasted_job_without_rebroadcast():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(status="sweep_broadcasted", sweep_txid="0xsweep")
    client = Mock()
    client.acquire_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 0,
        "lock_token": "sender-lock-token-1",
        "lock_expires_at": None,
    }
    client.confirm_evm_sender_nonce_used.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 1,
    }
    client.release_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "released": True,
    }
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
            patch.object(claim_once, "_pending_nonce", return_value=0), \
            patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "get_receipt_with_confirmations", return_value=({"status": 1, "gasUsed": 50000}, 1)), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "send_native_transfer") as send_native_transfer, \
         patch.object(claim_once, "send_erc20_transfer") as send_erc20_transfer:
        claim_once.run_once(client=client, config=config)

    send_native_transfer.assert_not_called()
    send_erc20_transfer.assert_not_called()
    client.mark_confirmed.assert_called_once_with(
        public_id="job-1",
        claim_token=job["claim_token"],
    )
    client.mark_rescheduled.assert_not_called()


def test_run_once_marks_failed_when_derived_address_mismatches_job():
    option = _make_option()
    config = _make_config(option)
    job = _make_job()
    client = Mock()
    client.acquire_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 0,
        "lock_token": "sender-lock-token-1",
        "lock_expires_at": None,
    }
    client.confirm_evm_sender_nonce_used.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 1,
    }
    client.release_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "released": True,
    }
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = "0x" + "44" * 20

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "_build_option_web3") as build_option_web3:
        claim_once.run_once(client=client, config=config)

    build_option_web3.assert_not_called()
    client.mark_failed.assert_called_once()
    assert client.mark_failed.call_args.kwargs["claim_token"] == job["claim_token"]
    assert "Derived address mismatch" in client.mark_failed.call_args.kwargs["error"]


def test_run_once_reschedules_when_token_balance_is_insufficient():
    option = _make_option()
    config = _make_config(option)
    job = _make_job(amount=250)
    client = Mock()
    client.acquire_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 0,
        "lock_token": "sender-lock-token-1",
        "lock_expires_at": None,
    }
    client.confirm_evm_sender_nonce_used.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "next_nonce": 1,
    }
    client.release_evm_sender_lock.return_value = {
        "chain": option.chain,
        "address": job["source_address"],
        "released": True,
    }
    client.claim_jobs.return_value = [job]

    deriver = Mock()
    deriver.derive_address.return_value = job["source_address"]
    deriver.derive_private_key.return_value = "0x" + "33" * 32

    w3 = Mock()

    with patch.object(claim_once, "EvmDeriver", return_value=deriver), \
         patch.object(claim_once, "_pending_nonce", return_value=0),\
         patch.object(claim_once, "_build_option_web3", return_value=w3), \
         patch.object(claim_once, "_estimate_erc20_transfer_gas", return_value=50000), \
         patch.object(claim_once, "_compute_effective_gas_price_wei", return_value=1), \
         patch.object(claim_once, "address_from_private_key", return_value=job["source_address"]), \
         patch.object(claim_once, "get_native_balance", return_value=50000), \
         patch.object(claim_once, "get_erc20_balance", return_value=249), \
         patch.object(claim_once, "send_native_transfer") as send_native_transfer, \
         patch.object(claim_once, "send_erc20_transfer") as send_erc20_transfer:
        claim_once.run_once(client=client, config=config)

    send_native_transfer.assert_not_called()
    send_erc20_transfer.assert_not_called()
    client.mark_ready_to_sweep.assert_not_called()
    client.mark_sweep_broadcasted.assert_not_called()
    client.mark_confirmed.assert_not_called()
    client.mark_failed.assert_not_called()
    client.mark_rescheduled.assert_called_once_with(
        public_id="job-1",
        claim_token=job["claim_token"],
        next_retry_in_seconds=30,
        error="Source wallet token balance is lower than the expected sweep amount",
        error_code="SWEEP_TOKEN_BALANCE_NOT_READY",
        retryable=True,
        increment_retry_count=True,
    )