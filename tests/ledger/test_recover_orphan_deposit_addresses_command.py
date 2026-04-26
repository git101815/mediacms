# tests/ledger/test_recover_orphan_deposit_addresses_command.py
from datetime import timedelta
from io import StringIO
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.utils import timezone

from ledger.models import DepositSession, OrphanDepositRecoveryAudit
from sweeper_service.app.config import SweepOptionConfig
from ledger.management.commands import recover_orphan_deposit_addresses as command_module

from .base import BaseLedgerTestCase


class TestRecoverOrphanDepositAddressesCommand(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")

    def _make_session(self, *, deposit_address: str, status: str = DepositSession.STATUS_SWEPT) -> DepositSession:
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address=deposit_address.lower(),
            address_derivation_ref="m/44'/60'/0'/0/7",
            derivation_index=7,
            expires_at=timezone.now() - timedelta(days=10),
            status=status,
            required_confirmations=12,
            min_amount=1_000_000,
        )
        DepositSession.objects.filter(id=session.id).update(updated_at=timezone.now() - timedelta(days=10))
        session.refresh_from_db()
        return session

    def _make_runtime(self, *, deposit_address: str):
        deriver = Mock()
        deriver.derive_address.return_value = deposit_address.lower()
        deriver.derive_private_key.return_value = "0x" + "22" * 32

        option = SweepOptionConfig(
            key="ethereum-usdt",
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            rpc_urls=["https://rpc.example"],
            funding_private_key="0x" + "11" * 32,
            destination_address="0x9999999999999999999999999999999999999999",
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

        return command_module.RecoveryRuntimeConfig(
            deriver=deriver,
            option_index={("ethereum", "USDT", "0xdac17f958d2ee523a2206206994597c13d831ec7"): option},
            request_timeout_seconds=10.0,
            rpc_max_lag_blocks=64,
            rpc_max_reference_lag_blocks=64,
            reference_heads_base_url="",
            reference_heads_shared_secret="",
            reference_heads_timeout_seconds=5.0,
            reference_heads_max_age_seconds=60,
        )

    @patch("ledger.management.commands.recover_orphan_deposit_addresses.build_web3")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.choose_best_rpc_url")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._load_runtime_config_from_path")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_native_balance")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_erc20_balance")
    def test_empty_address_is_finalized_once_and_not_rescanned(
        self, mocked_token_balance, mocked_native_balance, mocked_load_runtime, mocked_choose_rpc, mocked_build_web3
    ):
        session = self._make_session(deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        mocked_load_runtime.return_value = self._make_runtime(deposit_address=session.deposit_address)
        mocked_choose_rpc.return_value = "https://rpc.example"
        mocked_build_web3.return_value = Mock()
        mocked_token_balance.return_value = 0
        mocked_native_balance.return_value = 0

        call_command(
            "recover_orphan_deposit_addresses",
            config_path="/tmp/dummy.json",
            chain="ethereum",
            native_price_usd="3000",
            older_than_hours=1,
            stdout=StringIO(),
        )

        audit = OrphanDepositRecoveryAudit.objects.get(chain="ethereum", deposit_address=session.deposit_address)
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_EMPTY_FINAL)

    @patch("ledger.management.commands.recover_orphan_deposit_addresses.release_evm_sender_lock")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.confirm_evm_sender_nonce_used")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.acquire_evm_sender_lock")
    def test_sender_lock_helper_confirms_on_success(self, mocked_acquire, mocked_confirm, mocked_release):
        cmd = command_module.Command()
        cmd._lock_actor = self.operator
        cmd._lock_service_name = "orphan-recovery-command"

        w3 = Mock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.get_transaction_count.return_value = 12

        mocked_acquire.return_value = {"lock_token": "lock-1", "next_nonce": 5}

        with patch("ledger.management.commands.recover_orphan_deposit_addresses.sign_transaction") as mocked_sign, \
             patch("ledger.management.commands.recover_orphan_deposit_addresses.send_signed_transaction") as mocked_send:
            mocked_sign.return_value = {"raw_transaction": b"raw", "txid": "0xtx"}
            mocked_send.return_value = "0xtx"

            txid = cmd._broadcast_with_sender_lock(
                actor=self.operator,
                service_name="orphan-recovery-command",
                chain="ethereum",
                address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                w3=w3,
                signer_private_key="0x" + "11" * 32,
                tx_builder=lambda nonce: {"nonce": nonce},
            )

        self.assertEqual(txid, "0xtx")
        mocked_acquire.assert_called_once()
        mocked_confirm.assert_called_once()
        mocked_release.assert_not_called()

    @patch("ledger.management.commands.recover_orphan_deposit_addresses.release_evm_sender_lock")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.confirm_evm_sender_nonce_used")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.acquire_evm_sender_lock")
    def test_sender_lock_helper_releases_on_prebroadcast_error(self, mocked_acquire, mocked_confirm, mocked_release):
        cmd = command_module.Command()
        cmd._lock_actor = self.operator
        cmd._lock_service_name = "orphan-recovery-command"

        w3 = Mock()
        w3.to_checksum_address.side_effect = lambda x: x
        w3.eth.get_transaction_count.return_value = 10
        mocked_acquire.return_value = {"lock_token": "lock-2", "next_nonce": None}

        def broken_builder(_nonce):
            raise RuntimeError("pre-send error")

        with self.assertRaises(RuntimeError):
            cmd._broadcast_with_sender_lock(
                actor=self.operator,
                service_name="orphan-recovery-command",
                chain="ethereum",
                address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                w3=w3,
                signer_private_key="0x" + "11" * 32,
                tx_builder=broken_builder,
            )

        mocked_confirm.assert_not_called()
        mocked_release.assert_called_once()