from datetime import timedelta
from io import StringIO
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from ledger.models import DepositSession, OrphanDepositRecoveryAudit
from sweeper_service.app.config import SweepOptionConfig
from ledger.management.commands import recover_orphan_deposit_addresses as command_module

from .base import BaseLedgerTestCase


class TestRecoverOrphanDepositAddressesCommand(BaseLedgerTestCase):
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
        DepositSession.objects.filter(id=session.id).update(
            updated_at=timezone.now() - timedelta(days=10)
        )
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
            option_index={
                ("ethereum", "USDT", "0xdac17f958d2ee523a2206206994597c13d831ec7"): option
            },
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
        self,
        mocked_token_balance,
        mocked_native_balance,
        mocked_load_runtime,
        mocked_choose_rpc,
        mocked_build_web3,
    ):
        session = self._make_session(
            deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        mocked_load_runtime.return_value = self._make_runtime(
            deposit_address=session.deposit_address
        )
        mocked_choose_rpc.return_value = "https://rpc.example"
        mocked_build_web3.return_value = Mock()
        mocked_token_balance.return_value = 0
        mocked_native_balance.return_value = 0

        stdout = StringIO()
        call_command(
            "recover_orphan_deposit_addresses",
            config_path="/tmp/dummy.json",
            chain="ethereum",
            native_price_usd="3000",
            older_than_hours=1,
            stdout=stdout,
        )

        audit = OrphanDepositRecoveryAudit.objects.get(
            chain="ethereum",
            deposit_address=session.deposit_address,
        )
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_EMPTY_FINAL)

        with patch(
            "ledger.management.commands.recover_orphan_deposit_addresses.get_erc20_balance",
            side_effect=AssertionError("should not rescan terminal empty addresses"),
        ), patch(
            "ledger.management.commands.recover_orphan_deposit_addresses.get_native_balance",
            side_effect=AssertionError("should not rescan terminal empty addresses"),
        ):
            call_command(
                "recover_orphan_deposit_addresses",
                config_path="/tmp/dummy.json",
                chain="ethereum",
                native_price_usd="3000",
                older_than_hours=1,
                stdout=StringIO(),
            )

    @patch("ledger.management.commands.recover_orphan_deposit_addresses._compute_effective_gas_price_wei")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._estimate_erc20_transfer_gas")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.build_web3")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.choose_best_rpc_url")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._load_runtime_config_from_path")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_native_balance")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_erc20_balance")
    def test_unprofitable_residual_is_marked_dust_final(
        self,
        mocked_token_balance,
        mocked_native_balance,
        mocked_load_runtime,
        mocked_choose_rpc,
        mocked_build_web3,
        mocked_estimate_gas,
        mocked_effective_gas_price,
    ):
        session = self._make_session(
            deposit_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        )
        mocked_load_runtime.return_value = self._make_runtime(
            deposit_address=session.deposit_address
        )
        mocked_choose_rpc.return_value = "https://rpc.example"
        mocked_build_web3.return_value = Mock()
        mocked_token_balance.return_value = 1_000_000
        mocked_native_balance.return_value = 0
        mocked_estimate_gas.return_value = 50000
        mocked_effective_gas_price.return_value = 1_000_000_000

        call_command(
            "recover_orphan_deposit_addresses",
            config_path="/tmp/dummy.json",
            chain="ethereum",
            native_price_usd="3000",
            older_than_hours=1,
            stdout=StringIO(),
        )

        audit = OrphanDepositRecoveryAudit.objects.get(
            chain="ethereum",
            deposit_address=session.deposit_address,
        )
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_DUST_FINAL)
        self.assertEqual(audit.decision_reason, "below_profit_threshold")

    @patch("ledger.management.commands.recover_orphan_deposit_addresses.wait_for_confirmations")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.send_erc20_transfer")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.send_native_transfer")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.address_from_private_key")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._compute_effective_gas_price_wei")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._estimate_erc20_transfer_gas")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.build_web3")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.choose_best_rpc_url")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._load_runtime_config_from_path")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_native_balance")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_erc20_balance")
    def test_profitable_token_residual_is_recovered(
        self,
        mocked_token_balance,
        mocked_native_balance,
        mocked_load_runtime,
        mocked_choose_rpc,
        mocked_build_web3,
        mocked_estimate_gas,
        mocked_effective_gas_price,
        mocked_address_from_private_key,
        mocked_send_native,
        mocked_send_token,
        mocked_wait,
    ):
        session = self._make_session(
            deposit_address="0xcccccccccccccccccccccccccccccccccccccccc"
        )
        mocked_load_runtime.return_value = self._make_runtime(
            deposit_address=session.deposit_address
        )
        mocked_choose_rpc.return_value = "https://rpc.example"
        mocked_build_web3.return_value = Mock()
        mocked_token_balance.side_effect = [20_000_000, 0]
        mocked_estimate_gas.return_value = 50000
        mocked_effective_gas_price.return_value = 1_000_000_000
        mocked_address_from_private_key.return_value = "0xffffffffffffffffffffffffffffffffffffffff"
        mocked_send_native.return_value = "0xfunding"
        mocked_send_token.return_value = "0xtoken"

        def native_balance_side_effect(*, w3, address):
            if address.lower() == session.deposit_address.lower():
                return 0
            return 10**18

        mocked_native_balance.side_effect = native_balance_side_effect

        call_command(
            "recover_orphan_deposit_addresses",
            config_path="/tmp/dummy.json",
            chain="ethereum",
            native_price_usd="3000",
            older_than_hours=1,
            commit=True,
            stdout=StringIO(),
        )

        audit = OrphanDepositRecoveryAudit.objects.get(
            chain="ethereum",
            deposit_address=session.deposit_address,
        )
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_SWEPT_TOKEN_FINAL)
        self.assertEqual(audit.funding_txid, "0xfunding")
        self.assertEqual(audit.token_sweep_txid, "0xtoken")
        self.assertEqual(audit.native_sweep_txid, "")
        mocked_send_native.assert_called_once()
        mocked_send_token.assert_called_once()
        self.assertGreaterEqual(mocked_wait.call_count, 2)

    @patch("ledger.management.commands.recover_orphan_deposit_addresses.wait_for_confirmations")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.send_native_transfer")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._compute_native_transfer_fee_wei")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.build_web3")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.choose_best_rpc_url")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses._load_runtime_config_from_path")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_native_balance")
    @patch("ledger.management.commands.recover_orphan_deposit_addresses.get_erc20_balance")
    def test_profitable_native_residual_is_recovered(
        self,
        mocked_token_balance,
        mocked_native_balance,
        mocked_load_runtime,
        mocked_choose_rpc,
        mocked_build_web3,
        mocked_native_fee,
        mocked_send_native,
        mocked_wait,
    ):
        session = self._make_session(
            deposit_address="0xdddddddddddddddddddddddddddddddddddddddd"
        )
        mocked_load_runtime.return_value = self._make_runtime(
            deposit_address=session.deposit_address
        )
        mocked_choose_rpc.return_value = "https://rpc.example"
        mocked_build_web3.return_value = Mock()
        mocked_token_balance.return_value = 0
        mocked_native_balance.side_effect = [20_000_000_000_000_000, 0]
        mocked_native_fee.return_value = 21_000_000_000_000
        mocked_send_native.return_value = "0xnative"

        call_command(
            "recover_orphan_deposit_addresses",
            config_path="/tmp/dummy.json",
            chain="ethereum",
            native_price_usd="3000",
            older_than_hours=1,
            commit=True,
            stdout=StringIO(),
        )

        audit = OrphanDepositRecoveryAudit.objects.get(
            chain="ethereum",
            deposit_address=session.deposit_address,
        )
        self.assertEqual(audit.status, OrphanDepositRecoveryAudit.STATUS_SWEPT_NATIVE_FINAL)
        self.assertEqual(audit.native_sweep_txid, "0xnative")
        mocked_send_native.assert_called_once()
        mocked_wait.assert_called_once()