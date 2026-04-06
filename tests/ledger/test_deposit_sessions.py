from datetime import timedelta

from django.core.exceptions import ValidationError
from django.utils import timezone

from ledger.models import DepositSession, ObservedOnchainTransfer, TokenWallet
from ledger.services import (
    create_deposit_session,
    record_onchain_observation,
    credit_confirmed_deposit_session,
    get_external_asset_clearing_wallet,
)
from .base import BaseLedgerTestCase


class TestDepositSessions(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")
    def test_create_deposit_session_creates_unique_address_session(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/1",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
            metadata={"source": "test"},
        )

        self.assertEqual(session.user_id, self.u1.id)
        self.assertEqual(session.wallet_id, self.w1.id)
        self.assertEqual(session.status, DepositSession.STATUS_AWAITING_PAYMENT)
        self.assertEqual(session.chain, "ethereum")
        self.assertEqual(session.asset_code, "USDT")
        self.assertEqual(session.required_confirmations, 12)
        self.assertEqual(session.min_amount, 100)

    def test_record_onchain_observation_is_idempotent_by_event_key(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x2222222222222222222222222222222222222222",
            address_derivation_ref="m/44'/60'/0'/0/2",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )

        first = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xabc",
            log_index=7,
            block_number=123,
            from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            to_address="0x2222222222222222222222222222222222222222",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=3,
            raw_payload={"txid": "0xabc", "log_index": 7},
        )

        second = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xabc",
            log_index=7,
            block_number=124,
            from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            to_address="0x2222222222222222222222222222222222222222",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=12,
            raw_payload={"txid": "0xabc", "log_index": 7, "block_number": 124},
        )

        self.assertEqual(first.id, second.id)
        self.assertEqual(ObservedOnchainTransfer.objects.count(), 1)

        second.refresh_from_db()
        session.refresh_from_db()

        self.assertEqual(second.confirmations, 12)
        self.assertEqual(second.status, ObservedOnchainTransfer.STATUS_CONFIRMED)
        self.assertEqual(session.status, DepositSession.STATUS_CONFIRMING)
        self.assertEqual(session.confirmations, 12)

    def test_credit_confirmed_deposit_session_creates_single_ledger_credit(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x3333333333333333333333333333333333333333",
            address_derivation_ref="m/44'/60'/0'/0/3",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xdef",
            log_index=4,
            block_number=999,
            from_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            to_address="0x3333333333333333333333333333333333333333",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=6,
            raw_payload={"txid": "0xdef", "log_index": 4},
        )

        txn = credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        session.refresh_from_db()
        observed.refresh_from_db()
        self.w1.refresh_from_db()

        clearing_wallet = get_external_asset_clearing_wallet()
        clearing_wallet.refresh_from_db()

        self.assertEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(observed.status, ObservedOnchainTransfer.STATUS_CREDITED)
        self.assertEqual(session.credited_ledger_txn_id, txn.id)
        self.assertEqual(observed.credited_ledger_txn_id, txn.id)
        self.assertEqual(self.w1.balance, 250)
        self.assertEqual(clearing_wallet.balance, -250)

        replay_txn = credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        self.assertEqual(replay_txn.id, txn.id)
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 250)

    def test_credit_confirmed_deposit_session_rejects_contract_mismatch(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x4444444444444444444444444444444444444444",
            address_derivation_ref="m/44'/60'/0'/0/4",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        with self.assertRaises(ValidationError):
            record_onchain_observation(
                actor=self.operator,
                deposit_session=session,
                chain="ethereum",
                txid="0x987",
                log_index=1,
                block_number=1,
                from_address="0xcccccccccccccccccccccccccccccccccccccccc",
                to_address="0x4444444444444444444444444444444444444444",
                token_contract_address="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                asset_code="USDT",
                amount=250,
                confirmations=6,
                raw_payload={},
            )