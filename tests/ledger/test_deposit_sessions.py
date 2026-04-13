from datetime import timedelta
from unittest.mock import patch

from django.core.exceptions import PermissionDenied, ValidationError
from django.test import override_settings
from django.utils import timezone

from ledger.models import DepositAddress, DepositSession, DepositSweepJob, ObservedOnchainTransfer
from ledger.services import (
    cancel_user_deposit_session,
    create_deposit_session,
    credit_confirmed_deposit_session,
    delete_user_deposit_session,
    enqueue_deposit_sweep_job,
    expire_stale_deposit_sessions,
    get_external_asset_clearing_wallet,
    open_user_deposit_session,
    record_onchain_observation,
)

from .base import BaseLedgerTestCase


class TestDepositSessions(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")
        self.grant_perm(self.operator, "can_manage_deposit_sessions")

        self.route = DepositAddress.objects.create(
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            display_label="Ethereum · USDT",
            address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="m/44'/60'/0'/0/0",
            derivation_index=0,
            required_confirmations=12,
            min_amount=100,
            session_ttl_seconds=3600,
        )

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_user_deposit_session_creates_session_with_derivation_fields(self, mocked_derive):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "m/44'/60'/0'/0/42",
        )

        session = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
        )

        self.assertEqual(session.wallet_id, self.w1.id)
        self.assertEqual(session.status, DepositSession.STATUS_AWAITING_PAYMENT)
        self.assertEqual(session.route_key, "ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7")
        self.assertEqual(session.deposit_address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(session.derivation_index, 0)
        self.assertEqual(session.derivation_path, "m/44'/60'/0'/0/42")
        self.assertEqual(session.address_derivation_ref, "m/44'/60'/0'/0/42")
        self.assertEqual(session.required_confirmations, 12)
        self.assertEqual(session.min_amount, 100)

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_user_deposit_session_reuses_existing_active_session(self, mocked_derive):
        mocked_derive.return_value = (
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "m/44'/60'/0'/0/1",
        )

        first = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
        )
        second = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
        )

        self.assertEqual(first.id, second.id)
        self.assertEqual(DepositSession.objects.count(), 1)

    @patch("ledger.services._derive_session_deposit_address")
    def test_open_user_deposit_session_does_not_reuse_expired_session(self, mocked_derive):
        mocked_derive.side_effect = [
            ("0xcccccccccccccccccccccccccccccccccccccccc", "m/44'/60'/0'/0/2"),
            ("0xdddddddddddddddddddddddddddddddddddddddd", "m/44'/60'/0'/0/3"),
        ]

        first = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
        )
        DepositSession.objects.filter(id=first.id).update(
            expires_at=timezone.now() - timedelta(seconds=1)
        )

        second = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
        )

        self.assertNotEqual(first.id, second.id)
        self.assertEqual(DepositSession.objects.count(), 2)

    def test_open_user_deposit_session_rejects_wallet_of_another_user(self):
        with self.assertRaises(PermissionDenied):
            open_user_deposit_session(
                actor=self.u1,
                wallet=self.w2,
                option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
            )

    @override_settings(DEPOSIT_EVM_ACCOUNT_XPUB="")
    def test_open_user_deposit_session_rejects_missing_xpub(self):
        with self.assertRaises(ValidationError):
            open_user_deposit_session(
                actor=self.u1,
                wallet=self.w1,
                option_key="ethereum:USDT:0xdac17f958d2ee523a2206206994597c13d831ec7",
            )

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

    def test_cancel_user_deposit_session_rejects_observed_transaction(self):
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

        record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0x987",
            log_index=1,
            block_number=1,
            from_address="0xcccccccccccccccccccccccccccccccccccccccc",
            to_address="0x4444444444444444444444444444444444444444",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=1,
            raw_payload={},
        )

        with self.assertRaises(ValidationError):
            cancel_user_deposit_session(actor=self.u1, deposit_session=session)

    def test_expire_stale_deposit_sessions_expires_only_active_stale_ones(self):
        stale = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x5555555555555555555555555555555555555555",
            address_derivation_ref="m/44'/60'/0'/0/5",
            expires_at=timezone.now() - timedelta(minutes=5),
            required_confirmations=6,
            min_amount=100,
        )
        fresh = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x6666666666666666666666666666666666666666",
            address_derivation_ref="m/44'/60'/0'/0/6",
            expires_at=timezone.now() + timedelta(minutes=5),
            required_confirmations=6,
            min_amount=100,
        )

        count = expire_stale_deposit_sessions(actor=self.operator, limit=100)

        stale.refresh_from_db()
        fresh.refresh_from_db()

        self.assertEqual(count, 1)
        self.assertEqual(stale.status, DepositSession.STATUS_EXPIRED)
        self.assertEqual(fresh.status, DepositSession.STATUS_AWAITING_PAYMENT)

    def test_delete_user_deposit_session_allows_pending_or_canceled_without_observation(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x7777777777777777777777777777777777777777",
            address_derivation_ref="m/44'/60'/0'/0/7",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        cancel_user_deposit_session(actor=self.u1, deposit_session=session)
        delete_user_deposit_session(actor=self.u1, deposit_session=session)

        self.assertFalse(DepositSession.objects.filter(id=session.id).exists())

    def test_delete_user_deposit_session_rejects_when_observation_exists(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x8888888888888888888888888888888888888888",
            address_derivation_ref="m/44'/60'/0'/0/8",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0x654",
            log_index=2,
            block_number=2,
            from_address="0xdddddddddddddddddddddddddddddddddddddddd",
            to_address="0x8888888888888888888888888888888888888888",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=1,
            raw_payload={},
        )

        with self.assertRaises(ValidationError):
            delete_user_deposit_session(actor=self.u1, deposit_session=session)

    def test_mark_sweep_job_confirmed_path_marks_session_swept(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x9999999999999999999999999999999999999999",
            address_derivation_ref="m/44'/60'/0'/0/100",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xabc123",
            log_index=7,
            block_number=123456,
            from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            to_address="0x9999999999999999999999999999999999999999",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=6,
            raw_payload={"txid": "0xabc123", "log_index": 7},
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        job = DepositSweepJob.objects.get(observed_transfer=observed)
        self.assertEqual(job.status, DepositSweepJob.STATUS_PENDING)

        self.assertEqual(session.status, DepositSession.STATUS_CREDITED)