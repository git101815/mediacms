from datetime import timedelta

from django.core.exceptions import ValidationError
from django.utils import timezone
from datetime import datetime, timezone as dt_timezone
from ledger.models import DepositSweepJob, DepositSession, ObservedOnchainTransfer
from ledger.services import (
    create_deposit_session,
    record_onchain_observation,
    credit_confirmed_deposit_session,
    enqueue_deposit_sweep_job,
)
from .base import BaseLedgerTestCase


class TestDepositSweepJobs(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")

    def _create_confirmed_deposit(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x1111111111111111111111111111111111111111",
            address_derivation_ref="evm:ethereum:external:100",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xabc",
            log_index=7,
            block_number=123456,
            from_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            to_address="0x1111111111111111111111111111111111111111",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=6,
            raw_payload={"txid": "0xabc", "log_index": 7},
        )

        txn = credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        session.refresh_from_db()
        observed.refresh_from_db()

        return session, observed, txn

    def test_credit_confirmed_deposit_session_creates_single_sweep_job(self):
        session, observed, txn = self._create_confirmed_deposit()

        job = DepositSweepJob.objects.get(observed_transfer=observed)

        self.assertEqual(job.deposit_session_id, session.id)
        self.assertEqual(job.status, DepositSweepJob.STATUS_PENDING)
        self.assertEqual(job.amount, observed.amount)
        self.assertEqual(job.source_address, session.deposit_address)
        self.assertEqual(job.address_derivation_ref, session.address_derivation_ref)
        self.assertEqual(job.derivation_index, 100)

    def test_replaying_credit_does_not_create_second_sweep_job(self):
        session, observed, txn = self._create_confirmed_deposit()

        replay_txn = credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        self.assertEqual(replay_txn.id, txn.id)
        self.assertEqual(DepositSweepJob.objects.filter(observed_transfer=observed).count(), 1)

    def test_enqueue_rejects_non_credited_deposit(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0x2222222222222222222222222222222222222222",
            address_derivation_ref="evm:ethereum:external:101",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=6,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xdef",
            log_index=8,
            block_number=123457,
            from_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            to_address="0x2222222222222222222222222222222222222222",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=6,
            raw_payload={"txid": "0xdef", "log_index": 8},
        )

        with self.assertRaises(ValidationError):
            enqueue_deposit_sweep_job(
                actor=self.operator,
                deposit_session=session,
                observed_transfer=observed,
            )

    def test_enqueue_detects_immutable_mismatch_on_existing_job(self):
        session, observed, txn = self._create_confirmed_deposit()

        job = DepositSweepJob.objects.get(observed_transfer=observed)
        job.amount = job.amount + 1
        job.save(update_fields=["amount"])

        with self.assertRaises(ValidationError):
            enqueue_deposit_sweep_job(
                actor=self.operator,
                deposit_session=session,
                observed_transfer=observed,
            )