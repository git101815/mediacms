from datetime import timedelta, timezone as dt_timezone
from unittest.mock import patch

from django.test import override_settings
from django.utils import timezone

from ledger.models import DepositSession, DepositSweepJob, ObservedOnchainTransfer
from ledger.services import (
    claim_deposit_sweep_jobs,
    create_deposit_session,
    credit_confirmed_deposit_session,
    list_active_deposit_watch_targets,
    mark_sweep_job_confirmed,
    mark_sweep_job_ready_to_sweep,
    mark_sweep_job_sweep_broadcasted,
    record_onchain_observation,
)

from .base import BaseLedgerTestCase


@override_settings(LEDGER_RESIDUAL_DEPOSIT_WATCH_SECONDS=3600)
class TestResidualDepositWatchActivity(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")
        self.grant_perm(self.operator, "can_view_deposit_sessions")

        self.option_rows = [
            {
                "chain": "ethereum",
                "asset_code": "USDT",
                "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            }
        ]

    def _create_credited_session(self):
        session = create_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="USDT",
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            address_derivation_ref="m/44'/60'/0'/0/700",
            expires_at=timezone.now() + timedelta(hours=1),
            required_confirmations=12,
            min_amount=100,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xprimary",
            log_index=1,
            block_number=100,
            from_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            to_address=session.deposit_address,
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=250,
            confirmations=12,
            raw_payload={"source": "residual-watch-test", "txid": "0xprimary"},
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )
        session.refresh_from_db()
        return session

    def _record_residual_transfer(self, *, session, txid, log_index=2):
        return record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid=txid,
            log_index=log_index,
            block_number=200 + log_index,
            from_address="0xcccccccccccccccccccccccccccccccccccccccc",
            to_address=session.deposit_address,
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=50,
            confirmations=12,
            raw_payload={"source": "residual-watch-test", "txid": txid},
        )

    def _assert_session_is_residual_watch_target(self, *, session, now_value):
        targets = list_active_deposit_watch_targets(
            actor=self.operator,
            option_rows=self.option_rows,
        )
        self.assertEqual(len(targets), 1)
        self.assertEqual(len(targets[0]["targets"]), 1)

        target = targets[0]["targets"][0]
        self.assertEqual(target["session_public_id"], str(session.public_id))
        self.assertEqual(target["deposit_address"], session.deposit_address)
        self.assertEqual(target["watch_reason"], "residual")
        self.assertEqual(target["auto_credit"], False)

    @patch("ledger.services.timezone.now")
    def test_residual_observation_refreshes_watch_window(self, mocked_now):
        base_now = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        mocked_now.return_value = base_now

        session = self._create_credited_session()
        DepositSession.objects.filter(id=session.id).update(
            updated_at=base_now - timedelta(seconds=3500)
        )
        session.refresh_from_db()

        observed = self._record_residual_transfer(
            session=session,
            txid="0xresidual1",
            log_index=2,
        )

        observed.refresh_from_db()
        session.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(observed.status, ObservedOnchainTransfer.STATUS_CONFIRMED)
        self.assertTrue((observed.raw_payload or {}).get("ledger_residual_deposit"))
        self.assertEqual(self.w1.balance, 25000)
        self.assertEqual(
            DepositSweepJob.objects.filter(observed_transfer=observed).count(),
            1,
        )
        self.assertGreaterEqual(session.updated_at, base_now)

        mocked_now.return_value = base_now + timedelta(seconds=1800)
        self._assert_session_is_residual_watch_target(
            session=session,
            now_value=mocked_now.return_value,
        )

    @patch("ledger.services.timezone.now")
    def test_residual_sweep_confirmation_refreshes_watch_window(self, mocked_now):
        base_now = timezone.datetime(2026, 4, 6, 12, 0, 0, tzinfo=dt_timezone.utc)
        mocked_now.return_value = base_now

        session = self._create_credited_session()
        observed = self._record_residual_transfer(
            session=session,
            txid="0xresidual2",
            log_index=3,
        )

        job = DepositSweepJob.objects.get(observed_transfer=observed)
        DepositSession.objects.filter(id=session.id).update(
            updated_at=base_now - timedelta(seconds=3500)
        )
        session.refresh_from_db()

        claimed = claim_deposit_sweep_jobs(
            actor=self.operator,
            service_name="residual-watch-test-sweeper",
            option_rows=self.option_rows,
            limit=10,
            lease_seconds=300,
        )
        claimed_job = next(row for row in claimed if row["public_id"] == str(job.public_id))
        claim_token = claimed_job["claim_token"]

        mark_sweep_job_ready_to_sweep(
            actor=self.operator,
            public_id=job.public_id,
            service_name="residual-watch-test-sweeper",
            claim_token=claim_token,
        )
        mark_sweep_job_sweep_broadcasted(
            actor=self.operator,
            public_id=job.public_id,
            service_name="residual-watch-test-sweeper",
            claim_token=claim_token,
            sweep_txid="0xsweepresidual2",
            destination_address=session.deposit_address,
        )
        mark_sweep_job_confirmed(
            actor=self.operator,
            public_id=job.public_id,
            service_name="residual-watch-test-sweeper",
            claim_token=claim_token,
        )

        session.refresh_from_db()
        self.assertGreaterEqual(session.updated_at, base_now)

        mocked_now.return_value = base_now + timedelta(seconds=1800)
        self._assert_session_is_residual_watch_target(
            session=session,
            now_value=mocked_now.return_value,
        )


    def _record_residual_balance_snapshot(self, *, session, amount, detected_block_number):
        return record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="",
            log_index=None,
            block_number=None,
            detected_block_number=detected_block_number,
            detection_method="balance_verification",
            from_address="",
            to_address=session.deposit_address,
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=amount,
            confirmations=12,
            raw_payload={
                "source": "residual-balance-snapshot-test",
                "detected_block_number": detected_block_number,
                "amount": amount,
            },
        )

    def _confirm_sweep_job(self, *, job, service_name, sweep_txid):
        claimed = claim_deposit_sweep_jobs(
            actor=self.operator,
            service_name=service_name,
            option_rows=self.option_rows,
            limit=10,
            lease_seconds=300,
        )
        claimed_job = next(row for row in claimed if row["public_id"] == str(job.public_id))
        claim_token = claimed_job["claim_token"]

        mark_sweep_job_ready_to_sweep(
            actor=self.operator,
            public_id=job.public_id,
            service_name=service_name,
            claim_token=claim_token,
        )
        mark_sweep_job_sweep_broadcasted(
            actor=self.operator,
            public_id=job.public_id,
            service_name=service_name,
            claim_token=claim_token,
            sweep_txid=sweep_txid,
            destination_address=job.source_address,
        )
        mark_sweep_job_confirmed(
            actor=self.operator,
            public_id=job.public_id,
            service_name=service_name,
            claim_token=claim_token,
        )

    def test_residual_balance_snapshots_coalesce_into_one_active_residual_sweep_job(self):
        session = self._create_credited_session()
        primary_job = DepositSweepJob.objects.get(metadata__source="credited_deposit")
        self._confirm_sweep_job(
            job=primary_job,
            service_name="residual-balance-primary-sweeper",
            sweep_txid="0xprimarysweep",
        )
        session.refresh_from_db()
        self.assertEqual(session.status, DepositSession.STATUS_SWEPT)

        first = self._record_residual_balance_snapshot(
            session=session,
            amount=50,
            detected_block_number=300,
        )
        first_job = DepositSweepJob.objects.get(observed_transfer=first)
        self.assertEqual(first_job.amount, 50)
        self.assertEqual((first_job.metadata or {}).get("source"), "residual_deposit")

        second = self._record_residual_balance_snapshot(
            session=session,
            amount=120,
            detected_block_number=301,
        )

        self.assertFalse(DepositSweepJob.objects.filter(observed_transfer=second).exists())

        first_job.refresh_from_db()
        second.refresh_from_db()

        self.assertEqual(first_job.amount, 120)
        self.assertEqual(
            DepositSweepJob.objects.filter(
                chain=session.chain,
                asset_code=session.asset_code,
                token_contract_address=session.token_contract_address,
                source_address=session.deposit_address,
                status__in=[
                    DepositSweepJob.STATUS_PENDING,
                    DepositSweepJob.STATUS_FUNDING_BROADCASTED,
                    DepositSweepJob.STATUS_READY_TO_SWEEP,
                    DepositSweepJob.STATUS_SWEEP_BROADCASTED,
                ],
            ).count(),
            1,
        )

        metadata = first_job.metadata or {}
        coalesced = metadata.get("coalesced_residual_balance_observations") or []
        self.assertEqual(len(coalesced), 1)
        self.assertEqual(coalesced[0]["event_key"], second.event_key)
        self.assertEqual(coalesced[0]["amount"], 120)

        raw_payload = second.raw_payload or {}
        self.assertTrue(raw_payload.get("residual_coalesced_into_active_sweep_job"))
        self.assertEqual(raw_payload.get("coalesced_sweep_job_public_id"), str(first_job.public_id))
        self.assertEqual(raw_payload.get("coalesced_sweep_job_amount"), 120)

    def test_residual_balance_snapshot_coalesces_into_existing_primary_sweep_job_before_first_sweep(self):
        session = self._create_credited_session()
        primary_job = DepositSweepJob.objects.get(metadata__source="credited_deposit")
        self.assertEqual(primary_job.status, DepositSweepJob.STATUS_PENDING)
        self.assertEqual(primary_job.amount, 250)

        residual_snapshot = self._record_residual_balance_snapshot(
            session=session,
            amount=375,
            detected_block_number=400,
        )

        self.assertFalse(DepositSweepJob.objects.filter(observed_transfer=residual_snapshot).exists())

        primary_job.refresh_from_db()
        residual_snapshot.refresh_from_db()

        self.assertEqual(primary_job.amount, 375)
        self.assertEqual(DepositSweepJob.objects.filter(status=DepositSweepJob.STATUS_PENDING).count(), 1)

        metadata = primary_job.metadata or {}
        self.assertTrue(metadata.get("has_coalesced_residual_balance_observations"))
        self.assertEqual(metadata.get("latest_residual_balance_observed_amount"), 375)

        raw_payload = residual_snapshot.raw_payload or {}
        self.assertTrue(raw_payload.get("residual_coalesced_into_active_sweep_job"))
        self.assertEqual(raw_payload.get("coalesced_sweep_job_public_id"), str(primary_job.public_id))

