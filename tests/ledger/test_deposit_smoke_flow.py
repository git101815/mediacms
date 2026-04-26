from unittest.mock import patch

from ledger.models import DepositSession, DepositSweepJob
from ledger.services import (
    claim_deposit_sweep_jobs,
    credit_confirmed_deposit_session,
    list_active_deposit_watch_targets,
    mark_sweep_job_confirmed,
    mark_sweep_job_funding_broadcasted,
    mark_sweep_job_ready_to_sweep,
    mark_sweep_job_sweep_broadcasted,
    open_user_deposit_session,
    record_onchain_observation,
)
from tests.ledger.test_deposit_sessions import TestDepositSessions


class TestDepositSmokeFlow(TestDepositSessions):
    @patch("ledger.services._derive_session_deposit_address")
    def test_full_deposit_smoke_flow(self, mocked_derive):
        self.grant_perm(self.operator, "can_view_deposit_sessions")
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")

        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "m/44'/60'/0'/0/0",
        )

        option_key = self.default_deposit_option_key()
        option_rows = [
            {
                "chain": "ethereum",
                "asset_code": "USDT",
                "token_contract_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            }
        ]
        service_name = "smoke-test-sweeper"

        session = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key=option_key,
            token_pack=self.default_token_pack,
        )

        self.assertEqual(session.wallet_id, self.w1.id)
        self.assertEqual(session.status, DepositSession.STATUS_AWAITING_PAYMENT)
        self.assertEqual(session.route_key, option_key)
        self.assertEqual(session.derivation_index, 1)
        self.assertEqual(session.derivation_path, "m/44'/60'/0'/0/0")
        self.assertEqual(session.deposit_address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(session.min_amount, self.default_token_pack.gross_stable_amount)

        watch_targets = list_active_deposit_watch_targets(
            actor=self.operator,
            option_rows=option_rows,
        )
        self.assertEqual(len(watch_targets), 1)
        self.assertIn("targets", watch_targets[0])
        self.assertEqual(len(watch_targets[0]["targets"]), 1)
        self.assertEqual(
            watch_targets[0]["targets"][0]["session_public_id"],
            str(session.public_id),
        )
        self.assertEqual(
            watch_targets[0]["targets"][0]["deposit_address"],
            session.deposit_address,
        )

        observed = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="0xsmokeflow0001",
            log_index=1,
            block_number=123456,
            from_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            to_address=session.deposit_address,
            token_contract_address="0xdac17f958d2ee523a2206206994597c13d831ec7",
            asset_code="USDT",
            amount=session.min_amount,
            confirmations=session.required_confirmations,
            raw_payload={"source": "smoke-flow-test", "txid": "0xsmokeflow0001"},
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=observed,
            created_by=self.u1,
        )

        session.refresh_from_db()
        self.assertEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(session.observed_txid, "0xsmokeflow0001")
        self.assertEqual(session.confirmations, session.required_confirmations)

        job = DepositSweepJob.objects.get(deposit_session=session, observed_transfer=observed)
        self.assertEqual(job.status, DepositSweepJob.STATUS_PENDING)

        claimed = claim_deposit_sweep_jobs(
            actor=self.operator,
            service_name=service_name,
            option_rows=option_rows,
            limit=10,
            lease_seconds=300,
        )
        self.assertEqual(len(claimed), 1)
        claim_token = claimed[0]["claim_token"]
        self.assertTrue(claim_token)
        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_PENDING)
        self.assertEqual(job.claimed_by_service, service_name)
        self.assertIsNotNone(job.claim_expires_at)

        mark_sweep_job_funding_broadcasted(
            actor=self.operator,
            service_name=service_name,
            claim_token=claim_token,
            public_id=job.public_id,
            gas_funding_txid="0xfunding0001",
            destination_address=session.deposit_address,
        )

        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_FUNDING_BROADCASTED)
        self.assertEqual(job.gas_funding_txid, "0xfunding0001")

        mark_sweep_job_ready_to_sweep(
            actor=self.operator,
            service_name=service_name,
            claim_token=claim_token,
            public_id=job.public_id,
        )

        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_READY_TO_SWEEP)

        mark_sweep_job_sweep_broadcasted(
            actor=self.operator,
            service_name=service_name,
            claim_token=claim_token,
            public_id=job.public_id,
            sweep_txid="0xsweep0001",
            destination_address=session.deposit_address,
        )

        job.refresh_from_db()
        self.assertEqual(job.status, DepositSweepJob.STATUS_SWEEP_BROADCASTED)
        self.assertEqual(job.sweep_txid, "0xsweep0001")

        mark_sweep_job_confirmed(
            actor=self.operator,
            service_name=service_name,
            claim_token=claim_token,
            public_id=job.public_id,
        )

        job.refresh_from_db()
        session.refresh_from_db()

        self.assertEqual(job.status, DepositSweepJob.STATUS_CONFIRMED)
        self.assertEqual(session.status, DepositSession.STATUS_SWEPT)

        watch_targets_after_sweep = list_active_deposit_watch_targets(
            actor=self.operator,
            option_rows=option_rows,
        )
        self.assertEqual(len(watch_targets_after_sweep), 1)
        self.assertEqual(watch_targets_after_sweep[0]["targets"], [])