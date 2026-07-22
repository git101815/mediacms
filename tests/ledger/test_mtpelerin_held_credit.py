from datetime import timedelta
from unittest.mock import patch

from django.urls import reverse
from django.utils import timezone

from ledger.models import DepositSession, LedgerHold, LedgerTransaction
from ledger.services import ingest_deposit_observation_event
from tests.ledger.base import BaseLedgerTestCase


class MtPelerinOnchainOnlyTests(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        for codename in [
            "can_credit_confirmed_deposits",
            "can_record_onchain_observations",
            "can_manage_deposit_sweep_jobs",
        ]:
            self.grant_perm(self.operator, codename)

        self.session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="base",
            asset_code="USDC",
            token_contract_address=(
                "0x1111111111111111111111111111111111111111"
            ),
            route_key=(
                "base:USDC:"
                "0x1111111111111111111111111111111111111111"
            ),
            display_label="Bank transfer (Mt Pelerin · EUR)",
            derivation_index=987654,
            derivation_path="m/44'/60'/0'/0/987654",
            deposit_address=(
                "0x2222222222222222222222222222222222222222"
            ),
            address_derivation_ref="m/44'/60'/0'/0/987654",
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            min_amount=1_000_000,
            expected_onchain_raw_amount=1_000_000,
            required_confirmations=1,
            expires_at=timezone.now() + timedelta(days=21),
            created_by=self.u1,
            metadata={
                "payment_provider": {
                    "key": "mtpelerin",
                    "status": "LAUNCH_READY",
                    "label": "Bank transfer (Mt Pelerin · EUR)",
                },
                "payment_method": {
                    "key": "mtpelerin:eur",
                    "type": "provider",
                    "label": "Bank transfer (Mt Pelerin · EUR)",
                },
                "token_pack": self.default_token_pack_snapshot(),
                "amount_unit": "canonical_stable",
                "expected_canonical_stable_amount": 1_000_000,
                "expected_route_raw_amount": "1000000",
            },
        )

        metadata = dict(self.session.metadata)
        provider = dict(metadata["payment_provider"])
        provider["checkout_url"] = reverse(
            "wallet_mtpelerin_launch",
            kwargs={"public_id": self.session.public_id},
        )
        metadata["payment_provider"] = provider
        self.session.metadata = metadata
        self.session.save(update_fields=["metadata", "updated_at"])

    @patch("files.views.prepare_mtpelerin_browser_launch")
    def test_launch_redirects_to_direct_link_without_crediting_wallet(
        self,
        mocked_prepare,
    ):
        direct_url = (
            "https://widget.mtpelerin.com/"
            "?_ctkn=public-key&type=direct-link&tab=buy"
        )
        mocked_prepare.return_value = {"checkout_url": direct_url}
        self.client.force_login(self.u1)

        response = self.client.get(
            reverse(
                "wallet_mtpelerin_launch",
                kwargs={"public_id": self.session.public_id},
            )
        )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], direct_url)
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w1.held_balance, 0)
        self.assertFalse(
            LedgerTransaction.objects.filter(
                kind="mtpelerin_deposit_pending"
            ).exists()
        )
        self.assertFalse(LedgerHold.objects.filter(wallet=self.w1).exists())

    def test_confirmed_usdc_is_the_only_wallet_credit_signal(self):
        result = ingest_deposit_observation_event(
            actor=self.operator,
            session_public_id=self.session.public_id,
            chain=self.session.chain,
            txid="0xabc",
            log_index=0,
            block_number=100,
            detected_block_number=100,
            from_address=(
                "0x3333333333333333333333333333333333333333"
            ),
            to_address=self.session.deposit_address,
            token_contract_address=self.session.token_contract_address,
            asset_code=self.session.asset_code,
            amount=1_000_000,
            confirmations=1,
            detection_method="event",
            raw_payload={
                "source": "test_mtpelerin_onchain_settlement",
            },
        )

        self.w1.refresh_from_db()
        self.session.refresh_from_db()

        self.assertIsNotNone(result["ledger_txn"])
        self.assertEqual(result["ledger_txn"].kind, "deposit")
        self.assertEqual(
            self.w1.balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(self.w1.held_balance, 0)
        self.assertEqual(
            self.session.status,
            DepositSession.STATUS_CREDITED,
        )
        self.assertNotIn(
            "mtpelerin_pending_credit",
            self.session.metadata,
        )
        self.assertNotIn(
            "mtpelerin_browser_events",
            self.session.metadata,
        )
        self.assertFalse(
            LedgerTransaction.objects.filter(
                kind="mtpelerin_deposit_pending"
            ).exists()
        )
        self.assertFalse(LedgerHold.objects.filter(wallet=self.w1).exists())
