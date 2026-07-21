from datetime import timedelta

from django.core.exceptions import ValidationError
from django.test import override_settings
from django.utils import timezone

from ledger.models import (
    DepositSession,
    LedgerHold,
    LedgerTransaction,
    ObservedOnchainTransfer,
)
from ledger.mtpelerin_deposits import (
    record_mtpelerin_browser_event,
)
from ledger.services import (
    credit_confirmed_deposit_session,
    expire_stale_deposit_sessions,
    ingest_deposit_observation_event,
)
from tests.ledger.base import BaseLedgerTestCase


class MtPelerinHeldCreditTests(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        for codename in [
            "can_manage_wallet_holds",
            "can_manage_deposit_sessions",
            "can_credit_confirmed_deposits",
            "can_record_onchain_observations",
            "can_manage_deposit_sweep_jobs",
        ]:
            self.grant_perm(self.operator, codename)

        self.settings_override = override_settings(
            LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME=(
                self.operator.username
            )
        )
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)

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
            expires_at=timezone.now() + timedelta(seconds=12_345),
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

    def _submit_payment(self):
        return record_mtpelerin_browser_event(
            session=self.session,
            actor=self.u1,
            event_type="paymentSubmitted",
            event_data={
                "paymentType": "bankTransfer",
                "paymentId": "payment-123",
                "email": "must-not-be-stored@example.invalid",
            },
        )

    def test_payment_submitted_credits_total_and_holds_same_amount(self):
        result = self._submit_payment()

        self.w1.refresh_from_db()
        self.session.refresh_from_db()
        pending = self.session.metadata["mtpelerin_pending_credit"]

        self.assertEqual(
            result["pending_credit_status"],
            "held",
        )
        self.assertEqual(
            self.w1.balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(
            self.w1.held_balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(
            self.w1.balance - self.w1.held_balance,
            0,
        )
        self.assertEqual(pending["status"], "held")
        self.assertEqual(
            pending["expires_at"],
            self.session.expires_at.isoformat(),
        )
        self.assertNotIn(
            "email",
            self.session.metadata["mtpelerin_browser_events"][
                "payment_submitted"
            ],
        )

    def test_payment_submitted_is_idempotent(self):
        first = self._submit_payment()
        second = self._submit_payment()

        self.w1.refresh_from_db()
        self.assertEqual(
            first["pending_credit_status"],
            second["pending_credit_status"],
        )
        self.assertEqual(
            self.w1.balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(
            self.w1.held_balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(
            LedgerTransaction.objects.filter(
                external_id=(
                    "mtpelerin-pending-credit:"
                    f"{self.session.public_id}"
                )
            ).count(),
            1,
        )
        self.assertEqual(
            LedgerHold.objects.filter(
                metadata__deposit_session_public_id=str(
                    self.session.public_id
                )
            ).count(),
            1,
        )

    def test_confirmed_usdc_releases_hold_without_second_user_credit(self):
        self._submit_payment()
        pending_txn_id = (
            self.session.__class__.objects.get(
                id=self.session.id
            ).metadata["mtpelerin_pending_credit"]["ledger_txn_id"]
        )
        observed = ObservedOnchainTransfer.objects.create(
            deposit_session=self.session,
            event_key="base:0xabc:0",
            chain=self.session.chain,
            txid="0xabc",
            log_index=0,
            block_number=100,
            detected_block_number=100,
            from_address=(
                "0x3333333333333333333333333333333333333333"
            ),
            to_address=self.session.deposit_address,
            token_contract_address=(
                self.session.token_contract_address
            ),
            asset_code=self.session.asset_code,
            amount=1_000_000,
            onchain_raw_amount=1_000_000,
            confirmations=1,
            detection_method="event",
            status=ObservedOnchainTransfer.STATUS_CONFIRMED,
            confirmed_at=timezone.now(),
            raw_payload={
                "amount_unit": "canonical_stable",
                "canonical_stable_amount": 1_000_000,
                "onchain_raw_amount": "1000000",
            },
        )

        credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=self.session,
            observed_transfer=observed,
        )

        self.w1.refresh_from_db()
        self.session.refresh_from_db()
        observed.refresh_from_db()
        pending = self.session.metadata["mtpelerin_pending_credit"]

        self.assertEqual(
            self.w1.balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(self.w1.held_balance, 0)
        self.assertEqual(
            self.session.status,
            DepositSession.STATUS_CREDITED,
        )
        self.assertEqual(
            self.session.credited_ledger_txn_id,
            pending_txn_id,
        )
        self.assertEqual(pending["status"], "settled")
        self.assertEqual(
            observed.credited_ledger_txn_id,
            pending_txn_id,
        )

    def test_expiry_releases_hold_and_reverses_pending_credit(self):
        self._submit_payment()
        self.session.refresh_from_db()
        self.session.expires_at = timezone.now() - timedelta(seconds=1)
        self.session.save(update_fields=["expires_at", "updated_at"])

        expired_count = expire_stale_deposit_sessions(
            actor=self.operator,
            limit=10,
        )

        self.w1.refresh_from_db()
        self.session.refresh_from_db()
        pending = self.session.metadata["mtpelerin_pending_credit"]

        self.assertEqual(expired_count, 1)
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w1.held_balance, 0)
        self.assertEqual(
            self.session.status,
            DepositSession.STATUS_EXPIRED,
        )
        self.assertEqual(pending["status"], "expired")
        self.assertTrue(
            LedgerTransaction.objects.filter(
                external_id=(
                    "mtpelerin-pending-expiry:"
                    f"{self.session.public_id}"
                ),
                status=LedgerTransaction.STATUS_REVERSED,
            ).exists()
        )
        self.assertFalse(
            LedgerHold.objects.filter(
                id=pending["hold_id"],
                released=False,
            ).exists()
        )

    def test_payment_submitted_after_session_deadline_is_rejected(self):
        self.session.expires_at = timezone.now() - timedelta(seconds=1)
        self.session.save(update_fields=["expires_at", "updated_at"])

        with self.assertRaisesMessage(
            ValidationError,
            "Mt Pelerin session has expired",
        ):
            self._submit_payment()

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(self.w1.held_balance, 0)

    def test_confirmed_net_amount_releases_hold_below_gross_amount(self):
        metadata = dict(self.session.metadata or {})
        token_pack = dict(metadata["token_pack"])
        net_amount = int(token_pack["net_stable_amount"])
        gross_amount = net_amount + 100_000

        token_pack.update(
            {
                "gross_stable_amount": gross_amount,
                "fee_stable_amount": gross_amount - net_amount,
                "fixed_fee_stable_amount": gross_amount - net_amount,
            }
        )
        metadata["token_pack"] = token_pack
        metadata["expected_canonical_stable_amount"] = gross_amount
        metadata["expected_route_raw_amount"] = str(gross_amount)

        self.session.min_amount = gross_amount
        self.session.expected_onchain_raw_amount = gross_amount
        self.session.metadata = metadata
        self.session.save(
            update_fields=[
                "min_amount",
                "expected_onchain_raw_amount",
                "metadata",
                "updated_at",
            ]
        )
        self._submit_payment()

        result = ingest_deposit_observation_event(
            actor=self.operator,
            session_public_id=self.session.public_id,
            chain=self.session.chain,
            txid="0xnetthreshold",
            log_index=0,
            block_number=101,
            detected_block_number=101,
            from_address=(
                "0x3333333333333333333333333333333333333333"
            ),
            to_address=self.session.deposit_address,
            token_contract_address=self.session.token_contract_address,
            asset_code=self.session.asset_code,
            amount=net_amount,
            confirmations=1,
            detection_method="event",
            raw_payload={
                "amount_unit": "canonical_stable",
                "canonical_stable_amount": net_amount,
                "onchain_raw_amount": str(net_amount),
            },
        )

        self.w1.refresh_from_db()
        self.session.refresh_from_db()

        self.assertIsNotNone(result["ledger_txn"])
        self.assertEqual(
            self.session.status,
            DepositSession.STATUS_CREDITED,
        )
        self.assertEqual(
            self.w1.balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(self.w1.held_balance, 0)
        self.assertFalse(
            LedgerTransaction.objects.filter(
                kind="mtpelerin_deposit_fee",
            ).exists()
        )
