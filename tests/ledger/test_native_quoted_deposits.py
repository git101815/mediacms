from datetime import timedelta
from unittest.mock import patch

from django.utils import timezone

from ledger.models import DepositAddress, DepositSession, TokenWallet
from ledger.services import (
    credit_confirmed_deposit_session,
    list_active_deposit_watch_targets,
    open_user_deposit_session,
    record_onchain_observation,
)

from .base import BaseLedgerTestCase


class TestNativeQuotedDeposits(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.grant_perm(self.operator, "can_record_onchain_observations")
        self.grant_perm(self.operator, "can_credit_confirmed_deposits")
        self.grant_perm(self.operator, "can_manage_deposit_sweep_jobs")
        self.grant_perm(self.operator, "can_view_deposit_sessions")

        self.route = DepositAddress.objects.create(
            chain="ethereum",
            asset_code="ETH",
            token_contract_address="",
            display_label="Ethereum · ETH",
            address="0x9999999999999999999999999999999999999999",
            address_derivation_ref="m/44'/60'/0'/0/999",
            derivation_index=999,
            required_confirmations=12,
            min_amount=1_000_000,
            session_ttl_seconds=3600,
            metadata={
                "provisioned_by": "test",
                "option_key": "ethereum-eth",
                "amount_semantics": "native_quoted",
            },
        )

    def _quote(self, price: str) -> dict:
        now = timezone.now()
        return {
            "asset": "ETH",
            "currency": "USD",
            "price": price,
            "source": "test",
            "observed_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=2)).isoformat(),
        }

    @patch("ledger.services._derive_session_deposit_address")
    def test_native_session_has_no_fake_expected_raw_amount(self, mocked_derive):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01",
            "m/44'/60'/0'/0/1001",
        )

        session = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:ETH:native",
            token_pack=self.default_token_pack,
            payment_method_key="crypto:eth",
            payment_method_type="crypto",
            payment_method_label="ETH",
        )

        self.assertEqual(session.chain, "ethereum")
        self.assertEqual(session.asset_code, "ETH")
        self.assertEqual(session.token_contract_address, "")
        self.assertIsNone(session.expected_onchain_raw_amount)
        self.assertEqual(
            (session.metadata or {}).get("amount_semantics"),
            "native_quoted",
        )
        self.assertIsNone(
            (session.metadata or {}).get("expected_route_raw_amount")
        )

        watch = list_active_deposit_watch_targets(
            actor=self.operator,
            option_rows=[
                {
                    "chain": "ethereum",
                    "asset_code": "ETH",
                    "token_contract_address": "",
                }
            ],
        )
        target = watch[0]["targets"][0]
        self.assertEqual(target["amount_semantics"], "native_quoted")
        self.assertEqual(target["onchain_min_amount"], "0")

    @patch("ledger.services._derive_session_deposit_address")
    def test_first_native_quote_and_raw_threshold_are_immutable(self, mocked_derive):
        mocked_derive.return_value = (
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02",
            "m/44'/60'/0'/0/1002",
        )

        session = open_user_deposit_session(
            actor=self.u1,
            wallet=self.w1,
            option_key="ethereum:ETH:native",
            token_pack=self.default_token_pack,
            payment_method_key="crypto:eth",
            payment_method_type="crypto",
            payment_method_label="ETH",
        )

        raw_amount = 10**16  # 0.01 ETH
        first = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="",
            log_index=None,
            block_number=100,
            detected_block_number=100,
            from_address="",
            to_address=session.deposit_address,
            token_contract_address="",
            asset_code="ETH",
            amount=raw_amount,
            confirmations=1,
            detection_method="balance_verification",
            raw_payload={"runtime_price_quote": self._quote("100")},
        )
        first_amount = int(first.amount)
        self.assertEqual(first_amount, 1_000_000)

        session.refresh_from_db()
        native_lock = (session.metadata or {}).get("native_quoted_lock")
        self.assertIsInstance(native_lock, dict)
        self.assertEqual(native_lock["event_key"], first.event_key)
        self.assertEqual(native_lock["raw_amount"], str(raw_amount))
        self.assertEqual(
            native_lock["canonical_stable_amount"],
            first_amount,
        )
        self.assertEqual(native_lock["detected_block_number"], 100)

        second = record_onchain_observation(
            actor=self.operator,
            deposit_session=session,
            chain="ethereum",
            txid="",
            log_index=None,
            block_number=100,
            detected_block_number=100,
            from_address="",
            to_address=session.deposit_address,
            token_contract_address="",
            asset_code="ETH",
            amount=raw_amount,
            confirmations=12,
            detection_method="balance_verification",
            raw_payload={},
        )

        second.refresh_from_db()
        session.refresh_from_db()
        self.assertEqual(int(second.amount), first_amount)
        self.assertEqual(int(session.observed_amount), first_amount)
        self.assertEqual(int(session.expected_onchain_raw_amount), raw_amount)
        self.assertEqual(second.confirmations, 12)

        watch = list_active_deposit_watch_targets(
            actor=self.operator,
            option_rows=[
                {
                    "chain": "ethereum",
                    "asset_code": "ETH",
                    "token_contract_address": "",
                }
            ],
        )
        target = watch[0]["targets"][0]
        self.assertEqual(target["onchain_min_amount"], str(raw_amount))
        self.assertEqual(
            target["native_quoted_lock"]["event_key"],
            first.event_key,
        )
        self.assertEqual(
            target["native_quoted_lock"]["raw_amount"],
            str(raw_amount),
        )

        txn = credit_confirmed_deposit_session(
            actor=self.operator,
            deposit_session=session,
            observed_transfer=second,
            created_by=self.u1,
        )
        session.refresh_from_db()
        self.w1.refresh_from_db()

        self.assertEqual(session.status, DepositSession.STATUS_CREDITED)
        self.assertEqual(
            self.w1.balance,
            self.default_token_pack.token_amount,
        )
        self.assertEqual(session.credited_ledger_txn_id, txn.id)
        self.assertTrue(hasattr(second, "sweep_job"))

    def test_native_observation_rejects_quote_for_wrong_asset(self):
        session = DepositSession.objects.create(
            user=self.u1,
            wallet=self.w1,
            chain="ethereum",
            asset_code="ETH",
            token_contract_address="",
            route_key="ethereum:ETH:native",
            display_label="Ethereum · ETH",
            deposit_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa03",
            address_derivation_ref="m/44'/60'/0'/0/1003",
            derivation_index=1003,
            derivation_path="m/44'/60'/0'/0/1003",
            status=DepositSession.STATUS_AWAITING_PAYMENT,
            min_amount=1_000_000,
            expected_onchain_raw_amount=None,
            required_confirmations=12,
            expires_at=timezone.now() + timedelta(hours=1),
            created_by=self.u1,
            metadata={
                "amount_semantics": "native_quoted",
                "token_pack": self.default_token_pack_snapshot(),
            },
        )
        wrong = self._quote("100")
        wrong["asset"] = "POL"

        with self.assertRaisesMessage(Exception, "Runtime price asset must be ETH"):
            record_onchain_observation(
                actor=self.operator,
                deposit_session=session,
                chain="ethereum",
                txid="",
                log_index=None,
                block_number=100,
                detected_block_number=100,
                from_address="",
                to_address=session.deposit_address,
                token_contract_address="",
                asset_code="ETH",
                amount=10**16,
                confirmations=1,
                detection_method="balance_verification",
                raw_payload={"runtime_price_quote": wrong},
            )
