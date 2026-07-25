from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.test import override_settings
from django.utils import timezone

from ledger.dashboard import config
from ledger.dashboard.daily_rewards import claim_daily_reward, get_daily_reward_date
from ledger.dashboard.models import (
    DailyRewardClaim,
    DailyRewardState,
    RewardChestGrant,
)
from ledger.dashboard.reward_chests import grant_reward_chest, open_reward_chest
from ledger.models import (
    LEDGER_RISK_STATUS_BLOCKED,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
)
from tests.ledger.base import BaseLedgerTestCase


@override_settings(TIME_ZONE="Europe/Moscow")
class TestRewardChests(BaseLedgerTestCase):
    def instant(self, year=2026, month=7, day=24, hour=12):
        return datetime(year, month, day, hour, tzinfo=datetime_timezone.utc)

    def grant(self, *, user=None, chest_key="daily_standard", source_ref="test:1"):
        return grant_reward_chest(
            user=user or self.u1,
            chest_key=chest_key,
            source_type="test",
            source_ref=source_ref,
            metadata={"test": True},
        )

    def test_config_requires_exact_drop_rate_total(self):
        broken = {
            "broken": {
                "label": "Broken",
                "asset": "chest",
                "drops": (
                    {
                        "key": "only",
                        "chance_bps": 9_999,
                        "amount": 100,
                    },
                ),
            }
        }
        with patch.object(config, "REWARD_CHESTS", broken):
            with self.assertRaises(ImproperlyConfigured):
                config.get_reward_chest_definition("broken")

    def test_config_rejects_duplicate_drop_keys_and_excessive_expected_value(self):
        duplicate = {
            "broken": {
                "label": "Broken",
                "asset": "chest",
                "drops": (
                    {"key": "same", "chance_bps": 5_000, "amount": 100},
                    {"key": "same", "chance_bps": 5_000, "amount": 200},
                ),
            }
        }
        with patch.object(config, "REWARD_CHESTS", duplicate):
            with self.assertRaises(ImproperlyConfigured):
                config.get_reward_chest_definition("broken")

        excessive = {
            "broken": {
                "label": "Broken",
                "asset": "bigchest",
                "drops": (
                    {
                        "key": "too_large_ev",
                        "chance_bps": 10_000,
                        "amount": config.REWARD_CHEST_MAX_EXPECTED_VALUE_TOKENS + 1,
                    },
                ),
            }
        }
        with patch.object(config, "REWARD_CHESTS", excessive):
            with self.assertRaises(ImproperlyConfigured):
                config.get_reward_chest_definition("broken")

    def test_grant_is_idempotent_by_business_source(self):
        first = self.grant(source_ref="grant-idempotent")
        second = self.grant(source_ref="grant-idempotent")

        self.assertEqual(first.pk, second.pk)
        self.assertEqual(first.status, RewardChestGrant.STATUS_PENDING)
        self.assertEqual(RewardChestGrant.objects.count(), 1)
        self.assertTrue(first.config_snapshot.get("fingerprint"))

    def test_source_reference_cannot_be_reused_for_another_user_or_chest(self):
        self.grant(source_ref="shared-source")

        with self.assertRaises(ValidationError):
            self.grant(user=self.u2, source_ref="shared-source")
        with self.assertRaises(ValidationError):
            self.grant(chest_key="daily_mega", source_ref="shared-source")

    def test_roll_boundaries_select_configured_drops_and_credit_balanced_ledger(self):
        first_grant = self.grant(source_ref="boundary-first")
        second_grant = self.grant(source_ref="boundary-second")
        definition = config.get_reward_chest_definition("daily_standard")
        first_drop = definition.drops[0]
        second_drop = definition.drops[1]

        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=first_drop.cumulative_end_bps - 1,
        ):
            first = open_reward_chest(user=self.u1, grant=first_grant)
        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=first_drop.cumulative_end_bps,
        ):
            second = open_reward_chest(user=self.u1, grant=second_grant)

        self.assertEqual(first["drop_key"], first_drop.key)
        self.assertEqual(first["amount_units"], first_drop.amount_units)
        self.assertEqual(second["drop_key"], second_drop.key)
        self.assertEqual(second["amount_units"], second_drop.amount_units)

        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()
        total = first_drop.amount_units + second_drop.amount_units
        self.assertEqual(self.w1.balance, total)
        self.assertEqual(self.issuance.balance, -total)

        for result in (first, second):
            entries = list(LedgerEntry.objects.filter(txn=result["txn"]))
            self.assertEqual(len(entries), 2)
            self.assertEqual(sum(entry.delta for entry in entries), 0)
            self.assertEqual(
                LedgerOutbox.objects.filter(
                    txn=result["txn"],
                    topic="ledger.reward_chest.opened",
                ).count(),
                1,
            )

    def test_open_is_idempotent_and_does_not_reroll(self):
        grant = self.grant(source_ref="open-idempotent")
        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ) as mocked_roll:
            first = open_reward_chest(user=self.u1, grant=grant)
            second = open_reward_chest(user=self.u1, grant=grant)

        self.assertTrue(first["opened"])
        self.assertFalse(second["opened"])
        self.assertTrue(second["already_opened"])
        self.assertEqual(first["txn"].pk, second["txn"].pk)
        self.assertEqual(mocked_roll.call_count, 1)
        self.assertEqual(
            LedgerTransaction.objects.filter(kind="reward_chest").count(),
            1,
        )

    def test_owner_wallet_and_expiry_are_enforced_before_opening(self):
        grant = self.grant(source_ref="owner-only")
        with self.assertRaises(PermissionDenied):
            open_reward_chest(user=self.u2, grant=grant)

        self.w1.risk_status = LEDGER_RISK_STATUS_BLOCKED
        self.w1.save(update_fields=["risk_status"])
        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow"
        ) as mocked_roll:
            with self.assertRaises(ValidationError):
                open_reward_chest(user=self.u1, grant=grant)
        mocked_roll.assert_not_called()

        self.w1.risk_status = "clear"
        self.w1.save(update_fields=["risk_status"])
        grant.expires_at = timezone.now() - timedelta(seconds=1)
        grant.save(update_fields=["expires_at"])
        with self.assertRaises(ValidationError):
            open_reward_chest(user=self.u1, grant=grant)

        grant.refresh_from_db()
        self.assertEqual(grant.status, RewardChestGrant.STATUS_PENDING)
        self.assertIsNone(grant.ledger_txn_id)

    def test_granted_snapshot_survives_later_config_changes(self):
        grant = self.grant(source_ref="snapshot-stable")
        original = config.get_reward_chest_definition("daily_standard")
        changed = dict(config.REWARD_CHESTS)
        changed["daily_standard"] = {
            "label": "Changed",
            "asset": "chest",
            "drops": (
                {
                    "key": "changed",
                    "chance_bps": 10_000,
                    "amount": 9_999,
                },
            ),
        }

        with patch.object(config, "REWARD_CHESTS", changed), patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ):
            result = open_reward_chest(user=self.u1, grant=grant)

        self.assertEqual(result["drop_key"], original.drops[0].key)
        self.assertEqual(result["amount_units"], original.drops[0].amount_units)

    def test_tampered_snapshot_is_rejected_without_credit(self):
        grant = self.grant(source_ref="snapshot-tampered")
        snapshot = dict(grant.config_snapshot)
        drops = [dict(row) for row in snapshot["drops"]]
        drops[0]["amount"] = drops[0]["amount"] + 1
        snapshot["drops"] = drops
        grant.config_snapshot = snapshot
        grant.save(update_fields=["config_snapshot"])

        with self.assertRaises(ImproperlyConfigured):
            open_reward_chest(user=self.u1, grant=grant)

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(LedgerTransaction.objects.filter(kind="reward_chest").count(), 0)

    def test_daily_reward_chest_opens_seamlessly_and_links_audit_rows(self):
        chest_day = ({"kind": "chest", "chest": "daily_standard"},)
        standard = config.get_reward_chest_definition("daily_standard")
        jackpot = standard.drops[-1]

        with patch.object(config, "DAILY_REWARDS", chest_day), patch.object(
            config, "DAILY_REWARD_WINDOW_SIZE", 1
        ), patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=config.REWARD_CHEST_TOTAL_CHANCE_BPS - 1,
        ):
            result = claim_daily_reward(user=self.u1, at=self.instant())

        claim = DailyRewardClaim.objects.get(user=self.u1)
        state = DailyRewardState.objects.get(user=self.u1)
        grant = claim.reward_chest_grant

        self.assertTrue(result["claimed"])
        self.assertEqual(result["reward_kind"], "chest")
        self.assertEqual(result["drop_key"], jackpot.key)
        self.assertEqual(result["amount_units"], jackpot.amount_units)
        self.assertIsNotNone(grant)
        self.assertEqual(grant.status, RewardChestGrant.STATUS_OPENED)
        self.assertEqual(claim.ledger_txn_id, grant.ledger_txn_id)
        self.assertEqual(claim.amount, jackpot.amount_units)
        self.assertEqual(claim.config_snapshot["drop_key"], jackpot.key)
        self.assertEqual(claim.ledger_txn.kind, "reward_chest")
        self.assertEqual(
            LedgerOutbox.objects.filter(
                txn=claim.ledger_txn,
                topic="ledger.daily_reward.claimed",
            ).count(),
            1,
        )
        self.assertEqual(state.current_streak, 1)

    def test_daily_reward_chest_same_day_cannot_reroll(self):
        chest_day = ({"kind": "chest", "chest": "daily_standard"},)
        with patch.object(config, "DAILY_REWARDS", chest_day), patch.object(
            config, "DAILY_REWARD_WINDOW_SIZE", 1
        ), patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ) as mocked_roll:
            first = claim_daily_reward(user=self.u1, at=self.instant())
            second = claim_daily_reward(user=self.u1, at=self.instant(hour=20))

        self.assertTrue(first["claimed"])
        self.assertFalse(second["claimed"])
        self.assertTrue(second["already_claimed"])
        self.assertEqual(mocked_roll.call_count, 1)
        self.assertEqual(RewardChestGrant.objects.count(), 1)
        self.assertEqual(DailyRewardClaim.objects.count(), 1)
