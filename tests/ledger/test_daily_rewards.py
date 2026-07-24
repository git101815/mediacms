from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.test import override_settings
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.daily_rewards import claim_daily_reward, get_daily_reward_date
from ledger.dashboard.models import DailyRewardClaim, DailyRewardState
from ledger.models import (
    LEDGER_RISK_STATUS_BLOCKED,
    LEDGER_RISK_STATUS_REVIEW,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
    TokenWallet,
)
from tests.ledger.base import BaseLedgerTestCase


@override_settings(TIME_ZONE="Europe/Moscow")
class TestDailyRewards(BaseLedgerTestCase):
    def instant(self, year=2026, month=7, day=24, hour=12):
        return datetime(year, month, day, hour, tzinfo=datetime_timezone.utc)

    def test_first_claim_posts_balanced_ledger_and_audit_state(self):
        before = int(self.w1.balance)
        result = claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()
        claim = DailyRewardClaim.objects.get(user=self.u1)
        state = DailyRewardState.objects.get(user=self.u1)

        expected = config.DAILY_REWARDS[0]["amount"] * 1_000_000
        self.assertTrue(result["claimed"])
        self.assertEqual(claim.amount, expected)
        self.assertEqual(self.w1.balance, before + expected)
        self.assertEqual(self.issuance.balance, -expected)
        self.assertEqual(state.current_streak, 1)
        self.assertEqual(state.total_claims, 1)
        self.assertEqual(state.last_claim_date, result["reward_date"])

        entries = list(LedgerEntry.objects.filter(txn=claim.ledger_txn))
        self.assertEqual(len(entries), 2)
        self.assertEqual(sum(entry.delta for entry in entries), 0)
        self.assertEqual(
            LedgerOutbox.objects.filter(
                txn=claim.ledger_txn,
                topic="ledger.daily_reward.claimed",
            ).count(),
            1,
        )
        self.assertEqual(claim.config_snapshot["amount_units"], expected)

    def test_second_claim_same_day_is_idempotent(self):
        first = claim_daily_reward(user=self.u1, at=self.instant())
        self.w1.refresh_from_db()
        balance_after_first = int(self.w1.balance)

        second = claim_daily_reward(
            user=self.u1,
            at=self.instant(hour=23),
        )
        self.w1.refresh_from_db()

        self.assertFalse(second["claimed"])
        self.assertTrue(second["already_claimed"])
        self.assertEqual(first["claim"].pk, second["claim"].pk)
        self.assertEqual(self.w1.balance, balance_after_first)
        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 1)
        self.assertEqual(
            LedgerTransaction.objects.filter(kind="daily_reward").count(),
            1,
        )

    def test_consecutive_day_increments_streak_and_uses_next_reward(self):
        first = claim_daily_reward(user=self.u1, at=self.instant(day=24))
        second = claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(first["streak_day"], 1)
        self.assertEqual(second["streak_day"], 2)
        self.assertEqual(second["cycle_day"], 2)
        self.assertEqual(
            second["amount_units"],
            config.DAILY_REWARDS[1]["amount"] * 1_000_000,
        )

    def test_missing_a_day_resets_streak(self):
        claim_daily_reward(user=self.u1, at=self.instant(day=20))
        result = claim_daily_reward(user=self.u1, at=self.instant(day=22))

        self.assertEqual(result["streak_day"], 1)
        self.assertEqual(result["cycle_day"], 1)

    def test_cycle_wraps_after_last_configured_day(self):
        previous_date = get_daily_reward_date(self.instant(day=24))
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=len(config.DAILY_REWARDS),
            total_claims=len(config.DAILY_REWARDS),
            last_claim_date=previous_date,
        )
        result = claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(result["streak_day"], len(config.DAILY_REWARDS) + 1)
        self.assertEqual(result["cycle_day"], 1)

    def test_blocked_and_review_wallets_cannot_claim(self):
        self.w1.risk_status = LEDGER_RISK_STATUS_BLOCKED
        self.w1.save(update_fields=["risk_status"])
        with self.assertRaises(ValidationError):
            claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.risk_status = LEDGER_RISK_STATUS_REVIEW
        self.w1.review_required = True
        self.w1.save(update_fields=["risk_status", "review_required"])
        with self.assertRaises(ValidationError):
            claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 0)

    def test_future_state_is_rejected_without_credit(self):
        reward_date = get_daily_reward_date(self.instant(day=24))
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=5,
            total_claims=5,
            last_claim_date=reward_date + timedelta(days=1),
        )
        with self.assertRaises(ValidationError):
            claim_daily_reward(user=self.u1, at=self.instant(day=24))
        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)

    def test_inactive_user_and_disabled_feature_cannot_claim(self):
        self.u1.is_active = False
        self.u1.save(update_fields=["is_active"])
        with self.assertRaises(PermissionDenied):
            claim_daily_reward(user=self.u1, at=self.instant())

        self.u1.is_active = True
        self.u1.save(update_fields=["is_active"])
        with patch.object(config, "DAILY_REWARDS_ENABLED", False):
            with self.assertRaises(ValidationError):
                claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 0)

    def test_config_rejects_invalid_or_dangerous_amounts(self):
        with patch.object(config, "DAILY_REWARDS", ({"amount": 0, "asset": "coins"},)):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_definitions()
        with patch.object(
            config,
            "DAILY_REWARDS",
            ({"amount": config.DAILY_REWARD_MAX_TOKENS_PER_CLAIM + 1, "asset": "coins"},),
        ):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_definitions()

    def test_claim_endpoint_requires_login_and_post(self):
        url = reverse("wallet_claim_daily_reward")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 302)

        self.client.force_login(self.u1)
        response = self.client.get(url)
        self.assertEqual(response.status_code, 405)

    def test_claim_endpoint_changes_balance_once(self):
        self.client.force_login(self.u1)
        url = reverse("wallet_claim_daily_reward")

        first = self.client.post(url)
        second = self.client.post(url)

        self.assertEqual(first.status_code, 302)
        self.assertEqual(second.status_code, 302)
        self.assertEqual(DailyRewardClaim.objects.filter(user=self.u1).count(), 1)
        self.assertEqual(
            LedgerTransaction.objects.filter(kind="daily_reward").count(),
            1,
        )
