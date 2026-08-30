from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.test import override_settings
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.daily_rewards import (
    build_daily_rewards_context,
    claim_daily_reward,
    get_daily_reward_date,
    prepare_daily_reward_chest,
)
from ledger.dashboard.models import (
    DailyRewardClaim,
    DailyRewardState,
    RewardChestGrant,
)
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

    def test_daily_chest_waits_for_click_confirmation(self):
        chest_schedule = (
            {"kind": "chest", "chest": "small_chest"},
        )
        before = int(self.w1.balance)

        with (
            patch.object(config, "DAILY_REWARDS", chest_schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
            patch(
                "ledger.dashboard.reward_chests.timezone.now",
                return_value=self.instant(),
            ),
        ):
            prepared = prepare_daily_reward_chest(
                user=self.u1,
                at=self.instant(),
            )

            prepared["grant"].refresh_from_db()
            self.w1.refresh_from_db()
            self.assertEqual(
                prepared["grant"].status,
                RewardChestGrant.STATUS_PENDING,
            )
            self.assertEqual(self.w1.balance, before)
            self.assertFalse(
                DailyRewardClaim.objects.filter(user=self.u1).exists()
            )

            with patch(
                "ledger.dashboard.reward_chests.secrets.randbelow",
                return_value=0,
            ):
                result = claim_daily_reward(
                    user=self.u1,
                    at=self.instant(),
                    grant_public_id=str(prepared["grant"].public_id),
                )

        self.assertTrue(result["claimed"])
        prepared["grant"].refresh_from_db()
        self.w1.refresh_from_db()
        self.assertEqual(
            prepared["grant"].status,
            RewardChestGrant.STATUS_OPENED,
        )
        self.assertGreater(self.w1.balance, before)
        self.assertEqual(
            DailyRewardClaim.objects.filter(user=self.u1).count(),
            1,
        )

    def test_second_claim_same_day_is_idempotent(self):
        first = claim_daily_reward(user=self.u1, at=self.instant())
        self.w1.refresh_from_db()
        balance_after_first = int(self.w1.balance)

        second = claim_daily_reward(
            user=self.u1,
            at=self.instant(hour=20),
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

    def test_missing_day_resets_display_context_to_day_one(self):
        reward_date = get_daily_reward_date(self.instant(day=24))
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=29,
            total_claims=29,
            last_claim_date=reward_date - timedelta(days=2),
        )

        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(day=24),
        )

        self.assertEqual(context["streak"], 1)
        self.assertEqual(context["cycle_day"], 1)
        self.assertFalse(context["claimed_today"])
        self.assertTrue(context["can_claim"])
        self.assertEqual(context["current_reward"]["status"], "current")

    def test_production_daily_cycle_matches_reworked_economy(self):
        definitions = config.get_daily_reward_definitions()
        self.assertEqual(len(definitions), 30)

        fixed_amounts = [
            reward.amount_tokens
            for reward in definitions
            if reward.kind == "fixed"
        ]
        self.assertEqual(sum(fixed_amounts), 205)
        self.assertEqual(
            fixed_amounts,
            [
                5, 5, 10, 5,
                5, 10, 15, 5,
                5, 10, 15, 5,
                5, 10, 15, 5,
                5, 10, 15, 5,
                5, 10, 20, 5,
            ],
        )

        chest_schedule = {
            reward.day: reward.chest_key
            for reward in definitions
            if reward.kind == "chest"
        }
        self.assertEqual(
            chest_schedule,
            {
                5: "small_chest",
                10: "small_chest",
                15: "medium_chest",
                20: "small_chest",
                25: "small_chest",
                30: "monthly_huge_chest",
            },
        )

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

    def test_claimed_current_day_is_exposed_as_claimed(self):
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=2,
            total_claims=2,
            last_claim_date=reward_date,
        )

        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(),
        )

        day_two = next(
            reward
            for reward in context["window"]
            if reward["day"] == 2
        )
        modal_day_two = next(
            reward
            for reward in context["all_rewards"]
            if reward["day"] == 2
        )

        self.assertTrue(context["claimed_today"])
        self.assertEqual(context["cycle_day"], 2)
        self.assertEqual(context["current_reward"]["status"], "claimed")
        self.assertEqual(day_two["status"], "claimed")
        self.assertEqual(modal_day_two["status"], "claimed")

    def test_next_streak_chest_comes_from_reward_rotation(self):
        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(),
        )
        expected_reward = next(
            reward
            for reward in config.get_daily_reward_definitions()
            if reward.kind == "chest"
        )
        chest = config.get_reward_chest_definition(
            expected_reward.chest_key
        )

        self.assertEqual(context["cycle_day"], 1)
        self.assertEqual(
            context["next_chest"]["day"],
            expected_reward.day,
        )
        self.assertEqual(
            context["next_chest"]["chest_key"],
            expected_reward.chest_key,
        )
        self.assertEqual(
            context["next_chest"]["days_until_unlock"],
            expected_reward.day - 1,
        )
        self.assertEqual(
            context["next_chest"]["image_path"],
            chest.closed_image,
        )

    def test_next_streak_chest_advances_after_current_day_is_claimed(self):
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=29,
            total_claims=29,
            last_claim_date=reward_date,
        )

        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(),
        )
        chest = config.get_reward_chest_definition("monthly_huge_chest")

        self.assertTrue(context["claimed_today"])
        self.assertEqual(context["cycle_day"], 29)
        self.assertEqual(context["next_chest"]["day"], 30)
        self.assertEqual(
            context["next_chest"]["chest_key"],
            "monthly_huge_chest",
        )
        self.assertEqual(context["next_chest"]["days_until_unlock"], 1)
        self.assertEqual(context["next_chest"]["image_path"], chest.closed_image)

    def test_next_streak_chest_wraps_with_the_reward_cycle(self):
        definitions = config.get_daily_reward_definitions()
        cycle_length = len(definitions)
        expected_reward = next(
            reward
            for reward in definitions
            if reward.kind == "chest"
        )
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=cycle_length,
            total_claims=cycle_length,
            last_claim_date=reward_date,
        )

        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(),
        )
        chest = config.get_reward_chest_definition(
            expected_reward.chest_key
        )

        self.assertTrue(context["claimed_today"])
        self.assertEqual(context["cycle_day"], cycle_length)
        self.assertEqual(
            context["next_chest"]["day"],
            expected_reward.day,
        )
        self.assertEqual(
            context["next_chest"]["chest_key"],
            expected_reward.chest_key,
        )
        self.assertEqual(
            context["next_chest"]["days_until_unlock"],
            expected_reward.day,
        )
        self.assertEqual(
            context["next_chest"]["image_path"],
            chest.closed_image,
        )

    def test_fixed_reward_assets_come_from_central_wallet_config(self):
        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(),
        )
        current_reward = context["current_reward"]
        asset = config.get_daily_reward_asset_definition(
            "coins",
            amount_tokens=current_reward["amount_tokens"],
        )

        self.assertEqual(current_reward["image_path"], asset["image"])
        self.assertEqual(
            current_reward["button_image_path"],
            asset["button_image"],
        )
        self.assertEqual(
            current_reward["asset_tier_min_amount"],
            asset["tier_min_amount"],
        )
        self.assertEqual(context["assets"], config.get_wallet_asset_paths())

    def test_coin_asset_tiers_follow_reward_amount(self):
        tiers = config.DAILY_REWARD_ASSETS["coins"]["tiers"]
        self.assertGreaterEqual(len(tiers), 2)

        lower_tier = tiers[0]
        upper_tier = tiers[1]
        below_threshold = config.get_daily_reward_asset_definition(
            "coins",
            amount_tokens=upper_tier["min_amount"] - 1,
        )
        at_threshold = config.get_daily_reward_asset_definition(
            "coins",
            amount_tokens=upper_tier["min_amount"],
        )
        assets = config.get_wallet_asset_paths()

        self.assertEqual(
            below_threshold["image"],
            assets[lower_tier["image_asset"]],
        )
        self.assertEqual(
            below_threshold["tier_min_amount"],
            lower_tier["min_amount"],
        )
        self.assertEqual(
            at_threshold["image"],
            assets[upper_tier["image_asset"]],
        )
        self.assertEqual(
            at_threshold["tier_min_amount"],
            upper_tier["min_amount"],
        )
        self.assertNotEqual(
            below_threshold["image"],
            at_threshold["image"],
        )

    def test_daily_reward_context_uses_large_coin_asset_at_threshold(self):
        tiers = config.DAILY_REWARD_ASSETS["coins"]["tiers"]
        self.assertGreaterEqual(len(tiers), 2)
        threshold = tiers[1]["min_amount"]

        target_reward = next(
            reward
            for reward in config.get_daily_reward_definitions()
            if (
                reward.kind == "fixed"
                and reward.asset == "coins"
                and reward.day > 1
                and config.get_daily_reward_asset_definition(
                    "coins",
                    amount_tokens=reward.amount_tokens,
                )["tier_min_amount"] == threshold
            )
        )
        expected_asset = config.get_daily_reward_asset_definition(
            "coins",
            amount_tokens=target_reward.amount_tokens,
        )
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=target_reward.day - 1,
            total_claims=target_reward.day - 1,
            last_claim_date=reward_date - timedelta(days=1),
        )

        context = build_daily_rewards_context(
            user=self.u1,
            claim_url="/wallet/daily-reward/claim/",
            at=self.instant(),
        )

        self.assertEqual(context["cycle_day"], target_reward.day)
        self.assertEqual(
            context["current_reward"]["amount_tokens"],
            target_reward.amount_tokens,
        )
        self.assertEqual(
            context["current_reward"]["image_path"],
            expected_asset["image"],
        )
        self.assertEqual(
            context["current_reward"]["asset_tier_min_amount"],
            threshold,
        )

    def test_coin_asset_tiers_reject_invalid_order(self):
        changed = {
            "coins": {
                "button_asset": "token_icon",
                "tiers": (
                    {
                        "min_amount": 200,
                        "image_asset": "daily_reward_coins_pile",
                    },
                    {
                        "min_amount": 1,
                        "image_asset": "daily_reward_coins_few",
                    },
                ),
            },
        }
        with patch.object(config, "DAILY_REWARD_ASSETS", changed):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_asset_definition(
                    "coins",
                    amount_tokens=200,
                )

    def test_wallet_asset_paths_are_validated(self):
        changed = dict(config.WALLET_ASSETS)
        changed["hero_art"] = "../outside-static.png"
        with patch.object(config, "WALLET_ASSETS", changed):
            with self.assertRaises(ImproperlyConfigured):
                config.get_wallet_asset_paths()

    def test_token_pack_asset_path_is_centralized(self):
        self.assertEqual(
            config.get_wallet_token_pack_image_path("token_pkg_500"),
            config.WALLET_TOKEN_PACK_ASSETS["token_pkg_500"],
        )

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
