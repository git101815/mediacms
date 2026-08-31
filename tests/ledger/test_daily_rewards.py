from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

from django.core.exceptions import ImproperlyConfigured, PermissionDenied, ValidationError
from django.test import override_settings
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.daily_rewards import (
    build_daily_rewards_context,
    claim_daily_reward,
    get_daily_reward_cycle_reset,
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

    def fixed_asset(self):
        return next(iter(config.DAILY_REWARD_ASSETS))

    def fixed_schedule(self, *amounts):
        asset = self.fixed_asset()
        return tuple(
            {"kind": "fixed", "amount": int(amount), "asset": asset}
            for amount in amounts
        )

    def test_first_claim_posts_balanced_ledger_and_audit_state(self):
        schedule = self.fixed_schedule(2)
        before = int(self.w1.balance)
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            result = claim_daily_reward(user=self.u1, at=self.instant())

        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()
        claim = DailyRewardClaim.objects.get(user=self.u1)
        state = DailyRewardState.objects.get(user=self.u1)

        expected = schedule[0]["amount"] * (10 ** config.PLATFORM_TOKEN_DECIMALS)
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

    def test_daily_chest_stays_pending_until_cycle_reset(self):
        chest_key = next(iter(config.get_reward_chest_definitions()))
        chest_schedule = (
            {"kind": "chest", "chest": chest_key},
            *self.fixed_schedule(1, 1),
        )
        before = int(self.w1.balance)
        prepared_at = self.instant()
        next_day = prepared_at + timedelta(days=1)

        with (
            patch.object(config, "DAILY_REWARDS", chest_schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
            patch(
                "ledger.dashboard.reward_chests.timezone.now",
                return_value=prepared_at,
            ),
        ):
            prepared = prepare_daily_reward_chest(
                user=self.u1,
                at=prepared_at,
            )
            expected_reset = get_daily_reward_cycle_reset(
                at=prepared_at,
                cycle_day=prepared["cycle_day"],
                cycle_length=len(chest_schedule),
            )

            prepared["grant"].refresh_from_db()
            state = DailyRewardState.objects.get(user=self.u1)
            self.w1.refresh_from_db()
            self.assertEqual(
                prepared["grant"].status,
                RewardChestGrant.STATUS_PENDING,
            )
            self.assertEqual(prepared["grant"].expires_at, expected_reset)
            self.assertEqual(self.w1.balance, before)
            self.assertEqual(state.last_claim_date, prepared["reward_date"])
            self.assertFalse(
                DailyRewardClaim.objects.filter(user=self.u1).exists()
            )

            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=next_day,
            )
            self.assertIsNotNone(context["pending_chest"])
            self.assertEqual(
                context["pending_chest"]["grant_public_id"],
                str(prepared["grant"].public_id),
            )
            self.assertEqual(context["cycle_day"], 2)
            self.assertTrue(context["can_claim"])

            next_day_result = claim_daily_reward(
                user=self.u1,
                at=next_day,
            )
            self.assertTrue(next_day_result["claimed"])
            self.assertEqual(next_day_result["cycle_day"], 2)
            balance_before_chest_open = int(
                TokenWallet.objects.get(pk=self.w1.pk).balance
            )

            with patch(
                "ledger.dashboard.reward_chests.secrets.randbelow",
                return_value=0,
            ):
                result = claim_daily_reward(
                    user=self.u1,
                    at=next_day,
                    grant_public_id=str(prepared["grant"].public_id),
                )

        self.assertTrue(result["claimed"])
        prepared["grant"].refresh_from_db()
        self.w1.refresh_from_db()
        self.assertEqual(
            prepared["grant"].status,
            RewardChestGrant.STATUS_OPENED,
        )
        self.assertGreater(self.w1.balance, balance_before_chest_open)
        self.assertEqual(
            DailyRewardClaim.objects.filter(user=self.u1).count(),
            2,
        )

    def test_pending_daily_chest_expires_at_cycle_reset(self):
        chest_key = next(iter(config.get_reward_chest_definitions()))
        chest_schedule = (
            {"kind": "chest", "chest": chest_key},
            *self.fixed_schedule(1),
        )
        prepared_at = self.instant()
        with (
            patch.object(config, "DAILY_REWARDS", chest_schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
            patch(
                "ledger.dashboard.reward_chests.timezone.now",
                return_value=prepared_at,
            ),
        ):
            prepared = prepare_daily_reward_chest(
                user=self.u1,
                at=prepared_at,
            )
            with self.assertRaises(ValidationError):
                claim_daily_reward(
                    user=self.u1,
                    at=prepared["grant"].expires_at,
                    grant_public_id=str(prepared["grant"].public_id),
                )


    def test_second_claim_same_day_is_idempotent(self):
        schedule = self.fixed_schedule(1)
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
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
        schedule = self.fixed_schedule(1, 2)
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            first = claim_daily_reward(user=self.u1, at=self.instant(day=24))
            second = claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(first["streak_day"], 1)
        self.assertEqual(second["streak_day"], 2)
        self.assertEqual(second["cycle_day"], 2)
        self.assertEqual(
            second["amount_units"],
            schedule[1]["amount"] * (10 ** config.PLATFORM_TOKEN_DECIMALS),
        )

    def test_missing_a_day_resets_streak(self):
        schedule = self.fixed_schedule(1, 2)
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            claim_daily_reward(user=self.u1, at=self.instant(day=20))
            result = claim_daily_reward(user=self.u1, at=self.instant(day=22))

        self.assertEqual(result["streak_day"], 1)
        self.assertEqual(result["cycle_day"], 1)

    def test_missing_day_resets_display_context_to_day_one(self):
        reward_date = get_daily_reward_date(self.instant(day=24))
        prior_streak = len(config.get_daily_reward_definitions())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=prior_streak,
            total_claims=prior_streak,
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

    def test_daily_cycle_config_is_structurally_valid(self):
        definitions = config.get_daily_reward_definitions()
        self.assertTrue(definitions)
        self.assertEqual(
            [reward.day for reward in definitions],
            list(range(1, len(definitions) + 1)),
        )
        self.assertLessEqual(
            int(config.DAILY_REWARD_WINDOW_SIZE),
            len(definitions),
        )
        for reward in definitions:
            if reward.kind == "fixed":
                self.assertGreater(reward.amount_tokens, 0)
                config.get_daily_reward_asset_definition(
                    reward.asset,
                    amount_tokens=reward.amount_tokens,
                )
            else:
                chest = config.get_reward_chest_definition(reward.chest_key)
                self.assertEqual(chest.key, reward.chest_key)


    def test_cycle_wraps_after_last_configured_day(self):
        schedule = self.fixed_schedule(1, 2, 3)
        cycle_length = len(schedule)
        previous_date = get_daily_reward_date(self.instant(day=24))
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=cycle_length,
            total_claims=cycle_length,
            last_claim_date=previous_date,
        )
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            result = claim_daily_reward(user=self.u1, at=self.instant(day=25))

        self.assertEqual(result["streak_day"], cycle_length + 1)
        self.assertEqual(result["cycle_day"], 1)

    def test_blocked_and_review_wallets_cannot_claim(self):
        schedule = self.fixed_schedule(1)
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
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
        schedule = self.fixed_schedule(1, 2, 3)
        claimed_day = 2
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=claimed_day,
            total_claims=claimed_day,
            last_claim_date=reward_date,
        )

        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", len(schedule)),
        ):
            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=self.instant(),
            )

        window_reward = next(
            reward
            for reward in context["window"]
            if reward["day"] == claimed_day
        )
        modal_reward = next(
            reward
            for reward in context["all_rewards"]
            if reward["day"] == claimed_day
        )

        self.assertTrue(context["claimed_today"])
        self.assertEqual(context["cycle_day"], claimed_day)
        self.assertEqual(context["current_reward"]["status"], "claimed")
        self.assertEqual(window_reward["status"], "claimed")
        self.assertEqual(modal_reward["status"], "claimed")


    def test_next_streak_chest_comes_from_reward_rotation(self):
        chest_key = next(iter(config.get_reward_chest_definitions()))
        schedule = (
            *self.fixed_schedule(1, 1),
            {"kind": "chest", "chest": chest_key},
        )
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=self.instant(),
            )
        chest = config.get_reward_chest_definition(chest_key)

        self.assertEqual(context["cycle_day"], 1)
        self.assertEqual(context["next_chest"]["day"], len(schedule))
        self.assertEqual(context["next_chest"]["chest_key"], chest_key)
        self.assertEqual(
            context["next_chest"]["days_until_unlock"],
            len(schedule) - 1,
        )
        self.assertEqual(context["next_chest"]["image_path"], chest.closed_image)

    def test_next_streak_chest_advances_after_current_day_is_claimed(self):
        chest_key = next(iter(config.get_reward_chest_definitions()))
        schedule = (
            *self.fixed_schedule(1, 1),
            {"kind": "chest", "chest": chest_key},
        )
        target_day = len(schedule)
        previous_day = target_day - 1
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=previous_day,
            total_claims=previous_day,
            last_claim_date=reward_date,
        )

        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=self.instant(),
            )
        chest = config.get_reward_chest_definition(chest_key)

        self.assertTrue(context["claimed_today"])
        self.assertEqual(context["cycle_day"], previous_day)
        self.assertEqual(context["next_chest"]["day"], target_day)
        self.assertEqual(context["next_chest"]["chest_key"], chest_key)
        self.assertEqual(context["next_chest"]["days_until_unlock"], 1)
        self.assertEqual(context["next_chest"]["image_path"], chest.closed_image)

    def test_next_streak_chest_wraps_with_the_reward_cycle(self):
        chest_key = next(iter(config.get_reward_chest_definitions()))
        schedule = (
            {"kind": "chest", "chest": chest_key},
            *self.fixed_schedule(1, 1),
        )
        cycle_length = len(schedule)
        reward_date = get_daily_reward_date(self.instant())
        DailyRewardState.objects.create(
            user=self.u1,
            current_streak=cycle_length,
            total_claims=cycle_length,
            last_claim_date=reward_date,
        )

        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=self.instant(),
            )
        chest = config.get_reward_chest_definition(chest_key)

        self.assertTrue(context["claimed_today"])
        self.assertEqual(context["cycle_day"], cycle_length)
        self.assertEqual(context["next_chest"]["day"], 1)
        self.assertEqual(context["next_chest"]["chest_key"], chest_key)
        self.assertEqual(context["next_chest"]["days_until_unlock"], 1)
        self.assertEqual(context["next_chest"]["image_path"], chest.closed_image)


    def test_fixed_reward_assets_come_from_central_wallet_config(self):
        schedule = self.fixed_schedule(1)
        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=self.instant(),
            )
        current_reward = context["current_reward"]
        asset = config.get_daily_reward_asset_definition(
            current_reward["asset"],
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


    def test_daily_reward_asset_tiers_follow_reward_amount(self):
        asset_key, raw_asset = next(
            (key, value)
            for key, value in config.DAILY_REWARD_ASSETS.items()
            if isinstance(value, dict) and len(value.get("tiers") or ()) >= 2
        )
        tiers = raw_asset["tiers"]
        lower_tier = tiers[0]
        upper_tier = tiers[1]
        self.assertGreater(upper_tier["min_amount"], lower_tier["min_amount"])
        below_threshold = config.get_daily_reward_asset_definition(
            asset_key,
            amount_tokens=upper_tier["min_amount"] - 1,
        )
        at_threshold = config.get_daily_reward_asset_definition(
            asset_key,
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

    def test_daily_reward_context_uses_configured_asset_tier(self):
        asset_key, raw_asset = next(
            (key, value)
            for key, value in config.DAILY_REWARD_ASSETS.items()
            if isinstance(value, dict) and len(value.get("tiers") or ()) >= 2
        )
        threshold = raw_asset["tiers"][1]["min_amount"]
        schedule = (
            {"kind": "fixed", "amount": threshold, "asset": asset_key},
        )
        expected_asset = config.get_daily_reward_asset_definition(
            asset_key,
            amount_tokens=threshold,
        )

        with (
            patch.object(config, "DAILY_REWARDS", schedule),
            patch.object(config, "DAILY_REWARD_WINDOW_SIZE", 1),
        ):
            context = build_daily_rewards_context(
                user=self.u1,
                claim_url="/wallet/daily-reward/claim/",
                at=self.instant(),
            )

        self.assertEqual(context["current_reward"]["amount_tokens"], threshold)
        self.assertEqual(
            context["current_reward"]["image_path"],
            expected_asset["image"],
        )
        self.assertEqual(
            context["current_reward"]["asset_tier_min_amount"],
            expected_asset["tier_min_amount"],
        )

    def test_daily_reward_asset_tiers_reject_invalid_order(self):
        asset_key = self.fixed_asset()
        valid_asset = config.DAILY_REWARD_ASSETS[asset_key]
        button_asset = valid_asset["button_asset"]
        valid_tier = valid_asset["tiers"][0]
        changed = {
            asset_key: {
                "button_asset": button_asset,
                "tiers": (
                    {
                        "min_amount": 2,
                        "image_asset": valid_tier["image_asset"],
                    },
                    {
                        "min_amount": 1,
                        "image_asset": valid_tier["image_asset"],
                    },
                ),
            },
        }
        with patch.object(config, "DAILY_REWARD_ASSETS", changed):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_asset_definition(
                    asset_key,
                    amount_tokens=2,
                )


    def test_wallet_asset_paths_are_validated(self):
        changed = dict(config.WALLET_ASSETS)
        changed["hero_art"] = "../outside-static.png"
        with patch.object(config, "WALLET_ASSETS", changed):
            with self.assertRaises(ImproperlyConfigured):
                config.get_wallet_asset_paths()

    def test_token_pack_asset_path_is_centralized(self):
        pack_key = next(iter(config.WALLET_TOKEN_PACK_ASSETS))
        self.assertEqual(
            config.get_wallet_token_pack_image_path(pack_key),
            config.WALLET_TOKEN_PACK_ASSETS[pack_key],
        )

    def test_config_rejects_invalid_or_dangerous_amounts(self):
        asset_key = self.fixed_asset()
        with patch.object(
            config,
            "DAILY_REWARDS",
            ({"amount": 0, "asset": asset_key},),
        ):
            with self.assertRaises(ImproperlyConfigured):
                config.get_daily_reward_definitions()
        with patch.object(
            config,
            "DAILY_REWARDS",
            ({
                "amount": config.DAILY_REWARD_MAX_TOKENS_PER_CLAIM + 1,
                "asset": asset_key,
            },),
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
