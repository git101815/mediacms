from unittest.mock import patch

from allauth.account.models import EmailAddress
from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.quests import (
    QUEST_REWARD_OUTBOX_TOPIC,
    QUEST_REWARD_TRANSACTION_KIND,
    build_quest_board_context,
    claim_quest_reward,
    get_quest_definitions,
)
from ledger.models import LedgerEntry, LedgerOutbox, LedgerTransaction
from tests.ledger.base import BaseLedgerTestCase


class TestQuestBoard(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        if not self.u1.email:
            self.u1.email = f"quest-{self.u1.pk}@example.test"
            self.u1.save(update_fields=["email"])

        self.email_address, _created = EmailAddress.objects.update_or_create(
            user=self.u1,
            email=self.u1.email,
            defaults={
                "primary": True,
                "verified": False,
            },
        )

    def _verify_email(self):
        self.email_address.verified = True
        self.email_address.save(update_fields=["verified"])

    def _context(self):
        return build_quest_board_context(user=self.u1)

    def test_config_contains_only_confirm_email_and_three_empty_slots(self):
        definitions = get_quest_definitions()
        context = self._context()

        self.assertEqual(len(definitions), 1)
        self.assertEqual(definitions[0].key, "confirm_email")
        self.assertEqual(context["active_count"], 1)
        self.assertEqual(context["slot_count"], 4)
        self.assertEqual(
            sum(1 for row in context["slots"] if row["empty"]),
            3,
        )

    def test_one_time_schedule_is_not_exposed(self):
        context = self._context()

        self.assertEqual(context["reset_label"], "One-time")
        self.assertFalse(context["show_schedule"])

        with patch.object(config, "QUEST_BOARD_RESET_LABEL", "Daily"):
            recurring_context = self._context()

        self.assertEqual(recurring_context["reset_label"], "Daily")
        self.assertTrue(recurring_context["show_schedule"])

    def test_unverified_email_cannot_claim(self):
        context = self._context()
        quest = context["slots"][0]

        self.assertEqual(quest["key"], "confirm_email")
        self.assertEqual(quest["current"], 0)
        self.assertEqual(quest["target"], 1)
        self.assertFalse(quest["complete"])
        self.assertFalse(quest["can_claim"])
        self.assertEqual(quest["action_url"], reverse("account_email"))

        with self.assertRaises(ValidationError):
            claim_quest_reward(
                user=self.u1,
                quest_key="confirm_email",
            )

        self.w1.refresh_from_db()
        self.assertEqual(self.w1.balance, 0)
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=QUEST_REWARD_TRANSACTION_KIND,
            ).count(),
            0,
        )

    def test_verified_email_can_claim_configured_reward_once(self):
        self._verify_email()
        definition = get_quest_definitions()[0]
        context = self._context()
        quest = context["slots"][0]

        self.assertTrue(quest["complete"])
        self.assertTrue(quest["can_claim"])
        self.assertFalse(quest["claimed"])

        before_user = int(self.w1.balance)
        before_issuance = int(self.issuance.balance)

        first = claim_quest_reward(
            user=self.u1,
            quest_key=definition.key,
        )
        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()

        self.assertTrue(first["claimed"])
        self.assertFalse(first["already_claimed"])
        self.assertEqual(
            self.w1.balance,
            before_user + definition.reward_units,
        )
        self.assertEqual(
            self.issuance.balance,
            before_issuance - definition.reward_units,
        )

        second = claim_quest_reward(
            user=self.u1,
            quest_key=definition.key,
        )
        self.w1.refresh_from_db()

        self.assertFalse(second["claimed"])
        self.assertTrue(second["already_claimed"])
        self.assertEqual(
            self.w1.balance,
            before_user + definition.reward_units,
        )
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=QUEST_REWARD_TRANSACTION_KIND,
            ).count(),
            1,
        )
        self.assertEqual(
            LedgerEntry.objects.filter(txn=first["txn"]).count(),
            2,
        )
        self.assertEqual(
            LedgerOutbox.objects.filter(
                txn=first["txn"],
                topic=QUEST_REWARD_OUTBOX_TOPIC,
            ).count(),
            1,
        )

        claimed_context = self._context()
        claimed_quest = claimed_context["slots"][0]
        self.assertTrue(claimed_quest["claimed"])
        self.assertFalse(claimed_quest["can_claim"])
        self.assertEqual(claimed_quest["status"], "claimed")

    def test_claim_endpoint_requires_post(self):
        self._verify_email()
        self.client.force_login(self.u1)
        url = reverse(
            "wallet_claim_quest",
            kwargs={"quest_key": "confirm_email"},
        )

        self.assertEqual(self.client.get(url).status_code, 405)

        response = self.client.post(url)
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=QUEST_REWARD_TRANSACTION_KIND,
            ).count(),
            1,
        )

    def test_invalid_or_oversized_config_is_rejected(self):
        invalid = (
            {
                "key": "confirm_email",
                "title": "Confirm Email",
                "description": "Verify your email address",
                "condition": "unknown_condition",
                "icon_asset": "quest_confirm_email",
                "action_label": "Confirm",
                "action_url_name": "account_email",
                "reward": {
                    "kind": "fixed",
                    "amount": 50,
                    "asset": "coins",
                },
            },
        )
        with patch.object(config, "QUEST_BOARD_QUESTS", invalid):
            with self.assertRaises(ImproperlyConfigured):
                get_quest_definitions()

        with patch.object(config, "QUEST_BOARD_SLOT_COUNT", 0):
            with self.assertRaises(ImproperlyConfigured):
                get_quest_definitions()
