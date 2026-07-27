from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase
from django.urls import reverse

from files.views import (
    _build_guest_wallet_context,
    _build_reward_chest_preview_catalog,
)
from ledger.dashboard import config


class TestRewardChestPreviewCatalog(TestCase):
    def test_catalog_contains_drop_labels_but_no_drop_rates(self):
        catalog = _build_reward_chest_preview_catalog()

        self.assertEqual(
            {row["key"] for row in catalog},
            set(config.REWARD_CHESTS),
        )

        for row in catalog:
            self.assertEqual(
                set(row),
                {"key", "label", "drop_labels"},
            )
            self.assertTrue(row["drop_labels"])

        medium = next(
            row
            for row in catalog
            if row["key"] == "medium_chest"
        )
        definition = config.get_reward_chest_definition(
            "medium_chest"
        )

        self.assertEqual(
            medium["drop_labels"],
            [drop.label for drop in definition.drops],
        )

    def test_guest_wallet_context_contains_catalog(self):
        request = RequestFactory().get("/wallet")
        context = _build_guest_wallet_context(request)

        self.assertEqual(
            context["reward_chest_catalog"],
            _build_reward_chest_preview_catalog(),
        )

    def test_authenticated_wallet_context_contains_catalog(self):
        user = get_user_model().objects.create_user(
            username="reward_chest_preview_user",
            password="test-password",
        )
        self.client.force_login(user)

        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.context["reward_chest_catalog"],
            _build_reward_chest_preview_catalog(),
        )
