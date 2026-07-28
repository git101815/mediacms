import json

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from files.views import _build_reward_chest_preview_catalog
from ledger.dashboard import config


class TestRewardChestPreviewCatalog(TestCase):
    def test_catalog_contains_visual_drops_but_no_rates(self):
        catalog = _build_reward_chest_preview_catalog()

        self.assertEqual(
            {row["key"] for row in catalog},
            set(config.REWARD_CHESTS),
        )
        self.assertNotIn("chance", json.dumps(catalog).lower())

        for row in catalog:
            self.assertEqual(
                set(row),
                {"key", "label", "drops"},
            )
            definition = config.get_reward_chest_definition(
                row["key"]
            )
            self.assertEqual(
                len(row["drops"]),
                len(definition.drops),
            )

            for preview_drop, definition_drop in zip(
                row["drops"],
                definition.drops,
            ):
                self.assertEqual(
                    set(preview_drop),
                    {
                        "key",
                        "label",
                        "rarity",
                        "rarity_label",
                        "image_url",
                    },
                )
                self.assertEqual(
                    preview_drop["key"],
                    definition_drop.key,
                )
                self.assertEqual(
                    preview_drop["label"],
                    definition_drop.label,
                )
                expected_path = (
                    config.get_reward_chest_drop_image_path(
                        chest_key=row["key"],
                        drop_key=definition_drop.key,
                    )
                )
                self.assertTrue(
                    preview_drop["image_url"].endswith(
                        expected_path
                    )
                )

            self.assertEqual(
                len({
                    preview_drop["image_url"]
                    for preview_drop in row["drops"]
                }),
                len(row["drops"]),
            )

        huge = next(
            row for row in catalog
            if row["key"] == "big_chest"
        )
        self.assertEqual(
            [drop["rarity"] for drop in huge["drops"]],
            [
                "common",
                "uncommon",
                "rare",
                "epic",
                "legendary",
            ],
        )

    def test_guest_wallet_context_contains_catalog(self):
        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.context["reward_chest_catalog"],
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
