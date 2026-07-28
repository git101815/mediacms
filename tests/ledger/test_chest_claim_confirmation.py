from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse


class TestChestClaimConfirmationRouting(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="chest_confirmation_user",
            password="test-password",
        )
        self.client.force_login(self.user)
        self.headers = {
            "HTTP_X_REQUESTED_WITH": "XMLHttpRequest",
            "HTTP_ACCEPT": "application/json",
        }

    def test_weekly_initial_post_only_prepares(self):
        url = reverse(
            "wallet_open_weekly_quest",
            kwargs={
                "cycle_key": "2026-W31",
                "quest_key": "share_site",
            },
        )
        prepared = {
            "grant": SimpleNamespace(
                public_id="11111111-1111-1111-1111-111111111111"
            )
        }

        with (
            patch(
                "ledger.dashboard.views.prepare_weekly_quest_reward",
                return_value=prepared,
            ) as prepare_mock,
            patch(
                "ledger.dashboard.views.open_weekly_quest_reward",
            ) as open_mock,
            patch(
                "ledger.dashboard.views."
                "_build_pending_chest_opening_payload",
                return_value={
                    "pending": True,
                    "grant_public_id": (
                        "11111111-1111-1111-1111-111111111111"
                    ),
                },
            ),
        ):
            response = self.client.post(url, **self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["opening"]["pending"])
        prepare_mock.assert_called_once()
        open_mock.assert_not_called()

    def test_weekly_confirmation_opens_exact_grant(self):
        url = reverse(
            "wallet_open_weekly_quest",
            kwargs={
                "cycle_key": "2026-W31",
                "quest_key": "share_site",
            },
        )
        public_id = "11111111-1111-1111-1111-111111111111"

        with (
            patch(
                "ledger.dashboard.views.prepare_weekly_quest_reward",
            ) as prepare_mock,
            patch(
                "ledger.dashboard.views.open_weekly_quest_reward",
                return_value={"opened": True},
            ) as open_mock,
            patch(
                "ledger.dashboard.views._build_chest_opening_payload",
                return_value={"pending": False},
            ),
        ):
            response = self.client.post(
                url,
                data={
                    "confirm_open": "1",
                    "grant_public_id": public_id,
                },
                **self.headers,
            )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["opening"]["pending"])
        prepare_mock.assert_not_called()
        open_mock.assert_called_once_with(
            user=self.user,
            cycle_key="2026-W31",
            quest_key="share_site",
            grant_public_id=public_id,
        )
