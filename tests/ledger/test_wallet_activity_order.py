from datetime import timedelta

from django.test import SimpleTestCase
from django.utils import timezone

from files.views import _build_wallet_activity_rows


class TestWalletActivityChronology(SimpleTestCase):
    def test_rows_are_sorted_by_time_not_status_or_source(self):
        now = timezone.now()
        rows = _build_wallet_activity_rows(
            deposit_rows=[
                {
                    "created_at": now - timedelta(minutes=5),
                    "status": "canceled",
                },
            ],
            request_rows=[
                {
                    "created_at": now - timedelta(minutes=2),
                    "status": "canceled",
                },
            ],
            transaction_rows=[
                {
                    "created_at": now,
                    "status": "posted",
                },
                {
                    "created_at": now - timedelta(minutes=10),
                    "status": "posted",
                },
            ],
        )

        self.assertEqual(
            [row["activity_type"] for row in rows],
            ["transaction", "request", "deposit", "transaction"],
        )
        self.assertEqual(
            [row["status"] for row in rows],
            ["posted", "canceled", "canceled", "posted"],
        )
        self.assertEqual(
            [row["created_at"] for row in rows],
            sorted(
                [row["created_at"] for row in rows],
                reverse=True,
            ),
        )

    def test_limit_is_applied_after_global_sort(self):
        now = timezone.now()
        rows = _build_wallet_activity_rows(
            deposit_rows=[
                {
                    "created_at": now - timedelta(minutes=3),
                    "status": "canceled",
                },
            ],
            request_rows=[
                {
                    "created_at": now - timedelta(minutes=2),
                    "status": "pending",
                },
            ],
            transaction_rows=[
                {
                    "created_at": now - timedelta(minutes=1),
                    "status": "posted",
                },
            ],
            limit=2,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            [row["activity_type"] for row in rows],
            ["transaction", "request"],
        )
