from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import TestCase

from ledger.models import P2PMakerProfile


class P2PMakerProfileTests(TestCase):
    def _user(self, suffix):
        return get_user_model().objects.create_user(
            username=f"p2p-maker-{suffix}",
            email=f"p2p-maker-{suffix}@example.test",
        )

    def test_safe_defaults_and_one_hot_payment_methods(self):
        maker = P2PMakerProfile.objects.create(user=self._user("defaults"))

        self.assertEqual(maker.status, P2PMakerProfile.STATUS_PAUSED)
        self.assertFalse(maker.accepting_orders)
        self.assertFalse(maker.paypal_enabled)
        self.assertFalse(maker.revolut_enabled)
        self.assertFalse(maker.sepa_enabled)
        self.assertFalse(maker.wise_enabled)
        self.assertFalse(maker.bank_transfer_enabled)
        self.assertEqual(maker.max_concurrent_orders, 1)
        self.assertEqual(maker.completed_orders, 0)
        self.assertEqual(maker.canceled_orders, 0)
        self.assertEqual(maker.disputed_orders, 0)
        self.assertEqual(maker.rating_count, 0)
        self.assertEqual(maker.total_volume, 0)
        self.assertIsNone(maker.rating)
        self.assertIsNone(maker.avg_response_time_seconds)
        self.assertIsNone(maker.avg_completion_time_seconds)
        self.assertEqual(maker.user.p2p_maker_profile, maker)

    def test_profile_stores_pool_settings_and_platform_value_stats(self):
        maker = P2PMakerProfile.objects.create(
            user=self._user("configured"),
            status=P2PMakerProfile.STATUS_ACTIVE,
            accepting_orders=True,
            paypal_enabled=True,
            revolut_enabled=True,
            min_order_amount=10_000_000,
            max_order_amount=500_000_000,
            commission_percent=Decimal("4.25"),
            max_concurrent_orders=3,
            completed_orders=42,
            canceled_orders=2,
            disputed_orders=1,
            avg_response_time_seconds=37,
            avg_completion_time_seconds=418,
            rating=Decimal("4.80"),
            rating_count=25,
            total_volume=12_345_678_000,
        )

        self.assertTrue(maker.paypal_enabled)
        self.assertTrue(maker.revolut_enabled)
        self.assertEqual(maker.min_order_amount, 10_000_000)
        self.assertEqual(maker.max_order_amount, 500_000_000)
        self.assertEqual(maker.total_volume, 12_345_678_000)
        self.assertEqual(maker.commission_percent, Decimal("4.25"))
        self.assertEqual(maker.rating, Decimal("4.80"))

    def test_max_order_cannot_be_lower_than_min_order(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                P2PMakerProfile.objects.create(
                    user=self._user("bad-range"),
                    min_order_amount=20_000_000,
                    max_order_amount=10_000_000,
                )

    def test_commission_must_be_between_zero_and_one_hundred_percent(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                P2PMakerProfile.objects.create(
                    user=self._user("bad-commission"),
                    commission_percent=Decimal("100.01"),
                )

    def test_rating_must_be_between_zero_and_five(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                P2PMakerProfile.objects.create(
                    user=self._user("bad-rating"),
                    rating=Decimal("5.01"),
                    rating_count=1,
                )
