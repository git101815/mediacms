from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from ledger.models import TokenWallet


@override_settings(LOGIN_URL="/accounts/login")
class TestPublicWalletPreview(TestCase):
    def test_anonymous_wallet_renders_preview_without_creating_wallet(self):
        before_count = TokenWallet.objects.count()

        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.context["wallet_auth_required"])
        self.assertEqual(response.context["available_balance_display"], "100")
        self.assertEqual(response.context["total_balance_display"], "100")
        self.assertEqual(response.context["held_balance_display"], "0")
        self.assertEqual(response.context["daily_rewards"]["cycle_day"], 1)
        self.assertTrue(response.context["daily_rewards"]["preview"])
        self.assertTrue(response.context["daily_rewards"]["can_claim"])
        self.assertEqual(TokenWallet.objects.count(), before_count)
        self.assertContains(
            response,
            'data-wallet-auth-required="true"',
        )
        self.assertIn(
            "/accounts/login?next=",
            response.context["wallet_login_url"],
        )

    def test_authenticated_wallet_uses_real_balance(self):
        user = get_user_model().objects.create_user(
            username="public_wallet_preview_user",
            password="test-password",
        )
        wallet = TokenWallet.objects.get(
            user=user,
            wallet_type=TokenWallet.TYPE_USER,
        )
        wallet.balance = 7 * 1_000_000
        wallet.allow_negative = False
        wallet.save(update_fields=["balance", "allow_negative"])
        self.client.force_login(user)

        response = self.client.get(reverse("wallet"))

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.context["wallet_auth_required"])
        self.assertEqual(response.context["available_balance_display"], "7")
        self.assertFalse(response.context["daily_rewards"]["preview"])

    def test_wallet_mutations_still_require_login(self):
        protected_posts = (
            (reverse("wallet_claim_daily_reward"), {}),
            (reverse("wallet_open_bonus_vault"), {}),
            (
                reverse(
                    "wallet_claim_quest",
                    kwargs={"quest_key": "confirm_email"},
                ),
                {},
            ),
            (reverse("wallet_deposit_request"), {}),
            (reverse("wallet_withdrawal_request"), {}),
            (reverse("wallet_purchase_ad_free"), {}),
        )

        for url, payload in protected_posts:
            with self.subTest(url=url):
                response = self.client.post(url, payload)
                self.assertEqual(response.status_code, 302)
                self.assertIn("/accounts/login", response["Location"])
