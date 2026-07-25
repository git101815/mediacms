import hashlib
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import RequestFactory
from django.urls import resolve, reverse

from ledger.dashboard import config
from ledger.dashboard.referrals import (
    REFERRAL_REWARD_OUTBOX_TOPIC,
    REFERRAL_REWARD_TRANSACTION_KIND,
    assign_referrer_from_signup,
    award_referral_for_purchase,
    build_referral_context,
    ensure_referral_code,
)
from ledger.models import (
    LEDGER_ACTION_PURCHASE,
    LedgerEntry,
    LedgerOutbox,
    LedgerTransaction,
)
from tests.ledger.base import BaseLedgerTestCase
from users.forms import SignupForm


class TestReferralProgram(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        self.factory = RequestFactory()
        self.user_model = get_user_model()

    def _invitee(self, suffix):
        return self.user_model.objects.create_user(
            username=f"referral_invitee_{suffix}",
            email=f"referral_invitee_{suffix}@example.test",
            password="test-password",
            name="Referral invitee",
        )

    def _assign(self, invitee):
        code = ensure_referral_code(user=self.u1)
        url = reverse(
            "account_referral_signup",
            kwargs={"referral_code": code},
        )
        request = self.factory.post(url)
        request.resolver_match = resolve(url)
        referrer = assign_referrer_from_signup(
            request=request,
            user=invitee,
        )
        invitee.refresh_from_db()
        self.assertEqual(referrer.pk, self.u1.pk)
        self.assertEqual(invitee.referred_by_id, self.u1.pk)
        return invitee

    def _purchase(self, invitee, suffix):
        external_id = f"test:referral:purchase:{invitee.pk}:{suffix}"
        return LedgerTransaction.objects.create(
            kind=LEDGER_ACTION_PURCHASE,
            external_id=external_id,
            request_hash=hashlib.sha256(external_id.encode()).hexdigest(),
            created_by=invitee,
            memo="Referral test purchase",
            metadata={
                "product": "test",
                "price_tokens": 500 * (10 ** config.PLATFORM_TOKEN_DECIMALS),
            },
        )

    def test_code_is_generated_when_wallet_context_needs_it(self):
        self.assertIsNone(self.u1.referral_code)

        request = self.factory.get("/wallet")
        request.user = self.u1
        first = build_referral_context(
            user=self.u1,
            request=request,
        )
        self.u1.refresh_from_db()
        generated_code = self.u1.referral_code

        self.assertTrue(generated_code)
        self.assertEqual(first["code"], generated_code)

        second = build_referral_context(
            user=self.u1,
            request=request,
        )
        self.u1.refresh_from_db()
        self.assertEqual(self.u1.referral_code, generated_code)
        self.assertEqual(second["code"], generated_code)

    def test_referral_url_is_signup_page_without_cookie(self):
        code = ensure_referral_code(user=self.u1)
        response = self.client.get(
            reverse(
                "account_referral_signup",
                kwargs={"referral_code": code},
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, f"/r/{code}")

    def test_signup_persists_referred_by_in_user_row(self):
        invitee = self._invitee("signup")
        code = ensure_referral_code(user=self.u1)
        url = reverse(
            "account_referral_signup",
            kwargs={"referral_code": code},
        )
        request = self.factory.post(url)
        request.resolver_match = resolve(url)

        form = SignupForm(data={"name": "Referral invitee"})
        self.assertTrue(form.is_valid())
        form.signup(request, invitee)

        invitee.refresh_from_db()
        self.assertEqual(invitee.referred_by_id, self.u1.pk)

        second_referrer = self._invitee("second_referrer")
        second_code = ensure_referral_code(user=second_referrer)
        second_url = reverse(
            "account_referral_signup",
            kwargs={"referral_code": second_code},
        )
        second_request = self.factory.post(second_url)
        second_request.resolver_match = resolve(second_url)
        form.signup(second_request, invitee)

        invitee.refresh_from_db()
        self.assertEqual(invitee.referred_by_id, self.u1.pk)

    def test_first_purchase_rewards_referrer_once(self):
        invitee = self._assign(self._invitee("reward"))
        purchase = self._purchase(invitee, "first")

        before_user = int(self.w1.balance)
        before_issuance = int(self.issuance.balance)
        first = award_referral_for_purchase(
            purchase_txn_id=purchase.pk,
        )

        self.assertTrue(first["awarded"])
        reward_units = config.REFERRAL_REWARD_TOKENS * (
            10 ** config.PLATFORM_TOKEN_DECIMALS
        )
        self.w1.refresh_from_db()
        self.issuance.refresh_from_db()
        self.assertEqual(self.w1.balance, before_user + reward_units)
        self.assertEqual(self.issuance.balance, before_issuance - reward_units)

        reward_txn = LedgerTransaction.objects.get(
            kind=REFERRAL_REWARD_TRANSACTION_KIND,
        )
        self.assertEqual(
            reward_txn.metadata["qualifying_purchase_id"],
            purchase.pk,
        )
        self.assertEqual(
            reward_txn.metadata["invitee_user_id"],
            invitee.pk,
        )
        self.assertEqual(
            LedgerEntry.objects.filter(txn=reward_txn).count(),
            2,
        )
        self.assertEqual(
            LedgerOutbox.objects.filter(
                txn=reward_txn,
                topic=REFERRAL_REWARD_OUTBOX_TOPIC,
            ).count(),
            1,
        )

        second_purchase = self._purchase(invitee, "second")
        second = award_referral_for_purchase(
            purchase_txn_id=second_purchase.pk,
        )
        self.assertFalse(second["awarded"])
        self.assertEqual(second["reason"], "already_rewarded")
        self.assertEqual(
            LedgerTransaction.objects.filter(
                kind=REFERRAL_REWARD_TRANSACTION_KIND,
            ).count(),
            1,
        )

    def test_reward_cap_is_serialized_on_referrer_user(self):
        first_invitee = self._assign(self._invitee("cap_one"))
        second_invitee = self._assign(self._invitee("cap_two"))

        with patch.object(
            config,
            "REFERRAL_MAX_REWARDED_FRIENDS",
            1,
        ):
            first = award_referral_for_purchase(
                purchase_txn_id=self._purchase(
                    first_invitee,
                    "cap_one",
                ).pk,
            )
            second = award_referral_for_purchase(
                purchase_txn_id=self._purchase(
                    second_invitee,
                    "cap_two",
                ).pk,
            )

        self.assertTrue(first["awarded"])
        self.assertFalse(second["awarded"])
        self.assertEqual(second["reason"], "cap_reached")

    def test_context_uses_user_rows_for_counts_and_earnings(self):
        invitee = self._assign(self._invitee("context"))
        award_referral_for_purchase(
            purchase_txn_id=self._purchase(
                invitee,
                "context",
            ).pk,
        )

        request = self.factory.get("/wallet")
        request.user = self.u1
        context = build_referral_context(
            user=self.u1,
            request=request,
        )

        self.assertTrue(context["enabled"])
        self.assertEqual(context["joined_count"], 1)
        self.assertEqual(context["rewarded_count"], 1)
        self.assertEqual(context["pending_count"], 0)
        self.assertEqual(context["earned_display"], "200")
        self.assertIn("/r/", context["share_url"])
