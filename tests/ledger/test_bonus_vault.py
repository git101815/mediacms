from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.urls import reverse

from ledger.dashboard import config
from ledger.dashboard.bonus_vault import (
    build_bonus_vault_context,
    open_bonus_vault,
)
from ledger.dashboard.models import RewardChestGrant
from ledger.models import (
    LEDGER_ACTION_PURCHASE,
    LedgerEntry,
    LedgerTransaction,
    TokenWallet,
)
from ledger.services import get_system_wallet
from tests.ledger.base import BaseLedgerTestCase


class TestBonusVault(BaseLedgerTestCase):
    def setUp(self):
        super().setUp()
        start_at_patch = patch.object(config, "BONUS_VAULT_START_AT", None)
        start_at_patch.start()
        self.addCleanup(start_at_patch.stop)
        self.platform = get_system_wallet(
            TokenWallet.SYSTEM_PLATFORM_FEES,
            allow_negative=False,
        )
        self.w1.balance = 100_000 * (10 ** config.PLATFORM_TOKEN_DECIMALS)
        self.w1.save(update_fields=["balance", "updated_at"])

    def _record_purchase(self, amount_tokens: int, *, buyer=None, wallet=None):
        buyer = buyer or self.u1
        wallet = wallet or self.w1
        amount_units = amount_tokens * (10 ** config.PLATFORM_TOKEN_DECIMALS)

        wallet.refresh_from_db()
        self.platform.refresh_from_db()
        wallet.balance = int(wallet.balance) - amount_units
        self.platform.balance = int(self.platform.balance) + amount_units
        wallet.save(update_fields=["balance", "updated_at"])
        self.platform.save(update_fields=["balance", "updated_at"])

        txn = LedgerTransaction.objects.create(
            kind=LEDGER_ACTION_PURCHASE,
            status=LedgerTransaction.STATUS_POSTED,
            created_by=buyer,
            memo="Bonus Vault test purchase",
        )
        LedgerEntry.objects.create(
            txn=txn,
            wallet=wallet,
            delta=-amount_units,
            balance_after=wallet.balance,
        )
        LedgerEntry.objects.create(
            txn=txn,
            wallet=self.platform,
            delta=amount_units,
            balance_after=self.platform.balance,
        )
        return txn

    def _context(self):
        return build_bonus_vault_context(
            user=self.u1,
            wallet=self.w1,
            open_url="/wallet/bonus-vault/open",
        )

    def test_purchase_spend_fills_vault(self):
        self._record_purchase(2_500)

        context = self._context()

        self.assertEqual(context["progress_percent"], 25)
        self.assertEqual(context["remaining_tokens"], 7_500)
        self.assertEqual(context["ready_count"], 0)
        self.assertFalse(context["can_open"])

    def test_non_debit_purchase_entry_does_not_fill_vault(self):
        amount_units = 5_000 * (10 ** config.PLATFORM_TOKEN_DECIMALS)
        txn = LedgerTransaction.objects.create(
            kind=LEDGER_ACTION_PURCHASE,
            status=LedgerTransaction.STATUS_POSTED,
            created_by=self.u2,
            memo="Creator revenue",
        )
        LedgerEntry.objects.create(
            txn=txn,
            wallet=self.w1,
            delta=amount_units,
            balance_after=self.w1.balance + amount_units,
        )

        context = self._context()

        self.assertEqual(context["total_eligible_spend_units"], 0)
        self.assertEqual(context["progress_percent"], 0)

    def test_full_vault_opens_generic_reward_chest_once(self):
        self._record_purchase(config.BONUS_VAULT_THRESHOLD_TOKENS)
        balance_after_purchase = int(
            TokenWallet.objects.get(pk=self.w1.pk).balance
        )

        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ):
            result = open_bonus_vault(user=self.u1)

        self.w1.refresh_from_db()
        grant = RewardChestGrant.objects.get(
            user=self.u1,
            source_type=config.BONUS_VAULT_SOURCE_TYPE,
        )

        self.assertTrue(result["opened"])
        self.assertEqual(grant.status, RewardChestGrant.STATUS_OPENED)
        self.assertEqual(grant.chest_key, config.BONUS_VAULT_CHEST_KEY)
        self.assertEqual(
            grant.metadata["bonus_vault_threshold_tokens"],
            config.BONUS_VAULT_THRESHOLD_TOKENS,
        )
        self.assertEqual(
            self.w1.balance,
            balance_after_purchase + result["amount_units"],
        )

        with self.assertRaises(ValidationError):
            open_bonus_vault(user=self.u1)

    def test_multiple_completed_vaults_are_opened_one_by_one(self):
        self._record_purchase(config.BONUS_VAULT_THRESHOLD_TOKENS * 2)
        self.assertEqual(self._context()["ready_count"], 2)

        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ):
            open_bonus_vault(user=self.u1)

        self.assertEqual(self._context()["ready_count"], 1)

        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ):
            open_bonus_vault(user=self.u1)

        context = self._context()
        self.assertEqual(context["ready_count"], 0)
        self.assertEqual(context["progress_percent"], 0)
        self.assertEqual(
            RewardChestGrant.objects.filter(
                user=self.u1,
                source_type=config.BONUS_VAULT_SOURCE_TYPE,
                status=RewardChestGrant.STATUS_OPENED,
            ).count(),
            2,
        )

    def test_open_endpoint_requires_post_and_opens_ready_vault(self):
        self._record_purchase(config.BONUS_VAULT_THRESHOLD_TOKENS)
        self.client.force_login(self.u1)
        url = reverse("wallet_open_bonus_vault")

        self.assertEqual(self.client.get(url).status_code, 405)

        with patch(
            "ledger.dashboard.reward_chests.secrets.randbelow",
            return_value=0,
        ):
            response = self.client.post(url)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            RewardChestGrant.objects.filter(
                user=self.u1,
                source_type=config.BONUS_VAULT_SOURCE_TYPE,
                status=RewardChestGrant.STATUS_OPENED,
            ).count(),
            1,
        )
