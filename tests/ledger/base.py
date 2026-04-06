from django.contrib.auth.models import Permission
from django.test import TestCase
from django.apps import apps
from django.contrib.auth.management import create_permissions
from django.contrib.contenttypes.models import ContentType

from files.tests import create_account
from ledger.models import (
    LedgerHold,
    LedgerOutbox,
    LedgerSaga,
    LedgerTransaction,
    LedgerVelocityWindow,
    TokenWallet,
    DepositSession,
    ObservedOnchainTransfer,
    DepositAddress,
)
from ledger.services import get_system_wallet
PERMISSION_MODEL_BY_CODENAME = {
    "can_apply_raw_ledger_transaction": LedgerTransaction,
    "can_create_pending_ledger_transaction": LedgerTransaction,
    "can_reverse_ledger_transaction": LedgerTransaction,
    "can_impersonate_ledger_creator": LedgerTransaction,
    "can_manage_ledger_outbox": LedgerOutbox,
    "can_manage_ledger_sagas": LedgerSaga,
    "can_compensate_ledger_sagas": LedgerSaga,
    "can_manage_wallet_risk": TokenWallet,
    "can_view_wallet_risk": TokenWallet,
    "can_manage_wallet_holds": LedgerHold,
    "can_view_wallet_holds": LedgerHold,
    "can_view_wallet_velocity": LedgerVelocityWindow,
    "can_manage_deposit_sessions": DepositSession,
    "can_view_deposit_sessions": DepositSession,
    "can_credit_confirmed_deposits": DepositSession,
    "can_record_onchain_observations": ObservedOnchainTransfer,
    "can_view_onchain_transfers": ObservedOnchainTransfer,
    "can_manage_deposit_addresses": DepositAddress,
    "can_view_deposit_addresses": DepositAddress,
}
class BaseLedgerTestCase(TestCase):
    def setUp(self):
        self.u1 = create_account(password="pass12345")
        self.u2 = create_account(password="pass12345")
        self.operator = create_account(password="pass12345")

        self.w1 = self.u1.token_wallet
        self.w2 = self.u2.token_wallet

        self.issuance = get_system_wallet(
            TokenWallet.SYSTEM_ISSUANCE,
            allow_negative=True,
        )

        for codename in [
            "can_apply_raw_ledger_transaction",
            "can_create_pending_ledger_transaction",
            "can_reverse_ledger_transaction",
            "can_impersonate_ledger_creator",
            "can_manage_ledger_outbox",
            "can_manage_ledger_sagas",
            "can_compensate_ledger_sagas",
        ]:
            self.grant_perm(self.operator, codename)

    def grant_perm(self, user, codename):
        app_config = apps.get_app_config("ledger")
        create_permissions(app_config, verbosity=0)

        perm = (
            Permission.objects.filter(
                content_type__app_label="ledger",
                codename=codename,
            )
            .order_by("id")
            .first()
        )
        if perm is None:
            model = PERMISSION_MODEL_BY_CODENAME.get(codename)
            if model is None:
                raise Permission.DoesNotExist(f"Unknown ledger permission codename: {codename}")

            content_type = ContentType.objects.get_for_model(model)
            perm, _ = Permission.objects.get_or_create(
                content_type=content_type,
                codename=codename,
                defaults={"name": codename.replace("_", " ")},
            )

        user.user_permissions.add(perm)