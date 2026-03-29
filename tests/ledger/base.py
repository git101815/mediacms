from django.contrib.auth.models import Permission
from django.test import TestCase
from django.apps import apps
from django.contrib.auth.management import create_permissions

from files.tests import create_account
from ledger.models import TokenWallet
from ledger.services import get_system_wallet


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
            raise Permission.DoesNotExist(f"ledger permission not found: {codename}")

        user.user_permissions.add(perm)