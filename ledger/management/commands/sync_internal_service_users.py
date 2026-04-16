from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.management.base import BaseCommand
from django.db import transaction


SERVICE_USER_SPECS = [
    {
        "setting": "LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME",
        "permissions": [
            "ledger.can_record_onchain_observations",
            "ledger.can_apply_raw_ledger_transaction",
            "ledger.can_credit_confirmed_deposits",
            "ledger.can_manage_deposit_sweep_jobs",
            "ledger.can_view_deposit_sessions",
            "ledger.can_view_onchain_transfers",
        ],
    },
    {
        "setting": "LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME",
        "permissions": [
            "ledger.can_manage_deposit_sweep_jobs",
            "ledger.can_view_deposit_sessions",
            "ledger.can_view_onchain_transfers",
        ],
    },
]

class Command(BaseCommand):
    help = "Create or update internal ledger service users and bind required permissions."

    @transaction.atomic
    def handle(self, *args, **options):
        user_model = get_user_model()

        for spec in SERVICE_USER_SPECS:
            username = getattr(settings, spec["setting"], "").strip()
            if not username:
                raise RuntimeError(f"{spec['setting']} is not configured")

            user, _created = user_model.objects.get_or_create(
                username=username,
                defaults={
                    "email": "",
                    "is_active": True,
                    "is_staff": False,
                    "is_superuser": False,
                },
            )

            user.is_active = True
            user.is_staff = False
            user.is_superuser = False
            user.save(update_fields=["is_active", "is_staff", "is_superuser"])

            expected_perms = set()
            for perm_name in spec["permissions"]:
                app_label, codename = perm_name.split(".", 1)
                perm = Permission.objects.get(
                    content_type__app_label=app_label,
                    codename=codename,
                )
                expected_perms.add(perm.id)

            current_perm_ids = set(user.user_permissions.values_list("id", flat=True))

            to_add = expected_perms - current_perm_ids
            to_remove = current_perm_ids - expected_perms

            if to_add:
                user.user_permissions.add(*to_add)
            if to_remove:
                user.user_permissions.remove(*to_remove)

            self.stdout.write(
                self.style.SUCCESS(
                    f"synchronized internal service user {username}"
                )
            )