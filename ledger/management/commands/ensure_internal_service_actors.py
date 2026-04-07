from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.management.base import BaseCommand
from django.db import transaction


class Command(BaseCommand):
    help = "Ensure internal service actors exist and have required permissions."

    def handle(self, *args, **options):
        with transaction.atomic():
            deposit_username = getattr(
                settings,
                "LEDGER_INTERNAL_DEPOSIT_SERVICE_USERNAME",
                "deposit-service",
            )
            sweeper_username = getattr(
                settings,
                "LEDGER_INTERNAL_SWEEPER_SERVICE_USERNAME",
                "sweeper-service",
            )

            deposit_user = self._ensure_service_user(
                username=deposit_username,
                email=f"{deposit_username}@localhost",
            )
            sweeper_user = self._ensure_service_user(
                username=sweeper_username,
                email=f"{sweeper_username}@localhost",
            )

            self._grant_permissions(
                deposit_user,
                [
                    ("ledger", "can_manage_deposit_addresses"),
                    ("ledger", "can_view_deposit_sessions"),
                ],
            )
            self._grant_permissions(
                sweeper_user,
                [
                    ("ledger", "can_manage_deposit_sweep_jobs"),
                ],
            )

        self.stdout.write(self.style.SUCCESS("Internal service actors ensured."))

    def _ensure_service_user(self, *, username: str, email: str):
        user_model = get_user_model()
        user, _created = user_model.objects.get_or_create(
            username=username,
            defaults={
                "email": email,
                "is_active": True,
            },
        )
        user.is_active = True
        user.email = user.email or email
        user.set_unusable_password()
        user.save(update_fields=["is_active", "email", "password"])
        return user

    def _grant_permissions(self, user, permissions):
        for app_label, codename in permissions:
            permission = Permission.objects.get(
                content_type__app_label=app_label,
                codename=codename,
            )
            user.user_permissions.add(permission)