from django.core.management.base import BaseCommand

from ledger.models import LedgerSaga


class Command(BaseCommand):
    help = "List failed or compensating sagas"

    def handle(self, *args, **options):
        failed = LedgerSaga.objects.filter(status=LedgerSaga.STATUS_FAILED).order_by("failed_at", "created_at")
        compensating = LedgerSaga.objects.filter(status=LedgerSaga.STATUS_COMPENSATING).order_by("created_at")

        self.stdout.write("FAILED SAGAS")
        for saga in failed:
            self.stdout.write(
                f"{saga.id} type={saga.saga_type} external_id={saga.external_id} error={saga.last_error}"
            )

        self.stdout.write("COMPENSATING SAGAS")
        for saga in compensating:
            self.stdout.write(
                f"{saga.id} type={saga.saga_type} external_id={saga.external_id}"
            )