from django.core.management.base import BaseCommand

from ledger.models import LedgerOutbox


class Command(BaseCommand):
    help = "List failed, dead-lettered, and stale pending outbox events"

    def add_arguments(self, parser):
        parser.add_argument("--pending-older-than", type=int, default=900)

    def handle(self, *args, **options):
        pending_older_than = options["pending_older_than"]

        failed = LedgerOutbox.objects.filter(status=LedgerOutbox.STATUS_FAILED).order_by("created_at")
        dead = LedgerOutbox.objects.filter(status=LedgerOutbox.STATUS_DEAD_LETTERED).order_by("dead_lettered_at", "created_at")

        self.stdout.write("FAILED OUTBOX EVENTS")
        for event in failed:
            self.stdout.write(
                f"{event.id} topic={event.topic} fail_count={event.fail_count} last_error={event.last_error}"
            )

        self.stdout.write("DEAD-LETTERED OUTBOX EVENTS")
        for event in dead:
            self.stdout.write(
                f"{event.id} topic={event.topic} redrive_count={event.redrive_count} reason={event.dead_letter_reason}"
            )

        self.stdout.write(f"PENDING OUTBOX EVENTS older than {pending_older_than}s must be inspected through services/admin")