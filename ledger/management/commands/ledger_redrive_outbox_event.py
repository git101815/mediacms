from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from ledger.models import LedgerOutbox
from ledger.services import redrive_dead_lettered_outbox_event, replay_failed_outbox_event


class Command(BaseCommand):
    help = "Replay a failed outbox event or redrive a dead-lettered outbox event"

    def add_arguments(self, parser):
        parser.add_argument("event_id", type=int)
        parser.add_argument("--actor-id", type=int, required=True)

    def handle(self, *args, **options):
        User = get_user_model()

        try:
            actor = User.objects.get(id=options["actor_id"])
        except User.DoesNotExist as exc:
            raise CommandError("Actor not found") from exc

        try:
            event = LedgerOutbox.objects.get(id=options["event_id"])
        except LedgerOutbox.DoesNotExist as exc:
            raise CommandError("Outbox event not found") from exc

        if event.status == LedgerOutbox.STATUS_FAILED:
            replay_failed_outbox_event(actor=actor, event=event)
            self.stdout.write(f"Replayed failed outbox event {event.id}")
            return

        if event.status == LedgerOutbox.STATUS_DEAD_LETTERED:
            redrive_dead_lettered_outbox_event(actor=actor, event=event)
            self.stdout.write(f"Redriven dead-lettered outbox event {event.id}")
            return

        raise CommandError("Only failed or dead-lettered events can be replayed")