import json

from django.core.management.base import BaseCommand

from files.models import Encoding
from ledger.models import DepositSession, DepositSweepJob


class Command(BaseCommand):
    help = "Show durable in-flight production work before deploy/shutdown."

    def add_arguments(self, parser):
        parser.add_argument("--json", action="store_true", dest="as_json")

    def handle(self, *args, **options):
        runpod = Encoding.objects.filter(worker="runpod", status="running")
        runpod_jobs = runpod.exclude(task_id="").values("task_id").distinct().count()
        runpod_without_job_id = runpod.filter(task_id="").count()

        sweep_counts = {
            status: DepositSweepJob.objects.filter(status=status).count()
            for status in (
                DepositSweepJob.STATUS_PENDING,
                DepositSweepJob.STATUS_FUNDING_BROADCASTED,
                DepositSweepJob.STATUS_READY_TO_SWEEP,
                DepositSweepJob.STATUS_SWEEP_BROADCASTED,
            )
        }
        deposit_counts = {
            status: DepositSession.objects.filter(status=status).count()
            for status in (
                DepositSession.STATUS_AWAITING_PAYMENT,
                DepositSession.STATUS_SEEN_ONCHAIN,
                DepositSession.STATUS_CONFIRMING,
                DepositSession.STATUS_CREDITED,
            )
        }

        result = {
            "runpod": {
                "running_encodings": runpod.count(),
                "running_jobs": runpod_jobs,
                "running_without_job_id": runpod_without_job_id,
            },
            "sweeps": sweep_counts,
            "deposit_sessions": deposit_counts,
        }

        if options["as_json"]:
            self.stdout.write(json.dumps(result, sort_keys=True))
            return

        self.stdout.write(json.dumps(result, indent=2, sort_keys=True))
