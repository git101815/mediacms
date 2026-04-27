from django.core.management.base import BaseCommand, CommandError

from ledger.services import (
    LEDGER_OPERATIONAL_FLAG_DEFAULTS,
    list_ledger_operation_flags,
    set_ledger_operation_flag,
)


class Command(BaseCommand):
    help = "List or update runtime ledger operational kill-switches."

    def add_arguments(self, parser):
        parser.add_argument(
            "key",
            nargs="?",
            help="Flag key to update. Omit with no state to list all flags.",
        )
        parser.add_argument(
            "state",
            nargs="?",
            choices=["on", "off", "true", "false", "1", "0", "enable", "disable", "enabled", "disabled"],
            help="New state for the flag.",
        )
        parser.add_argument(
            "--reason",
            default="",
            help="Optional reason stored next to the flag state.",
        )

    def handle(self, *args, **options):
        key = (options.get("key") or "").strip()
        state = (options.get("state") or "").strip().lower()
        reason = options.get("reason") or ""

        if not key and not state:
            self._print_flags()
            return

        if key == "list" and not state:
            self._print_flags()
            return

        if not key or not state:
            raise CommandError("Usage: manage.py ledger_flag <key> <on|off> [--reason ...]")

        if key not in LEDGER_OPERATIONAL_FLAG_DEFAULTS:
            allowed = ", ".join(sorted(LEDGER_OPERATIONAL_FLAG_DEFAULTS))
            raise CommandError(f"Unknown ledger flag '{key}'. Allowed flags: {allowed}")

        enabled = state in {"on", "true", "1", "enable", "enabled"}
        result = set_ledger_operation_flag(key=key, enabled=enabled, reason=reason)
        self.stdout.write(
            self.style.SUCCESS(
                f"{result['key']}={str(result['enabled']).lower()} reason={result['reason']}"
            )
        )

    def _print_flags(self):
        flags = list_ledger_operation_flags()
        for key in sorted(flags):
            item = flags[key]
            reason = f" reason={item['reason']}" if item.get("reason") else ""
            updated_at = f" updated_at={item['updated_at']}" if item.get("updated_at") else ""
            self.stdout.write(f"{key}={str(item['enabled']).lower()}{reason}{updated_at}")
