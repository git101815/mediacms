from django.apps import AppConfig

class LedgerConfig(AppConfig):
    name = "ledger"

    def ready(self):
        from . import signals  # noqa