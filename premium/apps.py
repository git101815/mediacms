from django.apps import AppConfig


class PremiumConfig(AppConfig):
    default_auto_field = "django.db.models.AutoField"
    name = "premium"

    def ready(self):
        from . import signals  # noqa: F401
        from . import subscription_tasks  # noqa: F401
