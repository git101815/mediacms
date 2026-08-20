from django.apps import AppConfig


class AdsConfig(AppConfig):
    default_auto_field = "django.db.models.AutoField"
    name = "ads"
    verbose_name = "Direct Ads"

    def ready(self):
        from . import signals  # noqa: F401
