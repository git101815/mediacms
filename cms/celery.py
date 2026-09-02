from __future__ import absolute_import

import logging
import os

from celery import Celery
from celery.signals import (
    after_setup_logger,
    after_setup_task_logger,
    worker_process_init,
)
from django.conf import settings
from django.db import connections

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "cms.settings")
app = Celery("cms")

app.config_from_object("django.conf:settings")
app.autodiscover_tasks()


def _configure_framework_log_levels():
    level_name = str(
        getattr(settings, "CELERY_FRAMEWORK_LOG_LEVEL", "")
    ).strip().upper()
    if not level_name:
        return
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid CELERY_FRAMEWORK_LOG_LEVEL={level_name!r}")
    for logger_name in (
        "celery.worker.strategy",
        "celery.app.trace",
        "celery.beat",
    ):
        logging.getLogger(logger_name).setLevel(level)


@after_setup_logger.connect
@after_setup_task_logger.connect
def quiet_celery_framework_chatter(**_):
    _configure_framework_log_levels()


@worker_process_init.connect
def close_db_pool_on_fork(**_):
    # psycopg's ConnectionPool is not fork-safe.
    for conn in connections.all():
        conn.close_pool()


app.conf.beat_schedule = getattr(settings, "CELERY_BEAT_SCHEDULE", {})
app.conf.broker_transport_options = getattr(
    settings,
    "CELERY_BROKER_TRANSPORT_OPTIONS",
    {"visibility_timeout": 3 * 60 * 60},
)
app.conf.broker_connection_retry_on_startup = getattr(
    settings, "CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP", True
)
# Keep the visibility timeout bounded so a future late-acked idempotent task
# cannot remain invisible for an entire day after a worker loss.

# setting this to settings.py file only is not respected. Setting here too
app.conf.task_always_eager = settings.CELERY_TASK_ALWAYS_EAGER


app.conf.worker_prefetch_multiplier = 1
