import os
from pathlib import Path
import socket
from urllib.parse import urlsplit

import pytest
from django.conf import settings
from django.db import connection
from django_redis import get_redis_connection


@pytest.mark.django_db
def test_testing_database_is_not_the_live_database():
    live_name = str(settings.TESTING_LIVE_DATABASE_NAME)
    active_name = str(connection.settings_dict["NAME"])
    configured_test_name = str(
        settings.DATABASES["default"]["TEST"]["NAME"]
    )

    assert configured_test_name.startswith("test_")
    assert configured_test_name != live_name
    assert active_name == configured_test_name
    assert active_name != live_name


def test_testing_redis_is_not_the_live_redis_database():
    live = urlsplit(
        str(settings.TESTING_LIVE_REDIS_LOCATION)
    )
    live_db = int((live.path or "/0").strip("/") or 0)

    redis = get_redis_connection("default")
    active_db = int(
        redis.connection_pool.connection_kwargs.get("db", 0)
        or 0
    )

    assert active_db == settings.TESTING_REDIS_DB
    assert active_db != live_db


def test_testing_file_roots_are_outside_repository():
    base = Path(settings.BASE_DIR).resolve()
    test_root = Path(settings.TESTING_ROOT).resolve()

    assert base not in test_root.parents
    assert test_root != base

    for value in (
        settings.MEDIA_ROOT,
        settings.STATIC_ROOT,
        settings.TEMP_DIRECTORY,
        settings.DB_BACKUP_DIR,
        settings.LOGS_DIR,
    ):
        resolved = Path(value).resolve()
        assert resolved == test_root or test_root in resolved.parents


def test_testing_never_uses_smtp_or_file_logging():
    assert (
        settings.EMAIL_BACKEND
        == "django.core.mail.backends.locmem.EmailBackend"
    )

    handlers = settings.LOGGING.get("handlers", {})
    assert handlers
    for config in handlers.values():
        assert config.get("class") != "logging.FileHandler"


def test_testing_runtime_paths_and_redis_location_are_explicit():
    assert settings.TESTING is True
    assert settings.TESTING_REDIS_DB == 15
    assert settings.REDIS_LOCATION != settings.TESTING_LIVE_REDIS_LOCATION
    assert settings.HLS_DIR.startswith(settings.MEDIA_ROOT)

    for value in (
        settings.MEDIA_ROOT,
        settings.HLS_DIR,
        settings.STATIC_ROOT,
        settings.TEMP_DIRECTORY,
        settings.DB_BACKUP_DIR,
        settings.LOGS_DIR,
    ):
        assert Path(value).is_dir()


def test_pytest_runtime_artifacts_are_outside_repository(request):
    base = Path(settings.BASE_DIR).resolve()
    test_root = Path(settings.TESTING_ROOT).resolve()

    pytest_temp_root = Path(os.environ["PYTEST_DEBUG_TEMPROOT"]).resolve()
    assert base not in pytest_temp_root.parents
    assert pytest_temp_root != base
    assert pytest_temp_root == test_root or test_root in pytest_temp_root.parents

    cache = getattr(request.config, "cache", None)
    assert cache is not None
    cache_root = Path(cache._cachedir).resolve()
    assert base not in cache_root.parents
    assert cache_root != base
    assert cache_root == test_root or test_root in cache_root.parents


def test_public_network_is_blocked_during_tests():
    with socket.socket() as sock:
        with pytest.raises(
            RuntimeError,
            match="External network is disabled",
        ):
            sock.connect(("1.1.1.1", 443))
