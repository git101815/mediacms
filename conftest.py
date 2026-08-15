import atexit
import ipaddress
import os
import shutil
import socket
from pathlib import Path
from urllib.parse import urlsplit

import pytest


_TRUE_VALUES = {"1", "true", "yes", "on"}
_TEST_ROOT = Path("/tmp/mediacms-pytest")


def _testing_enabled():
    return str(os.environ.get("TESTING", "")).strip().lower() in _TRUE_VALUES


def _settings():
    from django.conf import settings
    return settings


def _redis_connection():
    from django_redis import get_redis_connection
    return get_redis_connection("default")


def _redis_db_number(redis):
    value = redis.connection_pool.connection_kwargs.get("db", 0)
    return int(value or 0)


def _assert_isolated_runtime():
    if not _testing_enabled():
        raise pytest.UsageError(
            "Refusing to run this test suite without TESTING=True. "
            "Use the repository Docker test command."
        )

    settings = _settings()

    live_db = str(settings.TESTING_LIVE_DATABASE_NAME)
    test_db = str(
        settings.DATABASES["default"]
        .get("TEST", {})
        .get("NAME", "")
    )
    if (
        not test_db
        or test_db == live_db
        or not test_db.startswith("test_")
    ):
        raise pytest.UsageError(
            "Unsafe test database configuration: "
            f"live={live_db!r}, test={test_db!r}"
        )

    live_redis = urlsplit(
        str(settings.TESTING_LIVE_REDIS_LOCATION)
    )
    live_redis_db = int(
        (live_redis.path or "/0").strip("/") or 0
    )
    redis = _redis_connection()
    active_redis_db = _redis_db_number(redis)

    if active_redis_db == live_redis_db:
        raise pytest.UsageError(
            "Unsafe Redis configuration: tests are connected to "
            f"the live Redis DB {live_redis_db}."
        )
    if active_redis_db != int(settings.TESTING_REDIS_DB):
        raise pytest.UsageError(
            "Unexpected Redis test DB: "
            f"expected {settings.TESTING_REDIS_DB}, "
            f"got {active_redis_db}."
        )

    test_root = Path(settings.TESTING_ROOT).resolve()
    base_dir = Path(settings.BASE_DIR).resolve()
    if test_root == base_dir or base_dir in test_root.parents:
        raise pytest.UsageError(
            "Unsafe TESTING_ROOT: it must not live inside BASE_DIR."
        )

    return redis


def _cleanup_test_root():
    shutil.rmtree(_TEST_ROOT, ignore_errors=True)



def _network_host_is_test_safe(host):
    if isinstance(host, bytes):
        host = host.decode("ascii", errors="ignore")
    host = str(host or "").strip().lower()

    if not host:
        return True

    # Docker Compose service names are single-label names (db, redis, web,
    # frontend, etc.). They stay inside the compose/private network.
    if "." not in host:
        return True

    if host == "localhost" or host.endswith(".localhost"):
        return True

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        # A dotted public hostname is external by default.
        return False

    return bool(
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
    )


@pytest.fixture(autouse=True)
def _block_public_network(monkeypatch):
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex

    def guarded_connect(sock, address):
        if isinstance(address, tuple) and address:
            host = address[0]
            if not _network_host_is_test_safe(host):
                raise RuntimeError(
                    "External network is disabled under TESTING=True: "
                    f"{host!r}"
                )
        return original_connect(sock, address)

    def guarded_connect_ex(sock, address):
        if isinstance(address, tuple) and address:
            host = address[0]
            if not _network_host_is_test_safe(host):
                raise RuntimeError(
                    "External network is disabled under TESTING=True: "
                    f"{host!r}"
                )
        return original_connect_ex(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)


def pytest_configure(config):
    _assert_isolated_runtime()


def pytest_sessionstart(session):
    redis = _assert_isolated_runtime()

    # DB 15 is reserved by cms.settings specifically for TESTING=True.
    # Clearing it before the run makes repeated executions deterministic.
    redis.flushdb()

    # pytest's --basetemp is below this path and clears its own directory.
    # Remove leftovers from interrupted older runs before current tests start.
    media_root = _TEST_ROOT / "media"
    if media_root.exists():
        shutil.rmtree(media_root, ignore_errors=True)


def pytest_sessionfinish(session, exitstatus):
    try:
        redis = _assert_isolated_runtime()
        redis.flushdb()
    except Exception as exc:
        # A cleanup failure means idempotence is no longer guaranteed.
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
        terminal = session.config.pluginmanager.get_plugin(
            "terminalreporter"
        )
        if terminal is not None:
            terminal.write_line(
                f"TEST CLEANUP ERROR: {exc}",
                red=True,
            )


def pytest_unconfigure(config):
    _cleanup_test_root()


atexit.register(_cleanup_test_root)
