import logging
import re
from pathlib import Path

from cms.logging_handlers import ProcessSafeRotatingFileHandler


ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def _service_block(compose, service):
    match = re.search(rf"(?m)^  {re.escape(service)}:\s*$", compose)
    assert match is not None
    tail = compose[match.end():]
    next_service = re.search(r"(?m)^  [A-Za-z0-9_-]+:\s*$", tail)
    next_section = re.search(
        r"(?m)^(?:volumes|secrets|networks|configs):\s*$", tail
    )
    candidates = []
    if next_service:
        candidates.append(next_service.start())
    if next_section:
        candidates.append(next_section.start())
    end = min(candidates) if candidates else len(tail)
    return tail[:end]


def test_prod_celery_suppresses_success_spam_but_nonprod_can_keep_info():
    compose = _read("docker-compose-cloudflare.yaml")
    for service in ("celery_beat", "celery_worker"):
        block = _service_block(compose, service)
        assert "CELERY_LOG_LEVEL: ${CELERY_LOG_LEVEL:-WARNING}" in block

    for filename in (
        "supervisord-celery_beat.conf",
        "supervisord-celery_short.conf",
        "supervisord-celery_long.conf",
    ):
        conf = _read(f"deploy/docker/supervisord/{filename}")
        assert '--loglevel="${CELERY_LOG_LEVEL:-INFO}"' in conf
        assert "logs/celery_" not in conf
        assert "--loglevel=INFO" not in conf


def test_prod_docker_logs_are_bounded_for_every_service():
    compose = _read("docker-compose-cloudflare.yaml")
    assert "x-bounded-logging: &bounded_logging" in compose
    assert "driver: local" in compose
    assert 'max-size: "20m"' in compose
    assert 'max-file: "5"' in compose

    app_anchor = compose.split("x-mediacms-app: &mediacms_app", 1)[1].split(
        "x-mediacms-db-env:", 1
    )[0]
    assert "logging: *bounded_logging" in app_anchor

    inherited = ("migrations", "web", "celery_beat", "celery_worker")
    for service in inherited:
        assert "<<: *mediacms_app" in _service_block(compose, service)

    explicit = (
        "dfx_signer_service",
        "deposit_service",
        "sweeper_service",
        "db",
        "redis",
        "cloudflared",
    )
    for service in explicit:
        assert "logging: *bounded_logging" in _service_block(compose, service)


def test_staging_docker_logs_are_bounded_for_every_service():
    compose = _read("docker-compose.yaml")
    assert "x-bounded-logging: &bounded_logging" in compose
    assert "driver: local" in compose
    assert 'max-size: "20m"' in compose
    assert 'max-file: "5"' in compose

    app_anchor = compose.split("x-mediacms-app: &mediacms_app", 1)[1].split(
        "x-mediacms-db-env:", 1
    )[0]
    assert "logging: *bounded_logging" in app_anchor

    inherited = ("migrations", "web", "celery_beat", "celery_worker")
    for service in inherited:
        assert "<<: *mediacms_app" in _service_block(compose, service)

    explicit = (
        "staging_ingress",
        "dfx_signer_service",
        "deposit_service",
        "sweeper_service",
        "db",
        "redis",
    )
    for service in explicit:
        assert "logging: *bounded_logging" in _service_block(compose, service)


def test_debug_log_is_process_safe_and_bounded():
    settings = _read("cms/settings.py")
    assert '"class": "cms.logging_handlers.ProcessSafeRotatingFileHandler"' in settings
    assert '"maxBytes": 20 * 1024 * 1024' in settings
    assert '"backupCount": 4' in settings
    assert '"delay": True' in settings
    assert '"class": "logging.FileHandler"' not in settings


def _record(message):
    return logging.LogRecord(
        name="test.prod.logging",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


def test_process_safe_handler_reopens_after_peer_rollover(tmp_path):
    path = tmp_path / "debug.log"
    first = ProcessSafeRotatingFileHandler(
        path, maxBytes=128, backupCount=2, encoding="utf-8", delay=False
    )
    second = ProcessSafeRotatingFileHandler(
        path, maxBytes=128, backupCount=2, encoding="utf-8", delay=False
    )
    formatter = logging.Formatter("%(message)s")
    first.setFormatter(formatter)
    second.setFormatter(formatter)

    try:
        # first rotates while second still has the old inode open.
        first.emit(_record("x" * 256))
        second.emit(_record("marker-from-second"))
    finally:
        first.close()
        second.close()

    assert "marker-from-second" in path.read_text(encoding="utf-8")
    for backup in tmp_path.glob("debug.log.*"):
        if backup.name.endswith(".lock"):
            continue
        assert "marker-from-second" not in backup.read_text(encoding="utf-8")
