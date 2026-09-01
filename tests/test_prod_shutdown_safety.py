from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (ROOT / path).read_text()


def test_redis_is_persistent_and_aof_enabled_in_prod_compose():
    compose = _read("docker-compose-cloudflare.yaml")
    assert '"--appendonly", "yes"' in compose
    assert "redis_data:/data" in compose
    assert "mediacms-prod-redis-data" in compose


def test_prod_has_redundant_web_replicas():
    compose = _read("docker-compose-cloudflare.yaml")
    web = compose.split("  web:\n", 1)[1].split("\n  deposit_service:", 1)[0]
    assert "replicas: 2" in web


def test_celery_is_foreground_and_has_warm_stop_contract():
    for name in ("celery_short", "celery_long"):
        conf = _read(f"deploy/docker/supervisord/supervisord-{name}.conf")
        assert "celery multi" not in conf
        assert " -A cms worker " in conf
        assert "stopsignal=TERM" in conf
        assert "stopasgroup=true" in conf
        assert "killasgroup=true" in conf


def test_normal_deploy_never_uses_compose_down_or_remove_orphans():
    deploy = _read("deploy/scripts/prod_deploy.sh")
    assert "compose down" not in deploy
    assert "--remove-orphans" not in deploy


def test_shutdown_does_not_remove_orphans():
    shutdown = _read("deploy/scripts/prod_shutdown.sh")
    assert "compose down" in shutdown
    assert "--remove-orphans" not in shutdown


def test_orphan_cleanup_requires_explicit_confirmation():
    cleanup = _read("deploy/scripts/prod_cleanup_orphans.sh")
    assert "--apply" in cleanup
    assert "CONFIRM_PROJECT" in cleanup


def test_runpod_worker_callback_is_best_effort():
    handler = _read("runpod_worker/handler.py")
    assert "def callback_best_effort(" in handler
    assert 'callback_best_effort(job["callback_url"], payload)' in handler


def test_migrations_do_not_restart_forever():
    for filename in ("docker-compose.yaml", "docker-compose-cloudflare.yaml"):
        compose = _read(filename)
        migrations = compose.split("  migrations:\n", 1)[1].split("\n  web:", 1)[0]
        assert 'restart: "no"' in migrations
