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
    deploy = _read("deploy/scripts/prod_rolling_update.sh")
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


def test_health_wait_counts_exited_containers_and_exact_replicas():
    common = _read("deploy/scripts/rolling_update_common.sh")
    prod = _read("deploy/scripts/prod_rolling_update.sh")
    assert 'service_container_ids_all() { compose ps -a -q "$1"' in common
    assert '${#ids[@]} == expected' in common
    assert "temporary=$((EXPECTED_WEB_REPLICAS + 1))" in common
    assert '"production"' in prod
    assert '"scaled"' in prod


def test_preflight_failure_is_not_ignored():
    common = _read("deploy/scripts/prod_common.sh")
    assert "python manage.py prod_preflight || true" not in common
    assert "docker exec -i -w /home/mediacms.io/mediacms" in common
    assert "Production preflight requires at least one running web container" in common


def test_celery_drain_checks_redis_queue_even_without_worker():
    common = _read("deploy/scripts/prod_common.sh")
    assert "celery_queue_count()" in common
    assert "Celery worker is not running while" in common
    assert "Could not reliably inspect active/reserved Celery work" in common


def test_redis_migration_enables_live_aof_before_stopping_redis():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    enable = migration.index("CONFIG SET appendonly yes")
    stop = migration.index("compose stop redis")
    assert enable < stop
    assert "aof_rewrite_in_progress" in migration
    assert "aof_rewrite_scheduled" in migration
    assert "aof_last_bgrewrite_status" in migration
    assert "preserving copied Redis data" in migration
    assert "compose_crypto stop deposit_service" in migration
    assert "compose_crypto stop sweeper_service" in migration


def test_crypto_worker_updates_are_ordered_around_signer_health():
    common = _read("deploy/scripts/rolling_update_common.sh")
    main = common.split("rolling_update_main()", 1)[1]
    stop_worker = main.index("stop_crypto_loops_if_needed")
    signer_update = main.index("update_signer_if_needed")
    web_update = main.index("update_web\n")
    restart_worker = main.index("restart_crypto_loops_if_needed")
    assert stop_worker < signer_update < web_update < restart_worker
