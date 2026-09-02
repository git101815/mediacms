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
    stop = migration.index("stop_service_if_running redis")
    assert enable < stop
    assert "aof_rewrite_in_progress" in migration
    assert "aof_rewrite_scheduled" in migration
    assert "aof_last_bgrewrite_status" in migration
    assert "Copied Redis artifacts are preserved" in migration
    assert "stop_crypto_service_if_running deposit_service" in migration
    assert "stop_crypto_service_if_running sweeper_service" in migration




def test_redis_migration_builds_target_images_before_quiescing_legacy_stack():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    release = migration.index('export MEDIACMS_RELEASE_SHA="$(git_repo rev-parse HEAD)"')
    build_web = migration.index("compose build web")
    legacy_stop = migration.index("stop_service_if_running celery_beat")
    assert release < build_web < legacy_stop
    assert "compose_crypto build deposit_service" in migration
    assert "compose_crypto build dfx_signer_service" in migration
    assert "compose_crypto build sweeper_service" in migration
    assert "Legacy bind-mounted application containers detected" in migration


def test_redis_migration_recreates_image_isolated_services_with_verified_release_labels():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert "assert_release_label()" in migration
    assert "compose up -d --no-deps --force-recreate web" in migration
    assert "ensure_release_service celery_worker 120 0" in migration
    assert "ensure_release_service celery_beat 120 0" in migration
    assert "ensure_release_service deposit_service 120 1" in migration
    assert "ensure_release_service sweeper_service 120 1" in migration
    assert "assert_release_label web" in migration
    assert "assert_release_label celery_worker" in migration
    assert "assert_release_label celery_beat" in migration


def test_redis_migration_never_converges_postgres_via_compose_up_or_migrations():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert "compose up -d --no-deps redis" in migration
    assert "compose up -d db" not in migration
    assert "compose up -d db redis" not in migration
    assert "compose run --rm --no-deps migrations" in migration
    assert "compose run --rm migrations" not in migration


def test_crypto_worker_updates_are_ordered_around_signer_health():
    common = _read("deploy/scripts/rolling_update_common.sh")
    main = common.split("rolling_update_main()", 1)[1]
    stop_worker = main.index("stop_crypto_for_update")
    signer_update = main.index("update_signer_if_needed")
    web_update = main.index("update_web\n")
    restart_worker = main.index("restart_crypto_after_update")
    assert stop_worker < signer_update < web_update < restart_worker



def test_redis_migration_is_resumable_and_does_not_fast_exit_after_partial_persistence():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert "production.redis-migration.inprogress" in migration
    assert "production.redis-migration.complete" in migration
    assert "state_set phase" in migration
    assert "Resuming Redis migration at phase" in migration
    assert 'if redis_is_persistent && [[ ! -f "$STATE_FILE" && -f "$COMPLETE_FILE" ]]' in migration
    assert 'if redis_is_persistent && [[ ! -f "$STATE_FILE" && ! -f "$COMPLETE_FILE" ]]' in migration
    assert "State was preserved in $STATE_FILE" in migration


def test_redis_migration_cloudflared_restart_is_no_deps_verified_and_not_fail_open():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert "compose up -d --no-deps cloudflared" in migration
    assert "wait_healthy cloudflared 60 1" in migration
    assert "compose up -d cloudflared || true" not in migration
    assert "compose stop cloudflared >/dev/null || true" not in migration


def test_redis_migration_critical_stops_are_fail_closed():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert "stop_service_if_running web" in migration
    assert "stop_service_if_running redis" in migration
    assert "stop_service_if_running celery_worker" in migration
    assert "stop_crypto_service_if_running deposit_service" in migration
    assert "stop_crypto_service_if_running sweeper_service" in migration
    assert "compose stop web >/dev/null || true" not in migration
    assert "compose stop celery_worker >/dev/null || true" not in migration


def test_prod_shutdown_critical_stops_are_fail_closed():
    shutdown = _read("deploy/scripts/prod_shutdown.sh")
    assert "stop_service_if_running cloudflared" in shutdown
    assert "stop_service_if_running web" in shutdown
    assert "stop_service_if_running redis" in shutdown
    assert "stop_service_if_running db" in shutdown
    assert "compose stop web || true" not in shutdown
    assert "compose stop redis || true" not in shutdown
    assert "compose stop db || true" not in shutdown


def test_prod_common_git_helper_supports_sudo_owned_checkout():
    common = _read("deploy/scripts/prod_common.sh")
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert 'git_repo() { git -c "safe.directory=$PROD_ROOT" "$@"; }' in common
    assert "git_repo status --porcelain --untracked-files=all" in migration
    assert "git_repo rev-parse HEAD" in migration



def test_full_shutdown_requires_persistent_redis_and_legacy_safe_path():
    shutdown = _read("deploy/scripts/prod_shutdown.sh")
    common = _read("deploy/scripts/prod_common.sh")
    assert "if ! redis_is_persistent" in shutdown
    assert "Run the one-time persistence migration first" in shutdown
    assert "legacy_app_mounts_present" in shutdown
    assert "Legacy bind-mounted application containers detected" in shutdown
    assert "stop_service_if_running celery_worker" in shutdown
    assert "container_has_repo_root_mount()" in common



def test_all_destructive_prod_tools_share_one_mutation_lock():
    common = _read("deploy/scripts/prod_common.sh")
    assert "acquire_prod_mutation_lock()" in common
    assert "production.mutation.lock" in common
    for filename in (
        "deploy/scripts/prod_shutdown.sh",
        "deploy/scripts/prod_migrate_redis_persistence.sh",
        "deploy/scripts/prod_cleanup_orphans.sh",
    ):
        assert "acquire_prod_mutation_lock" in _read(filename)


def test_redis_migration_only_trusts_explicit_completion_marker():
    migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    assert '-f "$COMPLETE_FILE"' in migration
    assert "Validated and adopted existing durable Redis volume" in migration
    assert "validate_persistent_redis" in migration
