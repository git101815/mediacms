from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def test_old_prod_deploy_name_is_gone_and_two_explicit_updaters_exist():
    assert not (ROOT / "deploy/scripts/prod_deploy.sh").exists()
    assert (ROOT / "deploy/scripts/prod_rolling_update.sh").exists()
    assert (ROOT / "deploy/scripts/staging_rolling_update.sh").exists()


def test_rolling_updaters_never_destroy_the_stack():
    for path in (
        "deploy/scripts/prod_rolling_update.sh",
        "deploy/scripts/staging_rolling_update.sh",
        "deploy/scripts/rolling_update_common.sh",
    ):
        content = _read(path)
        assert "compose down" not in content
        assert "--remove-orphans" not in content
        assert " down --" not in content


def test_staging_updater_never_touches_dns_or_maintenance_switch():
    content = _read("deploy/scripts/staging_rolling_update.sh") + _read(
        "deploy/scripts/rolling_update_common.sh"
    )
    for forbidden in (
        "maintenance-on.sh",
        "maintenance-off.sh",
        ".env.maintenance",
        "TUNNEL_TOKEN",
        "cloudflare.com/client",
        "api.cloudflare.com",
    ):
        assert forbidden not in content


def test_frontend_build_is_one_shot_and_does_not_start_dev_stack_dependencies():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "run --rm --no-deps frontend" in common
    assert "npm install && npm run dist" in common
    assert "cp -a frontend/dist/static/. static/" in common


def test_production_web_rolls_with_extra_replica_but_staging_is_targeted_recreate():
    common = _read("deploy/scripts/rolling_update_common.sh")
    prod = _read("deploy/scripts/prod_rolling_update.sh")
    staging = _read("deploy/scripts/staging_rolling_update.sh")
    assert '"scaled"' in prod
    assert '"single"' in staging
    assert "temporary=$((EXPECTED_WEB_REPLICAS + 1))" in common
    assert '--force-recreate web' in common


def test_web_and_worker_do_not_mount_the_live_checkout_in_prod_or_staging():
    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):
        compose = _read(compose_path)
        for service, next_service in (("web", "deposit_service"), ("celery_worker", "db")):
            block = compose.split(f"  {service}:\n", 1)[1].split(f"\n  {next_service}:", 1)[0]
            assert "- ./:/home/mediacms.io/mediacms/" not in block
            assert "./static:/home/mediacms.io/mediacms/static" in block
            assert "./media_files:/home/mediacms.io/mediacms/media_files" in block
            assert "./logs:/home/mediacms.io/mediacms/logs" in block
            assert "./backup:/home/mediacms.io/mediacms/backup" in block


def test_database_and_redis_are_not_update_targets():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "compose build web" in common
    assert "compose build db" not in common
    assert "compose build redis" not in common
    assert "force-recreate db" not in common
    assert "force-recreate redis" not in common


def test_pending_migrations_are_checked_before_celery_is_drained():
    common = _read("deploy/scripts/rolling_update_common.sh")
    checker = common.index("check_pending_migrations\n")
    frontend = common.index("build_frontend_dist\n")
    drain = common.index("drain_celery\n")
    migrate = common.index("run_migrations\n")
    assert checker < frontend < drain < migrate


def test_migration_checker_is_fail_closed_for_destructive_or_ambiguous_ops():
    checker = _read("deploy/scripts/check_rolling_migrations.py")
    assert 'name == "CreateModel"' in checker
    assert 'name == "AddField"' in checker
    assert 'name == "AddIndexConcurrently"' in checker
    assert "RemoveField" not in checker.split("SAFE_STATE_ONLY", 1)[1].split("def _safe_add_field", 1)[0]
    assert "not on the unattended rolling allow-list" in checker
    assert "ALLOW_REVIEWED_ROLLING_MIGRATIONS=1" in checker


def test_release_sha_is_recorded_only_after_final_preflight():
    common = _read("deploy/scripts/rolling_update_common.sh")
    tail = common.split("rolling_update_main()", 1)[1]
    final_preflight = tail.rindex("app_preflight")
    record = tail.rindex("record_release")
    assert final_preflight < record
    assert '.deploy-state' in common


def test_failure_after_drain_is_fail_closed_instead_of_restarting_unknown_workers():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "Celery remains intentionally stopped (fail-closed)" in common
    failure = common.split("rolling_failure_notice()", 1)[1].split("rolling_update_main()", 1)[0]
    assert "compose up" not in failure
