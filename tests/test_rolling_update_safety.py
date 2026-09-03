from pathlib import Path
import os
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[1]


def _read(path):
    return (ROOT / path).read_text(encoding="utf-8")


def _service_block(compose_text, service):
    lines = compose_text.splitlines(True)
    marker = f"  {service}:\n"
    start = next(i for i, line in enumerate(lines) if line == marker)
    block = []
    for line in lines[start + 1 :]:
        if line.startswith("  ") and not line.startswith("    "):
            break
        block.append(line)
    return "".join(block)


def _bash(script, *, env=None):
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    return subprocess.run(
        ["bash", "-c", script],
        cwd=ROOT,
        env=full_env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


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


def test_frontend_build_is_image_verified_reproducible_and_release_scoped():
    common = _read("deploy/scripts/rolling_update_common.sh")
    dockerfile = _read("frontend/Dockerfile.dev")
    assert '"${FRONTEND_COMPOSE[@]}" build frontend' in common
    assert "run --rm --no-deps frontend npm run dist" in common
    assert "RUN npm ci --no-audit --no-fund" in dockerfile
    assert "RUN test -x ./node_modules/.bin/mediacms-scripts" in dockerfile
    assert "RUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist" not in dockerfile
    assert 'cp -a frontend/dist/static/. "$STATIC_RELEASE_DIR/"' in common


def test_every_application_release_rebuilds_frontend_for_fresh_static_snapshot():
    common = _read("deploy/scripts/rolling_update_common.sh")
    build = common.split("build_frontend_dist()", 1)[1].split("prepare_static_release()", 1)[0]
    assert "frontend_build_needed() { app_update_needed; }" in common
    assert "frontend_build_needed || return 0" in build
    assert "(( FRONTEND_CHANGED )) || return 0" not in build
    assert 'cp -a frontend/dist/static/. "$STATIC_RELEASE_DIR/"' in common


def test_backend_only_release_requires_frontend_build_but_crypto_only_does_not():
    script = (
        "source deploy/scripts/rolling_update_common.sh\n"
        "MAIN_IMAGE_CHANGED=1\n"
        "APP_CONFIG_CHANGED=0\n"
        "FRONTEND_CHANGED=0\n"
        "STATIC_CHANGED=0\n"
        "DEPOSIT_IMAGE_CHANGED=0\n"
        "DEPOSIT_CONFIG_CHANGED=0\n"
        "SWEEPER_IMAGE_CHANGED=0\n"
        "SWEEPER_CONFIG_CHANGED=0\n"
        "SIGNER_IMAGE_CHANGED=0\n"
        "SIGNER_CONFIG_CHANGED=0\n"
        "frontend_build_needed || exit 11\n"
        "MAIN_IMAGE_CHANGED=0\n"
        "DEPOSIT_IMAGE_CHANGED=1\n"
        "if frontend_build_needed; then exit 12; fi\n"
    )
    result = _bash(script)
    assert result.returncode == 0, result.stderr


def test_prod_app_and_migrations_are_image_isolated_with_release_static_snapshot():
    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):
        compose = _read(compose_path)
        for service in ("web", "celery_worker", "celery_beat"):
            block = _service_block(compose, service)
            assert "- ./:/home/mediacms.io/mediacms/" not in block
            assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected:ro" in block
            assert "./media_files:/home/mediacms.io/mediacms/media_files" in block
            assert "./logs:/home/mediacms.io/mediacms/logs" in block
            assert "./backup:/home/mediacms.io/mediacms/backup" in block
            assert 'io.mediacms.release: "${MEDIACMS_RELEASE_SHA:-unmanaged}"' in block

        migrations = _service_block(compose, "migrations")
        assert "- ./:/home/mediacms.io/mediacms/" not in migrations
        assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected" in migrations


def test_release_labels_cover_resumable_web_signer_and_workers():
    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml", "docker-compose-dev.yaml"):
        compose = _read(compose_path)
        for service in (
            "web",
            "celery_worker",
            "celery_beat",
            "dfx_signer_service",
            "deposit_service",
            "sweeper_service",
        ):
            assert "io.mediacms.release" in _service_block(compose, service)


def test_production_web_converges_with_at_most_one_extra_replica():
    common = _read("deploy/scripts/rolling_update_common.sh")
    prod = _read("deploy/scripts/prod_rolling_update.sh")
    staging = _read("deploy/scripts/staging_rolling_update.sh")
    assert '"scaled"' in prod
    assert '"scaled"' in staging
    assert "temporary=$((EXPECTED_WEB_REPLICAS + 1))" in common
    assert "current_release_healthy_count web" in common
    assert "cannot free a web scale slot" in common
    assert "web convergence left a non-current release replica" in common
    assert '--force-recreate web' in common


def test_database_redis_cloudflared_compose_changes_fail_closed():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert 'db|redis)' in common
    assert "PostgreSQL/Redis are intentionally outside rolling application updates" in common
    assert 'cloudflared)' in common
    assert "tunnel infrastructure is intentionally outside rolling application updates" in common
    assert "compose build db" not in common
    assert "compose build redis" not in common
    assert "force-recreate db" not in common
    assert "force-recreate redis" not in common


def test_compose_crypto_changes_are_classified_per_service():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "deposit_service)" in common and "DEPOSIT_CONFIG_CHANGED=1" in common
    assert "sweeper_service)" in common and "SWEEPER_CONFIG_CHANGED=1" in common
    assert "dfx_signer_service)" in common and "SIGNER_CONFIG_CHANGED=1" in common
    assert "classify_compose_changes.py" in common


def test_runpod_worker_is_outside_rolling_stack_scope():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "RUNPOD_CHANGED" not in common
    assert "runpod_worker/*" in common  # ignored by main-image classification
    assert "outside this Docker stack" in common


def test_migration_checker_uses_current_checkout_migrations_service():
    common = _read("deploy/scripts/rolling_update_common.sh")
    checker = _read("deploy/scripts/check_rolling_migrations.py")

    assert "compose run --rm --no-deps migrations python deploy/scripts/check_rolling_migrations.py" in common
    assert "compose run --rm --no-deps web python deploy/scripts/check_rolling_migrations.py" not in common

    # Executing a Python file by path sets sys.path[0] to deploy/scripts,
    # so an isolated production image must add the repository root itself
    # before importing cms.settings.
    assert "REPO_ROOT = Path(__file__).resolve().parents[2]" in checker
    assert "sys.path.insert(0, repo_root_str)" in checker
    assert checker.index("sys.path.insert(0, repo_root_str)") < checker.index(
        'os.environ.setdefault("DJANGO_SETTINGS_MODULE", "cms.settings")'
    )


def test_migration_checker_is_fail_closed_for_destructive_or_ambiguous_ops():
    checker = _read("deploy/scripts/check_rolling_migrations.py")
    assert 'name == "CreateModel"' in checker
    assert 'name == "AddField"' in checker
    assert 'name == "AddIndexConcurrently"' in checker
    assert "not on the unattended rolling allow-list" in checker
    assert "ALLOW_REVIEWED_ROLLING_MIGRATIONS=1" in checker


def test_normal_isolated_release_builds_and_checks_before_celery_drain():
    common = _read("deploy/scripts/rolling_update_common.sh")
    main = common.split("rolling_update_main()", 1)[1]
    frontend = main.index("build_frontend_dist")
    build = main.index("build_required_images")
    snapshot = main.index("prepare_static_release")
    checker = main.index("check_pending_migrations")
    normal_drain = main.index("if (( ! LEGACY_BOOTSTRAP )); then ensure_celery_drained; fi")
    migrate = main.index("run_migrations_once")
    assert frontend < build < snapshot < checker < normal_drain < migrate


def test_legacy_bootstrap_quiesces_celery_before_building_new_checkout():
    common = _read("deploy/scripts/rolling_update_common.sh")
    main = common.split("rolling_update_main()", 1)[1]
    legacy_drain = main.index("if (( LEGACY_BOOTSTRAP )) && app_update_needed; then ensure_celery_drained; fi")
    build = main.index("build_required_images")
    assert legacy_drain < build
    assert "container_has_repo_root_mount" in common
    assert "one-time bootstrap mode" in common


def test_release_sha_is_atomic_and_only_recorded_after_final_preflight():
    common = _read("deploy/scripts/rolling_update_common.sh")
    tail = common.split("rolling_update_main()", 1)[1]
    final_preflight = tail.rindex("app_preflight")
    record = tail.rindex("record_release")
    assert final_preflight < record
    assert 'mv "$tmp" "$STATE_FILE"' in common
    assert 'rm -f "$INPROGRESS_FILE"' in common


def test_interrupted_drain_is_resumable_even_if_worker_is_stopped_with_queued_work():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "resuming with Celery already drained from a previous attempt" in common
    assert "progress_set celery_drained 1" in common
    assert "Rerun the same updater; it will resume without requiring empty queues." in common


def test_deposit_only_update_does_not_stop_sweeper(tmp_path):
    log = tmp_path / "calls.log"
    script = textwrap.dedent(
        f"""
        source deploy/scripts/rolling_update_common.sh
        ENVIRONMENT_NAME=test
        DEPOSIT_IMAGE_CHANGED=1
        DEPOSIT_CONFIG_CHANGED=0
        SWEEPER_IMAGE_CHANGED=0
        SWEEPER_CONFIG_CHANGED=0
        SIGNER_IMAGE_CHANGED=0
        SIGNER_CONFIG_CHANGED=0
        capture_crypto_initial_state() {{ DEPOSIT_WAS_RUNNING=1; SWEEPER_WAS_RUNNING=1; SIGNER_WAS_RUNNING=1; }}
        service_container_ids() {{ echo fake; }}
        compose_crypto() {{ echo "$*" >> {log}; }}
        stop_crypto_for_update
        """
    )
    result = _bash(script)
    assert result.returncode == 0, result.stderr
    calls = log.read_text()
    assert "stop deposit_service" in calls
    assert "stop sweeper_service" not in calls


def test_sweeper_only_update_stops_only_sweeper_but_shared_image_updates_signer_by_classification():
    common = _read("deploy/scripts/rolling_update_common.sh")
    classify = common.split("classify_release()", 1)[1].split("app_update_needed()", 1)[0]
    assert "SWEEPER_IMAGE_CHANGED=1" in classify
    assert "SIGNER_IMAGE_CHANGED=1" in classify


def test_signer_update_temporarily_stops_both_running_crypto_loops(tmp_path):
    log = tmp_path / "calls.log"
    script = textwrap.dedent(
        f"""
        source deploy/scripts/rolling_update_common.sh
        ENVIRONMENT_NAME=test
        DEPOSIT_IMAGE_CHANGED=0
        DEPOSIT_CONFIG_CHANGED=0
        SWEEPER_IMAGE_CHANGED=0
        SWEEPER_CONFIG_CHANGED=0
        SIGNER_IMAGE_CHANGED=1
        SIGNER_CONFIG_CHANGED=0
        capture_crypto_initial_state() {{ DEPOSIT_WAS_RUNNING=1; SWEEPER_WAS_RUNNING=1; SIGNER_WAS_RUNNING=1; }}
        service_container_ids() {{ echo fake; }}
        compose_crypto() {{ echo "$*" >> {log}; }}
        stop_crypto_for_update
        """
    )
    result = _bash(script)
    assert result.returncode == 0, result.stderr
    calls = log.read_text()
    assert "stop deposit_service" in calls
    assert "stop sweeper_service" in calls


def test_crypto_original_running_state_is_persisted_for_retry():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "deposit_was_running" in common
    assert "sweeper_was_running" in common
    assert "signer_was_running" in common
    assert "restoring original crypto service state from unfinished update" in common



def test_rolling_update_has_per_environment_nonblocking_lock():
    common = _read("deploy/scripts/rolling_update_common.sh")
    main = common.split("rolling_update_main()", 1)[1]
    assert "acquire_update_lock" in main
    assert 'flock -n "$ROLLING_LOCK_FD"' in common
    assert "another $ENVIRONMENT_NAME rolling update is already running" in common


def test_legacy_bootstrap_drain_does_not_launch_new_django_code_inside_legacy_container():
    common = _read("deploy/scripts/rolling_update_common.sh")
    legacy = common.split("drain_celery_legacy()", 1)[1].split("drain_celery_fresh()", 1)[0]
    assert "stop_service_if_running celery_beat" in legacy
    assert "stop_service_if_running celery_worker" in legacy
    assert "celery_work_count" not in legacy
    assert "docker exec" not in legacy
    assert "queued work" in legacy


def test_crypto_workers_flag_can_act_even_when_release_sha_is_already_current():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert '"${CRYPTO_WORKERS:-0}" != 1' in common
    classify = common.split("classify_release()", 1)[1].split("app_update_needed()", 1)[0]
    assert 'if [[ "${CRYPTO_WORKERS:-0}" == 1 ]]' in classify

def test_compose_change_classifier_reports_service_and_top_level_changes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    compose = repo / "compose.yaml"
    compose.write_text(
        "name: test\nservices:\n  web:\n    image: old\n  db:\n    image: postgres\nvolumes:\n  data:\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "add", "compose.yaml"], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repo, check=True)
    base = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()

    compose.write_text(
        "name: test\nservices:\n  web:\n    image: new\n  db:\n    image: postgres\nvolumes:\n  data:\n",
        encoding="utf-8",
    )
    classifier = ROOT / "deploy/scripts/classify_compose_changes.py"
    result = subprocess.run(
        ["python3", str(classifier), base, "compose.yaml"],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert result.stdout.splitlines() == ["service:web"]

    compose.write_text(
        "name: changed\nservices:\n  web:\n    image: new\n  db:\n    image: postgres\nvolumes:\n  data:\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        ["python3", str(classifier), base, "compose.yaml"],
        cwd=repo,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert "top-level" in result.stdout.splitlines()
    assert "service:web" in result.stdout.splitlines()

def test_compose_classifier_failure_is_fail_closed(tmp_path):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3"
    fake_python.write_text("#!/bin/sh\nexit 7\n", encoding="utf-8")
    fake_python.chmod(0o755)

    script = textwrap.dedent(
        "source deploy/scripts/rolling_update_common.sh\n"
        "ENVIRONMENT_NAME=test\n"
        "BASE_SHA=deadbeef\n"
        "COMPOSE_FILE=docker-compose-cloudflare.yaml\n"
        "changed_matches() { return 0; }\n"
        "classify_compose_delta\n"
    )
    result = _bash(
        script,
        env={"PATH": f"{fake_bin}:{os.environ.get('PATH', '')}"},
    )
    assert result.returncode == 2
    assert "failed to classify docker-compose-cloudflare.yaml changes" in result.stderr

def test_application_migrations_never_converge_db_or_redis_dependencies():
    common = _read("deploy/scripts/rolling_update_common.sh")
    run_migrations = common.split("run_migrations() {", 1)[1].split("\n}", 1)[0]
    assert "compose run --rm --no-deps migrations" in run_migrations
    assert "compose run --rm migrations" not in run_migrations




def test_rolling_git_is_sudo_safe_and_build_requires_clean_untracked_tree():
    common = _read("deploy/scripts/rolling_update_common.sh")
    classifier = _read("deploy/scripts/classify_compose_changes.py")
    assert 'git_repo() { git -c "safe.directory=$ROLLING_ROOT" "$@"; }' in common
    assert "git_repo status --porcelain --untracked-files=all" in common
    assert "working-tree changes or untracked files are present" in common
    assert '"-c", f"safe.directory={repo_root}"' in classifier


def test_rolling_critical_celery_and_crypto_stops_are_fail_closed():
    common = _read("deploy/scripts/rolling_update_common.sh")
    legacy = common.split("drain_celery_legacy()", 1)[1].split("drain_celery_fresh()", 1)[0]
    fresh = common.split("drain_celery_fresh()", 1)[1].split("ensure_celery_drained()", 1)[0]
    crypto = common.split("stop_crypto_for_update()", 1)[1].split("remove_nonrunning_service_containers()", 1)[0]
    assert "stop_service_if_running celery_worker" in fresh
    assert "compose stop celery_worker >/dev/null || true" not in fresh
    assert "compose stop celery_beat >/dev/null || true" not in legacy
    assert "stop_crypto_service_if_running deposit_service" in crypto
    assert "stop_crypto_service_if_running sweeper_service" in crypto


def test_rolling_requires_signer_and_production_ingress_health_before_marking_release():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "verify_runtime_dependencies()" in common
    verify = common.split("verify_runtime_dependencies()", 1)[1].split("legacy_preflight()", 1)[0]
    assert "healthy_service_count dfx_signer_service" in verify
    assert "production signer is not deployed" in verify
    assert '[[ "$ENVIRONMENT_NAME" == "production" ]]' in verify
    assert "wait_healthy cloudflared 60 1" in verify
    main = common.split("rolling_update_main()", 1)[1]
    assert main.count("verify_runtime_dependencies") >= 2
    assert main.rindex("verify_runtime_dependencies") < main.rindex("record_release")


def test_docker_build_context_excludes_local_secrets_and_deploy_state():
    dockerignore = _read(".dockerignore")
    assert "```" not in dockerignore
    for entry in (
        ".env.*",
        "**/.env.*",
        "cms/local_settings.py",
        ".deploy-state/",
        "pids/",
        "deposit_service/data/state.json",
    ):
        assert entry in dockerignore



def test_production_rolling_lock_is_shared_with_other_prod_mutations():
    common = _read("deploy/scripts/rolling_update_common.sh")
    prod_common = _read("deploy/scripts/prod_common.sh")
    assert 'LOCK_FILE="$ROLLING_STATE_DIR/production.mutation.lock"' in common
    assert 'PROD_MUTATION_LOCK_FILE="$PROD_STATE_DIR/production.mutation.lock"' in prod_common


def test_inprogress_state_is_bound_to_exact_target_sha():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "bind_progress_to_target()" in common
    assert 'progress_set target_sha "$CURRENT_SHA"' in common
    assert "unfinished deployment targets" in common


def test_staging_does_not_require_an_undeployed_signer():
    common = _read("deploy/scripts/rolling_update_common.sh")
    verify = common.split("verify_runtime_dependencies()", 1)[1].split("legacy_preflight()", 1)[0]
    assert 'elif (( signer_count > 0 )); then' in verify
    assert "production signer is not deployed" in verify
    assert "wait_healthy dfx_signer_service 60 1" not in verify


def test_release_static_is_snapshot_mounted_not_live_checkout_static():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert 'STATIC_RELEASE_DIR="$ROLLING_STATE_DIR/static/$CURRENT_SHA"' in common
    assert "prepare_static_release()" in common
    assert "finalize_static_release()" in common
    for filename in ("docker-compose.yaml", "docker-compose-cloudflare.yaml"):
        compose = _read(filename)
        assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected:ro" in compose
        for service in ("web", "celery_worker", "celery_beat"):
            block = _service_block(compose, service)
            assert "./static:/home/mediacms.io/mediacms/static" not in block


def test_migrations_run_immutable_image_and_checker_changes_rebuild_it():
    common = _read("deploy/scripts/rolling_update_common.sh")
    classify = common.split("main_image_inputs_changed()", 1)[1].split("classify_compose_delta()", 1)[0]
    assert "deploy/scripts/check_rolling_migrations.py" in classify
    for filename in ("docker-compose.yaml", "docker-compose-cloudflare.yaml"):
        migrations = _service_block(_read(filename), "migrations")
        assert "./:/home/mediacms.io/mediacms/" not in migrations
        assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected" in migrations


def test_frontend_build_is_lockfile_reproducible():
    common = _read("deploy/scripts/rolling_update_common.sh")
    dockerfile = _read("frontend/Dockerfile.dev")
    gitignore = _read(".gitignore")
    assert "frontend/package-lock.json" not in gitignore
    assert "RUN npm ci --no-audit --no-fund" in dockerfile
    assert "npm install && npm run dist" not in common


def test_static_source_changes_trigger_image_rebuild_and_application_rotation():
    common = _read("deploy/scripts/rolling_update_common.sh")
    classifier = common.split("main_image_inputs_changed()", 1)[1].split("classify_compose_delta()", 1)[0]
    assert "STATIC_CHANGED=0" in common
    assert "changed_matches '^static/'" in common
    assert "static/*|" not in classifier
    assert "MAIN_IMAGE_CHANGED || APP_CONFIG_CHANGED || FRONTEND_CHANGED || STATIC_CHANGED" in common


def test_release_static_snapshot_starts_empty_and_is_filled_by_collectstatic():
    common = _read("deploy/scripts/rolling_update_common.sh")
    redis_migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")
    prepare = common.split("prepare_static_release()", 1)[1].split("finalize_static_release()", 1)[0]

    assert "cp -a static/." not in prepare
    assert 'mkdir -p "$tmp"' in prepare
    assert "Django collectstatic fills this clean directory from static/" in prepare
    assert "cp -a static/." not in redis_migration
    assert "live in static/ inside the target image" in redis_migration
    assert "populate static_collected/" in redis_migration

def test_celery_release_labels_are_verified_after_restart():
    common = _read("deploy/scripts/rolling_update_common.sh")
    restart = common.split("restart_celery()", 1)[1].split("restart_crypto_after_update()", 1)[0]
    assert "assert_current_release_service celery_worker 1" in restart
    assert "assert_current_release_service celery_beat 1" in restart


def test_signer_rotation_recreates_and_relabels_active_financial_loops():
    common = _read("deploy/scripts/rolling_update_common.sh")
    restart = common.split("restart_crypto_after_update()", 1)[1].split(
        "record_release()", 1
    )[0]
    assert "if deposit_update_needed || signer_update_needed; then" in restart
    assert "if sweeper_update_needed || signer_update_needed; then" in restart
    assert "compose_crypto up -d --no-deps --force-recreate deposit_service" in restart
    assert "compose_crypto up -d --no-deps --force-recreate sweeper_service" in restart
    assert "assert_current_release_service deposit_service 1" in restart
    assert "assert_current_release_service sweeper_service 1" in restart


def test_default_dev_compose_keeps_live_backend_sources_and_generated_static_out_of_mount_contract():
    dev = _read("docker-compose-dev.yaml")

    for service in ("migrations", "web", "celery_worker", "celery_beat"):
        block = _service_block(dev, service)
        assert "- ./:/home/mediacms.io/mediacms/" in block
        assert "MEDIACMS_STATIC_DIR" not in block

    for service, source_mount in (
        ("deposit_service", "./deposit_service/app:/app/app"),
        ("dfx_signer_service", "./sweeper_service/app:/app/app"),
        ("sweeper_service", "./sweeper_service/app:/app/app"),
    ):
        assert source_mount in _service_block(dev, service)

    assert not (ROOT / "docker-compose-dev-live.yaml").exists()


def test_dev_runtime_stop_grace_periods_match_production_contract():
    dev = _read("docker-compose-dev.yaml")
    expected = {
        "web": "90s",
        "deposit_service": "2m",
        "dfx_signer_service": "30s",
        "sweeper_service": "15m",
        "celery_beat": "30s",
        "celery_worker": "2h10m",
        "db": "60s",
        "redis": "60s",
    }
    for service, duration in expected.items():
        assert f"stop_grace_period: {duration}" in _service_block(dev, service)


def test_dev_frontend_dependencies_are_built_before_container_start():
    dev = _read("docker-compose-dev.yaml")
    dockerfile = _read("frontend/Dockerfile.dev")

    # .github/ is intentionally excluded from the Docker image. The
    # workflow itself executes npm ci in the host-side frontend job;
    # this test validates only the runtime/frontend image contract.
    frontend = _service_block(dev, "frontend")
    assert "context: ./frontend" in frontend
    assert "dockerfile: Dockerfile.dev" in frontend
    assert "command: npm run start" in frontend
    assert "frontend:/home/mediacms.io/mediacms/frontend/" not in frontend
    assert "/frontend/node_modules" not in frontend
    for mounted in ("src", "config", "packages", "dist"):
        assert f"frontend/{mounted}:/home/mediacms.io/mediacms/frontend/{mounted}" in frontend
    assert "RUN npm ci --no-audit --no-fund" in dockerfile
    assert "node_modules/.bin/mediacms-scripts" in dockerfile
    assert not (ROOT / "frontend/dev-entrypoint.sh").exists()

def test_entrypoint_never_mutates_readonly_release_static_mount():
    entrypoint = _read("deploy/docker/entrypoint.sh")
    assert "find /home/mediacms.io/mediacms" not in entrypoint
    assert 'APP_ROOT=/home/mediacms.io/mediacms' in entrypoint
    assert '"$APP_ROOT/static"' not in entrypoint
    assert 'chown -R www-data:"$TARGET_GID" "$path"' in entrypoint


def test_entrypoint_chowns_only_runtime_writable_directories():
    entrypoint = _read("deploy/docker/entrypoint.sh")
    ownership = entrypoint.split(
        "# Only runtime data directories need write ownership for www-data.", 1
    )[1].split("chmod +x", 1)[0]
    for directory in ("logs", "media_files", "backup"):
        assert f'"$APP_ROOT/{directory}"' in ownership
    assert "static" not in ownership


def test_collectstatic_has_canonical_source_and_distinct_generated_output():
    prestart = _read("deploy/docker/prestart.sh")
    dev = _read("docker-compose-dev.yaml")
    settings_py = _read("cms/settings.py")
    dev_settings = _read("cms/dev_settings.py")
    gitignore = _read(".gitignore")
    dockerignore = _read(".dockerignore")
    nginx = _read("deploy/docker/nginx_http_only.conf")

    assert 'echo "RUNNING COLLECTSTATIC"' in prestart
    assert "python manage.py collectstatic --noinput" in prestart
    assert "ENABLE_COLLECTSTATIC" not in prestart
    assert "ENABLE_COLLECTSTATIC" not in _service_block(dev, "migrations")
    assert 'STATICFILES_DIRS = [os.path.join(BASE_DIR, "static")]' in settings_py
    assert 'STATIC_ROOT = os.path.join(BASE_DIR, "static_collected")' in settings_py
    assert "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static'),)" in dev_settings
    assert "STATIC_ROOT = os.path.join(BASE_DIR, 'static_collected')" in dev_settings
    assert "/static_collected/" in gitignore
    assert "\n/static/\n" not in gitignore
    assert "static_collected/" in dockerignore
    assert "\n/static/\n" not in dockerignore
    assert (ROOT / "static/ads/ads.css").is_file()
    assert not (ROOT / "static_src").exists()
    assert not (ROOT / "static/vendor").exists()
    assert "alias /home/mediacms.io/mediacms/static_collected" in nginx

    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):
        compose = _read(compose_path)
        migrations = _service_block(compose, "migrations")
        assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected" in migrations
        assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected:ro" not in migrations

        for service in ("web", "celery_worker", "celery_beat"):
            runtime = _service_block(compose, service)
            assert "${MEDIACMS_STATIC_DIR:-./static_collected}:/home/mediacms.io/mediacms/static_collected:ro" in runtime

def test_celery_beat_schedule_lives_on_writable_runtime_volume():
    beat = _read("deploy/docker/supervisord/supervisord-celery_beat.conf")
    assert "user=www-data" in beat
    assert "--schedule=/home/mediacms.io/mediacms/logs/celerybeat-schedule" in beat


def test_redundant_nested_static_tree_is_removed_from_canonical_source():
    assert not (ROOT / "static/static").exists()
    for rel in (
        "static/images/social-media-icons/reddit.svg",
        "static/images/social-media-icons/telegram.svg",
        "static/images/social-media-icons/vk.svg",
        "static/images/social-media-icons/whatsapp.svg",
        "static/images/social-media-icons/x.svg",
        "static/images/wallet/cf-token.png",
    ):
        assert (ROOT / rel).is_file()

def test_dev_celery_framework_noise_is_targeted_and_logs_are_bounded():
    dev = _read("docker-compose-dev.yaml")
    celery_py = _read("cms/celery.py")
    settings_py = _read("cms/settings.py")

    for service in ("celery_worker", "celery_beat"):
        block = _service_block(dev, service)
        assert "CELERY_FRAMEWORK_LOG_LEVEL: WARNING" in block
        assert "driver: json-file" in block
        assert 'max-size: "20m"' in block
        assert 'max-file: "3"' in block

    short_conf = _read("deploy/docker/supervisord/supervisord-celery_short.conf")
    long_conf = _read("deploy/docker/supervisord/supervisord-celery_long.conf")
    assert 'CELERY_LOG_LEVEL:-INFO' in short_conf
    assert 'CELERY_LOG_LEVEL:-INFO' in long_conf
    for logger_name in (
        "celery.worker.strategy",
        "celery.app.trace",
        "celery.beat",
    ):
        assert logger_name in celery_py

    assert "CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True" in settings_py
    assert "broker_connection_retry_on_startup" in celery_py



def test_staging_has_stable_functional_ingress_and_true_scaled_web_roll():
    staging_compose = _read("docker-compose.yaml")
    staging_updater = _read("deploy/scripts/staging_rolling_update.sh")
    common = _read("deploy/scripts/rolling_update_common.sh")
    ingress_conf = _read("deploy/docker/staging_ingress.conf")

    web = _service_block(staging_compose, "web")
    ingress = _service_block(staging_compose, "staging_ingress")

    assert '"scaled"' in staging_updater
    assert '"80:80"' not in web
    assert 'expose:\n      - "80"' in web
    assert '"80:80"' in ingress
    assert "nginx:1.30.4-alpine" in ingress
    assert "staging_ingress.conf:/etc/nginx/nginx.conf:ro" in ingress
    assert "wget -q -O /dev/null http://127.0.0.1/" in ingress

    # Dynamic Docker DNS is what keeps a stable ingress useful while web IDs
    # change underneath it. The upload contract must not regress either.
    assert "resolver 127.0.0.11 valid=1s ipv6=off" in ingress_conf
    assert "server web:80 resolve" in ingress_conf
    assert "client_max_body_size 5800M" in ingress_conf
    assert "proxy_request_buffering off" in ingress_conf
    assert "proxy_next_upstream" in ingress_conf

    assert "STAGING_INGRESS_CHANGED=0" in common
    assert "STAGING_INGRESS_VALIDATED=0" in common
    assert "prepare_staging_ingress()" in common
    assert "compose run --rm --no-deps --entrypoint nginx staging_ingress" in common
    assert "compose create --no-deps staging_ingress" not in common
    assert "STAGING_INGRESS_PRECREATED" not in common
    assert "ensure_staging_ingress()" in common
    assert "require_staging_ingress_healthy()" in common
    assert "staging_ingress)" in common

    main = common.split("rolling_update_main()", 1)[1]
    assert main.index("prepare_staging_ingress") < main.index("update_web")
    assert main.index("update_web") < main.rindex("ensure_staging_ingress")
    assert main.rindex("require_staging_ingress_healthy") < main.rindex("record_release")


def test_prestart_failed_migration_is_fail_closed_and_password_is_not_logged(tmp_path):
    prestart = _read("deploy/docker/prestart.sh")
    assert prestart.startswith("#!/bin/bash\nset -e\n")
    assert "Created admin user with password" not in prestart

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python"
    fake_python.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ \"$1\" == \"-c\" ]]; then echo generatedpass; exit 0; fi\n"
        "if [[ \"$1\" == \"manage.py\" && \"$2\" == \"migrate\" ]]; then exit 41; fi\n"
        "exit 99\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env.get('PATH', '')}",
            "ENABLE_MIGRATIONS": "yes",
            "ADMIN_USER": "admin",
            "ADMIN_EMAIL": "admin@example.invalid",
        }
    )
    result = subprocess.run(
        ["bash", "deploy/docker/prestart.sh"],
        cwd=ROOT,
        env=env,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 41
    assert "RUNNING COLLECTSTATIC" not in result.stdout


def test_static_release_gc_preserves_live_states_and_recent_rollbacks(tmp_path):
    state = tmp_path / "state"
    static_root = state / "static"
    static_root.mkdir(parents=True)

    current = "a" * 40
    prod_release = "b" * 40
    prod_static = "3" * 40
    inprogress = "c" * 40
    recent_1 = "d" * 40
    recent_2 = "e" * 40
    old_1 = "1" * 40
    old_2 = "2" * 40

    # production.release may advance on a crypto-only deployment while web keeps
    # mounting an older application snapshot. That active static SHA must survive
    # regardless of age; two additional recent snapshots stay as rollback copies.
    ordered = [old_1, prod_static, old_2, inprogress, recent_1, recent_2, current]
    base = 1_700_000_000
    for offset, sha in enumerate(ordered):
        directory = static_root / sha
        directory.mkdir()
        os.utime(directory, (base + offset, base + offset))

    (state / "staging.release").write_text(current + "\n", encoding="utf-8")
    (state / "staging.static-release").write_text(current + "\n", encoding="utf-8")
    (state / "production.release").write_text(prod_release + "\n", encoding="utf-8")
    (state / "production.static-release").write_text(prod_static + "\n", encoding="utf-8")
    (state / "production.redis-migration.inprogress").write_text(
        f"target_sha={inprogress}\nphase=80\n", encoding="utf-8"
    )

    stale_tmp = static_root / f"{old_1}.tmp.123"
    stale_tmp.mkdir()
    os.utime(stale_tmp, (base, base))
    protected_tmp = static_root / f"{inprogress}.tmp.456"
    protected_tmp.mkdir()
    os.utime(protected_tmp, (base, base))

    script = (
        "source deploy/scripts/rolling_update_common.sh\n"
        f"CURRENT_SHA={current}\n"
        "ENVIRONMENT_NAME=test\n"
        "STATIC_RELEASE_KEEP_COUNT=2\n"
        "STATIC_TMP_MAX_AGE_MINUTES=1\n"
        "cleanup_static_releases\n"
    )
    result = _bash(script, env={"ROLLING_STATE_DIR": str(state)})
    assert result.returncode == 0, result.stderr

    assert (static_root / current).is_dir()
    assert (static_root / prod_static).is_dir()
    assert (static_root / inprogress).is_dir()
    assert (static_root / recent_1).is_dir()
    assert (static_root / recent_2).is_dir()
    assert not (static_root / old_1).exists()
    assert not (static_root / old_2).exists()
    assert not stale_tmp.exists()
    assert protected_tmp.exists()


def test_static_gc_skips_pruning_until_all_deployed_environments_have_static_state(tmp_path):
    state = tmp_path / "state"
    static_root = state / "static"
    static_root.mkdir(parents=True)
    active = "a" * 40
    newer = "b" * 40
    for offset, sha in enumerate((active, newer)):
        directory = static_root / sha
        directory.mkdir()
        os.utime(directory, (1_700_000_000 + offset, 1_700_000_000 + offset))

    (state / "production.release").write_text(newer + "\n", encoding="utf-8")
    # Deliberately no production.static-release: this is the upgrade state from
    # the old GC implementation and pruning must fail safe.
    script = (
        "source deploy/scripts/rolling_update_common.sh\n"
        f"CURRENT_SHA={newer}\n"
        "ENVIRONMENT_NAME=test\n"
        "STATIC_RELEASE_KEEP_COUNT=0\n"
        "cleanup_static_releases\n"
    )
    result = _bash(script, env={"ROLLING_STATE_DIR": str(state)})
    assert result.returncode == 0, result.stderr
    assert (static_root / active).is_dir()
    assert "skipping static GC" in result.stderr


def test_static_state_bootstraps_from_existing_web_snapshot_without_overwriting_it(tmp_path):
    state = tmp_path / "state"
    static_root = state / "static"
    static_root.mkdir(parents=True)
    active = "a" * 40
    (static_root / active).mkdir()

    script = (
        "source deploy/scripts/rolling_update_common.sh\n"
        f"ROLLING_STATE_DIR={str(state)!r}\n"
        'STATIC_STATE_FILE="$ROLLING_STATE_DIR/production.static-release"\n'
        "ENVIRONMENT_NAME=production\n"
        "service_container_ids_all() { printf 'web1\nweb2\n'; }\n"
        f"static_snapshot_sha_from_container() {{ printf '{active}\n'; }}\n"
        "bootstrap_static_release_state_from_web\n"
        'cat "$STATIC_STATE_FILE"\n'
    )
    result = _bash(script)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith(active)
    assert (state / "production.static-release").read_text(encoding="utf-8").strip() == active


def test_static_gc_runs_only_on_success_paths_after_release_state_or_noop_gate():
    common = _read("deploy/scripts/rolling_update_common.sh")
    main = common.split("rolling_update_main()", 1)[1]
    assert "cleanup_static_releases()" in common
    assert 'STATIC_RELEASE_KEEP_COUNT="${STATIC_RELEASE_KEEP_COUNT:-3}"' in common
    assert '"$ROLLING_STATE_DIR"/*.release' in common
    assert '"$ROLLING_STATE_DIR"/*.static-release' in common
    assert '"$ROLLING_STATE_DIR"/*.inprogress' in common
    assert '"$ROLLING_STATE_DIR"/*.complete' in common
    assert "static_release_state_coverage_complete" in common
    assert "bootstrap_static_release_state_from_web" in main
    assert "if app_update_needed; then\n    # Only an application rollout changes the snapshot mounted by web." in main

    final_static_record = main.rindex("record_static_release")
    final_record = main.rindex("record_release")
    assert final_static_record < final_record
    final_gc = main.rindex("cleanup_static_releases")
    assert final_record < final_gc

def test_cold_start_retarget_is_allowed_only_before_any_application_mutation():
    start = _read("deploy/scripts/prod_start.sh")

    assert "cold_start_pre_mutation_resume_safe()" in start
    assert "retarget_pre_mutation_cold_start_if_needed()" in start
    assert 'progress_set previous_target_sha "$saved"' in start
    assert 'progress_set target_sha "$CURRENT_SHA"' in start

    helper = start.split("cold_start_pre_mutation_resume_safe()", 1)[1].split(
        "retarget_pre_mutation_cold_start_if_needed()", 1
    )[0]
    for marker in (
        "static_prepared",
        "static_finalized",
        "migrations_done",
        "celery_drained",
    ):
        assert marker in helper
    assert '$ROLLING_STATE_DIR/static/$saved' in helper

    # DB/Redis are explicitly allowed because the failed preflight may already
    # have started them. Every application/runtime service must still be down.
    assert "for service in web celery_beat celery_worker dfx_signer_service deposit_service sweeper_service cloudflared" in helper
    assert " db " not in helper
    assert " redis " not in helper

    retarget_call = start.rindex("\nretarget_pre_mutation_cold_start_if_needed\n")
    bind_call = start.rindex("\nbind_progress_to_target\n")
    assert retarget_call < bind_call


def test_cold_start_retarget_preserves_empty_redis_confirmation_state():
    start = _read("deploy/scripts/prod_start.sh")
    retarget = start.split("retarget_pre_mutation_cold_start_if_needed()", 1)[1].split(
        "# Do not hijack an interrupted rolling deployment.", 1
    )[0]

    # Retargeting changes only SHA metadata. It must not erase the confirmation
    # needed when the first failed cold start established a new empty Redis.
    assert 'rm -f "$INPROGRESS_FILE"' not in retarget
    assert "redis_empty_reset_confirmed" not in retarget



def test_frontend_build_contract_prevents_blank_bundle_and_nested_static():
    compose = _read("docker-compose-dev.yaml")
    frontend = _service_block(compose, "frontend")
    dockerfile = _read("frontend/Dockerfile.dev")
    dockerignore = _read("frontend/.dockerignore")
    common = _read("deploy/scripts/rolling_update_common.sh")

    assert "frontend/.env:/home/mediacms.io/mediacms/frontend/.env:ro" in frontend
    assert ".env" in dockerignore.splitlines()
    assert "COPY .env" not in dockerfile
    assert "&& npm run dist" not in dockerfile
    assert "frontend/dist/static/static" in common

    for config_path in (
        "frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts",
        "frontend/packages/scripts/dist/webpack-dev-env.js",
    ):
        config = _read(config_path)
        assert "if (dotenv.error)" in config or "if (dotenvResult.error)" in config
        assert "var frontendEnv = dotenv.parsed || {};" in config or "var frontendEnv = dotenvResult.parsed || {};" in config
        assert 'JSON.stringify(frontendEnv)' in config
        assert 'JSON.stringify(dotenv.parsed)' not in config
        assert "webpackStaticAssetName" in config
        assert config.count("emitFile: false") == 2
        assert config.count("publicPath: '/'") == 2
        assert "fallback: {" in config
        assert ".replace(/^\\/?static(?=\\/|$)/, '')" not in config


def test_frontend_copied_assets_have_single_physical_emitter():
    source = _read("frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts")
    dist = _read("frontend/packages/scripts/dist/webpack-dev-env.js")

    # CopyPlugin owns the physical static tree. Asset imports only
    # return /static/... URLs and must never emit a second copy.
    for config in (source, dist):
        assert "src/static/images" in config
        assert "src/static/lib" in config
        assert config.count("emitFile: false") == 2
        assert config.count("publicPath: '/'") == 2
        assert "webpackStaticAssetName" in config
