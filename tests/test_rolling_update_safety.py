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
        ["bash", "-lc", script],
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


def test_frontend_build_is_one_shot_and_additive():
    common = _read("deploy/scripts/rolling_update_common.sh")
    assert "run --rm --no-deps frontend" in common
    assert "npm install && npm run dist" in common
    assert "cp -a frontend/dist/static/. static/" in common
    assert "rm -rf static" not in common


def test_app_processes_are_image_isolated_but_migrations_keep_explicit_checkout_mount():
    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):
        compose = _read(compose_path)
        for service in ("web", "celery_worker", "celery_beat"):
            block = _service_block(compose, service)
            assert "- ./:/home/mediacms.io/mediacms/" not in block
            assert "./static:/home/mediacms.io/mediacms/static" in block
            assert "./media_files:/home/mediacms.io/mediacms/media_files" in block
            assert "./logs:/home/mediacms.io/mediacms/logs" in block
            assert "./backup:/home/mediacms.io/mediacms/backup" in block
            assert 'io.mediacms.release: "${MEDIACMS_RELEASE_SHA:-unmanaged}"' in block

        migrations = _service_block(compose, "migrations")
        assert "- ./:/home/mediacms.io/mediacms/" in migrations


def test_release_labels_cover_resumable_web_signer_and_workers():
    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):
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
    assert '"single"' in staging
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
    assert "compose run --rm --no-deps migrations python deploy/scripts/check_rolling_migrations.py" in common
    assert "compose run --rm --no-deps web python deploy/scripts/check_rolling_migrations.py" not in common


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
    build = main.index("build_required_images")
    checker = main.index("check_pending_migrations")
    frontend = main.index("build_frontend_dist")
    normal_drain = main.index("if (( ! LEGACY_BOOTSTRAP )); then ensure_celery_drained; fi")
    migrate = main.index("run_migrations")
    assert build < checker < frontend < normal_drain < migrate


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
    assert "compose stop celery_beat" in legacy
    assert "compose stop celery_worker" in legacy
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

