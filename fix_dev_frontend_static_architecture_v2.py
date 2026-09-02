#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
from pathlib import Path

EXPECTED_HEAD = "7c00bcf8fbae5c62cbbf713a3676be811efa980d"


def find_repo_root() -> Path:
    here = Path(__file__).resolve().parent
    seen: set[Path] = set()
    for candidate in [Path.cwd(), *Path.cwd().parents, here, *here.parents]:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if (candidate / "manage.py").is_file() and (candidate / "docker-compose-dev.yaml").is_file():
            return candidate
    raise SystemExit("[architecture-fix] cannot locate MediaCMS repository root")


ROOT = find_repo_root()


def run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=check,
    )


def git(*args: str) -> str:
    return run(["git", *args]).stdout.strip()


def read(rel: str) -> str:
    path = ROOT / rel
    if not path.is_file():
        raise SystemExit(f"[architecture-fix] missing expected file: {rel}")
    return path.read_text(encoding="utf-8")


def write(rel: str, text: str) -> None:
    path = ROOT / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print(f"[architecture-fix] updated {rel}")


def transition(rel: str, old: str, new: str, *, label: str = "") -> None:
    """Apply one exact old->new transition, or accept the already-applied state."""
    text = read(rel)
    old_count = text.count(old)
    new_count = text.count(new)
    if old_count == 1 and new_count == 0:
        write(rel, text.replace(old, new, 1))
        return
    if old_count == 0 and new_count == 1:
        print(f"[architecture-fix] already applied {rel}{': ' + label if label else ''}")
        return
    raise SystemExit(
        f"[architecture-fix] {rel}: cannot resolve transition"
        f"{(' (' + label + ')') if label else ''}; old={old_count}, new={new_count}"
    )


def verify_checkout() -> None:
    head = git("rev-parse", "HEAD")
    staged = git("diff", "--cached", "--name-only")
    print(f"[architecture-fix] repo: {ROOT}")
    print(f"[architecture-fix] HEAD: {head}")
    if head != EXPECTED_HEAD:
        raise SystemExit(f"[architecture-fix] expected HEAD {EXPECTED_HEAD}, got {head}")
    if staged:
        raise SystemExit("[architecture-fix] staged changes present; unstage them before recovery:\n" + staged)

    owned = {
        ".gitignore", ".dockerignore", "cms/settings.py", "cms/dev_settings.py",
        "deploy/docker/prestart.sh", "docker-compose-dev.yaml",
        "deploy/scripts/rolling_update_common.sh",
        "deploy/scripts/prod_migrate_redis_persistence.sh",
        "tests/test_rolling_update_safety.py", "frontend/dev-entrypoint.sh",
    }
    owned.update(tracked_static_files())
    changed = set(filter(None, git("diff", "--name-only").splitlines()))
    unexpected = sorted(changed - owned)
    if unexpected:
        raise SystemExit(
            "[architecture-fix] unrelated tracked changes present; refusing recovery:\n"
            + "\n".join(unexpected)
        )
    if changed:
        print(f"[architecture-fix] recovery mode: accepting {len(changed)} owned partially-applied tracked changes")


def tracked_static_files() -> list[str]:
    proc = run(["git", "ls-files", "-z", "--", "static"])
    return [item for item in proc.stdout.split("\0") if item]


def separate_static_source_and_output() -> None:
    tracked = tracked_static_files()
    if not tracked:
        raise SystemExit("[architecture-fix] expected tracked files below static/")
    vendor = [rel for rel in tracked if rel.startswith("static/vendor/")]
    source = [rel for rel in tracked if not rel.startswith("static/vendor/")]
    if not vendor or not source:
        raise SystemExit("[architecture-fix] unexpected tracked static layout at pinned HEAD")

    moved = already_moved = removed_vendor = already_removed_vendor = 0
    for rel in source:
        src_path = ROOT / rel
        dst = ROOT / "static_src" / Path(rel).relative_to("static")
        if src_path.is_file() and not dst.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src_path, dst)
            moved += 1
        elif not src_path.exists() and dst.is_file():
            already_moved += 1
        else:
            raise SystemExit(f"[architecture-fix] ambiguous static recovery state: {rel} -> {dst}")

    for rel in vendor:
        path = ROOT / rel
        dst = ROOT / "static_src" / Path(rel).relative_to("static")
        if dst.exists():
            raise SystemExit(f"[architecture-fix] historical vendor output unexpectedly exists in static_src: {dst}")
        if path.is_file():
            path.unlink()
            removed_vendor += 1
        elif not path.exists():
            already_removed_vendor += 1
        else:
            raise SystemExit(f"[architecture-fix] unexpected vendor path type: {rel}")

    print(
        "[architecture-fix] static split: "
        f"moved={moved}, already_moved={already_moved}, "
        f"vendor_removed={removed_vendor}, vendor_already_removed={already_removed_vendor}"
    )


def patch_static_settings() -> None:
    transition(
        "cms/settings.py",
        '''STATIC_URL = "/static/"  # where js/css files are stored on the filesystem\nMEDIA_URL = "/media/"  # URL where static files are served from the server\nSTATIC_ROOT = BASE_DIR + "/static/"\n''',
        '''STATIC_URL = "/static/"  # where js/css files are served\nMEDIA_URL = "/media/"  # URL where uploaded media files are served\n# Never scan STATIC_ROOT as a source. static_src/ is versioned input; static/\n# is generated collectstatic output (or a per-release bind mount in Docker).\nSTATICFILES_DIRS = [os.path.join(BASE_DIR, "static_src")]\nSTATIC_ROOT = os.path.join(BASE_DIR, "static")\n''',
    )
    transition(
        "cms/settings.py",
        '''    # BASE_DIR/static is a canonical source directory in this fork (for\n    # example static/ads/ads.css). In production nginx serves it directly.\n    # During tests STATIC_ROOT is redirected to TESTING_ROOT, so expose the\n    # repository static directory to Django's FileSystemFinder explicitly.\n    STATICFILES_DIRS = [\n        os.path.join(BASE_DIR, "static"),\n    ]\n''',
        '''    # Keep tests on the same source/output split as production.\n    STATICFILES_DIRS = [os.path.join(BASE_DIR, "static_src")]\n''',
    )
    transition(
        "cms/dev_settings.py",
        "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static'),)\nSTATIC_ROOT = os.path.join(BASE_DIR, 'static_collected')\n",
        "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static_src'),)\nSTATIC_ROOT = os.path.join(BASE_DIR, 'static_collected')\n",
    )


def patch_ignores() -> None:
    gitignore = read(".gitignore")
    marker = "# Generated collectstatic output; canonical sources live in static_src/\n/static/\n"
    if marker not in gitignore:
        gitignore = gitignore.rstrip() + "\n\n" + marker
        write(".gitignore", gitignore)

    dockerignore = read(".dockerignore")
    marker2 = "# Django collectstatic output is runtime/release data; sources are in static_src/\n/static/\n"
    if marker2 not in dockerignore:
        dockerignore = dockerignore.rstrip() + "\n\n" + marker2
        write(".dockerignore", dockerignore)


def restore_collectstatic() -> None:
    rel = "deploy/docker/prestart.sh"
    body = read(rel)
    desired = '''    echo "RUNNING COLLECTSTATIC"\n    python manage.py collectstatic --noinput\n'''
    legacy = '''    if [ X"${ENABLE_COLLECTSTATIC:-yes}" = X"yes" ]; then\n        echo "RUNNING COLLECTSTATIC"\n        python manage.py collectstatic --noinput\n    else\n        echo "Skipping collectstatic (ENABLE_COLLECTSTATIC=${ENABLE_COLLECTSTATIC:-yes})"\n    fi\n'''
    if body.count(desired) == 1 and "ENABLE_COLLECTSTATIC" not in body:
        print("[architecture-fix] collectstatic already unconditional in prestart.sh")
        return
    if body.count(legacy) == 1:
        write(rel, body.replace(legacy, desired, 1))
        return
    raise SystemExit("[architecture-fix] deploy/docker/prestart.sh: unexpected collectstatic layout")


def patch_dev_compose() -> None:
    rel = "docker-compose-dev.yaml"
    text = read(rel)
    text = text.replace(
        "      - ${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static\n", ""
    )
    text = text.replace(
        "      - ${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro\n", ""
    )
    if "MEDIACMS_STATIC_DIR" in text:
        raise SystemExit("[architecture-fix] unexpected remaining dev static release mount")
    text = text.replace('      ENABLE_COLLECTSTATIC: "no"\n', "")

    old_frontend = '''  frontend:
    image: node:20
    volumes:
      - ${PWD}/frontend:/home/mediacms.io/mediacms/frontend/
      - frontend_node_modules:/home/mediacms.io/mediacms/frontend/node_modules
      - frontend_npm_cache:/root/.npm
    working_dir: /home/mediacms.io/mediacms/frontend/
    command: bash ./dev-entrypoint.sh
    env_file:
      - ${PWD}/frontend/.env
    ports:
      - "8088:8088"
    depends_on:
      - web
'''
    new_frontend = '''  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile.dev
    working_dir: /home/mediacms.io/mediacms/frontend/
    # Keep node_modules inside the built image. Bind only mutable sources so a
    # source mount can never mask the installed mediacms-scripts binary.
    volumes:
      - ${PWD}/frontend/src:/home/mediacms.io/mediacms/frontend/src
      - ${PWD}/frontend/config:/home/mediacms.io/mediacms/frontend/config
      - ${PWD}/frontend/packages:/home/mediacms.io/mediacms/frontend/packages
      - ${PWD}/frontend/dist:/home/mediacms.io/mediacms/frontend/dist
      - ${PWD}/frontend/.babelrc:/home/mediacms.io/mediacms/frontend/.babelrc:ro
      - ${PWD}/frontend/tsconfig.json:/home/mediacms.io/mediacms/frontend/tsconfig.json:ro
    command: npm run start
    env_file:
      - ${PWD}/frontend/.env
    ports:
      - "8088:8088"
    depends_on:
      - web
'''
    if old_frontend in text and new_frontend not in text:
        text = text.replace(old_frontend, new_frontend, 1)
    elif new_frontend in text and old_frontend not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] unexpected frontend service layout")

    old_volumes = '''
volumes:
  frontend_node_modules:
  frontend_npm_cache:

secrets:
'''
    if old_volumes in text:
        text = text.replace(old_volumes, "\nsecrets:\n", 1)
    elif "frontend_node_modules:" in text or "frontend_npm_cache:" in text:
        raise SystemExit("[architecture-fix] unexpected partial frontend named-volume layout")
    write(rel, text)


def create_frontend_image() -> None:
    dockerfile = '''FROM node:20\n\nWORKDIR /home/mediacms.io/mediacms/frontend\n\n# Keep npm-ci cache tied only to dependency metadata. Local file: packages are\n# links in package-lock.json, so their package metadata must exist before npm ci.\nCOPY package.json package-lock.json ./\nCOPY packages/scripts/package.json packages/scripts/cli.js ./packages/scripts/\nCOPY packages/player/package.json ./packages/player/\nCOPY packages/vjs-plugin/package.json ./packages/vjs-plugin/\nCOPY packages/vjs-plugin-font-icons/package.json ./packages/vjs-plugin-font-icons/\nRUN npm ci --no-audit --no-fund\n\n# Source changes invalidate compilation, not dependency installation.\nCOPY . .\nRUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist\n\nCMD ["npm", "run", "start"]\n'''
    dockerfile_path = ROOT / "frontend/Dockerfile.dev"
    if dockerfile_path.exists():
        if read("frontend/Dockerfile.dev") != dockerfile:
            raise SystemExit("[architecture-fix] frontend/Dockerfile.dev exists with unexpected content")
        print("[architecture-fix] already created frontend/Dockerfile.dev")
    else:
        write("frontend/Dockerfile.dev", dockerfile)

    dockerignore = '''node_modules\n**/node_modules\n/dist\n.env\nnpm-debug.log*\n'''
    dockerignore_path = ROOT / "frontend/.dockerignore"
    if dockerignore_path.exists():
        if read("frontend/.dockerignore") != dockerignore:
            raise SystemExit("[architecture-fix] frontend/.dockerignore exists with unexpected content")
        print("[architecture-fix] already created frontend/.dockerignore")
    else:
        write("frontend/.dockerignore", dockerignore)

    obsolete = ROOT / "frontend/dev-entrypoint.sh"
    if obsolete.exists():
        obsolete.unlink()
        print("[architecture-fix] deleted obsolete frontend/dev-entrypoint.sh")


def patch_rolling_static_and_frontend() -> None:
    rel = "deploy/scripts/rolling_update_common.sh"
    transition(
        rel,
        "  if (( first_run )) || changed_matches '^static/'; then STATIC_CHANGED=1; fi\n",
        "  if (( first_run )) || changed_matches '^static_src/'; then STATIC_CHANGED=1; fi\n",
    )
    transition(
        rel,
        '''build_frontend_dist() {\n  (( FRONTEND_CHANGED )) || return 0\n  echo "rolling-update[$ENVIRONMENT_NAME]: building frontend in an isolated one-shot dev container"\n  [[ -f frontend/package-lock.json ]] || die "frontend/package-lock.json is required for reproducible frontend builds"\n  "${FRONTEND_COMPOSE[@]}" run --rm --no-deps frontend bash -lc 'npm ci --no-audit --no-fund && npm run dist'\n  [[ -d frontend/dist/static ]] || die "frontend build completed without frontend/dist/static"\n  FRONTEND_BUILT=1\n}\n''',
        '''build_frontend_dist() {\n  (( FRONTEND_CHANGED )) || return 0\n  echo "rolling-update[$ENVIRONMENT_NAME]: building reproducible frontend image and dist"\n  [[ -f frontend/package-lock.json ]] || die "frontend/package-lock.json is required for reproducible frontend builds"\n  "${FRONTEND_COMPOSE[@]}" build frontend\n  "${FRONTEND_COMPOSE[@]}" run --rm --no-deps frontend npm run dist\n  [[ -d frontend/dist/static ]] || die "frontend build completed without frontend/dist/static"\n  FRONTEND_BUILT=1\n}\n''',
    )
    transition(
        rel,
        '''  tmp="${STATIC_RELEASE_DIR}.tmp.$$"\n  rm -rf "$tmp"\n  mkdir -p "$tmp"\n  cp -a static/. "$tmp/"\n  rm -rf "$STATIC_RELEASE_DIR"\n  mv "$tmp" "$STATIC_RELEASE_DIR"\n  progress_set static_prepared 1\n''',
        '''  # STATIC_RELEASE_DIR is generated output, never a copy of the mutable\n  # checkout. Django collectstatic fills this clean directory from static_src/\n  # and installed-app finders during the migrations phase.\n  tmp="${STATIC_RELEASE_DIR}.tmp.$$"\n  rm -rf "$tmp"\n  mkdir -p "$tmp"\n  mkdir -p "$(dirname "$STATIC_RELEASE_DIR")"\n  rm -rf "$STATIC_RELEASE_DIR"\n  mv "$tmp" "$STATIC_RELEASE_DIR"\n  progress_set static_prepared 1\n''',
    )


def patch_redis_bootstrap() -> None:
    rel = "deploy/scripts/prod_migrate_redis_persistence.sh"
    transition(
        rel,
        '''# Build a release-owned static tree. Application containers never mount the\n# mutable checkout's ./static directly after this transition.\nif [[ ! -d "$REDIS_STATIC_DIR" ]]; then\n  (( phase < 80 )) || {\n    echo "Release static snapshot is missing after migrations were recorded complete; refusing unsafe reconstruction." >&2\n    exit 1\n  }\n  tmp_static="${REDIS_STATIC_DIR}.tmp.$$"\n  rm -rf "$tmp_static"\n  mkdir -p "$tmp_static"\n  cp -a static/. "$tmp_static/"\n  mkdir -p "$(dirname "$REDIS_STATIC_DIR")"\n  mv "$tmp_static" "$REDIS_STATIC_DIR"\nfi\n''',
        '''# Prepare a clean release-owned collectstatic output tree. Static sources\n# live in static_src/ inside the target image; migrations populate this mount.\nif [[ ! -d "$REDIS_STATIC_DIR" ]]; then\n  (( phase < 80 )) || {\n    echo "Release static snapshot is missing after migrations were recorded complete; refusing unsafe reconstruction." >&2\n    exit 1\n  }\n  tmp_static="${REDIS_STATIC_DIR}.tmp.$$"\n  rm -rf "$tmp_static"\n  mkdir -p "$tmp_static"\n  mkdir -p "$(dirname "$REDIS_STATIC_DIR")"\n  mv "$tmp_static" "$REDIS_STATIC_DIR"\nfi\n''',
    )
    transition(
        rel,
        '''  docker compose -p mediacms-frontend-build -f docker-compose-dev.yaml run --rm --no-deps frontend \\\n    bash -lc 'npm ci --no-audit --no-fund && npm run dist'\n''',
        '''  docker compose -p mediacms-frontend-build -f docker-compose-dev.yaml build frontend\n  docker compose -p mediacms-frontend-build -f docker-compose-dev.yaml run --rm --no-deps frontend npm run dist\n''',
    )


def patch_tests() -> None:
    rel = "tests/test_rolling_update_safety.py"
    text = read(rel)

    old = '''def test_frontend_build_is_one_shot_reproducible_and_release_scoped():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    assert "run --rm --no-deps frontend" in common\n    assert "npm ci --no-audit --no-fund && npm run dist" in common\n    assert 'cp -a frontend/dist/static/. "$STATIC_RELEASE_DIR/"' in common\n    assert "rm -rf static" not in common\n'''
    new = '''def test_frontend_build_is_image_verified_reproducible_and_release_scoped():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    dockerfile = _read("frontend/Dockerfile.dev")\n    assert '"${FRONTEND_COMPOSE[@]}" build frontend' in common\n    assert "run --rm --no-deps frontend npm run dist" in common\n    assert "RUN npm ci --no-audit --no-fund" in dockerfile\n    assert "RUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist" in dockerfile\n    assert 'cp -a frontend/dist/static/. "$STATIC_RELEASE_DIR/"' in common\n'''
    if old in text and new not in text:
        text = text.replace(old, new, 1)
    elif new in text and old not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] frontend rolling test anchor changed")

    old = '''def test_frontend_build_is_lockfile_reproducible():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    gitignore = _read(".gitignore")\n    assert "frontend/package-lock.json" not in gitignore\n    assert "npm ci --no-audit --no-fund" in common\n    assert "npm install && npm run dist" not in common\n\n\ndef test_static_only_changes_trigger_application_rotation():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    assert "STATIC_CHANGED=0" in common\n    assert "changed_matches '^static/'" in common\n    assert "MAIN_IMAGE_CHANGED || APP_CONFIG_CHANGED || FRONTEND_CHANGED || STATIC_CHANGED" in common\n'''
    new = '''def test_frontend_build_is_lockfile_reproducible():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    dockerfile = _read("frontend/Dockerfile.dev")\n    gitignore = _read(".gitignore")\n    assert "frontend/package-lock.json" not in gitignore\n    assert "RUN npm ci --no-audit --no-fund" in dockerfile\n    assert "npm install && npm run dist" not in common\n\n\ndef test_static_source_changes_trigger_image_rebuild_and_application_rotation():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    assert "STATIC_CHANGED=0" in common\n    assert "changed_matches '^static_src/'" in common\n    assert "MAIN_IMAGE_CHANGED || APP_CONFIG_CHANGED || FRONTEND_CHANGED || STATIC_CHANGED" in common\n\n\ndef test_release_static_snapshot_starts_empty_and_is_filled_by_collectstatic():\n    common = _read("deploy/scripts/rolling_update_common.sh")\n    redis_migration = _read("deploy/scripts/prod_migrate_redis_persistence.sh")\n    prepare = common.split("prepare_static_release()", 1)[1].split("finalize_static_release()", 1)[0]\n    assert "cp -a static/." not in prepare\n    assert "static_src/" in prepare\n    assert "cp -a static/." not in redis_migration\n    assert "static_src/" in redis_migration\n'''
    if old in text and new not in text:
        text = text.replace(old, new, 1)
    elif new in text and old not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] static/lockfile test anchor changed")

    old = '''def test_default_dev_compose_keeps_live_source_mounts_without_weakening_static_mounts():\n    dev = _read("docker-compose-dev.yaml")\n\n    for service in ("migrations", "web", "celery_worker", "celery_beat"):\n        assert "- ./:/home/mediacms.io/mediacms/" in _service_block(dev, service)\n\n    migrations = _service_block(dev, "migrations")\n    assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static" in migrations\n    assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro" not in migrations\n\n    for service in ("web", "celery_worker", "celery_beat"):\n        assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro" in _service_block(dev, service)\n\n    for service, source_mount in (\n        ("deposit_service", "./deposit_service/app:/app/app"),\n        ("dfx_signer_service", "./sweeper_service/app:/app/app"),\n        ("sweeper_service", "./sweeper_service/app:/app/app"),\n    ):\n        assert source_mount in _service_block(dev, service)\n\n    assert not (ROOT / "docker-compose-dev-live.yaml").exists()\n'''
    new = '''def test_default_dev_compose_keeps_live_backend_sources_and_generated_static_out_of_mount_contract():\n    dev = _read("docker-compose-dev.yaml")\n\n    for service in ("migrations", "web", "celery_worker", "celery_beat"):\n        block = _service_block(dev, service)\n        assert "- ./:/home/mediacms.io/mediacms/" in block\n        assert "MEDIACMS_STATIC_DIR" not in block\n\n    for service, source_mount in (\n        ("deposit_service", "./deposit_service/app:/app/app"),\n        ("dfx_signer_service", "./sweeper_service/app:/app/app"),\n        ("sweeper_service", "./sweeper_service/app:/app/app"),\n    ):\n        assert source_mount in _service_block(dev, service)\n\n    assert not (ROOT / "docker-compose-dev-live.yaml").exists()\n'''
    if old in text and new not in text:
        text = text.replace(old, new, 1)
    elif new in text and old not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] dev mount test anchor changed")

    old = '''def test_dev_frontend_reuses_dependencies_but_ci_stays_lockfile_strict():\n    dev = _read("docker-compose-dev.yaml")\n    entrypoint = _read("frontend/dev-entrypoint.sh")\n    ci = _read(".github/workflows/ci.yml")\n\n    frontend = _service_block(dev, "frontend")\n    assert "command: bash ./dev-entrypoint.sh" in frontend\n    assert "frontend_node_modules:/home/mediacms.io/mediacms/frontend/node_modules" in frontend\n    assert "frontend_npm_cache:/root/.npm" in frontend\n    assert "npm ci --prefer-offline --no-audit --no-fund" in entrypoint\n    assert "package-lock.json" in entrypoint\n    assert "packages/scripts" in entrypoint\n    assert "packages/player" in entrypoint\n    assert "exec npm run start" in entrypoint\n    assert "run: npm ci --no-audit --no-fund" in ci\n'''
    new = '''def test_dev_frontend_dependencies_are_built_before_container_start():\n    dev = _read("docker-compose-dev.yaml")\n    dockerfile = _read("frontend/Dockerfile.dev")\n    ci = _read(".github/workflows/ci.yml")\n\n    frontend = _service_block(dev, "frontend")\n    assert "context: ./frontend" in frontend\n    assert "dockerfile: Dockerfile.dev" in frontend\n    assert "command: npm run start" in frontend\n    assert "frontend:/home/mediacms.io/mediacms/frontend/" not in frontend\n    assert "/frontend/node_modules" not in frontend\n    for mounted in ("src", "config", "packages", "dist"):\n        assert f"frontend/{mounted}:/home/mediacms.io/mediacms/frontend/{mounted}" in frontend\n    assert "RUN npm ci --no-audit --no-fund" in dockerfile\n    assert "node_modules/.bin/mediacms-scripts" in dockerfile\n    assert not (ROOT / "frontend/dev-entrypoint.sh").exists()\n    assert "run: npm ci --no-audit --no-fund" in ci\n'''
    if old in text and new not in text:
        text = text.replace(old, new, 1)
    elif new in text and old not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] dev frontend test anchor changed")

    old = '''def test_collectstatic_runs_in_dev_with_separate_source_and_destination():\n    prestart = _read("deploy/docker/prestart.sh")\n    dev = _read("docker-compose-dev.yaml")\n    dev_settings = _read("cms/dev_settings.py")\n\n    assert 'echo "RUNNING COLLECTSTATIC"' in prestart\n    assert "python manage.py collectstatic --noinput" in prestart\n    assert "ENABLE_COLLECTSTATIC" not in prestart\n    assert "ENABLE_COLLECTSTATIC" not in _service_block(dev, "migrations")\n    assert "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static'),)" in dev_settings\n    assert "STATIC_ROOT = os.path.join(BASE_DIR, 'static_collected')" in dev_settings\n\n    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):\n        compose = _read(compose_path)\n        migrations = _service_block(compose, "migrations")\n        assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static" in migrations\n        assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro" not in migrations\n\n        for service in ("web", "celery_worker", "celery_beat"):\n            runtime = _service_block(compose, service)\n            assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro" in runtime\n'''
    new = '''def test_collectstatic_has_distinct_versioned_source_and_generated_outputs():\n    prestart = _read("deploy/docker/prestart.sh")\n    dev = _read("docker-compose-dev.yaml")\n    settings_py = _read("cms/settings.py")\n    dev_settings = _read("cms/dev_settings.py")\n    gitignore = _read(".gitignore")\n    dockerignore = _read(".dockerignore")\n\n    assert 'echo "RUNNING COLLECTSTATIC"' in prestart\n    assert "python manage.py collectstatic --noinput" in prestart\n    assert "ENABLE_COLLECTSTATIC" not in prestart\n    assert "ENABLE_COLLECTSTATIC" not in _service_block(dev, "migrations")\n    assert 'STATICFILES_DIRS = [os.path.join(BASE_DIR, "static_src")]' in settings_py\n    assert "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static_src'),)" in dev_settings\n    assert "STATIC_ROOT = os.path.join(BASE_DIR, 'static_collected')" in dev_settings\n    assert "/static/" in gitignore\n    assert "/static/" in dockerignore\n\n    tracked_output = subprocess.run(\n        ["git", "ls-files", "--", "static"], cwd=ROOT, text=True, capture_output=True, check=True\n    ).stdout.splitlines()\n    assert not [rel for rel in tracked_output if (ROOT / rel).exists()]\n    assert (ROOT / "static_src/ads/ads.css").is_file()\n    assert not (ROOT / "static_src/vendor").exists()\n\n    for compose_path in ("docker-compose-cloudflare.yaml", "docker-compose.yaml"):\n        compose = _read(compose_path)\n        migrations = _service_block(compose, "migrations")\n        assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static" in migrations\n        assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro" not in migrations\n\n        for service in ("web", "celery_worker", "celery_beat"):\n            runtime = _service_block(compose, service)\n            assert "${MEDIACMS_STATIC_DIR:-./static}:/home/mediacms.io/mediacms/static:ro" in runtime\n'''
    if old in text and new not in text:
        text = text.replace(old, new, 1)
    elif new in text and old not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] collectstatic test anchor changed")

    old = '''def test_redundant_nested_static_tree_is_removed():\n    assert not (ROOT / "static/static").exists()\n    for rel in (\n        "static/images/social-media-icons/reddit.svg",\n        "static/images/social-media-icons/telegram.svg",\n        "static/images/social-media-icons/vk.svg",\n        "static/images/social-media-icons/whatsapp.svg",\n        "static/images/social-media-icons/x.svg",\n        "static/images/wallet/cf-token.png",\n    ):\n        assert (ROOT / rel).is_file()\n'''
    new = '''def test_redundant_nested_static_tree_is_removed_from_canonical_source():\n    assert not (ROOT / "static_src/static").exists()\n    for rel in (\n        "static_src/images/social-media-icons/reddit.svg",\n        "static_src/images/social-media-icons/telegram.svg",\n        "static_src/images/social-media-icons/vk.svg",\n        "static_src/images/social-media-icons/whatsapp.svg",\n        "static_src/images/social-media-icons/x.svg",\n        "static_src/images/wallet/cf-token.png",\n    ):\n        assert (ROOT / rel).is_file()\n'''
    if old in text and new not in text:
        text = text.replace(old, new, 1)
    elif new in text and old not in text:
        pass
    else:
        raise SystemExit("[architecture-fix] nested static test anchor changed")

    write(rel, text.rstrip() + "\n")


def preflight_remaining_transitions() -> None:
    settings = read("cms/settings.py")
    if "STATICFILES_DIRS = [os.path.join(BASE_DIR, \"static_src\")]" not in settings and 'STATIC_ROOT = BASE_DIR + "/static/"' not in settings:
        raise SystemExit("[architecture-fix] cms/settings.py is neither fresh nor expected partial state")
    dev_settings = read("cms/dev_settings.py")
    if "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static_src'),)" not in dev_settings and "STATICFILES_DIRS = (os.path.join(BASE_DIR, 'static'),)" not in dev_settings:
        raise SystemExit("[architecture-fix] cms/dev_settings.py is neither fresh nor expected partial state")

    prestart = read("deploy/docker/prestart.sh")
    if 'echo "RUNNING COLLECTSTATIC"' not in prestart or "python manage.py collectstatic --noinput" not in prestart:
        raise SystemExit("[architecture-fix] prestart.sh lost collectstatic")

    dev = read("docker-compose-dev.yaml")
    if not ("command: bash ./dev-entrypoint.sh" in dev or ("dockerfile: Dockerfile.dev" in dev and "command: npm run start" in dev)):
        raise SystemExit("[architecture-fix] docker-compose-dev frontend is neither fresh nor expected patched state")

    common = read("deploy/scripts/rolling_update_common.sh")
    if not ("changed_matches '^static/'" in common or "changed_matches '^static_src/'" in common):
        raise SystemExit("[architecture-fix] rolling static classifier is neither fresh nor patched")
    if not ("npm ci --no-audit --no-fund && npm run dist" in common or '"${FRONTEND_COMPOSE[@]}" build frontend' in common):
        raise SystemExit("[architecture-fix] rolling frontend builder is neither fresh nor patched")

    redis = read("deploy/scripts/prod_migrate_redis_persistence.sh")
    if not ('cp -a static/. "$tmp_static/"' in redis or "static_src/" in redis):
        raise SystemExit("[architecture-fix] redis static bootstrap is neither fresh nor patched")

    tests = read("tests/test_rolling_update_safety.py")
    groups = (
        ("def test_frontend_build_is_one_shot_reproducible_and_release_scoped():", "def test_frontend_build_is_image_verified_reproducible_and_release_scoped():"),
        ("def test_default_dev_compose_keeps_live_source_mounts_without_weakening_static_mounts():", "def test_default_dev_compose_keeps_live_backend_sources_and_generated_static_out_of_mount_contract():"),
        ("def test_dev_frontend_reuses_dependencies_but_ci_stays_lockfile_strict():", "def test_dev_frontend_dependencies_are_built_before_container_start():"),
        ("def test_collectstatic_runs_in_dev_with_separate_source_and_destination():", "def test_collectstatic_has_distinct_versioned_source_and_generated_outputs():"),
    )
    for alternatives in groups:
        if not any(marker in tests for marker in alternatives):
            raise SystemExit(f"[architecture-fix] tests are neither fresh nor patched around {alternatives[0]}")
    print("[architecture-fix] preflight of fresh/partial states OK")


def validate() -> None:
    for rel in (
        "deploy/docker/prestart.sh",
        "deploy/scripts/rolling_update_common.sh",
        "deploy/scripts/prod_migrate_redis_persistence.sh",
    ):
        run(["bash", "-n", rel])

    for rel in (
        "cms/settings.py",
        "cms/dev_settings.py",
        "tests/test_rolling_update_safety.py",
    ):
        run(["python3", "-m", "py_compile", rel])

    yaml_probe = run(
        [
            "python3",
            "-c",
            "import pathlib,yaml; yaml.safe_load(pathlib.Path('docker-compose-dev.yaml').read_text())",
        ],
        check=False,
    )
    if yaml_probe.returncode and "No module named 'yaml'" not in yaml_probe.stderr:
        raise SystemExit(yaml_probe.stderr)

    # Before commit, git ls-files still lists deleted/moved old paths from the
    # index. What matters is that none of those tracked static paths still
    # exists in the working tree and canonical static_src files do exist.
    old_index_paths = [rel for rel in git("ls-files", "--", "static").splitlines() if rel]
    remaining = [rel for rel in old_index_paths if (ROOT / rel).exists()]
    if remaining:
        raise SystemExit(
            "[architecture-fix] old tracked static paths still exist: " + ", ".join(remaining[:10])
        )
    if not (ROOT / "static_src").is_dir():
        raise SystemExit("[architecture-fix] no canonical static_src directory was created")

    run(["git", "diff", "--check"])
    print("[architecture-fix] syntax/YAML/static-layout/diff validation OK")


def main() -> None:
    verify_checkout()
    preflight_remaining_transitions()
    separate_static_source_and_output()
    patch_static_settings()
    patch_ignores()
    restore_collectstatic()
    patch_dev_compose()
    create_frontend_image()
    patch_rolling_static_and_frontend()
    patch_redis_bootstrap()
    patch_tests()
    validate()

    print("[architecture-fix] applied successfully")
    print("[architecture-fix] run:")
    print("  pytest -q tests/test_rolling_update_safety.py")
    print("  sudo docker compose -f docker-compose-dev.yaml build frontend")
    print("  sudo docker compose -f docker-compose-dev.yaml up")
    print("  sudo docker compose -f docker-compose-dev.yaml exec -T frontend npm run dist")


if __name__ == "__main__":
    main()
