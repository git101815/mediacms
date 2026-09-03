#!/usr/bin/env python3
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

EXPECTED_HEAD = "b994162d70790b98947eea2f12e449a289f9532d"
GATE_SOURCE_COMMIT = "70b18abe81fe6cfbfad1eed68d391005006c8e66"
GATE_PATH = "static/js/cdn-gate-hold.js"
TRACKED_TARGETS = (
    ".gitignore",
    "docker-compose-dev.yaml",
    "frontend/Dockerfile.dev",
    "frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts",
    "frontend/packages/scripts/dist/webpack-dev-env.js",
    "tests/test_rolling_update_safety.py",
)


def locate_root() -> Path:
    p = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return Path(p.stdout.strip()).resolve()


ROOT = locate_root()


def run(args, *, check=True, text=True):
    return subprocess.run(
        args,
        cwd=ROOT,
        check=check,
        text=text,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def git(*args, check=True):
    return run(["git", *args], check=check).stdout.strip()


def replace_bytes(rel: str, old: bytes, new: bytes, label: str) -> None:
    path = ROOT / rel
    data = path.read_bytes()
    old_count = data.count(old)
    new_count = data.count(new)
    if old_count == 1 and new_count == 0:
        path.write_bytes(data.replace(old, new, 1))
        print(f"[frontend-blank-fix] patched {label}")
        return
    if old_count == 0 and new_count == 1:
        print(f"[frontend-blank-fix] already patched {label}")
        return
    raise SystemExit(
        f"[frontend-blank-fix] {rel}: unsafe anchor for {label}; "
        f"old={old_count}, new={new_count}"
    )


def replace_text(rel: str, old: str, new: str, label: str) -> None:
    replace_bytes(rel, old.encode("utf-8"), new.encode("utf-8"), label)


def historical_gate_source() -> bytes:
    result = run(
        ["git", "show", f"{GATE_SOURCE_COMMIT}:{GATE_PATH}"],
        text=False,
        check=False,
    )
    if result.returncode != 0 or not result.stdout:
        raise SystemExit(
            f"[frontend-blank-fix] cannot recover {GATE_PATH} from {GATE_SOURCE_COMMIT}"
        )
    return result.stdout


def verify_checkout() -> None:
    head = git("rev-parse", "HEAD")
    print(f"[frontend-blank-fix] repo: {ROOT}")
    print(f"[frontend-blank-fix] HEAD: {head}")
    if head != EXPECTED_HEAD:
        raise SystemExit(
            f"[frontend-blank-fix] expected HEAD {EXPECTED_HEAD}, got {head}. "
            "Refusing to patch a different revision."
        )

    dirty = []
    for rel in TRACKED_TARGETS:
        result = run(["git", "diff", "--quiet", "HEAD", "--", rel], check=False)
        if result.returncode != 0:
            dirty.append(rel)
    if dirty:
        raise SystemExit(
            "[frontend-blank-fix] targeted tracked files already have local changes; "
            "refusing to overwrite them:\n  " + "\n  ".join(dirty)
        )

    gate = ROOT / GATE_PATH
    expected_gate = historical_gate_source()
    if gate.exists() and (not gate.is_file() or gate.read_bytes() != expected_gate):
        raise SystemExit(
            f"[frontend-blank-fix] {GATE_PATH} already exists with unknown content; refusing overwrite"
        )


def patch_dockerfile() -> None:
    replace_text(
        "frontend/Dockerfile.dev",
        "# Source changes invalidate compilation, not dependency installation.\n"
        "COPY . .\n"
        "RUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist\n",
        "# Source changes invalidate dependency-independent image layers. Dist is\n"
        "# compiled only from a Compose container where frontend/.env is mounted.\n"
        "COPY . .\n"
        "RUN test -x ./node_modules/.bin/mediacms-scripts\n",
        "Dockerfile: do not compile dist before frontend/.env exists",
    )


def patch_compose() -> None:
    replace_text(
        "docker-compose-dev.yaml",
        "      - ${PWD}/frontend/.babelrc:/home/mediacms.io/mediacms/frontend/.babelrc:ro\n"
        "      - ${PWD}/frontend/tsconfig.json:/home/mediacms.io/mediacms/frontend/tsconfig.json:ro\n",
        "      - ${PWD}/frontend/.babelrc:/home/mediacms.io/mediacms/frontend/.babelrc:ro\n"
        "      - ${PWD}/frontend/tsconfig.json:/home/mediacms.io/mediacms/frontend/tsconfig.json:ro\n"
        "      - ${PWD}/frontend/.env:/home/mediacms.io/mediacms/frontend/.env:ro\n",
        "Compose: mount physical frontend/.env read-only",
    )


def patch_webpack_config() -> None:
    old_loader = b"var dotenv = require('dotenv').config({ path: path.resolve(__dirname + '../../../../.env') });\r\n"
    source_loader = (
        b"var frontendEnvPath = path.resolve(process.cwd(), '.env');\n"
        b"var dotenvResult = require('dotenv').config({ path: frontendEnvPath });\n"
        b"if (dotenvResult.error) {\n"
        b"\tthrow new Error('MediaCMS frontend build requires ' + frontendEnvPath + ': ' + dotenvResult.error.message);\n"
        b"}\n"
        b"var frontendEnv = dotenvResult.parsed || {};\n"
    )
    dist_loader = (
        b"var frontendEnvPath = path.resolve(process.cwd(), '.env');\n"
        b"var dotenvResult = require('dotenv').config({ path: frontendEnvPath });\n"
        b"if (dotenvResult.error) {\n"
        b"    throw new Error('MediaCMS frontend build requires ' + frontendEnvPath + ': ' + dotenvResult.error.message);\n"
        b"}\n"
        b"var frontendEnv = dotenvResult.parsed || {};\n"
    )

    replace_bytes(
        "frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts",
        old_loader,
        source_loader,
        "webpack source: fail closed on missing .env",
    )
    replace_bytes(
        "frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts",
        b'\t\tnew DefinePlugin({ "process.env": JSON.stringify(dotenv.parsed) }),\r\n',
        b'\t\tnew DefinePlugin({ "process.env": JSON.stringify(frontendEnv) }),\n',
        "webpack source: process.env cannot become undefined",
    )
    replace_bytes(
        "frontend/packages/scripts/dist/webpack-dev-env.js",
        old_loader,
        dist_loader,
        "webpack dist: fail closed on missing .env",
    )
    replace_bytes(
        "frontend/packages/scripts/dist/webpack-dev-env.js",
        b'        new DefinePlugin({ "process.env": JSON.stringify(dotenv.parsed) }),\r\n',
        b'        new DefinePlugin({ "process.env": JSON.stringify(frontendEnv) }),\n',
        "webpack dist: process.env cannot become undefined",
    )


def patch_gitignore_and_restore_gate() -> None:
    marker = (
        "# Keep the age-gate source versioned while legacy generated static/js stays ignored.\n"
        "!static/js/\n"
        "static/js/*\n"
        "!static/js/cdn-gate-hold.js\n"
    )
    path = ROOT / ".gitignore"
    text = path.read_text(encoding="utf-8")
    if marker not in text:
        if not text.endswith("\n"):
            text += "\n"
        path.write_text(text + marker, encoding="utf-8")
        print("[frontend-blank-fix] patched .gitignore gate exception")
    else:
        print("[frontend-blank-fix] already patched .gitignore gate exception")

    gate = ROOT / GATE_PATH
    gate.parent.mkdir(parents=True, exist_ok=True)
    expected = historical_gate_source()
    if not gate.exists():
        gate.write_bytes(expected)
        print(f"[frontend-blank-fix] restored historical {GATE_PATH}")
    else:
        print(f"[frontend-blank-fix] already restored {GATE_PATH}")


def patch_tests() -> None:
    replace_text(
        "tests/test_rolling_update_safety.py",
        '    assert "RUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist" in dockerfile\n',
        '    assert "RUN test -x ./node_modules/.bin/mediacms-scripts" in dockerfile\n'
        '    assert "RUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist" not in dockerfile\n',
        "test: dist compilation happens only in Compose run",
    )

    anchor = "def test_every_application_release_rebuilds_frontend_for_fresh_static_snapshot():\n"
    addition = """def test_frontend_build_has_physical_env_without_baking_it_into_image():
    compose = _read("docker-compose-dev.yaml")
    frontend = _service_block(compose, "frontend")
    dockerfile = _read("frontend/Dockerfile.dev")
    dockerignore = _read("frontend/.dockerignore")

    assert "frontend/.env:/home/mediacms.io/mediacms/frontend/.env:ro" in frontend
    assert "env_file:" in frontend
    assert "${PWD}/frontend/.env" in frontend
    assert ".env" in dockerignore.splitlines()
    assert "COPY .env" not in dockerfile
    assert "RUN npm run dist" not in dockerfile


def test_frontend_webpack_env_is_fail_closed_and_never_undefined():
    for rel in (
        "frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts",
        "frontend/packages/scripts/dist/webpack-dev-env.js",
    ):
        config = _read(rel)
        assert "path.resolve(process.cwd(), '.env')" in config
        assert "if (dotenvResult.error)" in config
        assert "MediaCMS frontend build requires" in config
        assert "var frontendEnv = dotenvResult.parsed || {};" in config
        assert 'JSON.stringify(frontendEnv)' in config
        assert 'JSON.stringify(dotenv.parsed)' not in config


def test_age_gate_static_source_is_versioned_and_not_gitignored():
    template = _read("templates/age_verification.html")
    ignore = _read(".gitignore")
    gate = ROOT / "static/js/cdn-gate-hold.js"

    assert 'src="/static/js/cdn-gate-hold.js"' in template
    assert gate.is_file()
    gate_text = gate.read_text(encoding="utf-8")
    assert "window.mcGateHoldStart = start;" in gate_text
    assert "window.mcGateRelease = release;" in gate_text
    assert "!static/js/" in ignore
    assert "static/js/*" in ignore
    assert "!static/js/cdn-gate-hold.js" in ignore


"""
    replace_text(
        "tests/test_rolling_update_safety.py",
        anchor,
        addition + anchor,
        "tests: frontend env and age-gate regressions",
    )


def validate() -> None:
    compose = (ROOT / "docker-compose-dev.yaml").read_text(encoding="utf-8")
    dockerfile = (ROOT / "frontend/Dockerfile.dev").read_text(encoding="utf-8")
    dockerignore = (ROOT / "frontend/.dockerignore").read_text(encoding="utf-8")
    tests = (ROOT / "tests/test_rolling_update_safety.py").read_text(encoding="utf-8")
    template = (ROOT / "templates/age_verification.html").read_text(encoding="utf-8")
    gate = ROOT / GATE_PATH

    if "frontend/.env:/home/mediacms.io/mediacms/frontend/.env:ro" not in compose:
        raise SystemExit("[frontend-blank-fix] frontend/.env is still absent from container filesystem")
    if "RUN test -x ./node_modules/.bin/mediacms-scripts && npm run dist" in dockerfile:
        raise SystemExit("[frontend-blank-fix] image build still compiles frontend without mounted .env")
    if ".env" not in dockerignore.splitlines():
        raise SystemExit("[frontend-blank-fix] frontend/.env must remain excluded from image build context")

    for rel in (
        "frontend/packages/scripts/lib/webpack-helpers/generateConfig.ts",
        "frontend/packages/scripts/dist/webpack-dev-env.js",
    ):
        config = (ROOT / rel).read_text(encoding="utf-8")
        if "path.resolve(process.cwd(), '.env')" not in config:
            raise SystemExit(f"[frontend-blank-fix] {rel} does not resolve frontend/.env from build cwd")
        if "if (dotenvResult.error)" not in config:
            raise SystemExit(f"[frontend-blank-fix] {rel} does not fail closed on missing .env")
        if 'JSON.stringify(dotenv.parsed)' in config:
            raise SystemExit(f"[frontend-blank-fix] {rel} can still define process.env as undefined")
        if 'JSON.stringify(frontendEnv)' not in config:
            raise SystemExit(f"[frontend-blank-fix] {rel} missing safe DefinePlugin env")

    if not gate.is_file() or gate.read_bytes() != historical_gate_source():
        raise SystemExit("[frontend-blank-fix] restored age gate differs from historical tracked source")
    if 'src="/static/js/cdn-gate-hold.js"' not in template:
        raise SystemExit("[frontend-blank-fix] age template no longer references the gate")

    ignored = run(["git", "check-ignore", "-q", GATE_PATH], check=False)
    if ignored.returncode == 0:
        raise SystemExit(f"[frontend-blank-fix] {GATE_PATH} is still ignored by git")

    compile(tests, "tests/test_rolling_update_safety.py", "exec")
    if shutil.which("node"):
        run(["node", "--check", "frontend/packages/scripts/dist/webpack-dev-env.js"])
        run(["node", "--check", GATE_PATH])
    run(["git", "diff", "--check"])
    print("[frontend-blank-fix] syntax, gitignore and diff validation OK")


def main() -> None:
    verify_checkout()
    patch_dockerfile()
    patch_compose()
    patch_webpack_config()
    patch_gitignore_and_restore_gate()
    patch_tests()
    validate()

    print("[frontend-blank-fix] applied successfully")
    print("[frontend-blank-fix] next checks:")
    print("  pytest -q tests/test_rolling_update_safety.py")
    print("  sudo docker compose -f docker-compose-dev.yaml build frontend")
    print("  sudo docker compose -f docker-compose-dev.yaml up -d --no-deps --force-recreate frontend")
    print("  sudo docker compose -f docker-compose-dev.yaml exec -T frontend test -f /home/mediacms.io/mediacms/frontend/.env")
    print("  sudo docker compose -f docker-compose-dev.yaml exec -T frontend npm run dist")
    print("  git status --short")
    print("  git diff --check")


if __name__ == "__main__":
    main()
