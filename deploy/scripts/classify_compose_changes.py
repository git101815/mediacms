"""Report which top-level Compose service blocks changed since a deployed commit.

Output is intentionally tiny and shell-friendly:
  service:<name>
  top-level

This is a textual/structural classifier, not a YAML evaluator. Changes outside the
`services:` mapping are reported as `top-level` so the rolling updater can fail
closed instead of guessing about shared anchors, volumes, secrets, or project
identity.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

SERVICE_RE = re.compile(r"^  ([A-Za-z0-9_.-]+):(?:\s|$)")
TOP_RE = re.compile(r"^[A-Za-z0-9_.-]+:(?:\s|$)")


def _git_show(ref: str, path: str) -> str:
    repo_root = Path.cwd().resolve()
    proc = subprocess.run(
        ["git", "-c", f"safe.directory={repo_root}", "show", f"{ref}:{path}"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        raise SystemExit(
            f"compose-classifier: cannot read {path!r} at {ref}: {proc.stderr.strip()}"
        )
    return proc.stdout


def _split(text: str) -> tuple[dict[str, str], str]:
    lines = text.splitlines(keepends=True)
    services_start = None
    for index, line in enumerate(lines):
        if line.rstrip("\r\n") == "services:":
            services_start = index
            break
    if services_start is None:
        raise SystemExit("compose-classifier: compose file has no top-level services mapping")

    services: dict[str, list[str]] = {}
    top_lines: list[str] = lines[: services_start + 1]
    current: str | None = None
    in_services = True

    for line in lines[services_start + 1 :]:
        if TOP_RE.match(line) and not line.startswith(" "):
            current = None
            in_services = False
            top_lines.append(line)
            continue

        if in_services:
            match = SERVICE_RE.match(line)
            if match:
                current = match.group(1)
                services[current] = [line]
                continue
            if current is not None:
                services[current].append(line)
                continue

        top_lines.append(line)

    return {name: "".join(block) for name, block in services.items()}, "".join(top_lines)


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: classify_compose_changes.py BASE_SHA COMPOSE_FILE", file=sys.stderr)
        return 2

    base_sha, compose_file = argv[1:]
    old_services, old_top = _split(_git_show(base_sha, compose_file))
    new_services, new_top = _split(Path(compose_file).read_text(encoding="utf-8"))

    if old_top != new_top:
        print("top-level")

    for name in sorted(set(old_services) | set(new_services)):
        if old_services.get(name) != new_services.get(name):
            print(f"service:{name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
