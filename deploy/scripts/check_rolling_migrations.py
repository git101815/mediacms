"""Refuse pending Django migrations that are not clearly rolling-safe.

The checker intentionally has a small allow-list. Ambiguous operations are not
called unsafe forever; they are called unsafe *for unattended rolling deploys*.
They require explicit review or an expand/contract release sequence.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# This checker is intentionally executed as a file inside the isolated
# migrations image:
#
#   python deploy/scripts/check_rolling_migrations.py
#
# In that invocation Python puts deploy/scripts/, not the repository root, at
# sys.path[0]. Make the application package importable explicitly instead of
# relying on a bind mount, the caller's cwd, or an ambient PYTHONPATH.
REPO_ROOT = Path(__file__).resolve().parents[2]
repo_root_str = str(REPO_ROOT)
if repo_root_str not in sys.path:
    sys.path.insert(0, repo_root_str)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "cms.settings")

import django

django.setup()

from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.executor import MigrationExecutor
from django.db.models import NOT_PROVIDED


SAFE_STATE_ONLY = {
    "AlterModelOptions",
    "AlterModelManagers",
}


def _safe_add_field(operation):
    field = operation.field
    # The automatic path is deliberately limited to PostgreSQL's simple
    # ADD COLUMN NULL-without-default shape. Indexes, unique constraints,
    # foreign keys and defaults can introduce validation/table locks or
    # rewrite behavior and therefore require review.
    if not bool(getattr(field, "null", False)):
        return False, "new field is not nullable"
    if bool(getattr(field, "unique", False)):
        return False, "new field is unique"
    if bool(getattr(field, "db_index", False)):
        return False, "new field requests an index"
    if getattr(field, "remote_field", None) is not None:
        return False, "new field creates a relation/constraint"
    if field.has_default():
        return False, "new field has a Python default"
    if getattr(field, "db_default", NOT_PROVIDED) is not NOT_PROVIDED:
        return False, "new field has a database default"
    return True, "nullable column without default/index/constraint"


def classify_operation(operation):
    name = type(operation).__name__
    if name == "CreateModel":
        return True, "creates a new table"
    if name == "AddField":
        return _safe_add_field(operation)
    if name == "AddIndexConcurrently":
        return True, "concurrent PostgreSQL index creation"
    if name in SAFE_STATE_ONLY:
        return True, "Django state-only metadata operation"
    if name == "SeparateDatabaseAndState":
        for nested in operation.database_operations:
            safe, reason = classify_operation(nested)
            if not safe:
                return False, f"database operation {type(nested).__name__}: {reason}"
        return True, "state/database split with safe database operations"
    return False, f"{name} is not on the unattended rolling allow-list"


def main() -> int:
    connection = connections[DEFAULT_DB_ALIAS]
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    plan = executor.migration_plan(targets)

    if not plan:
        print("rolling-migrations: no pending migrations")
        return 0

    unsafe = []
    print(f"rolling-migrations: {len(plan)} pending migration(s)")
    for migration, backwards in plan:
        label = f"{migration.app_label}.{migration.name}"
        if backwards:
            unsafe.append((label, "backwards migration required"))
            print(f"  REVIEW {label}: backwards migration required")
            continue

        if not migration.operations:
            print(f"  SAFE   {label}: no operations")
            continue

        migration_safe = True
        reasons = []
        for operation in migration.operations:
            safe, reason = classify_operation(operation)
            reasons.append(f"{type(operation).__name__}: {reason}")
            if not safe:
                migration_safe = False
        if migration_safe:
            print(f"  SAFE   {label}: " + "; ".join(reasons))
        else:
            reason = "; ".join(reasons)
            unsafe.append((label, reason))
            print(f"  REVIEW {label}: {reason}")

    if unsafe:
        print("\nRefusing unattended rolling migration:", file=sys.stderr)
        for label, reason in unsafe:
            print(f"  - {label}: {reason}", file=sys.stderr)
        print(
            "Use expand/contract, or after an explicit human review rerun with "
            "ALLOW_REVIEWED_ROLLING_MIGRATIONS=1.",
            file=sys.stderr,
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
