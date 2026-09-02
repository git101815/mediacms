from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import models
from django.db.migrations.operations.fields import AddField, AlterField, RemoveField
from django.db.migrations.operations.models import CreateModel
from django.db.migrations.operations.special import RunPython, RunSQL, SeparateDatabaseAndState

from deploy.scripts.check_rolling_migrations import classify_operation


def _safe(op):
    return classify_operation(op)[0]


def test_nullable_add_field_without_side_effects_is_safe():
    assert _safe(AddField("thing", "note", models.TextField(null=True)))


def test_add_field_with_default_index_relation_or_not_null_requires_review():
    assert not _safe(AddField("thing", "required", models.TextField(null=False)))
    assert not _safe(AddField("thing", "defaulted", models.IntegerField(null=True, default=0)))
    assert not _safe(AddField("thing", "indexed", models.TextField(null=True, db_index=True)))
    assert not _safe(AddField("thing", "owner", models.ForeignKey("auth.User", null=True, on_delete=models.CASCADE)))


def test_destructive_or_code_operations_require_review():
    assert not _safe(RemoveField("thing", "old"))
    assert not _safe(AlterField("thing", "name", models.TextField(null=True)))
    assert not _safe(RunPython(lambda apps, schema_editor: None))
    assert not _safe(RunSQL("SELECT 1"))


def test_concurrent_index_and_safe_separate_database_state_are_safe():
    assert _safe(AddIndexConcurrently("thing", models.Index(fields=["name"], name="thing_name_idx")))
    op = SeparateDatabaseAndState(
        database_operations=[AddField("thing", "note", models.TextField(null=True))],
        state_operations=[],
    )
    assert _safe(op)


def test_create_model_is_safe_because_it_does_not_rewrite_existing_tables():
    assert _safe(CreateModel("NewThing", fields=[("id", models.BigAutoField(primary_key=True))]))
