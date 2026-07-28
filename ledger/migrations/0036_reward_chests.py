# Generated for generic wallet Reward Chests.

import uuid

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ledger", "0035_daily_rewards"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="RewardChestGrant",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "public_id",
                    models.UUIDField(
                        db_index=True,
                        default=uuid.uuid4,
                        editable=False,
                        unique=True,
                    ),
                ),
                ("chest_key", models.CharField(db_index=True, max_length=64)),
                ("source_type", models.CharField(db_index=True, max_length=32)),
                ("source_ref", models.CharField(max_length=160)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending", "Pending"),
                            ("opened", "Opened"),
                            ("revoked", "Revoked"),
                        ],
                        db_index=True,
                        default="pending",
                        max_length=16,
                    ),
                ),
                ("config_version", models.PositiveIntegerField()),
                ("config_snapshot", models.JSONField(default=dict)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("roll", models.PositiveSmallIntegerField(blank=True, null=True)),
                ("drop_key", models.CharField(blank=True, default="", max_length=64)),
                ("drop_label", models.CharField(blank=True, default="", max_length=80)),
                ("rarity", models.CharField(blank=True, default="", max_length=64)),
                ("chance_bps", models.PositiveIntegerField(blank=True, null=True)),
                ("amount", models.BigIntegerField(blank=True, null=True)),
                ("expires_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("granted_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("opened_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("revoked_at", models.DateTimeField(blank=True, null=True)),
                (
                    "ledger_txn",
                    models.OneToOneField(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="reward_chest_grant",
                        to="ledger.ledgertransaction",
                    ),
                ),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="reward_chest_grants",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={"ordering": ["-granted_at", "-id"]},
        ),
        migrations.AddConstraint(
            model_name="rewardchestgrant",
            constraint=models.UniqueConstraint(
                fields=("source_type", "source_ref"),
                name="unique_reward_chest_source_ref",
            ),
        ),
        migrations.AddConstraint(
            model_name="rewardchestgrant",
            constraint=models.CheckConstraint(
                condition=models.Q(("amount__isnull", True), ("amount__gt", 0), _connector="OR"),
                name="reward_chest_amount_positive_if_set",
            ),
        ),
        migrations.AddConstraint(
            model_name="rewardchestgrant",
            constraint=models.CheckConstraint(
                condition=models.Q(
                    ("roll__isnull", True),
                    models.Q(("roll__gte", 0), ("roll__lt", 10000)),
                    _connector="OR",
                ),
                name="reward_chest_roll_in_range",
            ),
        ),
        migrations.AddConstraint(
            model_name="rewardchestgrant",
            constraint=models.CheckConstraint(
                condition=models.Q(
                    ("chance_bps__isnull", True),
                    models.Q(("chance_bps__gt", 0), ("chance_bps__lte", 10000)),
                    _connector="OR",
                ),
                name="reward_chest_chance_in_range",
            ),
        ),
        migrations.AddIndex(
            model_name="rewardchestgrant",
            index=models.Index(
                fields=["user", "status", "-granted_at"],
                name="ledger_chest_user_status_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="rewardchestgrant",
            index=models.Index(
                fields=["chest_key", "status"],
                name="ledger_chest_key_status_idx",
            ),
        ),
        migrations.AddField(
            model_name="dailyrewardclaim",
            name="reward_chest_grant",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="daily_reward_claim",
                to="ledger.rewardchestgrant",
            ),
        ),
    ]
