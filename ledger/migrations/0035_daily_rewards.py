# Generated for wallet dashboard daily rewards.

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ledger", "0034_remove_tokenpack_image"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="DailyRewardState",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("current_streak", models.PositiveIntegerField(default=0)),
                ("total_claims", models.PositiveIntegerField(default=0)),
                ("last_claim_date", models.DateField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("user", models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name="daily_reward_state", to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name="DailyRewardClaim",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("reward_date", models.DateField()),
                ("streak_day", models.PositiveIntegerField()),
                ("cycle_day", models.PositiveIntegerField()),
                ("amount", models.BigIntegerField()),
                ("config_version", models.PositiveIntegerField()),
                ("config_snapshot", models.JSONField(default=dict)),
                ("claimed_at", models.DateTimeField(auto_now_add=True)),
                ("ledger_txn", models.OneToOneField(on_delete=django.db.models.deletion.PROTECT, related_name="daily_reward_claim", to="ledger.ledgertransaction")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="daily_reward_claims", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-reward_date", "-id"]},
        ),
        migrations.AddConstraint(
            model_name="dailyrewardstate",
            constraint=models.CheckConstraint(condition=models.Q(("current_streak__gte", 0)), name="daily_reward_state_streak_nonnegative"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardstate",
            constraint=models.CheckConstraint(condition=models.Q(("total_claims__gte", 0)), name="daily_reward_state_claims_nonnegative"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.UniqueConstraint(fields=("user", "reward_date"), name="unique_daily_reward_claim_per_user_day"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.CheckConstraint(condition=models.Q(("streak_day__gt", 0)), name="daily_reward_claim_streak_positive"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.CheckConstraint(condition=models.Q(("cycle_day__gt", 0)), name="daily_reward_claim_cycle_positive"),
        ),
        migrations.AddConstraint(
            model_name="dailyrewardclaim",
            constraint=models.CheckConstraint(condition=models.Q(("amount__gt", 0)), name="daily_reward_claim_amount_positive"),
        ),
        migrations.AddIndex(
            model_name="dailyrewardclaim",
            index=models.Index(fields=["user", "-claimed_at"], name="ledger_daily_user_claim_idx"),
        ),
    ]
