# Generated for configurable rotating share quests.

import uuid

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("ledger", "0036_reward_chests"),
        ("files", "0018_dailyvideouploadquota"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="QuestOwnerIdentity",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cycle_key", models.CharField(db_index=True, max_length=10)),
                ("network_hash", models.CharField(max_length=64)),
                ("fingerprint_hash", models.CharField(max_length=64)),
                ("visitor_hash", models.CharField(max_length=64)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="quest_owner_identities", to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name="QuestShareCampaign",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("public_id", models.UUIDField(db_index=True, default=uuid.uuid4, editable=False, unique=True)),
                ("campaign_key", models.CharField(max_length=64, unique=True)),
                ("cycle_key", models.CharField(db_index=True, max_length=10)),
                ("quest_key", models.CharField(db_index=True, max_length=64)),
                ("campaign_type", models.CharField(choices=[("site", "Site"), ("video", "Video")], db_index=True, max_length=16)),
                ("expected_platform", models.CharField(blank=True, default="", max_length=32)),
                ("target_path", models.CharField(max_length=500)),
                ("preview_seen_at", models.DateTimeField(blank=True, db_index=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("media", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name="quest_share_campaigns", to="files.media")),
                ("owner", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="quest_share_campaigns", to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name="QuestQualifiedVisit",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("cycle_key", models.CharField(db_index=True, max_length=10)),
                ("visitor_hash", models.CharField(max_length=64)),
                ("network_hash", models.CharField(max_length=64)),
                ("fingerprint_hash", models.CharField(max_length=64)),
                ("landing_page", models.CharField(max_length=500)),
                ("second_page", models.CharField(blank=True, default="", max_length=500)),
                ("referer_host", models.CharField(blank=True, default="", max_length=255)),
                ("qualification_type", models.CharField(choices=[("site_second_page", "Site second page"), ("video_platform", "Video platform")], db_index=True, max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True, db_index=True)),
                ("campaign", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="qualified_visits", to="ledger.questsharecampaign")),
            ],
        ),
        migrations.AddIndex(
            model_name="questowneridentity",
            index=models.Index(fields=["user", "cycle_key"], name="quest_owner_cycle_idx"),
        ),
        migrations.AddConstraint(
            model_name="questowneridentity",
            constraint=models.UniqueConstraint(fields=("user", "cycle_key", "network_hash", "fingerprint_hash", "visitor_hash"), name="quest_owner_identity_unique"),
        ),
        migrations.AddIndex(
            model_name="questsharecampaign",
            index=models.Index(fields=["owner", "cycle_key", "quest_key"], name="quest_campaign_owner_idx"),
        ),
        migrations.AddIndex(
            model_name="questqualifiedvisit",
            index=models.Index(fields=["cycle_key", "qualification_type"], name="quest_visit_cycle_type_idx"),
        ),
        migrations.AddIndex(
            model_name="questqualifiedvisit",
            index=models.Index(fields=["campaign", "created_at"], name="quest_visit_campaign_idx"),
        ),
        migrations.AddConstraint(
            model_name="questqualifiedvisit",
            constraint=models.UniqueConstraint(fields=("cycle_key", "visitor_hash"), name="quest_visit_cycle_visitor_unique"),
        ),
        migrations.AddConstraint(
            model_name="questqualifiedvisit",
            constraint=models.UniqueConstraint(fields=("cycle_key", "network_hash"), name="quest_visit_cycle_network_unique"),
        ),
        migrations.AddConstraint(
            model_name="questqualifiedvisit",
            constraint=models.UniqueConstraint(fields=("cycle_key", "fingerprint_hash"), name="quest_visit_cycle_fp_unique"),
        ),
    ]
