# Generated for DB-backed wallet referrals.

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("users", "0005_alter_user_dfans_url"),
    ]

    operations = [
        migrations.AddField(
            model_name="user",
            name="referral_code",
            field=models.CharField(
                blank=True,
                db_index=True,
                max_length=32,
                null=True,
                unique=True,
            ),
        ),
        migrations.AddField(
            model_name="user",
            name="referred_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="referred_users",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
    ]
