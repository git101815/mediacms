from django.db import migrations


def forwards(apps, schema_editor):
    EncodeProfile = apps.get_model("files", "EncodeProfile")

    EncodeProfile.objects.filter(codec="h265").update(active=False)
    EncodeProfile.objects.filter(codec="vp9").update(active=False)

    for name, resolution in (
        ("av1-480", 480),
        ("av1-720", 720),
        ("av1-1080", 1080),
    ):
        EncodeProfile.objects.update_or_create(
            name=name,
            defaults={
                "extension": "mp4",
                "resolution": resolution,
                "codec": "av1",
                "active": True,
                "description": "",
            },
        )


def backwards(apps, schema_editor):
    EncodeProfile = apps.get_model("files", "EncodeProfile")
    EncodeProfile.objects.filter(codec="av1").update(active=False)


class Migration(migrations.Migration):
    dependencies = [
        ("files", "0015_media_hls_av1_file_alter_encodeprofile_codec"),
    ]

    operations = [
        migrations.RunPython(forwards, backwards),
    ]