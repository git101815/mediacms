import uuid
from datetime import date
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile

from files.models import DailyVideoUploadQuota, Media
from files.upload_limits import (
    DailyVideoUploadLimitReached,
    release_daily_video_upload,
    reserve_daily_video_upload,
)


QUOTA_DAY = date(2026, 7, 19)


@pytest.mark.django_db
def test_daily_video_upload_quota_blocks_at_configured_limit(
    django_user_model,
    settings,
):
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 2
    user = django_user_model.objects.create_user(
        username="daily_upload_limit",
    )

    with patch(
        "files.upload_limits.timezone.localdate",
        return_value=QUOTA_DAY,
    ):
        reserve_daily_video_upload(user)
        reserve_daily_video_upload(user)

        with pytest.raises(
            DailyVideoUploadLimitReached
        ) as exc_info:
            reserve_daily_video_upload(user)

    assert exc_info.value.limit == 2
    assert exc_info.value.used == 2
    assert DailyVideoUploadQuota.objects.get(
        user=user,
        day=QUOTA_DAY,
    ).used == 2


@pytest.mark.django_db
def test_failed_media_creation_can_release_reserved_slot(
    django_user_model,
    settings,
):
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 1
    user = django_user_model.objects.create_user(
        username="daily_upload_release",
    )

    with patch(
        "files.upload_limits.timezone.localdate",
        return_value=QUOTA_DAY,
    ):
        reservation = reserve_daily_video_upload(user)
        release_daily_video_upload(reservation)
        reserve_daily_video_upload(user)

    assert DailyVideoUploadQuota.objects.get(
        user=user,
        day=QUOTA_DAY,
    ).used == 1


@pytest.mark.django_db
def test_superuser_bypasses_daily_video_upload_quota(
    django_user_model,
    settings,
):
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 1
    user = django_user_model.objects.create_superuser(
        username="daily_upload_admin",
        email="admin@example.com",
        password="password",
    )

    for _index in range(3):
        assert reserve_daily_video_upload(user) is None

    assert not DailyVideoUploadQuota.objects.filter(
        user=user
    ).exists()


@pytest.mark.django_db
def test_zero_disables_daily_video_upload_quota(
    django_user_model,
    settings,
):
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 0
    user = django_user_model.objects.create_user(
        username="daily_upload_disabled",
    )

    for _index in range(3):
        assert reserve_daily_video_upload(user) is None

    assert not DailyVideoUploadQuota.objects.filter(
        user=user
    ).exists()


def _mock_media_side_effects():
    return (
        patch("files.models.Media.media_init"),
        patch("files.models.Media.update_search_vector"),
        patch("files.methods.notify_users"),
        patch("users.models.User.update_user_media"),
    )


@pytest.mark.django_db
def test_media_api_returns_429_after_daily_video_limit(
    django_user_model,
    client,
    settings,
    tmp_path,
):
    settings.CAN_ADD_MEDIA = "all"
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 1
    settings.MEDIA_ROOT = str(tmp_path)
    user = django_user_model.objects.create_user(
        username="daily_upload_api",
    )
    client.force_login(user)

    patches = _mock_media_side_effects()
    with (
        patch("files.views.uploaded_file_is_video", return_value=True),
        patches[0],
        patches[1],
        patches[2],
        patches[3],
    ):
        first = client.post(
            "/api/v1/media",
            {
                "title": "First video",
                "media_file": SimpleUploadedFile(
                    "first.mp4",
                    b"first-video",
                    content_type="video/mp4",
                ),
            },
        )
        second = client.post(
            "/api/v1/media",
            {
                "title": "Second video",
                "media_file": SimpleUploadedFile(
                    "second.mp4",
                    b"second-video",
                    content_type="video/mp4",
                ),
            },
        )

    assert first.status_code == 201
    assert second.status_code == 429
    assert second.json()["code"] == (
        "daily_video_upload_limit_reached"
    )
    assert Media.objects.filter(user=user).count() == 1


@pytest.mark.django_db
def test_fine_uploader_returns_429_after_daily_video_limit(
    django_user_model,
    client,
    settings,
    tmp_path,
):
    settings.CAN_ADD_MEDIA = "all"
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 1
    settings.MEDIA_ROOT = str(tmp_path)
    user = django_user_model.objects.create_user(
        username="daily_upload_fine_uploader",
    )
    client.force_login(user)

    def upload(filename):
        return client.post(
            "/fu/upload/",
            {
                "qquuid": str(uuid.uuid4()),
                "qqfilename": filename,
                "qqfile": SimpleUploadedFile(
                    filename,
                    b"video-data",
                    content_type="video/mp4",
                ),
            },
        )

    patches = _mock_media_side_effects()
    with (
        patch(
            "uploader.views.media_path_is_video",
            return_value=True,
        ),
        patches[0],
        patches[1],
        patches[2],
        patches[3],
    ):
        first = upload("first.mp4")
        second = upload("second.mp4")

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json()["code"] == (
        "daily_video_upload_limit_reached"
    )
    assert Media.objects.filter(user=user).count() == 1
