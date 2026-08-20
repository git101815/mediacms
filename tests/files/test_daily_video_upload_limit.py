import uuid
from datetime import date
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile

from files.models import DailyVideoUploadQuota, Media
from files.upload_limits import (
    DailyVideoUploadLimitReached,
    UnsupportedMediaUpload,
    get_daily_video_upload_status,
    media_path_is_video,
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


@pytest.mark.django_db
def test_daily_video_upload_status_reports_used_and_remaining(
    django_user_model,
    settings,
):
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 3
    user = django_user_model.objects.create_user(
        username="daily_upload_status",
    )
    DailyVideoUploadQuota.objects.create(
        user=user,
        day=QUOTA_DAY,
        used=2,
    )

    with patch(
        "files.upload_limits.timezone.localdate",
        return_value=QUOTA_DAY,
    ):
        quota = get_daily_video_upload_status(user)

    assert quota == {
        "enabled": True,
        "day": "2026-07-19",
        "timezone": settings.TIME_ZONE,
        "limit": 3,
        "used": 2,
        "remaining": 1,
    }


@pytest.mark.django_db
def test_daily_video_upload_quota_endpoint(
    django_user_model,
    client,
    settings,
):
    settings.MAX_VIDEO_UPLOADS_PER_DAY = 3
    user = django_user_model.objects.create_user(
        username="daily_upload_status_endpoint",
    )
    DailyVideoUploadQuota.objects.create(
        user=user,
        day=QUOTA_DAY,
        used=2,
    )
    client.force_login(user)

    with patch(
        "files.upload_limits.timezone.localdate",
        return_value=QUOTA_DAY,
    ):
        response = client.get("/fu/quota/")

    assert response.status_code == 200
    assert response.json()["limit"] == 3
    assert response.json()["used"] == 2
    assert response.json()["remaining"] == 1


def test_unrecognized_media_is_rejected_without_ffprobe():
    with (
        patch(
            "files.upload_limits.helpers.get_file_type",
            return_value=None,
        ),
        patch(
            "files.upload_limits.helpers.media_file_info",
            side_effect=AssertionError("ffprobe fallback must not run"),
        ) as media_file_info,
    ):
        with pytest.raises(UnsupportedMediaUpload):
            media_path_is_video("unknown-media.bin")

    media_file_info.assert_not_called()


def test_set_media_type_rejects_unknown_without_ffprobe():
    media = Media(media_file="unknown-media.bin")

    with (
        patch(
            "files.models.helpers.get_file_type",
            return_value=None,
        ),
        patch(
            "files.models.helpers.media_file_info",
            side_effect=AssertionError("ffprobe fallback must not run"),
        ) as media_file_info,
    ):
        media.set_media_type(save=False)

    assert media.media_type == ""
    assert media.encoding_status == "fail"
    media_file_info.assert_not_called()


def test_set_media_type_accepts_recognized_video_without_ffprobe():
    media = Media(media_file="recognized-video.mp4")

    with (
        patch("files.models.helpers.get_file_type", return_value="video"),
        patch("files.models.helpers.media_file_info") as media_file_info,
    ):
        media.set_media_type(save=False)

    assert media.media_type == "video"
    media_file_info.assert_not_called()


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
def test_media_api_rejects_unrecognized_media(
    django_user_model,
    client,
    settings,
):
    settings.CAN_ADD_MEDIA = "all"
    user = django_user_model.objects.create_user(
        username="unrecognized_media_api",
    )
    client.force_login(user)

    with patch(
        "files.views.uploaded_file_is_video",
        side_effect=UnsupportedMediaUpload(),
    ):
        response = client.post(
            "/api/v1/media",
            {
                "title": "Unknown media",
                "media_file": SimpleUploadedFile(
                    "unknown.mp4",
                    b"not-a-recognized-media-file",
                    content_type="video/mp4",
                ),
            },
        )

    assert response.status_code == 400
    assert "media_file" in response.json()
    assert Media.objects.filter(user=user).count() == 0


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
    assert first.json()["video_upload_quota"]["used"] == 1
    assert (
        first.json()["video_upload_quota"]["remaining"]
        == 0
    )
    assert second.status_code == 429
    assert second.json()["code"] == (
        "daily_video_upload_limit_reached"
    )
    assert second.json()["preventRetry"] is True
    assert second.json()["video_upload_quota"]["used"] == 1
    assert (
        second.json()["video_upload_quota"]["remaining"]
        == 0
    )
    assert Media.objects.filter(user=user).count() == 1


@pytest.mark.django_db
def test_fine_uploader_rejects_unrecognized_media(
    django_user_model,
    client,
    settings,
    tmp_path,
):
    settings.CAN_ADD_MEDIA = "all"
    settings.MEDIA_ROOT = str(tmp_path)
    user = django_user_model.objects.create_user(
        username="unrecognized_media_fine_uploader",
    )
    client.force_login(user)

    with patch(
        "uploader.views.media_path_is_video",
        side_effect=UnsupportedMediaUpload(),
    ):
        response = client.post(
            "/fu/upload/",
            {
                "qquuid": str(uuid.uuid4()),
                "qqfilename": "unknown.mp4",
                "qqfile": SimpleUploadedFile(
                    "unknown.mp4",
                    b"not-a-recognized-media-file",
                    content_type="video/mp4",
                ),
            },
        )

    assert response.status_code == 400
    assert response.json()["code"] == "unsupported_media_type"
    assert response.json()["preventRetry"] is True
    assert Media.objects.filter(user=user).count() == 0
