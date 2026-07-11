from unittest.mock import Mock, patch

import pytest
from django.urls import reverse
from django.utils import timezone

from files.models import EncodeProfile, Encoding, Media
from premium.models import PremiumMediaAsset, PremiumMediaUnlock


def create_media(*, user, friendly_token):
    Media.objects.bulk_create(
        [
            Media(
                user=user,
                friendly_token=friendly_token,
                title="Premium download test",
                media_file=f"tests/premium/{friendly_token}.mp4",
                media_type="video",
                state="public",
                encoding_status="success",
                is_reviewed=True,
                listable=True,
                allow_download=True,
            )
        ]
    )
    return Media.objects.get(friendly_token=friendly_token)


def create_preview_encoding(*, media):
    profile = EncodeProfile.objects.create(
        name="h264-720",
        extension="mp4",
        resolution=720,
        codec="h264",
        active=True,
    )
    return Encoding.objects.create(
        media=media,
        profile=profile,
        media_file="encoded/h264/720/preview.mp4",
        chunk=False,
        status="success",
        progress=100,
    )


def create_s3_premium_asset(*, media):
    return PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_READY,
        price_tokens=100 * 10**6,
        storage_backend=PremiumMediaAsset.STORAGE_S3,
        playback_format=PremiumMediaAsset.PLAYBACK_MP4,
        storage_bucket="private-premium",
        storage_key=(
            f"premium-media/users/{media.user_id}/"
            f"media/{media.friendly_token}/premium.mp4"
        ),
        file_name="premium original.mp4",
        content_type="video/mp4",
        codec="h264",
        video_height=1080,
        size_bytes=12_345,
        premium_published_at=timezone.now(),
    )


def grant_unlock(*, user, media):
    return PremiumMediaUnlock.objects.create(
        user=user,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_ADMIN,
    )


def set_gate_cookie(client, settings):
    cookie_name = getattr(settings, "MEDIA_GATE_COOKIE_NAME", "mc_gate")
    client.cookies[cookie_name] = "1"


@pytest.mark.django_db
def test_premium_download_page_replaces_preview_encodings(
    django_user_model,
    client,
    settings,
):
    creator = django_user_model.objects.create_user(
        username="premium_download_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadpage",
    )
    preview = create_preview_encoding(media=media)
    create_s3_premium_asset(media=media)
    grant_unlock(user=viewer, media=media)

    client.force_login(viewer)
    set_gate_cookie(client, settings)

    response = client.get(
        reverse(
            "media_download_page",
            kwargs={"friendly_token": media.friendly_token},
        ),
        {"playback": "premium"},
    )

    assert response.status_code == 200
    assert "Download premium video" in response.content.decode()
    assert (
        reverse(
            "media_download_start",
            kwargs={
                "friendly_token": media.friendly_token,
                "download_id": "premium",
            },
        )
        in response.content.decode()
    )
    assert f"Download 720 H264" not in response.content.decode()
    assert (
        reverse(
            "media_download_start",
            kwargs={
                "friendly_token": media.friendly_token,
                "download_id": preview.id,
            },
        )
        not in response.content.decode()
    )
    assert "playback=premium" in response.content.decode()


@pytest.mark.django_db
def test_preview_download_page_is_unchanged(
    django_user_model,
    client,
    settings,
):
    creator = django_user_model.objects.create_user(
        username="preview_download_creator",
    )
    media = create_media(
        user=creator,
        friendly_token="previewdownloadpage",
    )
    preview = create_preview_encoding(media=media)
    create_s3_premium_asset(media=media)

    set_gate_cookie(client, settings)

    response = client.get(
        reverse(
            "media_download_page",
            kwargs={"friendly_token": media.friendly_token},
        )
    )

    content = response.content.decode()

    assert response.status_code == 200
    assert "Download 720 H264" in content
    assert "Download premium video" not in content
    assert reverse(
        "media_download_start",
        kwargs={
            "friendly_token": media.friendly_token,
            "download_id": preview.id,
        },
    ) in content


@pytest.mark.django_db
def test_premium_download_page_requires_an_active_unlock(
    django_user_model,
    client,
    settings,
):
    creator = django_user_model.objects.create_user(
        username="premium_download_locked_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_locked_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadlocked",
    )
    create_s3_premium_asset(media=media)

    client.force_login(viewer)
    set_gate_cookie(client, settings)

    response = client.get(
        reverse(
            "media_download_page",
            kwargs={"friendly_token": media.friendly_token},
        ),
        {"playback": "premium"},
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_revoked_unlock_cannot_download_premium_asset(
    django_user_model,
    client,
    settings,
):
    creator = django_user_model.objects.create_user(
        username="premium_download_revoked_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_revoked_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadrevoked",
    )
    create_s3_premium_asset(media=media)
    unlock = grant_unlock(user=viewer, media=media)
    unlock.revoked_at = timezone.now()
    unlock.save(update_fields=["revoked_at"])

    client.force_login(viewer)
    set_gate_cookie(client, settings)

    response = client.post(
        reverse(
            "media_download_start",
            kwargs={
                "friendly_token": media.friendly_token,
                "download_id": "premium",
            },
        )
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_creator_owner_can_open_premium_download_page_without_unlock(
    django_user_model,
    client,
    settings,
):
    creator = django_user_model.objects.create_user(
        username="premium_download_owner",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadowner",
    )
    create_s3_premium_asset(media=media)

    client.force_login(creator)
    set_gate_cookie(client, settings)

    response = client.get(
        reverse(
            "media_download_page",
            kwargs={"friendly_token": media.friendly_token},
        ),
        {"playback": "premium"},
    )

    assert response.status_code == 200
    assert "Download premium video" in response.content.decode()


@pytest.mark.django_db
def test_premium_download_redirect_uses_short_lived_s3_attachment_url(
    django_user_model,
    client,
    settings,
):
    settings.DOWNLOAD_COOLDOWN_SECONDS = 0
    settings.PREMIUM_SIGNED_URL_TTL_SECONDS = 90

    creator = django_user_model.objects.create_user(
        username="premium_download_signed_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_signed_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadsigned",
    )
    asset = create_s3_premium_asset(media=media)
    grant_unlock(user=viewer, media=media)

    client.force_login(viewer)
    set_gate_cookie(client, settings)

    s3_client = Mock()
    s3_client.generate_presigned_url.return_value = (
        "https://private.example/premium.mp4?"
        "X-Amz-Signature=test"
    )

    with patch(
        "premium.downloads.get_premium_s3_client",
        return_value=s3_client,
    ):
        response = client.post(
            reverse(
                "media_download_start",
                kwargs={
                    "friendly_token": media.friendly_token,
                    "download_id": "premium",
                },
            )
        )

    assert response.status_code == 302
    assert response["Location"].startswith(
        "https://private.example/premium.mp4?"
    )
    cache_control = response["Cache-Control"]

    assert "private" in cache_control
    assert "no-store" in cache_control
    assert "max-age=0" in cache_control
    assert response["Pragma"] == "no-cache"

    s3_client.generate_presigned_url.assert_called_once()
    call = s3_client.generate_presigned_url.call_args

    assert call.args == ("get_object",)
    assert call.kwargs["ExpiresIn"] == 90
    assert call.kwargs["Params"] == {
        "Bucket": asset.storage_bucket,
        "Key": asset.storage_key,
        "ResponseContentType": "video/mp4",
        "ResponseContentDisposition": (
            'attachment; filename="premium_original.mp4"'
        ),
    }


@pytest.mark.django_db
def test_premium_download_does_not_support_direct_url_assets(
    django_user_model,
    client,
    settings,
):
    creator = django_user_model.objects.create_user(
        username="premium_download_direct_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_direct_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloaddirect",
    )
    PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_READY,
        price_tokens=100 * 10**6,
        storage_backend=PremiumMediaAsset.STORAGE_DIRECT_URL,
        playback_format=PremiumMediaAsset.PLAYBACK_MP4,
        direct_url="https://example.com/premium.mp4",
        file_name="premium.mp4",
        content_type="video/mp4",
        codec="h264",
        video_height=1080,
        size_bytes=12_345,
        premium_published_at=timezone.now(),
    )
    grant_unlock(user=viewer, media=media)

    client.force_login(viewer)
    set_gate_cookie(client, settings)

    response = client.get(
        reverse(
            "media_download_page",
            kwargs={"friendly_token": media.friendly_token},
        ),
        {"playback": "premium"},
    )

    assert response.status_code == 404


@pytest.mark.django_db
def test_premium_download_keeps_age_gate_requirement(
    django_user_model,
    client,
):
    creator = django_user_model.objects.create_user(
        username="premium_download_gate_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_gate_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadgate",
    )
    create_s3_premium_asset(media=media)
    grant_unlock(user=viewer, media=media)

    client.force_login(viewer)

    page_response = client.get(
        reverse(
            "media_download_page",
            kwargs={"friendly_token": media.friendly_token},
        ),
        {"playback": "premium"},
    )
    start_response = client.post(
        reverse(
            "media_download_start",
            kwargs={
                "friendly_token": media.friendly_token,
                "download_id": "premium",
            },
        )
    )

    assert page_response.status_code == 403
    assert start_response.status_code == 403


@pytest.mark.django_db
def test_premium_download_start_rechecks_access_before_signing(
    django_user_model,
    client,
    settings,
):
    settings.DOWNLOAD_COOLDOWN_SECONDS = 0

    creator = django_user_model.objects.create_user(
        username="premium_download_recheck_creator",
    )
    viewer = django_user_model.objects.create_user(
        username="premium_download_recheck_viewer",
    )
    media = create_media(
        user=creator,
        friendly_token="premiumdownloadrecheck",
    )
    create_s3_premium_asset(media=media)

    client.force_login(viewer)
    set_gate_cookie(client, settings)

    with patch(
        "premium.downloads.get_premium_s3_client",
    ) as s3_client_factory:
        response = client.post(
            reverse(
                "media_download_start",
                kwargs={
                    "friendly_token": media.friendly_token,
                    "download_id": "premium",
                },
            )
        )

    assert response.status_code == 404
    s3_client_factory.assert_not_called()
