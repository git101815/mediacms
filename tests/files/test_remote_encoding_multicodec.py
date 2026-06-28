import json

import pytest
from django.test import RequestFactory

from files.remote_encoding import build_runpod_payload, get_remote_profiles_for_media, sign_payload
from files.remote_encoding_views import remote_encoding_callback


@pytest.mark.django_db
def test_remote_profiles_include_enabled_h264_h265_av1_and_skip_disabled_or_invalid_profiles(
    settings,
    media_factory,
    profile_factory,
):
    settings.ENABLED_ENCODING_CODECS = ("h264", "h265", "av1")
    settings.MINIMUM_RESOLUTIONS_TO_ENCODE = [1080]

    media = media_factory(friendly_token="remoteprofiles", video_height=720)

    profile_factory(codec="h264", resolution=480, name="h264-480")
    profile_factory(codec="h264", resolution=1440, name="h264-1440")
    profile_factory(codec="h265", resolution=720, name="h265-720")
    profile_factory(codec="av1", resolution=1080, name="av1-1080")
    profile_factory(codec="vp9", resolution=480, name="vp9-480")
    profile_factory(codec=None, resolution=None, extension="gif", name="preview")
    profile_factory(codec="av1", resolution=720, active=False, name="inactive-av1-720")

    profiles = get_remote_profiles_for_media(media)

    assert profiles == [
        {"name": "h264-480", "codec": "h264", "resolution": 480},
        {"name": "h265-720", "codec": "h265", "resolution": 720},
        {"name": "av1-1080", "codec": "av1", "resolution": 1080},
    ]


@pytest.mark.django_db
def test_runpod_payload_contains_multicodec_profiles_and_signature(
    settings,
    media_factory,
    profile_factory,
):
    settings.ENABLED_ENCODING_CODECS = ("h264", "h265", "av1")
    settings.MINIMUM_RESOLUTIONS_TO_ENCODE = []
    settings.REMOTE_ENCODING_CALLBACK_SECRET = "test-secret"
    settings.REMOTE_ENCODING_SOURCE_BASE_URL = "https://source.example/media"
    settings.REMOTE_ENCODING_PUBLIC_BASE_URL = "https://cdn.example/mediafiles"
    settings.REMOTE_ENCODING_OUTPUT_PREFIX = "encoded"
    settings.REMOTE_ENCODING_HLS_SEGMENT_SECONDS = 4
    settings.FRONTEND_HOST = "https://site.example"

    media = media_factory(friendly_token="remotepayload", video_height=1080)

    for codec in ("h264", "h265", "av1"):
        profile_factory(codec=codec, resolution=720, name=f"{codec}-720")

    payload = build_runpod_payload(media)

    assert payload["version"] == 1
    assert payload["friendly_token"] == media.friendly_token
    assert payload["source_url"].endswith("/tests/remotepayload.mp4")
    assert payload["public_base_url"] == "https://cdn.example/mediafiles"
    assert payload["output_prefix"].endswith(f"/{media.uid.hex}")
    assert payload["segment_seconds"] == 4

    assert payload["profiles"] == [
        {"name": "h264-720", "codec": "h264", "resolution": 720},
        {"name": "h265-720", "codec": "h265", "resolution": 720},
        {"name": "av1-720", "codec": "av1", "resolution": 720},
    ]

    signature = payload.pop("signature")
    assert signature == sign_payload(payload)


@pytest.mark.django_db
def test_remote_callback_writes_h264_h265_and_av1_hls_fields(settings, media_factory):
    settings.REMOTE_ENCODING_CALLBACK_SECRET = "test-secret"

    media = media_factory(friendly_token="callbacksuccess")
    payload = {
        "media_id": media.id,
        "friendly_token": media.friendly_token,
        "status": "success",
        "outputs": {
            "h264": {"master_url": "https://cdn.example/h264/master.m3u8"},
            "h265": {"master_url": "https://cdn.example/hevc/master.m3u8"},
            "av1": {"master_url": "https://cdn.example/av1/master.m3u8"},
        },
    }
    payload["signature"] = sign_payload(payload)

    request = RequestFactory().post(
        "/callback",
        data=json.dumps(payload),
        content_type="application/json",
    )

    response = remote_encoding_callback(request, media.friendly_token)
    media.refresh_from_db()

    assert response.status_code == 200
    body = json.loads(response.content.decode("utf-8"))

    assert body == {
        "ok": True,
        "status": "success",
        "h264": True,
        "h265": True,
        "av1": True,
    }

    assert media.encoding_status == "success"
    assert media.hls_file == "https://cdn.example/h264/master.m3u8"
    assert media.hls_hevc_file == "https://cdn.example/hevc/master.m3u8"
    assert media.hls_av1_file == "https://cdn.example/av1/master.m3u8"


@pytest.mark.django_db
def test_remote_callback_accepts_hevc_alias(settings, media_factory):
    settings.REMOTE_ENCODING_CALLBACK_SECRET = "test-secret"

    media = media_factory(friendly_token="callbackhevc")
    payload = {
        "media_id": media.id,
        "friendly_token": media.friendly_token,
        "status": "success",
        "outputs": {
            "hevc": {"master_url": "https://cdn.example/hevc/master.m3u8"},
        },
    }
    payload["signature"] = sign_payload(payload)

    request = RequestFactory().post(
        "/callback",
        data=json.dumps(payload),
        content_type="application/json",
    )

    response = remote_encoding_callback(request, media.friendly_token)
    media.refresh_from_db()

    assert response.status_code == 200
    assert media.hls_hevc_file == "https://cdn.example/hevc/master.m3u8"


@pytest.mark.django_db
def test_remote_callback_rejects_invalid_signature_without_mutating_media(settings, media_factory):
    settings.REMOTE_ENCODING_CALLBACK_SECRET = "test-secret"

    media = media_factory(friendly_token="callbackbadsig")
    payload = {
        "media_id": media.id,
        "friendly_token": media.friendly_token,
        "status": "success",
        "outputs": {
            "h264": {"master_url": "https://cdn.example/h264/master.m3u8"},
        },
        "signature": "bad-signature",
    }

    request = RequestFactory().post(
        "/callback",
        data=json.dumps(payload),
        content_type="application/json",
    )

    response = remote_encoding_callback(request, media.friendly_token)
    media.refresh_from_db()

    assert response.status_code == 403
    assert media.hls_file == ""


@pytest.mark.django_db
def test_remote_callback_marks_media_failed_without_writing_outputs(settings, media_factory):
    settings.REMOTE_ENCODING_CALLBACK_SECRET = "test-secret"

    media = media_factory(friendly_token="callbackfail")
    payload = {
        "media_id": media.id,
        "friendly_token": media.friendly_token,
        "status": "fail",
        "outputs": {
            "h264": {"master_url": "https://cdn.example/h264/master.m3u8"},
        },
    }
    payload["signature"] = sign_payload(payload)

    request = RequestFactory().post(
        "/callback",
        data=json.dumps(payload),
        content_type="application/json",
    )

    response = remote_encoding_callback(request, media.friendly_token)
    media.refresh_from_db()

    assert response.status_code == 200
    assert media.encoding_status == "fail"
    assert media.hls_file == ""
    assert media.hls_hevc_file == ""
    assert media.hls_av1_file == ""