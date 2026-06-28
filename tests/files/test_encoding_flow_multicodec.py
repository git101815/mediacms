import pytest

from files import tasks as file_tasks


class DelayRecorder:
    def __init__(self):
        self.calls = []

    def delay(self, *args, **kwargs):
        self.calls.append((args, kwargs))


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_attr"),
    (
        ("h264", "create_hls"),
        ("h265", "create_hls_hevc_fmp4"),
        ("av1", "create_hls_av1_fmp4"),
    ),
)
def test_post_encode_actions_dispatches_expected_hls_task_by_codec(
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    codec,
    task_attr,
):
    media = media_factory(friendly_token=f"{codec}dispatch")
    profile = profile_factory(codec=codec, resolution=720)
    encoding = encoding_factory(media=media, profile=profile, media_file=f"encoded/{codec}/720/video.mp4")

    recorders = {
        "create_hls": DelayRecorder(),
        "create_hls_hevc_fmp4": DelayRecorder(),
        "create_hls_av1_fmp4": DelayRecorder(),
    }

    for attr, recorder in recorders.items():
        monkeypatch.setattr(file_tasks, attr, recorder)

    assert media.post_encode_actions(encoding=encoding, action="add") is True

    assert recorders[task_attr].calls == [((media.friendly_token,), {})]

    for attr, recorder in recorders.items():
        if attr != task_attr:
            assert recorder.calls == []


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("status", "action", "chunk"),
    (
        ("fail", "add", False),
        ("success", "delete", False),
        ("success", "add", True),
    ),
)
def test_post_encode_actions_does_not_dispatch_hls_for_invalid_encoding_states(
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    status,
    action,
    chunk,
):
    media = media_factory(friendly_token=f"nodispatch{status}{action}{chunk}")
    profile = profile_factory(codec="av1", resolution=720)
    encoding = encoding_factory(
        media=media,
        profile=profile,
        media_file="encoded/av1/720/video.mp4",
        status=status,
        chunk=chunk,
    )

    recorders = {
        "create_hls": DelayRecorder(),
        "create_hls_hevc_fmp4": DelayRecorder(),
        "create_hls_av1_fmp4": DelayRecorder(),
    }

    for attr, recorder in recorders.items():
        monkeypatch.setattr(file_tasks, attr, recorder)

    assert media.post_encode_actions(encoding=encoding, action=action) is True

    assert recorders["create_hls"].calls == []
    assert recorders["create_hls_hevc_fmp4"].calls == []
    assert recorders["create_hls_av1_fmp4"].calls == []


@pytest.mark.django_db
def test_encodings_info_groups_available_codecs_by_their_actual_resolution(
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
):
    configure_media_paths()
    media = media_factory(friendly_token="encodingsinfo")

    cases = (
        ("h264", 360),
        ("h265", 720),
        ("av1", 1080),
    )

    for codec, resolution in cases:
        profile = profile_factory(codec=codec, resolution=resolution)
        encoding_factory(
            media=media,
            profile=profile,
            media_file=f"encoded/{codec}/{resolution}/video.mp4",
        )

    info = media.encodings_info

    for codec, resolution in cases:
        assert set(info[resolution]) == {codec}
        assert info[resolution][codec]["url"].endswith(f"/encoded/{codec}/{resolution}/video.mp4")


@pytest.mark.django_db
def test_public_media_url_prefers_h264_720_when_available_for_external_compatibility(
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
):
    configure_media_paths()
    media = media_factory(friendly_token="publich264")

    for resolution in (480, 720, 1080):
        profile = profile_factory(codec="h264", resolution=resolution)
        encoding_factory(
            media=media,
            profile=profile,
            media_file=f"encoded/h264/{resolution}/video.mp4",
        )

    for codec in ("h265", "av1"):
        profile = profile_factory(codec=codec, resolution=1080)
        encoding_factory(
            media=media,
            profile=profile,
            media_file=f"encoded/{codec}/1080/video.mp4",
        )

    assert media.public_media_url.endswith("/encoded/h264/720/video.mp4")


@pytest.mark.django_db
def test_public_media_url_falls_back_to_highest_h264_when_720_is_not_available(
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
):
    configure_media_paths()
    media = media_factory(friendly_token="publich264fallback")

    for resolution in (360, 1080):
        profile = profile_factory(codec="h264", resolution=resolution)
        encoding_factory(
            media=media,
            profile=profile,
            media_file=f"encoded/h264/{resolution}/video.mp4",
        )

    for codec in ("h265", "av1"):
        profile = profile_factory(codec=codec, resolution=720)
        encoding_factory(
            media=media,
            profile=profile,
            media_file=f"encoded/{codec}/720/video.mp4",
        )

    assert media.public_media_url.endswith("/encoded/h264/1080/video.mp4")