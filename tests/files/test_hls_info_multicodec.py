import pytest

from files.helpers import url_from_path


@pytest.mark.django_db
def test_hls_info_maps_available_multicodec_playlists_from_existing_encodings(
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
):
    configure_media_paths()
    media = media_factory(friendly_token="hlsinfodynamic")

    codec_cases = {
        "h264": {
            "field": "hls_file",
            "group": None,
            "renditions": (360, 720),
            "playlist": lambda index: f"media-{index}/stream.m3u8",
            "iframe": lambda index: f"media-{index}/iframes.m3u8",
        },
        "h265": {
            "field": "hls_hevc_file",
            "group": "hevc",
            "renditions": (360, 720),
            "playlist": lambda index: f"video/hvc1/{index}/stream.m3u8",
            "iframe": lambda index: f"video/hvc1/{index}/iframes.m3u8",
        },
        "av1": {
            "field": "hls_av1_file",
            "group": "av1",
            "renditions": (480, 1080),
            "playlist": lambda index: f"video/av01/{index}/stream.m3u8",
            "iframe": lambda index: f"video/av01/{index}/iframes.m3u8",
        },
    }

    for codec, case in codec_cases.items():
        for resolution in case["renditions"]:
            profile = profile_factory(codec=codec, resolution=resolution)
            encoding_factory(
                media=media,
                profile=profile,
                media_file=f"encoded/{codec}/{resolution}/same-name.mp4",
            )

    media.hls_file = f"hls/{media.uid.hex}/master.m3u8"
    media.hls_hevc_file = f"hls/{media.uid.hex}/hevc/master.m3u8"
    media.hls_av1_file = f"hls/{media.uid.hex}/av1/master.m3u8"
    media.save(update_fields=["hls_file", "hls_hevc_file", "hls_av1_file"])

    def fail_exists(*args, **kwargs):
        raise AssertionError("hls_info must not touch the local filesystem")

    monkeypatch.setattr("files.models.os.path.exists", fail_exists)

    info = media.hls_info

    for codec, case in codec_cases.items():
        group = info if case["group"] is None else info[case["group"]]
        master_file = getattr(media, case["field"])
        master_url = url_from_path(master_file)

        assert group["master_file"] == master_url

        for index, resolution in enumerate(case["renditions"], start=1):
            assert group[f"{resolution}_playlist"] == master_url.replace(
                "master.m3u8",
                case["playlist"](index),
            )
            assert group[f"{resolution}_iframe"] == master_url.replace(
                "master.m3u8",
                case["iframe"](index),
            )


@pytest.mark.django_db
def test_hls_info_does_not_invent_resolution_playlists_without_matching_encodings(
    configure_media_paths,
    media_factory,
    monkeypatch,
):
    configure_media_paths()
    media = media_factory(friendly_token="missingmasters")

    media.hls_hevc_file = f"hls/{media.uid.hex}/hevc/master.m3u8"
    media.hls_av1_file = f"hls/{media.uid.hex}/av1/master.m3u8"
    media.save(update_fields=["hls_hevc_file", "hls_av1_file"])

    def fail_exists(*args, **kwargs):
        raise AssertionError("hls_info must not touch the local filesystem")

    monkeypatch.setattr("files.models.os.path.exists", fail_exists)

    info = media.hls_info

    assert info["hevc"]["master_file"] == url_from_path(media.hls_hevc_file)
    assert info["av1"]["master_file"] == url_from_path(media.hls_av1_file)

    assert "360_playlist" not in info["hevc"]
    assert "480_playlist" not in info["hevc"]
    assert "720_playlist" not in info["hevc"]
    assert "1080_playlist" not in info["hevc"]

    assert "360_playlist" not in info["av1"]
    assert "480_playlist" not in info["av1"]
    assert "720_playlist" not in info["av1"]
    assert "1080_playlist" not in info["av1"]

@pytest.mark.django_db
def test_hls_info_is_empty_when_media_has_no_hls(
    configure_media_paths,
    media_factory,
    monkeypatch,
):
    configure_media_paths()
    media = media_factory(friendly_token="nohls")

    def fail_exists(*args, **kwargs):
        raise AssertionError("hls_info must not touch the local filesystem")

    monkeypatch.setattr("files.models.os.path.exists", fail_exists)

    assert media.hls_info == {}

def test_url_from_path_preserves_absolute_remote_hls_urls():
    assert url_from_path("https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"
    assert url_from_path("/https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"