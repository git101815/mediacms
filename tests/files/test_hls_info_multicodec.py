import pytest

from files.helpers import url_from_path
from files.models import MediaHLSRendition


@pytest.mark.django_db
def test_hls_info_uses_hls_rendition_table_for_all_codecs(
    configure_media_paths,
    media_factory,
    monkeypatch,
):
    configure_media_paths()
    media = media_factory(friendly_token="hlsinfotable")

    MediaHLSRendition.objects.bulk_create(
        [
            MediaHLSRendition(
                media=media,
                codec="h264",
                resolution=480,
                master_file=f"hls/{media.uid.hex}/master.m3u8",
                playlist_file=f"hls/{media.uid.hex}/media-1/stream.m3u8",
                iframe_file=f"hls/{media.uid.hex}/media-1/iframes.m3u8",
            ),
            MediaHLSRendition(
                media=media,
                codec="h264",
                resolution=720,
                master_file=f"hls/{media.uid.hex}/master.m3u8",
                playlist_file=f"hls/{media.uid.hex}/media-2/stream.m3u8",
                iframe_file=f"hls/{media.uid.hex}/media-2/iframes.m3u8",
            ),
            MediaHLSRendition(
                media=media,
                codec="h265",
                resolution=480,
                master_file=f"hls/{media.uid.hex}/hevc/master.m3u8",
                playlist_file=f"hls/{media.uid.hex}/hevc/video/hvc1/stream.m3u8",
                iframe_file=f"hls/{media.uid.hex}/hevc/video/hvc1/iframes.m3u8",
            ),
            MediaHLSRendition(
                media=media,
                codec="h265",
                resolution=720,
                master_file=f"hls/{media.uid.hex}/hevc/master.m3u8",
                playlist_file=f"hls/{media.uid.hex}/hevc/video/hvc1/2/stream.m3u8",
                iframe_file=f"hls/{media.uid.hex}/hevc/video/hvc1/2/iframes.m3u8",
            ),
            MediaHLSRendition(
                media=media,
                codec="av1",
                resolution=480,
                master_file=f"hls/{media.uid.hex}/av1/master.m3u8",
                playlist_file=f"hls/{media.uid.hex}/av1/480/stream.m3u8",
                iframe_file="",
            ),
            MediaHLSRendition(
                media=media,
                codec="av1",
                resolution=1080,
                master_file=f"hls/{media.uid.hex}/av1/master.m3u8",
                playlist_file=f"hls/{media.uid.hex}/av1/1080/stream.m3u8",
                iframe_file="",
            ),
        ]
    )

    def fail_exists(*args, **kwargs):
        raise AssertionError("hls_info must not touch the filesystem")

    def fail_load(*args, **kwargs):
        raise AssertionError("hls_info must not parse manifests")

    monkeypatch.setattr("files.models.os.path.exists", fail_exists)
    monkeypatch.setattr("files.models.m3u8.load", fail_load)

    info = media.hls_info

    assert info["master_file"] == url_from_path(f"hls/{media.uid.hex}/master.m3u8")
    assert info["480_playlist"] == url_from_path(f"hls/{media.uid.hex}/media-1/stream.m3u8")
    assert info["480_iframe"] == url_from_path(f"hls/{media.uid.hex}/media-1/iframes.m3u8")
    assert info["720_playlist"] == url_from_path(f"hls/{media.uid.hex}/media-2/stream.m3u8")
    assert info["720_iframe"] == url_from_path(f"hls/{media.uid.hex}/media-2/iframes.m3u8")

    assert info["hevc"]["master_file"] == url_from_path(f"hls/{media.uid.hex}/hevc/master.m3u8")
    assert info["hevc"]["480_playlist"] == url_from_path(
        f"hls/{media.uid.hex}/hevc/video/hvc1/stream.m3u8"
    )
    assert info["hevc"]["480_iframe"] == url_from_path(
        f"hls/{media.uid.hex}/hevc/video/hvc1/iframes.m3u8"
    )
    assert info["hevc"]["720_playlist"] == url_from_path(
        f"hls/{media.uid.hex}/hevc/video/hvc1/2/stream.m3u8"
    )
    assert info["hevc"]["720_iframe"] == url_from_path(
        f"hls/{media.uid.hex}/hevc/video/hvc1/2/iframes.m3u8"
    )

    assert info["av1"]["master_file"] == url_from_path(f"hls/{media.uid.hex}/av1/master.m3u8")
    assert info["av1"]["480_playlist"] == url_from_path(
        f"hls/{media.uid.hex}/av1/480/stream.m3u8"
    )
    assert info["av1"]["1080_playlist"] == url_from_path(
        f"hls/{media.uid.hex}/av1/1080/stream.m3u8"
    )
    assert "480_iframe" not in info["av1"]
    assert "1080_iframe" not in info["av1"]


@pytest.mark.django_db
def test_hls_info_does_not_invent_resolution_playlists_from_encodings(
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
):
    configure_media_paths()
    media = media_factory(friendly_token="noinvent")

    for codec, resolutions in {
        "h264": (480, 720),
        "h265": (480, 720),
        "av1": (480, 1080),
    }.items():
        for resolution in resolutions:
            profile = profile_factory(codec=codec, resolution=resolution)
            encoding_factory(
                media=media,
                profile=profile,
                media_file=f"encoded/{codec}/{resolution}/video.mp4",
            )

    media.hls_file = f"hls/{media.uid.hex}/master.m3u8"
    media.hls_hevc_file = f"hls/{media.uid.hex}/hevc/master.m3u8"
    media.hls_av1_file = f"hls/{media.uid.hex}/av1/master.m3u8"
    media.save(update_fields=["hls_file", "hls_hevc_file", "hls_av1_file"])

    def fail_exists(*args, **kwargs):
        raise AssertionError("hls_info must not touch the filesystem")

    def fail_load(*args, **kwargs):
        raise AssertionError("hls_info must not parse manifests")

    monkeypatch.setattr("files.models.os.path.exists", fail_exists)
    monkeypatch.setattr("files.models.m3u8.load", fail_load)

    assert media.hls_info == {}


@pytest.mark.django_db
def test_hls_info_is_empty_when_media_has_no_hls_rendition_rows(
    configure_media_paths,
    media_factory,
    monkeypatch,
):
    configure_media_paths()
    media = media_factory(friendly_token="nohlsrows")

    def fail_exists(*args, **kwargs):
        raise AssertionError("hls_info must not touch the filesystem")

    def fail_load(*args, **kwargs):
        raise AssertionError("hls_info must not parse manifests")

    monkeypatch.setattr("files.models.os.path.exists", fail_exists)
    monkeypatch.setattr("files.models.m3u8.load", fail_load)

    assert media.hls_info == {}


def test_url_from_path_preserves_absolute_remote_hls_urls():
    assert url_from_path("https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"
    assert url_from_path("/https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"