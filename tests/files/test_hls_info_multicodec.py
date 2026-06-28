import pytest

from files.helpers import url_from_path


@pytest.mark.django_db
def test_hls_info_exposes_h264_hevc_and_av1_playlists_per_resolution(
    settings,
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
    write_hls_master,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="hlsinfomulti")

    for codec in ("h265", "av1"):
        for resolution in (480, 720, 1080):
            profile = profile_factory(codec=codec, resolution=resolution)
            encoding_factory(
                media=media,
                profile=profile,
                media_file=f"encoded/{codec}/{resolution}/same-name.mp4",
            )

    hls_root = media_root / "hls" / media.uid.hex

    media.hls_file = str(hls_root / "master.m3u8")
    media.hls_hevc_file = str(hls_root / "hevc" / "master.m3u8")
    media.hls_av1_file = str(hls_root / "av1" / "master.m3u8")
    media.save(update_fields=["hls_file", "hls_hevc_file", "hls_av1_file"])

    write_hls_master(
        media.hls_file,
        ((1080, "media-1"), (720, "media-2"), (480, "media-3")),
        codec_string="avc1.640028",
    )

    write_hls_master(
        media.hls_hevc_file,
        ((480, "video/hvc1/1"), (720, "video/hvc1/2"), (1080, "video/hvc1/3")),
        codec_string="hvc1.1.6.L120.90",
        forced_manifest_height=1080,
    )

    write_hls_master(
        media.hls_av1_file,
        ((480, "video/av01/1"), (720, "video/av01/2"), (1080, "video/av01/3")),
        codec_string="av01.0.08M.08",
        forced_manifest_height=1080,
    )

    info = media.hls_info

    assert info["480_playlist"].endswith("/media-3/stream.m3u8")
    assert info["720_playlist"].endswith("/media-2/stream.m3u8")
    assert info["1080_playlist"].endswith("/media-1/stream.m3u8")

    assert info["hevc"]["480_playlist"].endswith("/hevc/video/hvc1/1/stream.m3u8")
    assert info["hevc"]["720_playlist"].endswith("/hevc/video/hvc1/2/stream.m3u8")
    assert info["hevc"]["1080_playlist"].endswith("/hevc/video/hvc1/3/stream.m3u8")

    assert info["av1"]["480_playlist"].endswith("/av1/video/av01/1/stream.m3u8")
    assert info["av1"]["720_playlist"].endswith("/av1/video/av01/2/stream.m3u8")
    assert info["av1"]["1080_playlist"].endswith("/av1/video/av01/3/stream.m3u8")


@pytest.mark.django_db
def test_hls_info_does_not_expose_missing_multicodec_masters(
    configure_media_paths,
    media_factory,
):
    configure_media_paths()
    media = media_factory(friendly_token="missingmasters")

    media.hls_hevc_file = "/tmp/does-not-exist/hevc/master.m3u8"
    media.hls_av1_file = "/tmp/does-not-exist/av1/master.m3u8"
    media.save(update_fields=["hls_hevc_file", "hls_av1_file"])

    info = media.hls_info

    assert info["hevc"]["master_file"].endswith("/tmp/does-not-exist/hevc/master.m3u8")
    assert info["av1"]["master_file"].endswith("/tmp/does-not-exist/av1/master.m3u8")
    assert "480_playlist" not in info["hevc"]
    assert "480_playlist" not in info["av1"]


def test_url_from_path_preserves_absolute_remote_hls_urls():
    assert url_from_path("https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"
    assert url_from_path("/https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"