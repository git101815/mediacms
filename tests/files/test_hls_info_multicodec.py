import pytest

from files.helpers import url_from_path


@pytest.mark.django_db
def test_hls_info_maps_available_multicodec_playlists_from_existing_encodings(
    settings,
    configure_media_paths,
    media_factory,
    profile_factory,
    encoding_factory,
    write_hls_master,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="hlsinfodynamic")

    codec_cases = {
        "h265": {
            "field": "hls_hevc_file",
            "group": "hevc",
            "codec_string": "hvc1.1.6.L120.90",
            "renditions": ((360, "video/hvc1/1"), (720, "video/hvc1/2")),
        },
        "av1": {
            "field": "hls_av1_file",
            "group": "av1",
            "codec_string": "av01.0.08M.08",
            "renditions": ((480, "video/av01/1"), (1080, "video/av01/2")),
        },
    }

    for codec, case in codec_cases.items():
        for resolution, _uri_prefix in case["renditions"]:
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
        ((720, "media-720"), (360, "media-360")),
        codec_string="avc1.640028",
    )

    for case in codec_cases.values():
        write_hls_master(
            getattr(media, case["field"]),
            case["renditions"],
            codec_string=case["codec_string"],
            forced_manifest_height=1080,
        )

    info = media.hls_info

    assert info["720_playlist"].endswith("/media-720/stream.m3u8")
    assert info["360_playlist"].endswith("/media-360/stream.m3u8")

    for case in codec_cases.values():
        group = info[case["group"]]

        for resolution, uri_prefix in case["renditions"]:
            assert group[f"{resolution}_playlist"].endswith(f"/{case['group']}/{uri_prefix}/stream.m3u8")
            assert group[f"{resolution}_iframe"].endswith(f"/{case['group']}/{uri_prefix}/iframes.m3u8")


@pytest.mark.django_db
def test_hls_info_does_not_invent_resolution_playlists_when_multicodec_master_is_missing(
    configure_media_paths,
    media_factory,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="missingmasters")

    media.hls_hevc_file = str(media_root / "missing" / "hevc" / "master.m3u8")
    media.hls_av1_file = str(media_root / "missing" / "av1" / "master.m3u8")
    media.save(update_fields=["hls_hevc_file", "hls_av1_file"])

    info = media.hls_info

    assert info["hevc"]["master_file"].endswith("/missing/hevc/master.m3u8")
    assert info["av1"]["master_file"].endswith("/missing/av1/master.m3u8")
    assert "360_playlist" not in info["hevc"]
    assert "480_playlist" not in info["hevc"]
    assert "720_playlist" not in info["hevc"]
    assert "1080_playlist" not in info["hevc"]
    assert "360_playlist" not in info["av1"]
    assert "480_playlist" not in info["av1"]
    assert "720_playlist" not in info["av1"]
    assert "1080_playlist" not in info["av1"]


def test_url_from_path_preserves_absolute_remote_hls_urls():
    assert url_from_path("https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"
    assert url_from_path("/https://cdn.example/video/master.m3u8") == "https://cdn.example/video/master.m3u8"