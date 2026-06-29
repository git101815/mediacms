from pathlib import Path

import pytest

from files.models import MediaHLSRendition


@pytest.mark.django_db
def test_media_hls_rendition_builds_from_h264_master(
    configure_media_paths,
    media_factory,
    write_hls_master,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="h264master")

    master = media_root / "hls" / media.uid.hex / "master.m3u8"
    write_hls_master(
        master,
        [
            (480, "media-1"),
            (720, "media-2"),
            (1080, "media-3"),
        ],
        codec_string="avc1.4d401f,mp4a.40.2",
    )

    rows = MediaHLSRendition.replace_from_master(
        media=media,
        codec="h264",
        master_file=str(master),
        expected_resolutions=[480, 720, 1080],
    )

    assert [row.resolution for row in rows] == [480, 720, 1080]

    db_rows = list(MediaHLSRendition.objects.filter(media=media, codec="h264").order_by("resolution"))
    assert [row.resolution for row in db_rows] == [480, 720, 1080]
    assert db_rows[0].master_file == f"hls/{media.uid.hex}/master.m3u8"
    assert db_rows[0].playlist_file == f"hls/{media.uid.hex}/media-1/stream.m3u8"
    assert db_rows[0].iframe_file == f"hls/{media.uid.hex}/media-1/iframes.m3u8"


@pytest.mark.django_db
def test_media_hls_rendition_builds_from_single_hevc_master(
    configure_media_paths,
    media_factory,
    write_hls_master,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="singlehevc")

    master = media_root / "hls" / media.uid.hex / "hevc" / "master.m3u8"
    write_hls_master(
        master,
        [
            (480, "video/hvc1"),
        ],
        codec_string="hvc1.1.6.L93.B0,mp4a.40.2",
    )

    rows = MediaHLSRendition.replace_from_master(
        media=media,
        codec="h265",
        master_file=str(master),
        expected_resolutions=[480],
    )

    assert len(rows) == 1
    assert rows[0].resolution == 480
    assert rows[0].master_file == f"hls/{media.uid.hex}/hevc/master.m3u8"
    assert rows[0].playlist_file == f"hls/{media.uid.hex}/hevc/video/hvc1/stream.m3u8"
    assert rows[0].iframe_file == f"hls/{media.uid.hex}/hevc/video/hvc1/iframes.m3u8"


@pytest.mark.django_db
def test_media_hls_rendition_rejects_stale_hevc_master(
    configure_media_paths,
    media_factory,
    write_hls_master,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="stalehevc")

    master = media_root / "hls" / media.uid.hex / "hevc" / "master.m3u8"
    write_hls_master(
        master,
        [
            (480, "video/hvc1"),
        ],
        codec_string="hvc1.1.6.L93.B0,mp4a.40.2",
    )

    with pytest.raises(ValueError, match="expected=\\[480, 720, 1080\\] found=\\[480\\]"):
        MediaHLSRendition.replace_from_master(
            media=media,
            codec="h265",
            master_file=str(master),
            expected_resolutions=[480, 720, 1080],
        )

    assert MediaHLSRendition.objects.filter(media=media, codec="h265").count() == 0


@pytest.mark.django_db
def test_media_hls_rendition_replaces_existing_rows_atomically(
    configure_media_paths,
    media_factory,
    write_hls_master,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="replacehls")

    MediaHLSRendition.objects.create(
        media=media,
        codec="h264",
        resolution=480,
        master_file=f"hls/{media.uid.hex}/old/master.m3u8",
        playlist_file=f"hls/{media.uid.hex}/old/480.m3u8",
    )

    master = media_root / "hls" / media.uid.hex / "master.m3u8"
    write_hls_master(
        master,
        [
            (720, "media-1"),
            (1080, "media-2"),
        ],
        codec_string="avc1.4d401f,mp4a.40.2",
    )

    MediaHLSRendition.replace_from_master(
        media=media,
        codec="h264",
        master_file=str(master),
        expected_resolutions=[720, 1080],
    )

    rows = list(MediaHLSRendition.objects.filter(media=media, codec="h264").order_by("resolution"))

    assert [row.resolution for row in rows] == [720, 1080]
    assert all("/old/" not in row.playlist_file for row in rows)


@pytest.mark.django_db
def test_media_hls_rendition_builds_from_remote_payload_with_absolute_urls(
    configure_media_paths,
    media_factory,
):
    configure_media_paths()
    media = media_factory(friendly_token="payloadhls")

    rows = MediaHLSRendition.replace_from_payload(
        media=media,
        codec="av1",
        master_file="https://cdn.example/hls/uid/av1/master.m3u8",
        renditions=[
            {
                "resolution": 480,
                "width": 853,
                "height": 480,
                "playlist_url": "https://cdn.example/hls/uid/av1/480/stream.m3u8",
                "bandwidth": 900000,
                "codecs": "av01.0.08M.08",
            },
            {
                "resolution": 1080,
                "width": 1920,
                "height": 1080,
                "playlist_uri": "1080/stream.m3u8",
                "bandwidth": 3200000,
                "codecs": "av01.0.08M.08",
            },
        ],
    )

    assert [row.resolution for row in rows] == [480, 1080]

    db_rows = list(MediaHLSRendition.objects.filter(media=media, codec="av1").order_by("resolution"))

    assert db_rows[0].master_file == "https://cdn.example/hls/uid/av1/master.m3u8"
    assert db_rows[0].playlist_file == "https://cdn.example/hls/uid/av1/480/stream.m3u8"
    assert db_rows[1].playlist_file == "https://cdn.example/hls/uid/av1/1080/stream.m3u8"


@pytest.mark.django_db
def test_media_hls_rendition_payload_rejects_duplicate_resolution(
    configure_media_paths,
    media_factory,
):
    configure_media_paths()
    media = media_factory(friendly_token="dupepayload")

    with pytest.raises(ValueError, match="Duplicate HLS rendition"):
        MediaHLSRendition.replace_from_payload(
            media=media,
            codec="av1",
            master_file="https://cdn.example/hls/uid/av1/master.m3u8",
            renditions=[
                {
                    "resolution": 480,
                    "playlist_uri": "480/stream.m3u8",
                },
                {
                    "resolution": 480,
                    "playlist_uri": "480-copy/stream.m3u8",
                },
            ],
        )


@pytest.mark.django_db
def test_media_hls_rendition_storage_path_normalizes_media_root_paths(
    configure_media_paths,
    media_factory,
):
    media_root = configure_media_paths()
    media = media_factory(friendly_token="storagepath")

    absolute = str(Path(media_root) / "hls" / media.uid.hex / "master.m3u8")

    assert MediaHLSRendition.storage_path(absolute) == f"hls/{media.uid.hex}/master.m3u8"
    assert MediaHLSRendition.storage_path("https://cdn.example/hls/master.m3u8") == (
        "https://cdn.example/hls/master.m3u8"
    )
    assert MediaHLSRendition.storage_path("/https://cdn.example/hls/master.m3u8") == (
        "https://cdn.example/hls/master.m3u8"
    )