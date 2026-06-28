import shutil
from pathlib import Path

import pytest

from files.models import EncodeProfile, Encoding, Media

@pytest.fixture
def media_factory(django_user_model):
    def _create_media(*, friendly_token="multicodechls", video_height=1080):
        user = django_user_model.objects.create_user(username=f"user_{friendly_token}")

        Media.objects.bulk_create(
            [
                Media(
                    user=user,
                    friendly_token=friendly_token,
                    title=friendly_token,
                    media_file=f"tests/{friendly_token}.mp4",
                    media_type="video",
                    video_height=video_height,
                    state="public",
                    encoding_status="success",
                    is_reviewed=True,
                    listable=True,
                )
            ]
        )

        return Media.objects.get(friendly_token=friendly_token)

    return _create_media


@pytest.fixture
def profile_factory():
    def _create_profile(*, codec, resolution, active=True, extension="mp4", name=None):
        return EncodeProfile.objects.create(
            name=name or f"{codec or extension}-{resolution or 'na'}",
            extension=extension,
            resolution=resolution,
            codec=codec,
            active=active,
        )

    return _create_profile


@pytest.fixture
def encoding_factory():
    def _create_encoding(
        *,
        media,
        profile,
        media_file=None,
        chunk=False,
        status="success",
        progress=100,
    ):
        if media_file is None:
            media_file = f"encoded/{profile.codec}/{profile.resolution}/same-output-name.mp4"

        return Encoding.objects.create(
            media=media,
            profile=profile,
            media_file=media_file,
            chunk=chunk,
            status=status,
            progress=progress,
        )

    return _create_encoding


@pytest.fixture
def configure_media_paths(settings, tmp_path):
    def _configure():
        media_root = tmp_path / "media"
        media_root.mkdir(parents=True, exist_ok=True)

        settings.MEDIA_ROOT = str(media_root)
        settings.MEDIA_URL = "https://media.example/mediafiles/"

        return media_root

    return _configure


@pytest.fixture
def configure_hls_task_settings(settings, configure_media_paths, tmp_path):
    def _configure():
        media_root = configure_media_paths()

        temp_root = tmp_path / "tmp"
        bin_root = tmp_path / "bin"
        hls_root = media_root / "hls"

        temp_root.mkdir(parents=True, exist_ok=True)
        bin_root.mkdir(parents=True, exist_ok=True)
        hls_root.mkdir(parents=True, exist_ok=True)

        mp4dash = bin_root / "mp4dash"
        mp4fragment = bin_root / "mp4fragment"
        mp4dash.write_text("#!/bin/sh\n")
        mp4fragment.write_text("#!/bin/sh\n")

        settings.HLS_DIR = str(hls_root)
        settings.TEMP_DIRECTORY = str(temp_root)
        settings.MP4DASH_COMMAND = str(mp4dash)
        settings.MP4FRAGMENT_COMMAND = str(mp4fragment)
        settings.MINIMUM_RESOLUTIONS_TO_ENCODE = []

        return media_root, hls_root

    return _configure


@pytest.fixture
def write_hls_master():
    def _write(master_path, renditions, *, codec_string, forced_manifest_height=None):
        master_path = Path(master_path)
        master_path.parent.mkdir(parents=True, exist_ok=True)

        lines = ["#EXTM3U", "#EXT-X-VERSION:7"]

        for height, uri_prefix in renditions:
            playlist_path = master_path.parent / uri_prefix / "stream.m3u8"
            iframe_path = master_path.parent / uri_prefix / "iframes.m3u8"

            playlist_path.parent.mkdir(parents=True, exist_ok=True)
            playlist_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n")
            iframe_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n")

            manifest_height = forced_manifest_height or height
            manifest_width = int(round(manifest_height * 16 / 9))

            lines.append(
                f'#EXT-X-STREAM-INF:BANDWIDTH={manifest_height * 1000},'
                f'RESOLUTION={manifest_width}x{manifest_height},CODECS="{codec_string}"'
            )
            lines.append(f"{uri_prefix}/stream.m3u8")
            lines.append(
                f'#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH={manifest_height * 500},'
                f'RESOLUTION={manifest_width}x{manifest_height},CODECS="{codec_string}",'
                f'URI="{uri_prefix}/iframes.m3u8"'
            )

        master_path.write_text("\n".join(lines) + "\n")

    return _write


@pytest.fixture
def fake_successful_hls_run_command(settings):
    fragmented_outputs = []

    def _run_command(cmd):
        if cmd[0] == settings.MP4FRAGMENT_COMMAND:
            output_path = Path(cmd[2])
            output_path.write_bytes(b"fragmented")
            fragmented_outputs.append(output_path.name)
            return {"out": "", "error": ""}

        if cmd[0] == settings.MP4DASH_COMMAND:
            output_dir = Path(cmd[cmd.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            (output_dir / "master.m3u8").write_text("#EXTM3U\n")
            return {"out": "", "error": ""}

        if cmd[0] == "cp":
            shutil.copytree(cmd[2], cmd[3], dirs_exist_ok=True)
            return {"out": "", "error": ""}

        raise AssertionError(f"Unexpected command: {cmd}")

    _run_command.fragmented_outputs = fragmented_outputs
    return _run_command
