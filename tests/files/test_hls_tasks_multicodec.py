import shutil
from pathlib import Path

import pytest

from files import tasks
from files.models import EncodeProfile, MediaHLSRendition


CODEC_CASES = (
    ("h265", "create_hls_hevc_fmp4", "hls_hevc_file", "hevc", (360, 720)),
    ("av1", "create_hls_av1_fmp4", "hls_av1_file", "av1", (480, 1080)),
)


def _write_master(master_path, resolutions, fragment_prefix, codec_string):
    master_path = Path(master_path)
    master_path.parent.mkdir(parents=True, exist_ok=True)

    lines = ["#EXTM3U", "#EXT-X-VERSION:7"]

    for resolution in resolutions:
        uri_prefix = f"video/{fragment_prefix}/{resolution}"
        playlist_path = master_path.parent / uri_prefix / "stream.m3u8"
        iframe_path = master_path.parent / uri_prefix / "iframes.m3u8"

        playlist_path.parent.mkdir(parents=True, exist_ok=True)
        playlist_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")
        iframe_path.write_text("#EXTM3U\n#EXT-X-ENDLIST\n", encoding="utf-8")

        width = int(round(resolution * 16 / 9))

        lines.append(
            f'#EXT-X-STREAM-INF:BANDWIDTH={resolution * 1000},'
            f'RESOLUTION={width}x{resolution},CODECS="{codec_string}"'
        )
        lines.append(f"{uri_prefix}/stream.m3u8")
        lines.append(
            f'#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH={resolution * 500},'
            f'RESOLUTION={width}x{resolution},CODECS="{codec_string}",'
            f'URI="{uri_prefix}/iframes.m3u8"'
        )

    master_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fake_successful_run_command(settings, resolutions, fragment_prefix, codec_string):
    fragmented_outputs = []

    def run_command(cmd):
        if cmd[0] == settings.MP4FRAGMENT_COMMAND:
            output_path = Path(cmd[2])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"fragmented")
            fragmented_outputs.append(output_path.name)

            return {"out": "", "error": "", "returncode": 0}

        if cmd[0] == settings.MP4DASH_COMMAND:
            output_dir = Path(cmd[cmd.index("--output-dir") + 1])
            _write_master(
                output_dir / "master.m3u8",
                resolutions,
                fragment_prefix,
                codec_string,
            )

            return {"out": "", "error": "", "returncode": 0}

        if cmd[0] == "cp":
            shutil.copytree(cmd[2], cmd[3], dirs_exist_ok=True)

            return {"out": "", "error": "", "returncode": 0}

        raise AssertionError(f"Unexpected command: {cmd}")

    run_command.fragmented_outputs = fragmented_outputs

    return run_command


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_fragment_same_basename_inputs_to_unique_temp_files_and_index_renditions(
    configure_hls_task_settings,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    settings,
    codec,
    task_name,
    db_field,
    fragment_prefix,
    resolutions,
):
    EncodeProfile.objects.update(active=False)

    media_root, _hls_root = configure_hls_task_settings()
    media = media_factory(friendly_token=f"{fragment_prefix}frags")

    for resolution in resolutions:
        profile = profile_factory(codec=codec, resolution=resolution)

        rel_path = f"encoded/{codec}/{resolution}/same-output-name.mp4"
        full_path = media_root / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(f"{codec}-{resolution}".encode("utf-8"))

        encoding_factory(media=media, profile=profile, media_file=rel_path)

    codec_string = "hvc1.1.6.L93.B0,mp4a.40.2" if codec == "h265" else "av01.0.08M.08,mp4a.40.2"
    fake_run_command = _fake_successful_run_command(
        settings,
        resolutions=resolutions,
        fragment_prefix=fragment_prefix,
        codec_string=codec_string,
    )

    monkeypatch.setattr(tasks, "run_command", fake_run_command)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    fragmented_outputs = fake_run_command.fragmented_outputs

    assert result is True
    assert getattr(media, db_field) == f"hls/{media.uid.hex}/{fragment_prefix}/master.m3u8"
    assert len(fragmented_outputs) == len(resolutions)
    assert len(set(fragmented_outputs)) == len(resolutions)

    for resolution in resolutions:
        assert any(name.startswith(f"{fragment_prefix}-{resolution}-") for name in fragmented_outputs)

    indexed = list(
        MediaHLSRendition.objects.filter(media=media, codec=codec).order_by("resolution")
    )

    assert [row.resolution for row in indexed] == list(resolutions)

    for row in indexed:
        assert row.master_file == f"hls/{media.uid.hex}/{fragment_prefix}/master.m3u8"
        assert row.playlist_file == (
            f"hls/{media.uid.hex}/{fragment_prefix}/video/{fragment_prefix}/{row.resolution}/stream.m3u8"
        )
        assert row.iframe_file == (
            f"hls/{media.uid.hex}/{fragment_prefix}/video/{fragment_prefix}/{row.resolution}/iframes.m3u8"
        )


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_wait_for_all_active_renditions_before_writing_master(
    configure_hls_task_settings,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    codec,
    task_name,
    db_field,
    fragment_prefix,
    resolutions,
):
    EncodeProfile.objects.update(active=False)

    media_root, _hls_root = configure_hls_task_settings()
    media = media_factory(friendly_token=f"{codec}missing")

    profiles = {resolution: profile_factory(codec=codec, resolution=resolution) for resolution in resolutions}

    for resolution in resolutions[:-1]:
        rel_path = f"encoded/{codec}/{resolution}/same-output-name.mp4"
        full_path = media_root / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(f"{codec}-{resolution}".encode("utf-8"))

        encoding_factory(media=media, profile=profiles[resolution], media_file=rel_path)

    def fail_run_command(cmd):
        raise AssertionError(f"run_command should not be called while a rendition is missing: {cmd}")

    monkeypatch.setattr(tasks, "run_command", fail_run_command)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    assert result is False
    assert getattr(media, db_field) == ""
    assert MediaHLSRendition.objects.filter(media=media, codec=codec).count() == 0


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_do_not_write_db_field_after_fragment_failure(
    configure_hls_task_settings,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    codec,
    task_name,
    db_field,
    fragment_prefix,
    resolutions,
):
    EncodeProfile.objects.update(active=False)

    media_root, _hls_root = configure_hls_task_settings()
    media = media_factory(friendly_token=f"{codec}fragmentfail")

    for resolution in resolutions:
        profile = profile_factory(codec=codec, resolution=resolution)

        rel_path = f"encoded/{codec}/{resolution}/same-output-name.mp4"
        full_path = media_root / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(f"{codec}-{resolution}".encode("utf-8"))

        encoding_factory(media=media, profile=profile, media_file=rel_path)

    def fragment_failure(cmd):
        if cmd[0].endswith("mp4fragment"):
            return {"out": "", "error": "fragment failed", "returncode": 1}

        raise AssertionError(f"mp4dash/cp should not be called after fragment failure: {cmd}")

    monkeypatch.setattr(tasks, "run_command", fragment_failure)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    assert result is False
    assert getattr(media, db_field) == ""
    assert MediaHLSRendition.objects.filter(media=media, codec=codec).count() == 0


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_reuse_existing_valid_master_and_index_it_without_regeneration(
    configure_hls_task_settings,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    codec,
    task_name,
    db_field,
    fragment_prefix,
    resolutions,
):
    EncodeProfile.objects.update(active=False)

    media_root, hls_root = configure_hls_task_settings()
    media = media_factory(friendly_token=f"{codec}idempotent")

    for resolution in resolutions:
        profile = profile_factory(codec=codec, resolution=resolution)

        rel_path = f"encoded/{codec}/{resolution}/same-output-name.mp4"
        full_path = media_root / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(f"{codec}-{resolution}".encode("utf-8"))

        encoding_factory(media=media, profile=profile, media_file=rel_path)

    final_master = Path(hls_root) / media.uid.hex / fragment_prefix / "master.m3u8"
    codec_string = "hvc1.1.6.L93.B0,mp4a.40.2" if codec == "h265" else "av01.0.08M.08,mp4a.40.2"

    _write_master(
        final_master,
        resolutions=resolutions,
        fragment_prefix=fragment_prefix,
        codec_string=codec_string,
    )

    def fail_run_command(cmd):
        raise AssertionError(f"run_command should not be called when existing master is valid: {cmd}")

    monkeypatch.setattr(tasks, "run_command", fail_run_command)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    assert result is True
    assert getattr(media, db_field) == f"hls/{media.uid.hex}/{fragment_prefix}/master.m3u8"

    indexed = list(
        MediaHLSRendition.objects.filter(media=media, codec=codec).order_by("resolution")
    )

    assert [row.resolution for row in indexed] == list(resolutions)


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_regenerate_existing_stale_master(
    configure_hls_task_settings,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    settings,
    codec,
    task_name,
    db_field,
    fragment_prefix,
    resolutions,
):
    EncodeProfile.objects.update(active=False)

    media_root, hls_root = configure_hls_task_settings()
    media = media_factory(friendly_token=f"{codec}stale")

    for resolution in resolutions:
        profile = profile_factory(codec=codec, resolution=resolution)

        rel_path = f"encoded/{codec}/{resolution}/same-output-name.mp4"
        full_path = media_root / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_bytes(f"{codec}-{resolution}".encode("utf-8"))

        encoding_factory(media=media, profile=profile, media_file=rel_path)

    stale_master = Path(hls_root) / media.uid.hex / fragment_prefix / "master.m3u8"
    codec_string = "hvc1.1.6.L93.B0,mp4a.40.2" if codec == "h265" else "av01.0.08M.08,mp4a.40.2"

    _write_master(
        stale_master,
        resolutions=[resolutions[0]],
        fragment_prefix=fragment_prefix,
        codec_string=codec_string,
    )

    fake_run_command = _fake_successful_run_command(
        settings,
        resolutions=resolutions,
        fragment_prefix=fragment_prefix,
        codec_string=codec_string,
    )

    monkeypatch.setattr(tasks, "run_command", fake_run_command)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    assert result is True
    assert getattr(media, db_field) == f"hls/{media.uid.hex}/{fragment_prefix}/master.m3u8"

    indexed = list(
        MediaHLSRendition.objects.filter(media=media, codec=codec).order_by("resolution")
    )

    assert [row.resolution for row in indexed] == list(resolutions)
    assert len(fake_run_command.fragmented_outputs) == len(resolutions)