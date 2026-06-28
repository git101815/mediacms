from pathlib import Path

import pytest

from files import tasks
from files.models import EncodeProfile


CODEC_CASES = (
    ("h265", "create_hls_hevc_fmp4", "hls_hevc_file", "hevc", (360, 720)),
    ("av1", "create_hls_av1_fmp4", "hls_av1_file", "av1", (480, 1080)),
)


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_fragment_same_basename_inputs_to_unique_temp_files(
    configure_hls_task_settings,
    media_factory,
    profile_factory,
    encoding_factory,
    monkeypatch,
    fake_successful_hls_run_command,
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

    monkeypatch.setattr(tasks, "run_command", fake_successful_hls_run_command)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    fragmented_outputs = fake_successful_hls_run_command.fragmented_outputs

    assert result is True
    assert getattr(media, db_field).endswith(f"/{fragment_prefix}/master.m3u8")
    assert len(fragmented_outputs) == len(resolutions)
    assert len(set(fragmented_outputs)) == len(resolutions)

    for resolution in resolutions:
        assert any(name.startswith(f"{fragment_prefix}-{resolution}-") for name in fragmented_outputs)


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
            return {"out": "", "error": "fragment failed"}

        raise AssertionError(f"mp4dash/cp should not be called after fragment failure: {cmd}")

    monkeypatch.setattr(tasks, "run_command", fragment_failure)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    assert result is False
    assert getattr(media, db_field) == ""


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("codec", "task_name", "db_field", "fragment_prefix", "resolutions"),
    CODEC_CASES,
)
def test_multicodec_hls_tasks_are_idempotent_when_master_already_exists(
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
    final_master.parent.mkdir(parents=True, exist_ok=True)
    final_master.write_text("#EXTM3U\n")

    def fail_run_command(cmd):
        raise AssertionError(f"run_command should not be called when master exists: {cmd}")

    monkeypatch.setattr(tasks, "run_command", fail_run_command)

    task = getattr(tasks, task_name)
    result = task.run(media.friendly_token)

    media.refresh_from_db()

    assert result is True
    assert getattr(media, db_field) == str(final_master)