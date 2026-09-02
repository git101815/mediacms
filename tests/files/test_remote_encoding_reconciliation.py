from copy import deepcopy
from datetime import timedelta

import pytest
from django.utils import timezone
from django.test import override_settings

from files.remote_encoding import build_runpod_status_url, sign_payload
from files.tasks import _apply_reconciled_runpod_output, reconcile_remote_encodings


@pytest.mark.django_db
@override_settings(REMOTE_ENCODING_CALLBACK_SECRET="test-runpod-callback-secret")
def test_reconciler_applies_signed_runpod_result(media_factory, profile_factory, encoding_factory):
    media = media_factory()
    profile = profile_factory(codec="h264", resolution=720)
    encoding = encoding_factory(media=media, profile=profile, status="running", progress=0)
    type(encoding).objects.filter(pk=encoding.pk).update(worker="runpod", task_id="job-1")

    payload = {
        "version": 3,
        "media_id": media.id,
        "friendly_token": media.friendly_token,
        "status": "fail",
        "mode": "fill_missing_profiles",
        "requested_encoding_ids": [encoding.id],
        "preserve_media_on_fail": True,
        "encodings": [],
        "skipped": [],
        "error": "worker reported failure",
    }
    output = deepcopy(payload)
    output["signature"] = sign_payload(payload)

    assert _apply_reconciled_runpod_output(media, output) is True
    encoding.refresh_from_db()
    media.refresh_from_db()
    assert encoding.status == "fail"
    assert media.listable is True


@pytest.mark.django_db
@override_settings(REMOTE_ENCODING_CALLBACK_SECRET="test-runpod-callback-secret")
def test_reconciler_rejects_unsigned_output(media_factory, profile_factory, encoding_factory):
    media = media_factory()
    profile = profile_factory(codec="h264", resolution=720)
    encoding = encoding_factory(media=media, profile=profile, status="running", progress=0)
    type(encoding).objects.filter(pk=encoding.pk).update(worker="runpod", task_id="job-2")

    output = {
        "version": 3,
        "media_id": media.id,
        "friendly_token": media.friendly_token,
        "status": "fail",
        "requested_encoding_ids": [encoding.id],
        "preserve_media_on_fail": True,
        "error": "unsigned",
    }

    assert _apply_reconciled_runpod_output(media, output) is False
    encoding.refresh_from_db()
    assert encoding.status == "running"


@override_settings(RUNPOD_ENDPOINT_URL="https://api.runpod.ai/v2/example/run")
def test_runpod_status_url_quotes_job_id():
    assert build_runpod_status_url("abc/def") == "https://api.runpod.ai/v2/example/status/abc%2Fdef"



@pytest.mark.django_db
@override_settings(REMOTE_ENCODING_ENABLED=True)
def test_completed_runpod_job_with_invalid_output_is_finalized_failed(
    media_factory, profile_factory, encoding_factory, monkeypatch
):
    media = media_factory()
    profile = profile_factory(codec="h264", resolution=720)
    encoding = encoding_factory(media=media, profile=profile, status="running", progress=0)
    type(encoding).objects.filter(pk=encoding.pk).update(worker="runpod", task_id="job-invalid")
    monkeypatch.setattr(
        "files.remote_encoding.get_runpod_job_status",
        lambda _job_id: {"status": "COMPLETED", "output": {"unsigned": True}},
    )
    result = reconcile_remote_encodings(limit=1)
    encoding.refresh_from_db()
    assert result["failed"] == 1
    assert encoding.status == "fail"
    assert "valid signed MediaCMS result" in encoding.logs

@pytest.mark.django_db
@override_settings(REMOTE_ENCODING_ENABLED=True)
def test_completed_runpod_job_with_invalid_applied_payload_is_finalized_failed(
    media_factory, profile_factory, encoding_factory, monkeypatch
):
    media = media_factory()
    profile = profile_factory(codec="h264", resolution=720)
    encoding = encoding_factory(media=media, profile=profile, status="running", progress=0)
    type(encoding).objects.filter(pk=encoding.pk).update(
        worker="runpod", task_id="job-invalid-applied"
    )
    monkeypatch.setattr(
        "files.remote_encoding.get_runpod_job_status",
        lambda _job_id: {"status": "COMPLETED", "output": {"signed": True}},
    )

    def invalid_result(_media, _output):
        raise ValueError("Media mismatch")

    monkeypatch.setattr("files.tasks._apply_reconciled_runpod_output", invalid_result)

    result = reconcile_remote_encodings(limit=1)
    encoding.refresh_from_db()

    assert result["checked"] == 1
    assert result["failed"] == 1
    assert encoding.status == "fail"
    assert "invalid MediaCMS result" in encoding.logs



@pytest.mark.django_db
@override_settings(REMOTE_ENCODING_ENABLED=True)
def test_reconciler_rotates_checked_running_jobs_by_update_date(
    media_factory, profile_factory, encoding_factory, monkeypatch
):
    profile = profile_factory(codec="h264", resolution=720)
    media_old = media_factory()
    media_next = media_factory()
    old = encoding_factory(media=media_old, profile=profile, status="running", progress=0)
    next_encoding = encoding_factory(
        media=media_next, profile=profile, status="running", progress=0
    )

    now = timezone.now()
    type(old).objects.filter(pk=old.pk).update(
        worker="runpod",
        task_id="job-oldest",
        update_date=now - timedelta(hours=2),
    )
    type(next_encoding).objects.filter(pk=next_encoding.pk).update(
        worker="runpod",
        task_id="job-next",
        update_date=now - timedelta(hours=1),
    )

    calls = []

    def running_status(job_id):
        calls.append(job_id)
        return {"status": "RUNNING"}

    monkeypatch.setattr("files.remote_encoding.get_runpod_job_status", running_status)

    first = reconcile_remote_encodings(limit=1)
    second = reconcile_remote_encodings(limit=1)

    assert first["checked"] == 1
    assert second["checked"] == 1
    assert calls == ["job-oldest", "job-next"]
