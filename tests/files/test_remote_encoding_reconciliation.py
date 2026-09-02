from copy import deepcopy

import pytest
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
