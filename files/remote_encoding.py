import hashlib
import hmac
import json
from urllib.request import Request, urlopen

from django.conf import settings
from django.core.exceptions import ValidationError
from django.urls import reverse

from files.models import EncodeProfile


def remote_encoding_enabled():
    return bool(settings.REMOTE_ENCODING_ENABLED)


def _callback_secret():
    return settings.REMOTE_ENCODING_CALLBACK_SECRET


def sign_payload(payload):
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hmac.new(
        _callback_secret().encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()


def verify_signature(payload, signature):
    return hmac.compare_digest(sign_payload(payload), signature or "")


def _normalized_prefix(value):
    return "/".join(part for part in str(value or "").split("/") if part)


def build_source_url(media):
    return (
        settings.REMOTE_ENCODING_SOURCE_BASE_URL.rstrip("/")
        + "/"
        + media.media_file.name.lstrip("/")
    )


def build_callback_url(media):
    return settings.FRONTEND_HOST.rstrip("/") + reverse(
        "remote_encoding_callback",
        kwargs={"friendly_token": media.friendly_token},
    )


def get_remote_profiles_for_media(media):
    profiles = []
    enabled_codecs = tuple(getattr(settings, "ENABLED_ENCODING_CODECS", ("h264",)))

    for profile in EncodeProfile.objects.filter(active=True).order_by("resolution"):
        if profile.extension == "gif":
            continue

        if profile.codec not in enabled_codecs:
            continue

        if media.video_height and media.video_height < profile.resolution:
            if profile.resolution not in settings.MINIMUM_RESOLUTIONS_TO_ENCODE:
                continue

        profiles.append(
            {
                "id": int(profile.id),
                "name": profile.name,
                "codec": profile.codec,
                "extension": profile.extension,
                "resolution": int(profile.resolution),
            }
        )

    return profiles


def build_runpod_payload(media):
    profiles = get_remote_profiles_for_media(media)
    if not profiles:
        raise ValidationError("No remote encoding profiles available")

    payload = {
        "version": 1,
        "media_id": media.id,
        "media_uid": media.uid.hex,
        "friendly_token": media.friendly_token,
        "username": media.user.username,
        "source_name": media.media_file.name.split("/")[-1],
        "source_url": build_source_url(media),
        "callback_url": build_callback_url(media),
        "public_base_url": settings.REMOTE_ENCODING_PUBLIC_BASE_URL.rstrip("/"),
        "output_prefix": f"{settings.REMOTE_ENCODING_OUTPUT_PREFIX.strip('/')}/{media.uid.hex}",
        "encoded_output_prefix": _normalized_prefix(settings.MEDIA_ENCODING_DIR),
        "thumbnail_output_prefix": _normalized_prefix(settings.THUMBNAIL_UPLOAD_DIR),
        "segment_seconds": int(settings.REMOTE_ENCODING_HLS_SEGMENT_SECONDS),
        "sprite_seconds": int(getattr(settings, "SPRITE_NUM_SECS", 10)),
        "profiles": profiles,
    }

    payload["signature"] = sign_payload(payload)
    return payload


def submit_runpod_job(media):
    if not settings.RUNPOD_ENDPOINT_URL:
        raise ValidationError("RUNPOD_ENDPOINT_URL is not configured")

    request_payload = {
        "input": build_runpod_payload(media),
        "policy": {
            "executionTimeout": int(settings.RUNPOD_EXECUTION_TIMEOUT_MS),
            "ttl": int(settings.RUNPOD_JOB_TTL_MS),
        },
    }
    body = json.dumps(request_payload).encode("utf-8")

    request = Request(
        settings.RUNPOD_ENDPOINT_URL,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings.RUNPOD_API_KEY}",
        },
        method="POST",
    )

    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))