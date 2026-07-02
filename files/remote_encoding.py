import hashlib
import hmac
import json
from urllib.request import Request, urlopen

from django.conf import settings
from django.core.exceptions import ValidationError
from django.urls import reverse

from files.helpers import (
    AUDIO_BITRATES,
    AUDIO_ENCODERS,
    BUF_SIZE_MULTIPLIER,
    CRF_ENCODING_NUM_SECONDS,
    KEYFRAME_DISTANCE,
    MAX_RATE_MULTIPLIER,
    MIN_RATE_MULTIPLIER,
    VIDEO_BITRATES,
    VIDEO_CRFS,
    VIDEO_PROFILES,
)


def remote_encoding_enabled():
    return bool(getattr(settings, "REMOTE_ENCODING_ENABLED", False))


def _callback_secret():
    return settings.REMOTE_ENCODING_CALLBACK_SECRET


def sign_payload(payload):
    return hmac.new(
        _callback_secret().encode("utf-8"),
        _canonical_body(payload),
        hashlib.sha256,
    ).hexdigest()


def verify_signature(payload, signature):
    return hmac.compare_digest(sign_payload(payload), signature or "")


def _normalized_prefix(value):
    return "/".join(part for part in str(value or "").split("/") if part)


def _join_key(*parts):
    return "/".join(str(part).strip("/") for part in parts if str(part).strip("/"))


def _canonical_payload(value):
    return json.loads(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _canonical_body(payload):
    return json.dumps(
        _canonical_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _source_name(media):
    return media.media_file.name.split("/")[-1]


def build_source_url(media):
    return (
        settings.REMOTE_ENCODING_SOURCE_BASE_URL.rstrip("/")
        + "/"
        + media.media_file.name.lstrip("/")
    )


def build_source_object(media):
    return {
        "type": "s3",
        "bucket": getattr(
            settings,
            "REMOTE_ENCODING_SOURCE_BUCKET",
            getattr(settings, "AWS_STORAGE_BUCKET_NAME", "mediafiles"),
        ),
        "key": media.media_file.name,
        "endpoint_url": getattr(
            settings,
            "REMOTE_ENCODING_SOURCE_ENDPOINT_URL",
            getattr(settings, "AWS_S3_ENDPOINT_URL", "https://gateway.storjshare.io"),
        ),
        "region_name": getattr(settings, "REMOTE_ENCODING_SOURCE_REGION_NAME", "auto"),
        "addressing_style": getattr(settings, "REMOTE_ENCODING_SOURCE_ADDRESSING_STYLE", "path"),
    }


def build_callback_url(media):
    return settings.FRONTEND_HOST.rstrip("/") + reverse(
        "remote_encoding_callback",
        kwargs={"friendly_token": media.friendly_token},
    )


def _encoding_output_key(media, profile):
    source_name = _source_name(media)
    output_name = f"{source_name}.{profile.extension}"
    filename = f"{media.uid.hex}.{output_name}"

    return _join_key(
        _normalized_prefix(settings.MEDIA_ENCODING_DIR),
        profile.id,
        media.user.username,
        filename,
    )


def _asset_keys(media):
    source_name = _source_name(media)
    root = _normalized_prefix(settings.THUMBNAIL_UPLOAD_DIR)
    username = media.user.username

    return {
        "thumbnail_key": _join_key(root, "user", username, f"{source_name}.jpg"),
        "poster_key": _join_key(root, "user", username, f"{source_name}.poster.jpg"),
        "sprites_key": _join_key(root, "user", username, f"{source_name}sprites.jpg"),
    }


def _remote_encoder_map():
    return getattr(
        settings,
        "REMOTE_ENCODING_ENCODERS",
        {
            "h264": "h264_nvenc",
            "h265": "hevc_nvenc",
            "av1": "av1_nvenc",
        },
    )


def _remote_encoder_presets():
    return getattr(
        settings,
        "REMOTE_ENCODING_ENCODER_PRESETS",
        {
            "h264_nvenc": "p5",
            "hevc_nvenc": "p5",
            "av1_nvenc": getattr(settings, "AV1_NVENC_PRESET", "p5"),
            "libx264": getattr(settings, "FFMPEG_DEFAULT_PRESET", "medium"),
            "libx265": getattr(settings, "FFMPEG_DEFAULT_PRESET", "medium"),
            "libsvtav1": str(getattr(settings, "SVT_AV1_PRESET", 8)),
        },
    )


def build_encoding_policy():
    return {
        "version": 2,
        "ffmpeg": "ffmpeg",
        "ffprobe": "ffprobe",
        "crf_encoding_num_seconds": CRF_ENCODING_NUM_SECONDS,
        "max_rate_multiplier": MAX_RATE_MULTIPLIER,
        "min_rate_multiplier": MIN_RATE_MULTIPLIER,
        "buf_size_multiplier": BUF_SIZE_MULTIPLIER,
        "keyframe_distance": KEYFRAME_DISTANCE,
        "video_crfs": VIDEO_CRFS,
        "video_bitrates": VIDEO_BITRATES,
        "audio_encoders": AUDIO_ENCODERS,
        "audio_bitrates": AUDIO_BITRATES,
        "video_profiles": VIDEO_PROFILES,
        "minimum_resolutions_to_encode": [
            int(resolution)
            for resolution in getattr(settings, "MINIMUM_RESOLUTIONS_TO_ENCODE", [])
        ],
        "enabled_codecs": list(getattr(settings, "ENABLED_ENCODING_CODECS", ("h264",))),
        "default_preset": getattr(settings, "FFMPEG_DEFAULT_PRESET", "medium"),
        "svt_av1_preset": int(getattr(settings, "SVT_AV1_PRESET", 8)),
        "av1_nvenc_preset": getattr(settings, "AV1_NVENC_PRESET", "p5"),
        "remote_encoder_map": _remote_encoder_map(),
        "remote_encoder_presets": _remote_encoder_presets(),
    }


def get_remote_candidate_profiles(media):
    from files.models import EncodeProfile

    enabled_codecs = tuple(getattr(settings, "ENABLED_ENCODING_CODECS", ("h264",)))
    meaningful_height = int(media.video_height or 0) > 1

    profiles = []

    qs = EncodeProfile.objects.filter(active=True).order_by("resolution", "id")

    for profile in qs:
        if profile.extension not in ("mp4", "gif"):
            continue

        if profile.extension != "gif" and profile.codec not in enabled_codecs:
            continue

        if profile.extension != "gif" and meaningful_height:
            if media.video_height < profile.resolution:
                if profile.resolution not in settings.MINIMUM_RESOLUTIONS_TO_ENCODE:
                    continue

        profiles.append(profile)

    return profiles


def _prepare_remote_encoding(media, profile):
    from files.models import Encoding

    qs = list(
        Encoding.objects.filter(
            media=media,
            profile=profile,
            chunk=False,
        ).order_by("id")
    )

    if qs:
        encoding = qs[0]
        duplicate_ids = [item.id for item in qs[1:]]

        if duplicate_ids:
            Encoding.objects.filter(id__in=duplicate_ids).delete()
    else:
        encoding = Encoding.objects.create(
            media=media,
            profile=profile,
            chunk=False,
        )

    Encoding.objects.filter(pk=encoding.pk).update(
        status="running",
        progress=0,
        worker="runpod",
        logs="",
        commands="",
        temp_file="",
        task_id="",
    )

    return Encoding.objects.get(pk=encoding.pk)


def build_runpod_payload(media):
    profiles = get_remote_candidate_profiles(media)

    if not profiles:
        raise ValidationError("No remote encoding profiles available")

    jobs = []

    for profile in profiles:
        encoding = _prepare_remote_encoding(media, profile)
        output_key = _encoding_output_key(media, profile)

        jobs.append(
            {
                "encoding_id": encoding.id,
                "profile_id": profile.id,
                "profile_name": profile.name,
                "codec": profile.codec,
                "extension": profile.extension,
                "resolution": int(profile.resolution or 0),
                "output_key": output_key,
            }
        )

    payload = {
        "version": 2,
        "media_id": media.id,
        "media_uid": media.uid.hex,
        "friendly_token": media.friendly_token,
        "username": media.user.username,
        "source_name": _source_name(media),
        "source": build_source_object(media),
        "source_url": build_source_url(media),
        "callback_url": build_callback_url(media),
        "public_base_url": settings.REMOTE_ENCODING_PUBLIC_BASE_URL.rstrip("/"),
        "output_prefix": _join_key(
            settings.REMOTE_ENCODING_OUTPUT_PREFIX.strip("/"),
            media.uid.hex,
        ),
        "segment_seconds": int(settings.REMOTE_ENCODING_HLS_SEGMENT_SECONDS),
        "sprite_seconds": int(getattr(settings, "SPRITE_NUM_SECS", 10)),
        "assets": _asset_keys(media),
        "encoding_policy": build_encoding_policy(),
        "jobs": jobs,
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


def _profile_has_hls_rendition(media, profile):
    if profile.extension != "mp4":
        return False

    resolution = int(profile.resolution or 0)

    if not resolution:
        return False

    return media.hls_renditions.filter(
        codec=profile.codec,
        resolution=resolution,
    ).exists()


def _profile_has_successful_encoding(media, profile):
    from files.models import Encoding

    return Encoding.objects.filter(
        media=media,
        profile=profile,
        chunk=False,
        status="success",
    ).exclude(media_file="").exists()


def _profile_is_satisfied(media, profile):
    """
    A profile is satisfied if MediaCMS already has either:
    - a successful Encoding file for this profile;
    - or, for mp4 profiles, an HLS rendition row for this codec/resolution.

    This avoids paying RunPod for an output that is already usable.
    """
    if _profile_has_successful_encoding(media, profile):
        return True

    if _profile_has_hls_rendition(media, profile):
        return True

    return False


def get_unpackaged_remote_profiles(media):
    """
    MP4 exists but HLS row is missing.

    This should be fixed by local HLS packaging, not by RunPod.
    """
    profiles = []

    for profile in get_remote_candidate_profiles(media):
        if profile.extension != "mp4":
            continue

        if _profile_has_hls_rendition(media, profile):
            continue

        if _profile_has_successful_encoding(media, profile):
            profiles.append(profile)

    return profiles


def get_missing_remote_profiles(media):
    """
    Profiles MediaCMS expects, but this media does not have yet.

    This is the only list that RunPod should receive.
    """
    profiles = []

    for profile in get_remote_candidate_profiles(media):
        if _profile_is_satisfied(media, profile):
            continue

        profiles.append(profile)

    return profiles


def build_runpod_fill_missing_payload(media):
    unpackaged = get_unpackaged_remote_profiles(media)

    if unpackaged:
        labels = [
            f"{profile.id}:{profile.codec}:{profile.extension}:{int(profile.resolution or 0)}"
            for profile in unpackaged
        ]

        raise ValidationError(
            "Existing encoded MP4 missing HLS rows; package HLS locally before RunPod: "
            + ", ".join(labels)
        )

    profiles = get_missing_remote_profiles(media)

    if not profiles:
        raise ValidationError("No missing remote encoding profiles available")

    jobs = []
    requested_encoding_ids = []

    for profile in profiles:
        encoding = _prepare_remote_encoding(media, profile)
        output_key = _encoding_output_key(media, profile)

        requested_encoding_ids.append(encoding.id)

        jobs.append(
            {
                "encoding_id": encoding.id,
                "profile_id": profile.id,
                "profile_name": profile.name,
                "codec": profile.codec,
                "extension": profile.extension,
                "resolution": int(profile.resolution or 0),
                "output_key": output_key,
            }
        )

    payload = {
        "version": 3,
        "mode": "fill_missing_profiles",
        "requested_encoding_ids": requested_encoding_ids,
        "requested_profile_ids": [profile.id for profile in profiles],
        "require_h264": any(
            profile.codec == "h264" and profile.extension == "mp4"
            for profile in profiles
        ),
        "strict_requested_jobs": True,
        "merge_outputs": True,
        "preserve_media_on_fail": True,
        "skip_assets": True,
        "media_id": media.id,
        "media_uid": media.uid.hex,
        "friendly_token": media.friendly_token,
        "username": media.user.username,
        "source_name": _source_name(media),
        "source": build_source_object(media),
        "source_url": build_source_url(media),
        "callback_url": build_callback_url(media),
        "public_base_url": settings.REMOTE_ENCODING_PUBLIC_BASE_URL.rstrip("/"),
        "output_prefix": _join_key(
            settings.REMOTE_ENCODING_OUTPUT_PREFIX.strip("/"),
            media.uid.hex,
        ),
        "segment_seconds": int(settings.REMOTE_ENCODING_HLS_SEGMENT_SECONDS),
        "sprite_seconds": int(getattr(settings, "SPRITE_NUM_SECS", 10)),
        "assets": _asset_keys(media),
        "encoding_policy": build_encoding_policy(),
        "jobs": jobs,
    }

    payload["signature"] = sign_payload(payload)
    return payload


def submit_runpod_fill_missing_job(media):
    if not settings.RUNPOD_ENDPOINT_URL:
        raise ValidationError("RUNPOD_ENDPOINT_URL is not configured")

    request_payload = {
        "input": build_runpod_fill_missing_payload(media),
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