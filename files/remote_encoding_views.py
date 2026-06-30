import json

from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from files import helpers
from files.models import Encoding, Media, MediaHLSRendition
from files.remote_encoding import verify_signature


def _output_for(outputs, *keys):
    for key in keys:
        value = outputs.get(key) or {}

        if value.get("master_url"):
            return value

    return {}


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _update_media_from_payload(media, media_payload):
    if not media_payload:
        return []

    update_fields = []

    media_type = media_payload.get("media_type") or "video"
    if media_type:
        media.media_type = media_type
        update_fields.append("media_type")

    duration = _safe_int(media_payload.get("duration"))
    if duration is not None:
        media.duration = duration
        update_fields.append("duration")

    video_height = _safe_int(media_payload.get("video_height"))
    if video_height is not None:
        media.video_height = video_height
        update_fields.append("video_height")

    size_bytes = _safe_int(media_payload.get("size_bytes"))
    if size_bytes:
        media.size = helpers.show_file_size(size_bytes)
        update_fields.append("size")

    md5sum = media_payload.get("md5sum")
    if md5sum:
        media.md5sum = md5sum
        update_fields.append("md5sum")

    media_info = media_payload.get("media_info")
    if media_info:
        media.media_info = json.dumps(media_info)
        update_fields.append("media_info")

    thumbnail_time = _safe_float(media_payload.get("thumbnail_time"))
    if thumbnail_time is not None:
        media.thumbnail_time = thumbnail_time
        update_fields.append("thumbnail_time")

    thumbnail_file = media_payload.get("thumbnail_file")
    if thumbnail_file:
        media.thumbnail = MediaHLSRendition.storage_path(thumbnail_file)
        update_fields.append("thumbnail")

    poster_file = media_payload.get("poster_file")
    if poster_file:
        media.poster = MediaHLSRendition.storage_path(poster_file)
        update_fields.append("poster")

    sprites_file = media_payload.get("sprites_file")
    if sprites_file:
        media.sprites = MediaHLSRendition.storage_path(sprites_file)
        update_fields.append("sprites")

    return update_fields


def _update_encoding_row(media, item):
    encoding_id = _safe_int(item.get("encoding_id"))
    profile_id = _safe_int(item.get("profile_id"))
    media_file = item.get("media_file") or item.get("media_url") or ""

    if not profile_id or not media_file:
        return None

    update = {
        "media_file": MediaHLSRendition.storage_path(media_file),
        "status": item.get("status") or "success",
        "progress": 100,
        "worker": "runpod",
        "logs": item.get("logs") or "",
        "commands": item.get("commands") or "",
    }

    size_bytes = _safe_int(item.get("size_bytes"))
    if size_bytes:
        update["size"] = helpers.show_file_size(size_bytes)

    qs = Encoding.objects.filter(media=media, profile_id=profile_id, chunk=False)

    if encoding_id:
        qs = qs.filter(id=encoding_id)

    if qs.exists():
        qs.update(**update)
        return qs.first()

    encoding = Encoding.objects.create(
        media=media,
        profile_id=profile_id,
        chunk=False,
        status="pending",
        worker="runpod",
    )
    Encoding.objects.filter(pk=encoding.pk).update(**update)
    return Encoding.objects.filter(pk=encoding.pk).first()


def _update_encodings_from_payload(media, encodings):
    preview_file_path = ""

    for item in encodings or []:
        encoding = _update_encoding_row(media, item)

        if not encoding:
            continue

        if item.get("extension") == "gif" and item.get("media_file"):
            preview_file_path = MediaHLSRendition.storage_path(item["media_file"])

    return preview_file_path


@csrf_exempt
@require_POST
def remote_encoding_callback(request, friendly_token):
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"ok": False, "error": "Invalid JSON"}, status=400)

    signature = payload.pop("signature", "")

    if not verify_signature(payload, signature):
        return JsonResponse({"ok": False, "error": "Invalid signature"}, status=403)

    try:
        media = Media.objects.get(friendly_token=friendly_token)
    except Media.DoesNotExist:
        return JsonResponse({"ok": False, "error": "Media not found"}, status=404)

    if payload.get("media_id") != media.id or payload.get("friendly_token") != media.friendly_token:
        return JsonResponse({"ok": False, "error": "Media mismatch"}, status=400)

    if payload.get("status") != "success":
        Encoding.objects.filter(media=media, worker="runpod").exclude(status="success").update(
            status="fail",
            logs=payload.get("error", ""),
        )

        media.encoding_status = "fail"
        media.save(update_fields=["encoding_status", "listable"])

        return JsonResponse({"ok": True, "status": "fail"})

    outputs = payload.get("outputs") or {}
    encodings = payload.get("encodings") or []

    output_specs = [
        ("h264", "h264", "hls_file", _output_for(outputs, "h264")),
        ("h265", "h265", "hls_hevc_file", _output_for(outputs, "h265", "hevc")),
        ("av1", "av1", "hls_av1_file", _output_for(outputs, "av1")),
    ]

    try:
        with transaction.atomic():
            update_fields = ["encoding_status", "listable"]

            update_fields.extend(_update_media_from_payload(media, payload.get("media") or {}))

            preview_file_path = _update_encodings_from_payload(media, encodings)
            if preview_file_path:
                media.preview_file_path = preview_file_path
                update_fields.append("preview_file_path")

            for _output_key, codec, db_field, output in output_specs:
                master_url = output.get("master_url", "")

                if not master_url:
                    continue

                MediaHLSRendition.replace_from_payload(
                    media=media,
                    codec=codec,
                    master_file=master_url,
                    renditions=output.get("renditions") or [],
                )

                setattr(media, db_field, MediaHLSRendition.storage_path(master_url))
                update_fields.append(db_field)

            media.encoding_status = "success"
            media.save(update_fields=sorted(set(update_fields)))

    except ValueError as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=400)

    return JsonResponse(
        {
            "ok": True,
            "status": "success",
            "h264": bool(_output_for(outputs, "h264").get("master_url")),
            "h265": bool(_output_for(outputs, "h265", "hevc").get("master_url")),
            "av1": bool(_output_for(outputs, "av1").get("master_url")),
        }
    )