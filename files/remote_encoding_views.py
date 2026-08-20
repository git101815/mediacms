import json
import logging

from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from files import helpers
from files.models import Encoding, Media, MediaHLSRendition
from files.remote_encoding import verify_signature


logger = logging.getLogger(__name__)


def _record_premium_release_after_remote_publish(media_id):
    # Local imports keep the files app independent from premium at module load
    # time and avoid introducing an import cycle during Django startup.
    from premium.subscription_tasks import grant_premium_release_unlocks
    from premium.subscriptions import record_media_release

    media = Media.objects.select_related("user").get(pk=media_id)
    release, release_created = record_media_release(media=media)

    if release is None or not release_created:
        return

    try:
        grant_premium_release_unlocks.apply_async(
            args=[release.id],
            queue="short_tasks",
        )
    except Exception:
        # Keep the release row even if the immediate enqueue fails. The
        # existing pending-release processor can pick it up afterwards.
        logger.exception(
            "Could not enqueue premium release grants release_id=%s",
            release.id,
        )


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


def _media_update_from_payload(media_payload):
    if not media_payload:
        return {}

    update = {}

    media_type = media_payload.get("media_type") or "video"
    if media_type:
        update["media_type"] = media_type

    duration = _safe_int(media_payload.get("duration"))
    if duration is not None:
        update["duration"] = duration

    video_height = _safe_int(media_payload.get("video_height"))
    if video_height is not None:
        update["video_height"] = video_height

    size_bytes = _safe_int(media_payload.get("size_bytes"))
    if size_bytes:
        update["size"] = helpers.show_file_size(size_bytes)

    md5sum = media_payload.get("md5sum")
    if md5sum:
        update["md5sum"] = md5sum

    media_info = media_payload.get("media_info")
    if media_info:
        update["media_info"] = json.dumps(media_info)

    thumbnail_time = _safe_float(media_payload.get("thumbnail_time"))
    if thumbnail_time is not None:
        update["thumbnail_time"] = thumbnail_time

    thumbnail_file = media_payload.get("thumbnail_file")
    if thumbnail_file:
        update["thumbnail"] = MediaHLSRendition.storage_path(thumbnail_file)

    poster_file = media_payload.get("poster_file")
    if poster_file:
        update["poster"] = MediaHLSRendition.storage_path(poster_file)

    sprites_file = media_payload.get("sprites_file")
    if sprites_file:
        update["sprites"] = MediaHLSRendition.storage_path(sprites_file)

    return update


def _update_encoding_row(media, item):
    encoding_id = _safe_int(item.get("encoding_id"))
    profile_id = _safe_int(item.get("profile_id"))
    status = item.get("status") or "success"
    media_file = item.get("media_file") or item.get("media_url") or ""

    if not profile_id:
        return None

    if status == "success" and not media_file:
        return None

    update = {
        "status": status,
        "worker": "runpod",
        "logs": item.get("logs") or "",
        "commands": item.get("commands") or "",
    }

    if status == "success":
        update["progress"] = 100
        update["media_file"] = MediaHLSRendition.storage_path(media_file)
    else:
        update["progress"] = _safe_int(item.get("progress")) or 0

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


def _delete_skipped_encodings(media, skipped):
    for item in skipped or []:
        encoding_id = _safe_int(item.get("encoding_id"))
        profile_id = _safe_int(item.get("profile_id"))

        if not profile_id:
            continue

        qs = Encoding.objects.filter(
            media=media,
            profile_id=profile_id,
            chunk=False,
            worker="runpod",
        )

        if encoding_id:
            qs = qs.filter(id=encoding_id)

        qs.exclude(status="success").delete()


def _merge_hls_renditions(media, codec, master_file, renditions):
    rows = MediaHLSRendition.build_from_payload(
        media=media,
        codec=codec,
        master_file=master_file,
        renditions=renditions,
    )

    if not rows:
        return []

    resolutions = [row.resolution for row in rows]

    MediaHLSRendition.objects.filter(
        media=media,
        codec=codec,
        resolution__in=resolutions,
    ).delete()

    MediaHLSRendition.objects.bulk_create(rows)

    return rows


def _requested_encoding_ids_from_payload(payload):
    ids = set()

    for value in payload.get("requested_encoding_ids") or []:
        item = _safe_int(value)
        if item:
            ids.add(item)

    for item in payload.get("encodings") or []:
        encoding_id = _safe_int(item.get("encoding_id"))
        if encoding_id:
            ids.add(encoding_id)

    for item in payload.get("skipped") or []:
        encoding_id = _safe_int(item.get("encoding_id"))
        if encoding_id:
            ids.add(encoding_id)

    return sorted(ids)


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
        requested_encoding_ids = _requested_encoding_ids_from_payload(payload)

        with transaction.atomic():
            _update_encodings_from_payload(
                media,
                payload.get("encodings") or [],
            )

            nonfatal_skipped = [
                item
                for item in payload.get("skipped") or []
                if (
                    payload.get("mode") == "fill_missing_profiles"
                    and item.get("reason")
                    == "source_below_target_resolution"
                )
            ]
            if nonfatal_skipped:
                _delete_skipped_encodings(media, nonfatal_skipped)

            if requested_encoding_ids:
                Encoding.objects.filter(
                    media=media,
                    id__in=requested_encoding_ids,
                ).exclude(status="success").update(
                    status="fail",
                    logs=payload.get("error", ""),
                )
            else:
                # Legacy fallback only. New fill_missing payloads must always send IDs.
                Encoding.objects.filter(
                    media=media,
                    worker="runpod",
                    status="running",
                ).update(
                    status="fail",
                    logs=payload.get("error", ""),
                )

            media_update = _media_update_from_payload(
                payload.get("media") or {}
            )

            if payload.get("preserve_media_on_fail") is not True:
                media_update["encoding_status"] = "fail"
                media_update["listable"] = False

            if media_update:
                Media.objects.filter(pk=media.pk).update(**media_update)

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
            # A remote callback uses QuerySet.update(), so Django's Media
            # pre_save/post_save signals do not run. Lock the row and remember
            # the real pre-callback value so only the first False -> True
            # transition can become a subscription release.
            (
                was_listable,
                current_state,
                current_is_reviewed,
            ) = (
                Media.objects.select_for_update()
                .values_list(
                    "listable",
                    "state",
                    "is_reviewed",
                )
                .get(pk=media.pk)
            )
            was_listable = bool(was_listable)

            media_update = _media_update_from_payload(payload.get("media") or {})

            preview_file_path = _update_encodings_from_payload(media, encodings)
            if preview_file_path:
                media_update["preview_file_path"] = preview_file_path

            _delete_skipped_encodings(media, payload.get("skipped") or [])

            merge_outputs = payload.get("merge_outputs") is True

            for _output_key, codec, db_field, output in output_specs:
                master_url = output.get("master_url", "")

                if not master_url:
                    continue

                if merge_outputs:
                    _merge_hls_renditions(
                        media=media,
                        codec=codec,
                        master_file=master_url,
                        renditions=output.get("renditions") or [],
                    )

                    if not getattr(media, db_field):
                        media_update[db_field] = MediaHLSRendition.storage_path(master_url)
                else:
                    MediaHLSRendition.replace_from_payload(
                        media=media,
                        codec=codec,
                        master_file=master_url,
                        renditions=output.get("renditions") or [],
                    )

                    media_update[db_field] = MediaHLSRendition.storage_path(master_url)

            media_update["encoding_status"] = "success"
            media_update["listable"] = (
                current_state == "public"
                and current_is_reviewed is True
            )
            became_listable = (
                not was_listable
                and bool(media_update["listable"])
            )

            Media.objects.filter(pk=media.pk).update(**media_update)

            if became_listable:
                transaction.on_commit(
                    lambda media_id=media.pk: (
                        _record_premium_release_after_remote_publish(media_id)
                    )
                )

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