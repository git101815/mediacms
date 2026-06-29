import json

from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from files.models import Media, MediaHLSRendition
from files.remote_encoding import verify_signature


def _output_for(outputs, *keys):
    for key in keys:
        value = outputs.get(key) or {}

        if value.get("master_url"):
            return value

    return {}


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
        media.encoding_status = "fail"
        media.save(update_fields=["encoding_status", "listable"])

        return JsonResponse({"ok": True, "status": "fail"})

    outputs = payload.get("outputs") or {}

    output_specs = [
        ("h264", "h264", "hls_file", _output_for(outputs, "h264")),
        ("h265", "h265", "hls_hevc_file", _output_for(outputs, "h265", "hevc")),
        ("av1", "av1", "hls_av1_file", _output_for(outputs, "av1")),
    ]

    try:
        with transaction.atomic():
            update_fields = ["encoding_status", "listable"]

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
            media.save(update_fields=update_fields)
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