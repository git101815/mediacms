import json

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from files.models import Media
from files.remote_encoding import verify_signature


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

    if payload.get("status") != "success":
        media.encoding_status = "fail"
        media.save(update_fields=["encoding_status", "listable"])
        return JsonResponse({"ok": True, "status": "fail"})

    outputs = payload.get("outputs") or {}

    h264_master = outputs.get("h264", {}).get("master_url", "")
    av1_master = outputs.get("av1", {}).get("master_url", "")

    update_fields = ["encoding_status", "listable"]

    if h264_master:
        media.hls_file = h264_master
        update_fields.append("hls_file")

    if av1_master:
        media.hls_av1_file = av1_master
        update_fields.append("hls_av1_file")

    media.encoding_status = "success"
    media.save(update_fields=update_fields)

    return JsonResponse(
        {
            "ok": True,
            "status": "success",
            "h264": bool(h264_master),
            "av1": bool(av1_master),
        }
    )