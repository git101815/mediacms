import logging
import os
import shutil
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import HttpResponseRedirect, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views import generic
from django.views.decorators.cache import never_cache
from django.views.decorators.http import (
    require_GET,
    require_http_methods,
    require_POST,
)

from files.models import Media
from uploader.fineuploader import ChunkedFineUploader
from uploader.forms import (
    FineUploaderUploadForm,
    FineUploaderUploadSuccessForm,
)

from .models import PremiumMediaAsset, PremiumMediaUnlock
from .services import (
    build_premium_media_state,
    build_premium_playback_payload,
    format_token_amount,
    get_ready_or_draft_premium_asset,
    purchase_premium_media_with_tokens,
    update_creator_premium_asset_settings,
    user_can_manage_premium_media,
)
from .storage import get_premium_max_upload_size_bytes
from .tasks import (
    PREMIUM_UPLOAD_STATUS_CACHE_TIMEOUT_SECONDS,
    build_error_status,
    build_processing_status,
    finalize_premium_upload,
    premium_upload_status_cache_key,
    store_premium_upload_status,
)


logger = logging.getLogger(__name__)

PREMIUM_UPLOAD_FINALIZATION_LOCK_TIMEOUT_SECONDS = 60 * 60


def build_premium_watch_url(media):
    url = media.get_absolute_url()
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["playback"] = "premium"

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query),
            parts.fragment,
        )
    )


def public_premium_upload_status(payload):
    response = {
        "success": bool(payload.get("success")),
        "state": payload.get("state", ""),
        "processing": bool(payload.get("processing")),
        "upload_uuid": payload.get("upload_uuid", ""),
    }

    if payload.get("message"):
        response["message"] = payload["message"]

    if payload.get("error"):
        response["error"] = payload["error"]

    if payload.get("premium_asset"):
        response["premium_asset"] = payload["premium_asset"]

    if "retry_count" in payload:
        response["retry_count"] = int(
            payload.get("retry_count", 0)
        )

    return response


def premium_upload_status_matches(
    payload,
    *,
    media,
    actor,
):
    try:
        payload_media_id = int(payload.get("media_id", 0))
        payload_actor_id = int(payload.get("actor_id", 0))
    except (TypeError, ValueError):
        return False

    return (
        payload_media_id == int(media.pk)
        and payload_actor_id == int(actor.pk)
    )


def cleanup_failed_local_upload(
    premium_file_path,
    upload_directory,
):
    try:
        if premium_file_path and os.path.isfile(premium_file_path):
            os.remove(premium_file_path)
    except OSError:
        logger.exception(
            "Could not remove failed premium upload file path=%s",
            premium_file_path,
        )

    if upload_directory:
        shutil.rmtree(
            upload_directory,
            ignore_errors=True,
        )


@login_required
@require_POST
@never_cache
def purchase_media(request, friendly_token):
    media = get_object_or_404(
        Media,
        friendly_token=friendly_token,
    )

    try:
        result = purchase_premium_media_with_tokens(
            actor=request.user,
            media=media,
        )
    except ValidationError as exc:
        return JsonResponse(
            {
                "ok": False,
                "error": (
                    exc.messages[0]
                    if hasattr(exc, "messages") and exc.messages
                    else str(exc)
                ),
            },
            status=400,
        )

    premium = build_premium_media_state(
        user=request.user,
        media=media,
        request=request,
    )

    return JsonResponse(
        {
            "ok": True,
            **result,
            "premium": premium,
        }
    )


@login_required
@require_GET
@never_cache
def premium_playback(request, friendly_token):
    media = get_object_or_404(
        Media,
        friendly_token=friendly_token,
    )

    try:
        payload = build_premium_playback_payload(
            user=request.user,
            media=media,
            request=request,
        )
    except ValidationError as exc:
        return JsonResponse(
            {
                "ok": False,
                "error": (
                    exc.messages[0]
                    if hasattr(exc, "messages") and exc.messages
                    else str(exc)
                ),
            },
            status=403,
        )

    return JsonResponse(
        {
            "ok": True,
            **payload,
        }
    )


@login_required
@require_GET
@never_cache
def unlocked_media_api(request):
    unlocks = (
        PremiumMediaUnlock.objects.filter(
            user=request.user,
            revoked_at__isnull=True,
        )
        .select_related(
            "media",
            "media__user",
        )
        .order_by("-unlocked_at")
    )

    owned_assets = (
        PremiumMediaAsset.objects.filter(
            media__user=request.user,
            status=PremiumMediaAsset.STATUS_READY,
        )
        .select_related(
            "media",
            "media__user",
        )
        .order_by(
            "-premium_published_at",
            "-updated_at",
        )
    )

    results = []
    seen_media_ids = set()

    for unlock in unlocks[:100]:
        media = unlock.media
        seen_media_ids.add(media.id)

        results.append(
            {
                "friendly_token": media.friendly_token,
                "title": media.title,
                "url": build_premium_watch_url(media),
                "thumbnail_url": media.thumbnail_url,
                "creator": media.user.username,
                "source_type": unlock.source_type,
                "unlocked_at": unlock.unlocked_at.isoformat(),
            }
        )

    for asset in owned_assets[:100]:
        media = asset.media

        if media.id in seen_media_ids:
            continue

        results.append(
            {
                "friendly_token": media.friendly_token,
                "title": media.title,
                "url": build_premium_watch_url(media),
                "thumbnail_url": media.thumbnail_url,
                "creator": media.user.username,
                "source_type": "creator",
                "unlocked_at": (
                    asset.premium_published_at
                    or asset.updated_at
                ).isoformat(),
            }
        )

    return JsonResponse(
        {
            "ok": True,
            "results": results,
        }
    )


@login_required
@require_GET
@never_cache
def unlocked_media_page(request):
    unlocks = (
        PremiumMediaUnlock.objects.filter(
            user=request.user,
            revoked_at__isnull=True,
        )
        .select_related(
            "media",
            "media__user",
        )
        .order_by("-unlocked_at")
    )

    owned_assets = (
        PremiumMediaAsset.objects.filter(
            media__user=request.user,
            status=PremiumMediaAsset.STATUS_READY,
        )
        .select_related(
            "media",
            "media__user",
        )
        .order_by(
            "-premium_published_at",
            "-updated_at",
        )
    )

    items = []
    seen_media_ids = set()

    for unlock in unlocks:
        media = unlock.media
        seen_media_ids.add(media.id)

        items.append(
            {
                "media": media,
                "source_type": unlock.source_type,
                "unlocked_at": unlock.unlocked_at,
                "watch_url": build_premium_watch_url(media),
            }
        )

    for asset in owned_assets:
        media = asset.media

        if media.id in seen_media_ids:
            continue

        items.append(
            {
                "media": media,
                "source_type": "creator",
                "unlocked_at": (
                    asset.premium_published_at
                    or asset.updated_at
                ),
                "watch_url": build_premium_watch_url(media),
            }
        )

    items.sort(
        key=lambda item: item["unlocked_at"],
        reverse=True,
    )

    return render(
        request,
        "cms/unlocked.html",
        {
            "items": items,
        },
    )


@login_required
@require_http_methods(["GET", "POST"])
@never_cache
def creator_premium_asset_edit(request, friendly_token):
    media = get_object_or_404(
        Media,
        friendly_token=friendly_token,
    )

    if not user_can_manage_premium_media(
        user=request.user,
        media=media,
    ):
        return JsonResponse(
            {
                "ok": False,
                "error": "You cannot manage this premium media",
            },
            status=403,
        )

    asset = get_ready_or_draft_premium_asset(media)

    if request.method == "POST":
        price_display = request.POST.get(
            "price_tokens",
            "",
        )
        publish_now = (
            request.POST.get("publish_state") == "ready"
        )

        try:
            asset = update_creator_premium_asset_settings(
                actor=request.user,
                media=media,
                price_display=price_display,
                publish_now=publish_now,
            )
        except ValidationError as exc:
            messages.error(
                request,
                (
                    exc.messages[0]
                    if hasattr(exc, "messages") and exc.messages
                    else str(exc)
                ),
            )
        else:
            messages.success(
                request,
                "Premium video settings saved.",
            )

            return HttpResponseRedirect(
                reverse(
                    "premium_media_asset_edit",
                    kwargs={
                        "friendly_token": media.friendly_token,
                    },
                )
            )

    price_display = ""

    if asset is not None:
        price_display = format_token_amount(
            asset.price_tokens,
        )

    return render(
        request,
        "cms/premium_asset_edit.html",
        {
            "media": media,
            "asset": asset,
            "price_display": price_display,
            "max_upload_size_bytes": (
                get_premium_max_upload_size_bytes()
            ),
            "premium_upload_url": reverse(
                "premium_media_asset_upload",
                kwargs={
                    "friendly_token": media.friendly_token,
                },
            ),
        },
    )


@login_required
@require_GET
@never_cache
def premium_upload_status(
    request,
    friendly_token,
    upload_uuid,
):
    media = get_object_or_404(
        Media,
        friendly_token=friendly_token,
    )

    if not user_can_manage_premium_media(
        user=request.user,
        media=media,
    ):
        raise PermissionDenied

    payload = cache.get(
        premium_upload_status_cache_key(upload_uuid)
    )

    if payload is None:
        return JsonResponse(
            {
                "success": True,
                "state": "processing",
                "processing": True,
                "upload_uuid": str(upload_uuid),
                "message": "Waiting for premium upload finalization.",
            },
            status=202,
        )

    if not premium_upload_status_matches(
        payload,
        media=media,
        actor=request.user,
    ):
        return JsonResponse(
            {
                "success": False,
                "state": "error",
                "processing": False,
                "error": "Premium upload status was not found.",
            },
            status=404,
        )

    return JsonResponse(
        public_premium_upload_status(payload)
    )


class PremiumFineUploaderView(generic.FormView):
    http_method_names = ("post",)
    form_class_upload = FineUploaderUploadForm
    form_class_upload_success = FineUploaderUploadSuccessForm

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upload = None
        self.media = None

    @property
    def concurrent(self):
        return settings.CONCURRENT_UPLOADS

    @property
    def chunks_done(self):
        return (
            self.chunks_done_param_name
            in self.request.GET
        )

    @property
    def chunks_done_param_name(self):
        return settings.CHUNKS_DONE_PARAM_NAME

    @property
    def upload_uuid(self):
        return str(self.upload.uuid)

    @property
    def status_key(self):
        return premium_upload_status_cache_key(
            self.upload_uuid
        )

    @property
    def lock_key(self):
        return (
            f"premium-upload-finalization-lock:"
            f"{self.upload_uuid}"
        )

    def make_response(self, data, **kwargs):
        return JsonResponse(
            data,
            **kwargs,
        )

    def get_form(self, form_class=None):
        if self.chunks_done:
            form_class = self.form_class_upload_success
        else:
            form_class = self.form_class_upload

        return form_class(
            **self.get_form_kwargs()
        )

    def dispatch(self, request, *args, **kwargs):
        self.media = get_object_or_404(
            Media,
            friendly_token=kwargs.get(
                "friendly_token"
            ),
        )

        if not user_can_manage_premium_media(
            user=request.user,
            media=self.media,
        ):
            raise PermissionDenied

        return super().dispatch(
            request,
            *args,
            **kwargs,
        )

    def get_existing_status(self):
        payload = cache.get(self.status_key)

        if payload is None:
            return None

        if not premium_upload_status_matches(
            payload,
            media=self.media,
            actor=self.request.user,
        ):
            return None

        return payload

    def processing_response(self):
        payload = build_processing_status(
            upload_uuid=self.upload_uuid,
            media_id=self.media.pk,
            actor_id=self.request.user.pk,
        )

        return self.make_response(
            public_premium_upload_status(payload)
        )

    def remove_partial_combined_file(self):
        if not self.upload.real_path:
            return

        premium_file_path = os.path.join(
            settings.MEDIA_ROOT,
            self.upload.real_path,
        )

        try:
            if os.path.isfile(premium_file_path):
                os.remove(premium_file_path)
        except OSError:
            logger.exception(
                "Could not remove partial premium file "
                "upload_uuid=%s path=%s",
                self.upload_uuid,
                premium_file_path,
            )

    def queue_finalization(self):
        premium_file_path = os.path.join(
            settings.MEDIA_ROOT,
            self.upload.real_path,
        )
        upload_directory = os.path.join(
            settings.MEDIA_ROOT,
            self.upload.file_path,
        )

        processing_status = build_processing_status(
            upload_uuid=self.upload_uuid,
            media_id=self.media.pk,
            actor_id=self.request.user.pk,
        )
        store_premium_upload_status(
            self.upload_uuid,
            processing_status,
        )

        try:
            finalize_premium_upload.apply_async(
                kwargs={
                    "media_id": self.media.pk,
                    "actor_id": self.request.user.pk,
                    "premium_file_path": premium_file_path,
                    "upload_directory": upload_directory,
                    "original_filename": self.upload.filename,
                    "upload_uuid": self.upload_uuid,
                },
                queue="long_tasks",
            )
        except Exception:
            logger.exception(
                "Could not enqueue premium upload "
                "media_id=%s actor_id=%s upload_uuid=%s",
                self.media.pk,
                self.request.user.pk,
                self.upload_uuid,
            )

            error_status = build_error_status(
                upload_uuid=self.upload_uuid,
                media_id=self.media.pk,
                actor_id=self.request.user.pk,
                error=(
                    "Could not start premium upload "
                    "finalization. Upload the file again."
                ),
            )
            store_premium_upload_status(
                self.upload_uuid,
                error_status,
            )

            cleanup_failed_local_upload(
                premium_file_path,
                upload_directory,
            )

            return self.make_response(
                public_premium_upload_status(
                    error_status
                ),
                status=503,
            )

        return self.make_response(
            public_premium_upload_status(
                processing_status
            )
        )

    def combine_and_queue_chunks(self):
        existing_status = self.get_existing_status()

        if existing_status is not None:
            return self.make_response(
                public_premium_upload_status(
                    existing_status
                )
            )

        lock_acquired = cache.add(
            self.lock_key,
            {
                "media_id": self.media.pk,
                "actor_id": self.request.user.pk,
            },
            timeout=(
                PREMIUM_UPLOAD_FINALIZATION_LOCK_TIMEOUT_SECONDS
            ),
        )

        if not lock_acquired:
            return self.processing_response()

        try:
            existing_status = self.get_existing_status()

            if existing_status is not None:
                return self.make_response(
                    public_premium_upload_status(
                        existing_status
                    )
                )

            try:
                self.upload.combine_chunks()
            except FileNotFoundError:
                existing_status = self.get_existing_status()

                if existing_status is not None:
                    return self.make_response(
                        public_premium_upload_status(
                            existing_status
                        )
                    )

                self.remove_partial_combined_file()

                logger.warning(
                    "Premium chunk is missing "
                    "media_id=%s actor_id=%s "
                    "upload_uuid=%s total_parts=%s",
                    self.media.pk,
                    self.request.user.pk,
                    self.upload_uuid,
                    self.upload.total_parts,
                )

                error_status = build_error_status(
                    upload_uuid=self.upload_uuid,
                    media_id=self.media.pk,
                    actor_id=self.request.user.pk,
                    error=(
                        "One or more premium upload "
                        "chunks are missing. Retry the upload."
                    ),
                )
                store_premium_upload_status(
                    self.upload_uuid,
                    error_status,
                )

                return self.make_response(
                    public_premium_upload_status(
                        error_status
                    ),
                    status=400,
                )
            except Exception:
                self.remove_partial_combined_file()

                logger.exception(
                    "Premium chunk combination failed "
                    "media_id=%s actor_id=%s upload_uuid=%s",
                    self.media.pk,
                    self.request.user.pk,
                    self.upload_uuid,
                )

                error_status = build_error_status(
                    upload_uuid=self.upload_uuid,
                    media_id=self.media.pk,
                    actor_id=self.request.user.pk,
                    error=(
                        "Premium upload chunks could "
                        "not be combined."
                    ),
                )
                store_premium_upload_status(
                    self.upload_uuid,
                    error_status,
                )

                return self.make_response(
                    public_premium_upload_status(
                        error_status
                    ),
                    status=500,
                )

            return self.queue_finalization()

        finally:
            cache.delete(self.lock_key)

    def save_single_file_and_queue(self):
        existing_status = self.get_existing_status()

        if existing_status is not None:
            return self.make_response(
                public_premium_upload_status(
                    existing_status
                )
            )

        lock_acquired = cache.add(
            self.lock_key,
            {
                "media_id": self.media.pk,
                "actor_id": self.request.user.pk,
            },
            timeout=(
                PREMIUM_UPLOAD_FINALIZATION_LOCK_TIMEOUT_SECONDS
            ),
        )

        if not lock_acquired:
            return self.processing_response()

        try:
            existing_status = self.get_existing_status()

            if existing_status is not None:
                return self.make_response(
                    public_premium_upload_status(
                        existing_status
                    )
                )

            self.upload.save()

            return self.queue_finalization()

        except Exception:
            logger.exception(
                "Premium single-file upload failed "
                "media_id=%s actor_id=%s upload_uuid=%s",
                self.media.pk,
                self.request.user.pk,
                self.upload_uuid,
            )

            error_status = build_error_status(
                upload_uuid=self.upload_uuid,
                media_id=self.media.pk,
                actor_id=self.request.user.pk,
                error="Premium file could not be stored locally.",
            )
            store_premium_upload_status(
                self.upload_uuid,
                error_status,
            )

            return self.make_response(
                public_premium_upload_status(
                    error_status
                ),
                status=500,
            )

        finally:
            cache.delete(self.lock_key)

    def form_valid(self, form):
        self.upload = ChunkedFineUploader(
            form.cleaned_data,
            self.concurrent,
        )

        existing_status = self.get_existing_status()

        if existing_status is not None:
            return self.make_response(
                public_premium_upload_status(
                    existing_status
                )
            )

        if self.upload.concurrent and self.chunks_done:
            return self.combine_and_queue_chunks()

        if self.upload.total_parts == 1:
            return self.save_single_file_and_queue()

        self.upload.save()

        if self.upload.finished:
            return self.queue_finalization()

        return self.make_response(
            {
                "success": True,
            }
        )

    def form_invalid(self, form):
        return self.make_response(
            {
                "success": False,
                "error": "%s" % repr(form.errors),
            },
            status=400,
        )
