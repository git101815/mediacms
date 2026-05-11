import os
import shutil
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.core.files import File
from django.views import generic

from files.helpers import rm_file
from uploader.fineuploader import ChunkedFineUploader
from uploader.forms import FineUploaderUploadForm, FineUploaderUploadSuccessForm

from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from files.models import Media

from .models import PremiumMediaAsset, PremiumMediaUnlock

from .services import (
    build_premium_media_state,
    build_premium_playback_payload,
    purchase_premium_media_with_tokens,
    create_or_update_creator_premium_asset,
    format_token_amount,
    get_ready_or_draft_premium_asset,
    update_creator_premium_asset_settings,
    user_can_manage_premium_media,
    replace_creator_premium_asset_file,
)
from .storage import get_premium_max_upload_size_bytes

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

@login_required
@require_POST
@never_cache
def purchase_media(request, friendly_token):
    media = get_object_or_404(Media, friendly_token=friendly_token)

    try:
        result = purchase_premium_media_with_tokens(actor=request.user, media=media)
    except ValidationError as exc:
        return JsonResponse(
            {
                "ok": False,
                "error": exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc),
            },
            status=400,
        )

    premium = build_premium_media_state(user=request.user, media=media, request=request)

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
    media = get_object_or_404(Media, friendly_token=friendly_token)

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
                "error": exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc),
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
        .select_related("media", "media__user")
        .order_by("-unlocked_at")
    )

    owned_assets = (
        PremiumMediaAsset.objects.filter(
            media__user=request.user,
            status=PremiumMediaAsset.STATUS_READY,
        )
        .select_related("media", "media__user")
        .order_by("-premium_published_at", "-updated_at")
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
                    asset.premium_published_at or asset.updated_at
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
        .select_related("media", "media__user")
        .order_by("-unlocked_at")
    )

    owned_assets = (
        PremiumMediaAsset.objects.filter(
            media__user=request.user,
            status=PremiumMediaAsset.STATUS_READY,
        )
        .select_related("media", "media__user")
        .order_by("-premium_published_at", "-updated_at")
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
                "unlocked_at": asset.premium_published_at or asset.updated_at,
                "watch_url": build_premium_watch_url(media),
            }
        )

    items.sort(key=lambda item: item["unlocked_at"], reverse=True)

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
    media = get_object_or_404(Media, friendly_token=friendly_token)

    if not user_can_manage_premium_media(user=request.user, media=media):
        return JsonResponse(
            {
                "ok": False,
                "error": "You cannot manage this premium media",
            },
            status=403,
        )

    asset = get_ready_or_draft_premium_asset(media)

    if request.method == "POST":
        price_display = request.POST.get("price_tokens", "")
        publish_now = request.POST.get("publish_state") == "ready"

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
                exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc),
            )
        else:
            messages.success(request, "Premium video settings saved.")
            return HttpResponseRedirect(
                reverse("premium_media_asset_edit", kwargs={"friendly_token": media.friendly_token})
            )

    price_display = ""
    if asset is not None:
        price_display = format_token_amount(asset.price_tokens)

    return render(
        request,
        "cms/premium_asset_edit.html",
        {
            "media": media,
            "asset": asset,
            "price_display": price_display,
            "max_upload_size_bytes": get_premium_max_upload_size_bytes(),
            "premium_upload_url": reverse(
                "premium_media_asset_upload",
                kwargs={"friendly_token": media.friendly_token},
            ),
        },

    )

class PremiumFineUploaderView(generic.FormView):
    http_method_names = ("post",)
    form_class_upload = FineUploaderUploadForm
    form_class_upload_success = FineUploaderUploadSuccessForm

    @property
    def concurrent(self):
        return settings.CONCURRENT_UPLOADS

    @property
    def chunks_done(self):
        return self.chunks_done_param_name in self.request.GET

    @property
    def chunks_done_param_name(self):
        return settings.CHUNKS_DONE_PARAM_NAME

    def make_response(self, data, **kwargs):
        return JsonResponse(data, **kwargs)

    def get_form(self, form_class=None):
        if self.chunks_done:
            form_class = self.form_class_upload_success
        else:
            form_class = self.form_class_upload
        return form_class(**self.get_form_kwargs())

    def dispatch(self, request, *args, **kwargs):
        self.media = get_object_or_404(
            Media,
            friendly_token=kwargs.get("friendly_token"),
        )

        if not user_can_manage_premium_media(user=request.user, media=self.media):
            raise PermissionDenied

        return super().dispatch(request, *args, **kwargs)

    def form_valid(self, form):
        self.upload = ChunkedFineUploader(form.cleaned_data, self.concurrent)

        if self.upload.concurrent and self.chunks_done:
            try:
                self.upload.combine_chunks()
            except FileNotFoundError:
                return self.make_response(
                    {
                        "success": False,
                        "error": "Error with premium file uploading",
                    },
                    status=400,
                )
        elif self.upload.total_parts == 1:
            self.upload.save()
        else:
            self.upload.save()
            return self.make_response({"success": True})

        premium_file_path = os.path.join(settings.MEDIA_ROOT, self.upload.real_path)

        try:
            with open(premium_file_path, "rb") as source:
                premium_file = File(source, name=self.upload.filename)
                asset = replace_creator_premium_asset_file(
                    actor=self.request.user,
                    media=self.media,
                    uploaded_file=premium_file,
                )
        except ValidationError as exc:
            return self.make_response(
                {
                    "success": False,
                    "error": exc.messages[0] if hasattr(exc, "messages") and exc.messages else str(exc),
                },
                status=400,
            )
        finally:
            rm_file(premium_file_path)
            shutil.rmtree(
                os.path.join(settings.MEDIA_ROOT, self.upload.file_path),
                ignore_errors=True,
            )

        return self.make_response(
            {
                "success": True,
                "premium_asset": {
                    "id": asset.id,
                    "status": asset.status,
                    "file_name": asset.file_name,
                    "size_bytes": asset.size_bytes,
                },
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