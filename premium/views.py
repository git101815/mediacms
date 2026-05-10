from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from files.models import Media

from .models import PremiumMediaUnlock
from .services import (
    build_premium_media_state,
    build_premium_playback_payload,
    purchase_premium_media_with_tokens,
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

    results = []
    for unlock in unlocks[:100]:
        media = unlock.media
        results.append(
            {
                "friendly_token": media.friendly_token,
                "title": media.title,
                "url": media.get_absolute_url(),
                "thumbnail_url": media.thumbnail_url,
                "creator": media.user.username,
                "source_type": unlock.source_type,
                "unlocked_at": unlock.unlocked_at.isoformat(),
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

    return render(
        request,
        "cms/unlocked.html",
        {
            "unlocks": unlocks,
        },
    )