import base64
import hashlib
import time
from urllib.parse import unquote, urlsplit

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.http import Http404, HttpResponseForbidden, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.utils.encoding import iri_to_uri
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_GET, require_POST

from .methods import is_mediacms_editor
from .models import Encoding, Media


DOWNLOADABLE_EXTENSIONS = {"mp4", "webm"}


def has_gate_cookie(request):
    cookie_name = getattr(settings, "MEDIA_GATE_COOKIE_NAME", "mc_gate")
    return bool(request.COOKIES.get(cookie_name))


def user_can_access_media(request, media):
    if media.state != "private":
        return True

    user = request.user

    if not getattr(user, "is_authenticated", False):
        return False

    if media.user_id == user.id:
        return True

    if is_mediacms_editor(user):
        return True

    if (
        getattr(settings, "USE_RBAC", False)
        and user.has_member_access_to_media(media)
    ):
        return True

    return False


def get_download_media(request, friendly_token):
    media = get_object_or_404(
        Media.objects.select_related("user"),
        friendly_token=friendly_token,
        media_type="video",
    )

    if not media.allow_download:
        raise Http404("Downloads are disabled for this media")

    if not user_can_access_media(request, media):
        raise Http404("Media does not exist")

    return media


def get_download_options(media):
    encodings = (
        media.encodings.select_related("profile")
        .filter(
            chunk=False,
            status="success",
            progress=100,
            profile__extension__in=DOWNLOADABLE_EXTENSIONS,
        )
        .exclude(media_file="")
        .order_by("-profile__resolution", "profile__codec", "id")
    )

    options = []

    for encoding in encodings:
        resolution = encoding.profile.resolution
        codec = encoding.profile.codec or encoding.profile.extension

        label = "Download"

        if resolution:
            label += f" {resolution}"

        if codec:
            label += f" {codec.upper()}"

        options.append(
            {
                "id": str(encoding.id),
                "label": label,
                "size": encoding.size or "",
            }
        )

    if getattr(settings, "SHOW_ORIGINAL_MEDIA", False) and media.original_media_url:
        options.append(
            {
                "id": "original",
                "label": "Download original",
                "size": media.size or "",
            }
        )

    return options


def get_download_source(media, download_id):
    if download_id == "original":
        if not getattr(settings, "SHOW_ORIGINAL_MEDIA", False):
            raise Http404("Original downloads are disabled")

        if not media.original_media_url:
            raise Http404("Original file does not exist")

        return media.original_media_url

    try:
        encoding_id = int(download_id)
    except ValueError as exc:
        raise Http404("Invalid download option") from exc

    encoding = get_object_or_404(
        Encoding.objects.select_related("profile"),
        id=encoding_id,
        media=media,
        chunk=False,
        status="success",
        progress=100,
        profile__extension__in=DOWNLOADABLE_EXTENSIONS,
    )

    if not encoding.media_encoding_url:
        raise Http404("Encoded file does not exist")

    return encoding.media_encoding_url


def build_bunny_download_url(source_url):
    base_url = getattr(settings, "BUNNY_DOWNLOAD_BASE_URL", "").rstrip("/")
    token_key = getattr(settings, "BUNNY_DOWNLOAD_TOKEN_KEY", "")
    ttl_seconds = int(getattr(settings, "BUNNY_DOWNLOAD_TOKEN_TTL_SECONDS", 300))

    if not base_url:
        raise ImproperlyConfigured("BUNNY_DOWNLOAD_BASE_URL is not configured")

    if not token_key:
        raise ImproperlyConfigured("BUNNY_DOWNLOAD_TOKEN_KEY is not configured")

    parsed_base = urlsplit(base_url)

    if parsed_base.scheme != "https" or not parsed_base.netloc:
        raise ImproperlyConfigured("BUNNY_DOWNLOAD_BASE_URL must be an HTTPS URL")

    if ttl_seconds <= 0:
        raise ImproperlyConfigured("BUNNY_DOWNLOAD_TOKEN_TTL_SECONDS must be positive")

    raw_path = urlsplit(source_url).path
    url_path = iri_to_uri(raw_path)
    signature_path = unquote(url_path)

    if not url_path.startswith("/mediafiles/"):
        raise ImproperlyConfigured("Only /mediafiles/ downloads can be signed")

    expires = int(time.time()) + ttl_seconds
    hash_base = f"{token_key}{signature_path}{expires}"

    token = base64.b64encode(hashlib.sha256(hash_base.encode("utf-8")).digest())
    token = token.decode("utf-8").replace("\n", "")
    token = token.replace("+", "-").replace("/", "_").replace("=", "")

    return f"{base_url}{url_path}?token={token}&expires={expires}"


@never_cache
@require_GET
def media_download_page(request, friendly_token):
    if not has_gate_cookie(request):
        return HttpResponseForbidden("Age verification is required")

    media = get_download_media(request, friendly_token)

    return render(
        request,
        "cms/download.html",
        {
            "media": media,
            "download_options": get_download_options(media),
        },
    )


@never_cache
@csrf_protect
@require_POST
def media_download_start(request, friendly_token, download_id):
    if not has_gate_cookie(request):
        return HttpResponseForbidden("Age verification is required")

    media = get_download_media(request, friendly_token)
    source_url = get_download_source(media, download_id)

    response = HttpResponseRedirect(build_bunny_download_url(source_url))
    response["Cache-Control"] = "private, no-store, max-age=0"
    response["Pragma"] = "no-cache"

    return response