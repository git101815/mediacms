from urllib.parse import quote

from django.conf import settings


def banner_public_url(creative):
    image = getattr(creative, "image", None)
    if not image:
        return ""

    name = str(getattr(image, "name", "") or "").lstrip("/")
    if not name:
        return ""

    base = str(getattr(settings, "MEDIA_URL", "") or "").strip()
    if not base:
        return ""

    return f"{base.rstrip('/')}/{quote(name, safe='/')}"
