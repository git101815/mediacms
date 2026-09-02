from __future__ import annotations

import time

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from .providers import FORMAT_IN_VIDEO, FORMAT_POPUNDER

_COOKIE_NAMES = {
    FORMAT_POPUNDER: "mediacms_ads_popunder_cd",
    FORMAT_IN_VIDEO: "mediacms_ads_in_video_cd",
}

_SETTING_NAMES = {
    FORMAT_POPUNDER: "TABUNDER_COOLDOWN_SECONDS",
    FORMAT_IN_VIDEO: "PREROLLS_COOLDOWN_SECONDS",
}

_COOKIE_SALT = "ads.cooldown.v1"


def _metadata(ad_format):
    try:
        return _COOKIE_NAMES[ad_format], _SETTING_NAMES[ad_format]
    except KeyError as exc:
        raise ValueError(f"Unknown advertising format: {ad_format}") from exc


def cooldown_seconds(ad_format):
    _, setting_name = _metadata(ad_format)
    try:
        value = getattr(settings, setting_name)
    except AttributeError as exc:
        raise ImproperlyConfigured(
            f"Missing required advertising setting: {setting_name}"
        ) from exc
    try:
        seconds = int(value)
    except (TypeError, ValueError) as exc:
        raise ImproperlyConfigured(
            f"{setting_name} must be a non-negative integer"
        ) from exc
    if seconds < 0:
        raise ImproperlyConfigured(
            f"{setting_name} must be a non-negative integer"
        )
    return seconds


def cooldown_elapsed(request, ad_format, *, now=None):
    cookie_name, _ = _metadata(ad_format)
    current = int(time.time()) if now is None else int(now)
    raw = request.get_signed_cookie(
        cookie_name,
        default=None,
        salt=_COOKIE_SALT,
    )
    try:
        last = int(raw) if raw is not None else None
    except (TypeError, ValueError):
        last = None
    if last is None or last > current:
        return True
    return current - last >= cooldown_seconds(ad_format)


def mark_cooldown(request, response, ad_format, *, now=None):
    cookie_name, _ = _metadata(ad_format)
    current = int(time.time()) if now is None else int(now)
    max_age = cooldown_seconds(ad_format)
    response.set_signed_cookie(
        cookie_name,
        str(current),
        salt=_COOKIE_SALT,
        max_age=max_age,
        path="/",
        secure=bool(getattr(settings, "SESSION_COOKIE_SECURE", False)),
        httponly=True,
        samesite=getattr(settings, "SESSION_COOKIE_SAMESITE", "Lax"),
        domain=getattr(settings, "SESSION_COOKIE_DOMAIN", None),
    )
    return current
