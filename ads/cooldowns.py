
from __future__ import annotations

import time

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from .providers import FORMAT_IN_VIDEO, FORMAT_POPUNDER

_SESSION_KEYS = {
    FORMAT_POPUNDER: "tabunder_last_ts",
    FORMAT_IN_VIDEO: "preroll_last_ts",
}

_SETTING_NAMES = {
    FORMAT_POPUNDER: "TABUNDER_COOLDOWN_SECONDS",
    FORMAT_IN_VIDEO: "PREROLLS_COOLDOWN_SECONDS",
}


def _metadata(ad_format):
    try:
        return _SESSION_KEYS[ad_format], _SETTING_NAMES[ad_format]
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
    session_key, _ = _metadata(ad_format)
    current = int(time.time()) if now is None else int(now)
    last = request.session.get(session_key)
    if isinstance(last, bool) or not isinstance(last, int) or last > current:
        last = None
    if last is None:
        return True
    return current - last >= cooldown_seconds(ad_format)


def mark_cooldown(request, ad_format, *, now=None):
    session_key, _ = _metadata(ad_format)
    current = int(time.time()) if now is None else int(now)
    request.session[session_key] = current
    return current
