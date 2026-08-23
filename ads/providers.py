from __future__ import annotations

import random
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

PROVIDER_INTERNAL = "internal"
PROVIDER_CLICKAINE = "clickaine"
PROVIDER_PARTNER = "partner"
PROVIDERS = (
    PROVIDER_INTERNAL,
    PROVIDER_CLICKAINE,
    PROVIDER_PARTNER,
)

FORMAT_POPUNDER = "popunder"
FORMAT_IN_VIDEO = "in_video"


def _setting(name):
    try:
        return getattr(settings, name)
    except AttributeError as exc:
        raise ImproperlyConfigured(
            f"Missing required advertising setting: {name}"
        ) from exc


def _http_url_setting(name):
    value = str(_setting(name) or "").strip()
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ImproperlyConfigured(
            f"{name} must be an absolute HTTP(S) URL"
        )
    return value


def provider_weights():
    raw = _setting("ADS_PROVIDER_WEIGHTS")
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(
            "ADS_PROVIDER_WEIGHTS must be a dict"
        )

    keys = set(raw)
    expected = set(PROVIDERS)
    if keys != expected:
        missing = sorted(expected - keys)
        unknown = sorted(keys - expected)
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if unknown:
            details.append("unknown=" + ",".join(unknown))
        raise ImproperlyConfigured(
            "ADS_PROVIDER_WEIGHTS must contain exactly "
            "internal, clickaine and partner"
            + (" (" + "; ".join(details) + ")" if details else "")
        )

    weights = {}
    for provider in PROVIDERS:
        value = raw[provider]
        if isinstance(value, bool):
            raise ImproperlyConfigured(
                f"ADS_PROVIDER_WEIGHTS[{provider!r}] must be numeric"
            )
        try:
            weight = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f"ADS_PROVIDER_WEIGHTS[{provider!r}] must be numeric"
            ) from exc
        if not weight.is_finite() or weight < 0:
            raise ImproperlyConfigured(
                f"ADS_PROVIDER_WEIGHTS[{provider!r}] must be >= 0"
            )
        weights[provider] = weight

    if sum(weights.values(), Decimal("0")) != Decimal("100"):
        raise ImproperlyConfigured(
            "ADS_PROVIDER_WEIGHTS must sum to exactly 100"
        )
    return weights


def eligible_provider_weights(ad_format):
    weights = provider_weights()

    if ad_format == FORMAT_POPUNDER:
        clickaine_enabled = bool(
            _setting("CLICKAINE_POPUNDER_ENABLED")
        )
        if clickaine_enabled:
            _http_url_setting("CLICKAINE_POPUNDER_SCRIPT_URL")
    elif ad_format == FORMAT_IN_VIDEO:
        clickaine_enabled = bool(_setting("CLICKAINE_VAST_ENABLED"))
        if clickaine_enabled:
            _http_url_setting("CLICKAINE_VAST_URL")
        if weights[PROVIDER_PARTNER] > 0:
            raise ImproperlyConfigured(
                "partner has no in-video/VAST adapter; set its weight to 0"
            )
    else:
        raise ValueError(f"Unknown advertising format: {ad_format}")

    eligible = {}
    for provider, weight in weights.items():
        if weight <= 0:
            continue
        if provider == PROVIDER_CLICKAINE and not clickaine_enabled:
            continue
        if ad_format == FORMAT_IN_VIDEO and provider == PROVIDER_PARTNER:
            continue
        eligible[provider] = weight
    return eligible


def weighted_provider_order(ad_format, *, rng=None):
    remaining = dict(eligible_provider_weights(ad_format))
    if not remaining:
        return []

    random_source = rng or random.random
    ordered = []
    while remaining:
        total = sum(remaining.values(), Decimal("0"))
        bucket = Decimal(str(random_source())) * total
        cursor = Decimal("0")
        selected = None
        for provider, weight in remaining.items():
            cursor += weight
            if bucket < cursor:
                selected = provider
                break
        if selected is None:
            selected = next(reversed(remaining))
        ordered.append(selected)
        remaining.pop(selected)
    return ordered


def clickaine_popunder_script_url():
    return _http_url_setting("CLICKAINE_POPUNDER_SCRIPT_URL")


def clickaine_vast_url():
    return _http_url_setting("CLICKAINE_VAST_URL")
