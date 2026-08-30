
from __future__ import annotations

import random
import secrets
from decimal import Decimal, InvalidOperation
from urllib.parse import quote, urlsplit

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
FORMATS = (FORMAT_POPUNDER, FORMAT_IN_VIDEO)

_FORMAT_ENABLED_SETTINGS = {
    FORMAT_POPUNDER: "POPUNDER_ADS_ENABLED",
    FORMAT_IN_VIDEO: "IN_VIDEO_ADS_ENABLED",
}


def _setting(name):
    try:
        return getattr(settings, name)
    except AttributeError as exc:
        raise ImproperlyConfigured(
            f"Missing required advertising setting: {name}"
        ) from exc


def _require_format(ad_format):
    if ad_format not in FORMATS:
        raise ValueError(f"Unknown advertising format: {ad_format}")


def _http_url(value, *, setting_name):
    value = str(value or "").strip()
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ImproperlyConfigured(
            f"{setting_name} must be an absolute HTTP(S) URL"
        )
    return value


def _http_url_setting(name):
    return _http_url(_setting(name), setting_name=name)


def format_enabled(ad_format):
    _require_format(ad_format)
    return bool(_setting(_FORMAT_ENABLED_SETTINGS[ad_format]))


def _normalize_provider_weights(raw, *, setting_label):
    if not isinstance(raw, dict):
        raise ImproperlyConfigured(f"{setting_label} must be a dict")

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
            f"{setting_label} must contain exactly internal, clickaine and partner"
            + (" (" + "; ".join(details) + ")" if details else "")
        )

    weights = {}
    for provider in PROVIDERS:
        value = raw[provider]
        if isinstance(value, bool):
            raise ImproperlyConfigured(
                f"{setting_label}[{provider!r}] must be numeric"
            )
        try:
            weight = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f"{setting_label}[{provider!r}] must be numeric"
            ) from exc
        if not weight.is_finite() or weight < 0:
            raise ImproperlyConfigured(
                f"{setting_label}[{provider!r}] must be >= 0"
            )
        weights[provider] = weight

    if sum(weights.values(), Decimal("0")) != Decimal("100"):
        raise ImproperlyConfigured(
            f"{setting_label} must sum to exactly 100"
        )
    return weights


def provider_weights(ad_format):
    _require_format(ad_format)
    raw = _setting("ADS_PROVIDER_WEIGHTS")
    if not isinstance(raw, dict):
        raise ImproperlyConfigured("ADS_PROVIDER_WEIGHTS must be a dict")

    keys = set(raw)
    expected = set(FORMATS)
    if keys != expected:
        missing = sorted(expected - keys)
        unknown = sorted(keys - expected)
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if unknown:
            details.append("unknown=" + ",".join(unknown))
        raise ImproperlyConfigured(
            "ADS_PROVIDER_WEIGHTS must contain exactly popunder and in_video"
            + (" (" + "; ".join(details) + ")" if details else "")
        )

    return _normalize_provider_weights(
        raw[ad_format],
        setting_label=f"ADS_PROVIDER_WEIGHTS[{ad_format!r}]",
    )



def _partner_popunder_offers():
    raw = getattr(settings, "ADS_PARTNER_POPUNDER_OFFERS", ())
    if raw in (None, ()):
        return []
    if not isinstance(raw, (list, tuple)):
        raise ImproperlyConfigured(
            "ADS_PARTNER_POPUNDER_OFFERS must be a list"
        )

    offers = []
    for index, item in enumerate(raw):
        label = f"ADS_PARTNER_POPUNDER_OFFERS[{index}]"
        if not isinstance(item, dict):
            raise ImproperlyConfigured(f"{label} must be a dict")

        original_template = str(item.get("url_template") or "").strip()
        _http_url(
            original_template.replace("CLICKID", "probe"),
            setting_name=f"{label}.url_template",
        )

        try:
            weight = Decimal(str(item.get("weight", 0)))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ImproperlyConfigured(
                f"{label}.weight must be numeric"
            ) from exc

        if not weight.is_finite() or weight < 0:
            raise ImproperlyConfigured(
                f"{label}.weight must be >= 0"
            )

        if weight > 0:
            offers.append(
                {
                    "weight": weight,
                    "url_template": original_template,
                }
            )

    return offers




def eligible_provider_weights(ad_format):
    _require_format(ad_format)
    if not format_enabled(ad_format):
        return {}

    weights = provider_weights(ad_format)
    clickaine_enabled = False
    partner_enabled = False

    if ad_format == FORMAT_POPUNDER:
        clickaine_enabled = bool(_setting("CLICKAINE_POPUNDER_ENABLED"))
        if weights[PROVIDER_CLICKAINE] > 0 and clickaine_enabled:
            _http_url_setting("CLICKAINE_POPUNDER_SCRIPT_URL")

        if weights[PROVIDER_PARTNER] > 0:
            partner_enabled = bool(_partner_popunder_offers())
    else:
        clickaine_enabled = bool(_setting("CLICKAINE_VAST_ENABLED"))
        if weights[PROVIDER_CLICKAINE] > 0 and clickaine_enabled:
            _http_url_setting("CLICKAINE_VAST_URL")
        if weights[PROVIDER_PARTNER] > 0:
            raise ImproperlyConfigured(
                "partner has no in-video/VAST adapter; "
                "set its in_video weight to 0"
            )

    eligible = {}
    for provider, weight in weights.items():
        if weight <= 0:
            continue
        if provider == PROVIDER_CLICKAINE and not clickaine_enabled:
            continue
        if ad_format == FORMAT_POPUNDER:
            if provider == PROVIDER_PARTNER and not partner_enabled:
                continue
        elif provider == PROVIDER_PARTNER:
            continue
        eligible[provider] = weight
    return eligible



def has_eligible_provider(ad_format):
    return bool(eligible_provider_weights(ad_format))


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



def partner_popunder_url(*, rng=None, click_id=None):
    offers = _partner_popunder_offers()
    if not offers:
        raise ImproperlyConfigured(
            "partner popunder has no configured offers"
        )

    total = sum((offer["weight"] for offer in offers), Decimal("0"))
    random_source = rng or random.random
    bucket = Decimal(str(random_source())) * total
    cursor = Decimal("0")
    selected = offers[-1]

    for offer in offers:
        cursor += offer["weight"]
        if bucket < cursor:
            selected = offer
            break

    raw_click_id = click_id or secrets.token_urlsafe(16)
    encoded_click_id = quote(str(raw_click_id), safe="")
    url = selected["url_template"].replace("CLICKID", encoded_click_id)
    separator = "&" if "?" in url else "?"
    return url + separator + "focus=0"
