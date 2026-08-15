import re
from decimal import Decimal, ROUND_DOWN
from io import BytesIO
from pathlib import Path
from xml.etree import ElementTree

from django import forms
from django.conf import settings
from PIL import Image

from ledger.services import PLATFORM_TOKENS_PER_STABLECOIN

from .models import (
    AdCampaign,
    AdCampaignCreative,
    AdCreative,
    creative_format_for_campaign_placement,
)

TOKEN_SCALE = 10 ** 6
USD_TO_MICROTOKENS = (
    Decimal(PLATFORM_TOKENS_PER_STABLECOIN) * Decimal(TOKEN_SCALE)
)

# Ads is USD-denominated. Storage remains integer ledger units internally.
_DEFAULT_MIN_BID_USD = Decimal("0.000001")
# Banner creatives are the only uploaded direct-ad assets. Keep the hard
# application-side cap small even though nginx accepts much larger media uploads.
_MAX_BANNER_CREATIVE_BYTES = 5 * 1024 * 1024

_BANNER_DIMENSIONS = {
    AdCreative.PLACEMENT_HOME: (728, 90),
    AdCreative.PLACEMENT_SIDEBAR: (300, 250),
}
_RASTER_BANNER_MIME_BY_FORMAT = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "GIF": "image/gif",
}
_SVG_FORBIDDEN_ELEMENTS = {
    "script",
    "style",
    "foreignobject",
    "iframe",
    "object",
    "embed",
}


def _ad_type_for_placement(placement):
    if placement in {
        AdCampaign.PLACEMENT_HOME,
        AdCampaign.PLACEMENT_SIDEBAR,
    }:
        return "banner"
    if placement in {
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
    }:
        # local_settings uses one floor for the existing in-video inventory.
        return "preroll"
    if placement == AdCampaign.PLACEMENT_POPUNDER:
        return "popunder"
    return str(placement or "")


def _minimum_bid_usd(placement, pricing_model):
    ad_type = _ad_type_for_placement(placement)
    configured = getattr(
        settings,
        "ADS_MIN_BID_USD_BY_AD_TYPE",
        {},
    )

    raw = _DEFAULT_MIN_BID_USD
    type_config = configured.get(ad_type, {}) if isinstance(configured, dict) else {}
    if isinstance(type_config, dict):
        raw = type_config.get(pricing_model, _DEFAULT_MIN_BID_USD)
    elif type_config not in (None, ""):
        raw = type_config

    try:
        value = Decimal(str(raw))
    except Exception as exc:
        raise forms.ValidationError(
            f"Invalid minimum bid configured for {ad_type or 'unknown'} ads."
        ) from exc

    if value <= 0:
        raise forms.ValidationError(
            f"Minimum bid for {ad_type or 'unknown'} ads must be greater than zero."
        )
    return value


def _validate_http_url(value, *, field_name):
    value = str(value or "").strip()
    if not value:
        return
    if not value.lower().startswith(("http://", "https://")):
        raise forms.ValidationError(
            f"{field_name} must use http:// or https://."
        )


def _read_upload_bytes(upload):
    declared_size = getattr(upload, "size", None)
    if declared_size is not None:
        try:
            declared_size = int(declared_size)
        except (TypeError, ValueError):
            declared_size = None
    if (
        declared_size is not None
        and declared_size > _MAX_BANNER_CREATIVE_BYTES
    ):
        raise forms.ValidationError(
            "Banner creative must be 5 MB or smaller."
        )

    try:
        upload.seek(0)
    except Exception:
        try:
            upload.open("rb")
        except Exception:
            pass

    try:
        # Never trust only the declared upload size. The bounded read protects
        # the worker even for a malformed/custom UploadedFile implementation.
        data = upload.read(_MAX_BANNER_CREATIVE_BYTES + 1)
    except Exception as exc:
        raise forms.ValidationError(
            "Creative file could not be read."
        ) from exc
    finally:
        try:
            upload.seek(0)
        except Exception:
            pass

    if len(data) > _MAX_BANNER_CREATIVE_BYTES:
        raise forms.ValidationError(
            "Banner creative must be 5 MB or smaller."
        )

    return data


def _parse_svg_dimension(value):
    if value in (None, ""):
        return None
    match = re.fullmatch(
        r"\s*([0-9]+(?:\.[0-9]+)?)\s*(?:px)?\s*",
        str(value),
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return Decimal(match.group(1))


def _validate_svg_banner(upload, expected):
    raw = _read_upload_bytes(upload)
    upper = raw.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise forms.ValidationError(
            "SVG banners may not contain DTD or ENTITY declarations."
        )

    try:
        root = ElementTree.fromstring(raw)
    except Exception as exc:
        raise forms.ValidationError(
            "Creative must be a valid SVG file."
        ) from exc

    if root.tag.rsplit("}", 1)[-1].lower() != "svg":
        raise forms.ValidationError(
            "Creative must be a valid SVG file."
        )

    for element in root.iter():
        local_name = element.tag.rsplit("}", 1)[-1].lower()
        if local_name in _SVG_FORBIDDEN_ELEMENTS:
            raise forms.ValidationError(
                f"SVG element <{local_name}> is not allowed."
            )

        for raw_name, raw_value in element.attrib.items():
            attr = raw_name.rsplit("}", 1)[-1].lower()
            value = str(raw_value or "").strip().lower()

            if attr.startswith("on"):
                raise forms.ValidationError(
                    "SVG event-handler attributes are not allowed."
                )

            if attr in {"href", "src"} and value and not value.startswith("#"):
                raise forms.ValidationError(
                    "SVG external references are not allowed."
                )

            css_urls = re.findall(
                r"url\s*\(\s*[\"']?([^)\"']+)",
                value,
                flags=re.IGNORECASE,
            )
            if (
                "javascript:" in value
                or any(
                    not ref.strip().startswith("#")
                    for ref in css_urls
                )
            ):
                raise forms.ValidationError(
                    "SVG active or external content is not allowed."
                )

    width_attr = root.attrib.get("width")
    height_attr = root.attrib.get("height")

    if width_attr not in (None, "") or height_attr not in (None, ""):
        width = _parse_svg_dimension(width_attr)
        height = _parse_svg_dimension(height_attr)
        if width is None or height is None:
            raise forms.ValidationError(
                "SVG width and height must be numeric pixel dimensions."
            )
    else:
        viewbox = str(root.attrib.get("viewBox") or "").replace(",", " ").split()
        if len(viewbox) != 4:
            raise forms.ValidationError(
                "SVG banners must declare exact width/height or a numeric viewBox."
            )
        try:
            width = Decimal(viewbox[2])
            height = Decimal(viewbox[3])
        except Exception as exc:
            raise forms.ValidationError(
                "SVG banners must declare exact width/height or a numeric viewBox."
            ) from exc

    if (
        width != Decimal(expected[0])
        or height != Decimal(expected[1])
    ):
        raise forms.ValidationError(
            f"This format requires exactly {expected[0]}×{expected[1]} px "
            f"(uploaded: {width}×{height} px)."
        )

    return "image/svg+xml"


def _validate_banner_asset(upload, placement):
    if not upload or not placement:
        return ""

    expected = _BANNER_DIMENSIONS.get(placement)
    if expected is None:
        raise forms.ValidationError("Unknown banner format.")

    suffix = Path(getattr(upload, "name", "") or "").suffix.lower()
    if suffix == ".svg":
        return _validate_svg_banner(upload, expected)

    raw = _read_upload_bytes(upload)
    try:
        image = Image.open(BytesIO(raw))
        actual = image.size
        image_format = str(image.format or "").upper()
        image.verify()
    except Exception as exc:
        raise forms.ValidationError(
            "Banner must be a valid PNG, JPG, SVG or GIF image."
        ) from exc

    if image_format not in _RASTER_BANNER_MIME_BY_FORMAT:
        raise forms.ValidationError(
            "Banner must be a PNG, JPG, SVG or GIF image."
        )

    if tuple(actual) != expected:
        raise forms.ValidationError(
            f"This format requires exactly {expected[0]}×{expected[1]} px "
            f"(uploaded: {actual[0]}×{actual[1]} px)."
        )

    return _RASTER_BANNER_MIME_BY_FORMAT[image_format]


class AdCreativeForm(forms.ModelForm):
    class Meta:
        model = AdCreative
        fields = (
            "name",
            "placement",
            "image",
            "vast_url",
            "destination_url",
        )
        labels = {
            "image": "Banner file",
            "vast_url": "VAST URL",
            "destination_url": "Destination URL",
        }
        widgets = {
            # Clickaine-style templates already render their own floating labels.
            "name": forms.TextInput(),
            "vast_url": forms.URLInput(),
            "destination_url": forms.URLInput(),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["image"].required = False
        self.fields["vast_url"].required = False
        self.fields["destination_url"].required = False
        self.fields["image"].widget.attrs["accept"] = (
            ".png,.jpg,.jpeg,.gif,.svg,"
            "image/png,image/jpeg,image/gif,image/svg+xml"
        )

    def clean(self):
        cleaned = super().clean()
        placement = cleaned.get("placement")
        image = cleaned.get("image")
        vast_url = str(cleaned.get("vast_url") or "").strip()
        destination_url = str(
            cleaned.get("destination_url") or ""
        ).strip()

        if placement in _BANNER_DIMENSIONS:
            if not image:
                self.add_error(
                    "image",
                    "A banner file is required.",
                )
            else:
                try:
                    _validate_banner_asset(image, placement)
                except forms.ValidationError as exc:
                    self.add_error("image", exc)
            cleaned["vast_url"] = ""
            cleaned["destination_url"] = ""

        elif placement == AdCreative.PLACEMENT_IN_VIDEO:
            if not vast_url:
                self.add_error(
                    "vast_url",
                    "A VAST URL is required for in-video ads.",
                )
            else:
                try:
                    _validate_http_url(
                        vast_url,
                        field_name="VAST URL",
                    )
                except forms.ValidationError as exc:
                    self.add_error("vast_url", exc)
            cleaned["image"] = None
            cleaned["destination_url"] = ""

        elif placement == AdCreative.PLACEMENT_POPUNDER:
            if not destination_url:
                self.add_error(
                    "destination_url",
                    "A destination URL is required for popunder.",
                )
            else:
                try:
                    _validate_http_url(
                        destination_url,
                        field_name="Destination URL",
                    )
                except forms.ValidationError as exc:
                    self.add_error("destination_url", exc)
            cleaned["image"] = None
            cleaned["vast_url"] = ""

        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)

        if obj.is_banner:
            obj.vast_url = ""
            obj.destination_url = ""
        elif obj.is_in_video:
            obj.image = ""
            obj.destination_url = ""
        elif obj.is_popunder:
            obj.image = ""
            obj.vast_url = ""

        if commit:
            obj.save()
        return obj


class AdCampaignForm(forms.ModelForm):
    bid_usd = forms.DecimalField(
        label="Bid ($)",
        min_value=Decimal("0.000001"),
        max_digits=20,
        decimal_places=6,
        help_text=(
            "CPM: USD per 1,000 impressions. "
            "CPC: USD per valid click."
        ),
        widget=forms.TextInput(
            attrs={
                "inputmode": "decimal",
                "autocomplete": "off",
            }
        ),
    )
    creative_ids = forms.ModelMultipleChoiceField(
        queryset=AdCreative.objects.none(),
        required=True,
        label="Creatives",
        help_text=(
            "Select one or more creatives. Multiple creatives rotate evenly "
            "for A/B delivery."
        ),
    )

    class Meta:
        model = AdCampaign
        fields = (
            "name",
            "placement",
            "target_url",
            "pricing_model",
        )
        widgets = {
            # The template provides floating labels. Placeholders would render
            # on top of those labels on empty fields.
            "name": forms.TextInput(),
            "target_url": forms.URLInput(),
        }

    def __init__(self, *args, advertiser=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.advertiser = advertiser
        self.fields["target_url"].required = False

        if self.advertiser is None and self.instance and self.instance.pk:
            self.advertiser = self.instance.advertiser

        queryset = AdCreative.objects.none()
        if self.advertiser is not None:
            queryset = (
                AdCreative.objects
                .filter(advertiser=self.advertiser)
                .exclude(review_status=AdCreative.REVIEW_REJECTED)
                .order_by("placement", "-updated_at", "-id")
            )
        self.fields["creative_ids"].queryset = queryset

        # Keep the HTML constraint synchronized with local_settings.py.
        bid_widget = self.fields["bid_usd"].widget
        configured = getattr(
            settings,
            "ADS_MIN_BID_USD_BY_AD_TYPE",
            {},
        )
        for ad_type in ("banner", "preroll", "popunder"):
            type_config = (
                configured.get(ad_type, {})
                if isinstance(configured, dict)
                else {}
            )
            for pricing_model in (
                AdCampaign.PRICING_CPM,
                AdCampaign.PRICING_CPC,
            ):
                if isinstance(type_config, dict):
                    raw_min = type_config.get(
                        pricing_model,
                        _DEFAULT_MIN_BID_USD,
                    )
                elif type_config not in (None, ""):
                    raw_min = type_config
                else:
                    raw_min = _DEFAULT_MIN_BID_USD
                bid_widget.attrs[
                    f"data-min-{ad_type}-{pricing_model}"
                ] = str(raw_min)

        if (
            self.instance
            and self.instance.pk
            and "bid_usd" not in self.initial
        ):
            self.initial["bid_usd"] = (
                Decimal(self.instance.bid_microtokens)
                / USD_TO_MICROTOKENS
            )

        if (
            self.instance
            and self.instance.pk
            and "creative_ids" not in self.initial
        ):
            self.initial["creative_ids"] = list(
                self.instance.creative_links
                .filter(enabled=True)
                .values_list("creative_id", flat=True)
            )

    def clean_bid_usd(self):
        value = self.cleaned_data["bid_usd"]
        units = int(
            (Decimal(value) * USD_TO_MICROTOKENS).quantize(
                Decimal("1"),
                rounding=ROUND_DOWN,
            )
        )
        if units <= 0:
            raise forms.ValidationError(
                "Bid must be greater than zero."
            )
        return value

    def clean(self):
        cleaned = super().clean()
        placement = cleaned.get("placement")
        pricing_model = cleaned.get("pricing_model")
        bid_usd = cleaned.get("bid_usd")
        creatives = cleaned.get("creative_ids")

        if placement and pricing_model and bid_usd is not None:
            minimum = _minimum_bid_usd(
                placement,
                pricing_model,
            )
            if bid_usd < minimum:
                ad_type = _ad_type_for_placement(placement)
                self.add_error(
                    "bid_usd",
                    (
                        f"Minimum {pricing_model.upper()} bid for "
                        f"{ad_type} ads is ${minimum}."
                    ),
                )

        if placement in {
            AdCampaign.PLACEMENT_HOME,
            AdCampaign.PLACEMENT_SIDEBAR,
        }:
            target_url = str(
                cleaned.get("target_url") or ""
            ).strip()
            if not target_url:
                self.add_error(
                    "target_url",
                    "A destination URL is required for banner campaigns.",
                )
            else:
                try:
                    _validate_http_url(
                        target_url,
                        field_name="Destination URL",
                    )
                except forms.ValidationError as exc:
                    self.add_error("target_url", exc)
        else:
            cleaned["target_url"] = ""

        if not placement or creatives is None:
            return cleaned

        required_format = creative_format_for_campaign_placement(
            placement
        )
        incompatible = [
            creative
            for creative in creatives
            if creative.placement != required_format
        ]
        if incompatible:
            self.add_error(
                "creative_ids",
                "Every selected creative must match the campaign format.",
            )

        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        obj.bid_microtokens = int(
            (
                Decimal(self.cleaned_data["bid_usd"])
                * USD_TO_MICROTOKENS
            ).quantize(
                Decimal("1"),
                rounding=ROUND_DOWN,
            )
        )

        if obj.placement not in {
            AdCampaign.PLACEMENT_HOME,
            AdCampaign.PLACEMENT_SIDEBAR,
        }:
            obj.target_url = ""

        if commit:
            obj.save()
            self.save_creatives(obj)
        return obj

    def save_creatives(self, campaign):
        selected = list(self.cleaned_data.get("creative_ids") or [])
        selected_ids = {creative.pk for creative in selected}

        (
            AdCampaignCreative.objects
            .filter(campaign=campaign)
            .exclude(creative_id__in=selected_ids)
            .delete()
        )

        for creative in selected:
            AdCampaignCreative.objects.update_or_create(
                campaign=campaign,
                creative=creative,
                defaults={"enabled": True},
            )

        return selected
