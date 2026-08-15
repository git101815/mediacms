from decimal import Decimal, ROUND_DOWN

from django import forms
from django.conf import settings
from PIL import Image

from .models import AdCampaign, AdCampaignCreative, AdCreative

TOKEN_SCALE = 10 ** 6

# configurable-ad-min-bids-v1
_DEFAULT_MIN_BID_TOKENS = Decimal("0.000001")


def _ad_type_for_placement(placement):
    # The self-serve currently exposes only the two native banner slots.
    if placement in {
        AdCampaign.PLACEMENT_HOME,
        AdCampaign.PLACEMENT_SIDEBAR,
    }:
        return "banner"
    return str(placement or "")


def _minimum_bid_tokens(placement, pricing_model):
    ad_type = _ad_type_for_placement(placement)
    configured = getattr(
        settings,
        "ADS_MIN_BID_TOKENS_BY_AD_TYPE",
        {},
    )

    raw = _DEFAULT_MIN_BID_TOKENS
    type_config = configured.get(ad_type, {}) if isinstance(configured, dict) else {}
    if isinstance(type_config, dict):
        raw = type_config.get(pricing_model, _DEFAULT_MIN_BID_TOKENS)
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


def _validate_image_dimensions(upload, placement):
    if not upload or not placement:
        return

    expected = (
        (728, 90)
        if placement == AdCampaign.PLACEMENT_HOME
        else (300, 250)
    )
    try:
        image = Image.open(upload)
        actual = image.size
        image.verify()
        try:
            upload.seek(0)
        except Exception:
            pass
    except Exception as exc:
        raise forms.ValidationError(
            "Creative must be a valid image."
        ) from exc

    if tuple(actual) != expected:
        raise forms.ValidationError(
            f"This placement requires exactly {expected[0]}×{expected[1]} px "
            f"(uploaded: {actual[0]}×{actual[1]} px)."
        )


class AdCreativeForm(forms.ModelForm):
    class Meta:
        model = AdCreative
        fields = ("name", "placement", "image")
        widgets = {
            # Clickaine-style templates already render their own floating labels.
            "name": forms.TextInput(),
        }

    def clean(self):
        cleaned = super().clean()
        image = cleaned.get("image")
        placement = cleaned.get("placement")
        if image and placement:
            _validate_image_dimensions(image, placement)
        return cleaned


class AdCampaignForm(forms.ModelForm):
    bid_tokens = forms.DecimalField(
        label="Bid",
        min_value=Decimal("0.000001"),
        max_digits=20,
        decimal_places=6,
        help_text=(
            "CPM: tokens per 1,000 impressions. "
            "CPC: tokens per click."
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
        bid_widget = self.fields["bid_tokens"].widget
        configured = getattr(
            settings,
            "ADS_MIN_BID_TOKENS_BY_AD_TYPE",
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
                        _DEFAULT_MIN_BID_TOKENS,
                    )
                elif type_config not in (None, ""):
                    raw_min = type_config
                else:
                    raw_min = _DEFAULT_MIN_BID_TOKENS
                bid_widget.attrs[
                    f"data-min-{ad_type}-{pricing_model}"
                ] = str(raw_min)

        if (
            self.instance
            and self.instance.pk
            and "bid_tokens" not in self.initial
        ):
            self.initial["bid_tokens"] = (
                Decimal(self.instance.bid_microtokens)
                / Decimal(TOKEN_SCALE)
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

    def clean_bid_tokens(self):
        value = self.cleaned_data["bid_tokens"]
        units = int(
            (Decimal(value) * Decimal(TOKEN_SCALE)).quantize(
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
        bid_tokens = cleaned.get("bid_tokens")
        creatives = cleaned.get("creative_ids")

        if placement and pricing_model and bid_tokens is not None:
            minimum = _minimum_bid_tokens(
                placement,
                pricing_model,
            )
            if bid_tokens < minimum:
                ad_type = _ad_type_for_placement(placement)
                self.add_error(
                    "bid_tokens",
                    (
                        f"Minimum {pricing_model.upper()} bid for "
                        f"{ad_type} ads is {minimum} tokens."
                    ),
                )

        if not placement or creatives is None:
            return cleaned

        incompatible = [
            creative
            for creative in creatives
            if creative.placement != placement
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
            Decimal(self.cleaned_data["bid_tokens"])
            * Decimal(TOKEN_SCALE)
        )
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
