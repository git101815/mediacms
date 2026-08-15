from decimal import Decimal, ROUND_DOWN

from django import forms
from PIL import Image

from .models import AdCampaign, AdCampaignCreative, AdCreative

TOKEN_SCALE = 10 ** 6


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
        widget=forms.NumberInput(
            attrs={"step": "0.000001", "min": "0.000001"}
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
        creatives = cleaned.get("creative_ids")
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
