from decimal import Decimal, ROUND_DOWN

from django import forms
from PIL import Image

from .models import AdCampaign

TOKEN_SCALE = 10 ** 6


class AdCampaignForm(forms.ModelForm):
    bid_tokens = forms.DecimalField(
        label="Bid",
        min_value=Decimal("0.000001"),
        max_digits=20,
        decimal_places=6,
        help_text="CPM: tokens per 1,000 impressions. CPC: tokens per click.",
        widget=forms.NumberInput(attrs={"step": "0.000001", "min": "0.000001"}),
    )

    class Meta:
        model = AdCampaign
        fields = (
            "name",
            "placement",
            "target_url",
            "creative",
            "pricing_model",
        )
        widgets = {
            "name": forms.TextInput(attrs={"placeholder": "Campaign name"}),
            "target_url": forms.URLInput(attrs={"placeholder": "https://example.com/landing"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.pk and "bid_tokens" not in self.initial:
            self.initial["bid_tokens"] = (
                Decimal(self.instance.bid_microtokens) / Decimal(TOKEN_SCALE)
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
            raise forms.ValidationError("Bid must be greater than zero.")
        return value

    def clean(self):
        cleaned = super().clean()
        creative = cleaned.get("creative")
        placement = cleaned.get("placement")
        if not creative or not placement:
            return cleaned

        expected = (
            (728, 90)
            if placement == AdCampaign.PLACEMENT_HOME
            else (300, 250)
        )
        try:
            image = Image.open(creative)
            actual = image.size
            image.verify()
            try:
                creative.seek(0)
            except Exception:
                pass
        except Exception as exc:
            raise forms.ValidationError("Creative must be a valid image.") from exc

        if tuple(actual) != expected:
            raise forms.ValidationError(
                f"This placement requires exactly {expected[0]}×{expected[1]} px "
                f"(uploaded: {actual[0]}×{actual[1]} px)."
            )
        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        obj.bid_microtokens = int(
            Decimal(self.cleaned_data["bid_tokens"]) * Decimal(TOKEN_SCALE)
        )
        if commit:
            obj.save()
        return obj
