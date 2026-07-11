from decimal import Decimal

from django import forms
from django.db import transaction

from files.methods import is_mediacms_manager
from premium.models import CreatorSubscriptionPlan

from .models import Channel, User


TOKEN_SCALE = 10 ** 6
DEFAULT_SUBSCRIPTION_PLAN_CODE = "default"
DEFAULT_SUBSCRIPTION_PLAN_NAME = "Membership"
DEFAULT_SUBSCRIPTION_PERIOD_DAYS = 30
MAX_SUBSCRIPTION_PRICE = Decimal("9223372036854.77")


def _format_token_amount_for_form(value: int) -> str:
    amount = Decimal(int(value)) / Decimal(TOKEN_SCALE)
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


class SignupForm(forms.Form):
    name = forms.CharField(max_length=100, label="Name")

    def signup(self, request, user):
        user.name = self.cleaned_data["name"]
        user.save()


class UserForm(forms.ModelForm):
    subscriptions_enabled = forms.BooleanField(
        required=False,
        label="Enable creator subscriptions",
        help_text=(
            "Show a recurring subscription offer on your public profile. "
            "Subscribers keep permanent access to premium videos published "
            "during each paid period."
        ),
    )
    subscription_price = forms.DecimalField(
        required=False,
        min_value=Decimal("0.01"),
        max_value=MAX_SUBSCRIPTION_PRICE,
        max_digits=15,
        decimal_places=2,
        label="Subscription price",
        help_text=(
            "Price in tokens charged every 30 days. A new price applies to "
            "new subscriptions and to existing subscribers at their next renewal."
        ),
        widget=forms.NumberInput(
            attrs={
                "min": "0.01",
                "step": "0.01",
                "inputmode": "decimal",
            }
        ),
    )

    class Meta:
        model = User
        fields = (
            "name",
            "description",
            "global_media_description",
            "logo",
            "notification_on_comments",
            "is_featured",
            "advancedUser",
            "is_manager",
            "is_editor",
            "dfans_url",
            # "allow_contact",
        )

    def clean_logo(self):
        if "logo" not in self.changed_data:
            return self.cleaned_data.get("logo")
        image = self.cleaned_data.get("logo", False)
        if image:
            if image.size > 2 * 1024 * 1024:
                raise forms.ValidationError("Image file too large ( > 2mb )")
            return image
        else:
            raise forms.ValidationError("Please provide a logo")

    def __init__(self, user, *args, **kwargs):
        super(UserForm, self).__init__(*args, **kwargs)
        self._acting_user = user
        self._can_manage_creator_settings = bool(
            self.instance.pk
            and bool(getattr(self.instance, "advancedUser", False))
            and user.is_authenticated
            and self.instance.pk == user.pk
        )

        self.fields.pop("is_featured")
        if not is_mediacms_manager(user):
            self.fields.pop("advancedUser")
            self.fields.pop("is_manager")
            self.fields.pop("is_editor")
        if user.socialaccount_set.exists():
            # for Social Accounts do not allow to edit the name
            self.fields["name"].widget.attrs['readonly'] = True

        if not self._can_manage_creator_settings:
            self.fields.pop("dfans_url")
            self.fields.pop("subscriptions_enabled")
            self.fields.pop("subscription_price")
            return

        plan = CreatorSubscriptionPlan.objects.filter(
            creator=self.instance,
            code=DEFAULT_SUBSCRIPTION_PLAN_CODE,
        ).first()
        if plan is not None:
            self.fields["subscriptions_enabled"].initial = plan.is_active
            self.fields["subscription_price"].initial = (
                _format_token_amount_for_form(plan.price_tokens)
            )

    def clean(self):
        cleaned_data = super().clean()
        if not self._can_manage_creator_settings:
            return cleaned_data

        enabled = cleaned_data.get("subscriptions_enabled", False)
        price = cleaned_data.get("subscription_price")
        if enabled and price is None:
            self.add_error(
                "subscription_price",
                "Set a subscription price before enabling subscriptions.",
            )
        return cleaned_data

    @transaction.atomic
    def _save_subscription_settings(self, creator):
        enabled = bool(self.cleaned_data.get("subscriptions_enabled", False))
        price = self.cleaned_data.get("subscription_price")

        plan = (
            CreatorSubscriptionPlan.objects.select_for_update()
            .filter(
                creator=creator,
                code=DEFAULT_SUBSCRIPTION_PLAN_CODE,
            )
            .first()
        )

        if plan is None and not enabled and price is None:
            return None

        if price is None:
            price_tokens = int(plan.price_tokens)
        else:
            price_tokens = int(price * TOKEN_SCALE)

        if plan is None:
            plan = CreatorSubscriptionPlan(
                creator=creator,
                code=DEFAULT_SUBSCRIPTION_PLAN_CODE,
            )

        plan.name = DEFAULT_SUBSCRIPTION_PLAN_NAME
        plan.price_tokens = price_tokens
        plan.billing_period_days = DEFAULT_SUBSCRIPTION_PERIOD_DAYS
        plan.access_policy = CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES
        plan.is_active = enabled
        plan.save()
        plan.included_collections.clear()
        return plan

    def save(self, commit=True):
        saved_user = super().save(commit=commit)
        if self._can_manage_creator_settings:
            self._save_subscription_settings(saved_user)
        return saved_user


class ChannelForm(forms.ModelForm):
    class Meta:
        model = Channel
        fields = ("banner_logo",)

    def clean_banner_logo(self):
        if "banner_logo" not in self.changed_data:
            return self.cleaned_data.get("banner_logo")
        image = self.cleaned_data.get("banner_logo", False)
        if image:
            if image.size > 2 * 1024 * 1024:
                raise forms.ValidationError("Image file too large ( > 2mb )")
            return image
        else:
            raise forms.ValidationError("Please provide a banner")
