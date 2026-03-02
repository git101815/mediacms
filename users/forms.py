from django import forms

from files.methods import is_mediacms_manager

from .models import Channel, User


class SignupForm(forms.Form):
    name = forms.CharField(max_length=100, label="Name")

    def signup(self, request, user):
        user.name = self.cleaned_data["name"]
        user.save()


class UserForm(forms.ModelForm):
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
        self.fields.pop("is_featured")
        if not is_mediacms_manager(user):
            self.fields.pop("advancedUser")
            self.fields.pop("is_manager")
            self.fields.pop("is_editor")
        if user.socialaccount_set.exists():
            # for Social Accounts do not allow to edit the name
            self.fields["name"].widget.attrs['readonly'] = True


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
