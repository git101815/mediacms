import pytest

from users.forms import UserForm


CREATOR_NOTIFICATION_FIELDS = {
    "notification_on_premium_purchases",
    "notification_on_new_subscriptions",
    "notification_on_subscription_renewals",
}


@pytest.mark.django_db
def test_creator_notification_checkboxes_are_visible_on_own_profile(
    django_user_model,
):
    creator = django_user_model.objects.create_user(
        username="creator_notification_profile",
        email="creator-notify@example.com",
        advancedUser=True,
    )

    form = UserForm(creator, instance=creator)

    assert CREATOR_NOTIFICATION_FIELDS.issubset(form.fields)
    assert form["notification_on_premium_purchases"].value() is True
    assert form["notification_on_new_subscriptions"].value() is True
    assert form["notification_on_subscription_renewals"].value() is False


@pytest.mark.django_db
def test_creator_notification_checkboxes_are_hidden_for_non_creator(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="regular_notification_profile",
        email="regular-notify@example.com",
        advancedUser=False,
    )

    form = UserForm(user, instance=user)

    assert CREATOR_NOTIFICATION_FIELDS.isdisjoint(form.fields)


@pytest.mark.django_db
def test_creator_profile_saves_notification_checkboxes(
    django_user_model,
):
    creator = django_user_model.objects.create_user(
        username="creator_notification_save",
        email="creator-save@example.com",
        advancedUser=True,
        name="Creator",
    )

    form = UserForm(
        creator,
        data={
            "name": "Creator",
            "description": "",
            "global_media_description": "",
            "notification_on_comments": "on",
            "notification_on_new_subscriptions": "on",
            "dfans_url": "",
            "subscriptions_enabled": "",
            "subscription_price": "",
        },
        instance=creator,
    )

    assert form.is_valid(), form.errors
    saved = form.save()
    saved.refresh_from_db()

    assert saved.notification_on_premium_purchases is False
    assert saved.notification_on_new_subscriptions is True
    assert saved.notification_on_subscription_renewals is False
