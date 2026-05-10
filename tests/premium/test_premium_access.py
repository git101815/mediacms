import pytest

from files.models import Media
from premium.models import PremiumMediaAsset, PremiumMediaUnlock
from premium.services import user_can_access_premium_media


def create_test_media(*, user, friendly_token):
    Media.objects.bulk_create(
        [
            Media(
                user=user,
                friendly_token=friendly_token,
                title="Premium video",
                media_file="tests/premium/test.mp4",
                media_type="video",
                state="public",
                encoding_status="success",
                is_reviewed=True,
                listable=True,
            )
        ]
    )
    return Media.objects.get(friendly_token=friendly_token)


@pytest.mark.django_db
def test_user_cannot_access_locked_premium_media(django_user_model):
    creator = django_user_model.objects.create_user(username="creator")
    buyer = django_user_model.objects.create_user(username="buyer")

    media = create_test_media(
        user=creator,
        friendly_token="premiumlocked",
    )

    PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_READY,
        price_tokens=500000000,
        direct_url="https://example.com/full.mp4",
    )

    assert user_can_access_premium_media(user=buyer, media=media) is False


@pytest.mark.django_db
def test_user_can_access_unlocked_premium_media(django_user_model):
    creator = django_user_model.objects.create_user(username="creator2")
    buyer = django_user_model.objects.create_user(username="buyer2")

    media = create_test_media(
        user=creator,
        friendly_token="premiumunlock",
    )

    PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_READY,
        price_tokens=500000000,
        direct_url="https://example.com/full.mp4",
    )

    PremiumMediaUnlock.objects.create(
        user=buyer,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
    )

    assert user_can_access_premium_media(user=buyer, media=media) is True