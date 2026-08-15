from io import BytesIO
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from PIL import Image

from ads.forms import AdCampaignForm
from ads.models import AdCampaign
from ledger.services import get_wallet_available_balance


def _image(name, size):
    buf = BytesIO()
    Image.new("RGB", size).save(buf, format="PNG")
    return SimpleUploadedFile(name, buf.getvalue(), content_type="image/png")


@pytest.mark.django_db
def test_advertiser_flag_defaults_false(django_user_model):
    user = django_user_model.objects.create_user(username="ads-default-user")
    assert user.advertiserUser is False


@pytest.mark.django_db
def test_campaign_form_rejects_wrong_creative_dimensions():
    form = AdCampaignForm(
        data={
            "name": "Bad dimensions",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_tokens": "1",
        },
        files={"creative": _image("wrong.png", (300, 250))},
    )
    assert not form.is_valid()
    assert "requires exactly 728×90" in str(form.errors)


@pytest.mark.django_db
def test_advertiser_wallet_available_balance_subtracts_unsettled(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="ads-wallet-user",
        advertiserUser=True,
    )
    wallet = user.token_wallet
    wallet.balance = 10_000_000
    wallet.held_balance = 1_000_000
    wallet.save(update_fields=["balance", "held_balance", "updated_at"])

    with patch("ads.runtime.get_account_unsettled_microtokens", return_value=2_000_000):
        assert get_wallet_available_balance(wallet) == 7_000_000
