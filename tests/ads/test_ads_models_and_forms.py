from io import BytesIO
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import override_settings
from PIL import Image

from ads.forms import AdCampaignForm, AdCreativeForm
from ads.models import AdCampaign, AdCreative
from ledger.services import get_wallet_available_balance


def _image(name, size):
    buf = BytesIO()
    Image.new("RGB", size).save(buf, format="PNG")
    return SimpleUploadedFile(
        name,
        buf.getvalue(),
        content_type="image/png",
    )


@pytest.mark.django_db
def test_advertiser_flag_defaults_false(django_user_model):
    user = django_user_model.objects.create_user(
        username="ads-default-user"
    )
    assert user.advertiserUser is False


@pytest.mark.django_db
def test_creative_form_rejects_wrong_dimensions():
    form = AdCreativeForm(
        data={
            "name": "Bad dimensions",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={
            "image": _image("wrong.png", (300, 250)),
        },
    )
    assert not form.is_valid()
    assert "requires exactly 728×90" in str(form.errors)


@pytest.mark.django_db
def test_campaign_form_rejects_wrong_format_creative(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="ads-form-user",
        advertiserUser=True,
    )
    creative = AdCreative.objects.create(
        advertiser=user,
        name="Sidebar",
        placement=AdCreative.PLACEMENT_SIDEBAR,
        image=_image("sidebar.png", (300, 250)),
        review_status=AdCreative.REVIEW_APPROVED,
    )
    form = AdCampaignForm(
        data={
            "name": "Homepage campaign",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_tokens": "1",
            "creative_ids": [str(creative.pk)],
        },
        advertiser=user,
    )
    assert not form.is_valid()
    assert "must match the campaign format" in str(form.errors)


@pytest.mark.django_db
def test_campaign_form_rejects_other_advertiser_creative(
    django_user_model,
):
    owner = django_user_model.objects.create_user(
        username="ads-owner",
        advertiserUser=True,
    )
    other = django_user_model.objects.create_user(
        username="ads-other",
        advertiserUser=True,
    )
    creative = AdCreative.objects.create(
        advertiser=other,
        name="Other creative",
        placement=AdCreative.PLACEMENT_HOME,
        image=_image("other.png", (728, 90)),
        review_status=AdCreative.REVIEW_APPROVED,
    )
    form = AdCampaignForm(
        data={
            "name": "Campaign",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_tokens": "1",
            "creative_ids": [str(creative.pk)],
        },
        advertiser=owner,
    )
    assert not form.is_valid()
    assert "Select a valid choice" in str(form.errors)


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
    wallet.save(
        update_fields=[
            "balance",
            "held_balance",
            "updated_at",
        ]
    )

    with patch(
        "ads.runtime.get_account_unsettled_microtokens",
        return_value=2_000_000,
    ):
        assert get_wallet_available_balance(wallet) == 7_000_000



@pytest.mark.django_db
@override_settings(
    ADS_MIN_BID_TOKENS_BY_AD_TYPE={
        "banner": {
            "cpm": "2.5",
            "cpc": "0.25",
        },
        "preroll": {
            "cpm": "4",
            "cpc": "0.5",
        },
        "popunder": {
            "cpm": "3",
            "cpc": "0.4",
        },
    }
)
def test_campaign_form_enforces_configured_banner_minimum_bid(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="ads-min-bid-user",
        advertiserUser=True,
    )
    creative = AdCreative.objects.create(
        advertiser=user,
        name="Homepage creative",
        placement=AdCreative.PLACEMENT_HOME,
        image=_image("minimum.png", (728, 90)),
        review_status=AdCreative.REVIEW_APPROVED,
    )

    below_cpm = AdCampaignForm(
        data={
            "name": "Below CPM minimum",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_tokens": "2.49",
            "creative_ids": [str(creative.pk)],
        },
        advertiser=user,
    )
    assert not below_cpm.is_valid()
    assert "Minimum CPM bid for banner ads is 2.5 tokens" in str(
        below_cpm.errors
    )

    at_cpm = AdCampaignForm(
        data={
            "name": "At CPM minimum",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_tokens": "2.5",
            "creative_ids": [str(creative.pk)],
        },
        advertiser=user,
    )
    assert at_cpm.is_valid()

    below_cpc = AdCampaignForm(
        data={
            "name": "Below CPC minimum",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPC,
            "bid_tokens": "0.24",
            "creative_ids": [str(creative.pk)],
        },
        advertiser=user,
    )
    assert not below_cpc.is_valid()
    assert "Minimum CPC bid for banner ads is 0.25 tokens" in str(
        below_cpc.errors
    )
