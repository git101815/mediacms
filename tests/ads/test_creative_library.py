from io import BytesIO
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from PIL import Image

from ads.forms import AdCampaignForm
from ads.models import AdCampaign, AdCampaignCreative, AdCreative
from ads.runtime import _choose_creative


def _image(name, size):
    buf = BytesIO()
    Image.new("RGB", size).save(buf, format="PNG")
    return SimpleUploadedFile(
        name,
        buf.getvalue(),
        content_type="image/png",
    )


@pytest.mark.django_db
def test_one_creative_can_be_reused_by_multiple_campaigns(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="creative-reuse",
        advertiserUser=True,
    )
    creative = AdCreative.objects.create(
        advertiser=user,
        name="Reusable",
        placement=AdCreative.PLACEMENT_HOME,
        image=_image("reusable.png", (728, 90)),
        review_status=AdCreative.REVIEW_APPROVED,
    )

    for index in range(2):
        form = AdCampaignForm(
            data={
                "name": f"Campaign {index}",
                "placement": AdCampaign.PLACEMENT_HOME,
                "target_url": "https://example.com/",
                "pricing_model": AdCampaign.PRICING_CPM,
                "bid_usd": "1",
                "creative_ids": [str(creative.pk)],
            },
            advertiser=user,
        )
        assert form.is_valid(), form.errors
        campaign = form.save(commit=False)
        campaign.advertiser = user
        campaign.save()
        form.save_creatives(campaign)

    assert creative.campaigns.count() == 2


@pytest.mark.django_db
def test_campaign_can_have_multiple_creatives(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="creative-ab",
        advertiserUser=True,
    )
    creatives = [
        AdCreative.objects.create(
            advertiser=user,
            name=f"Creative {index}",
            placement=AdCreative.PLACEMENT_HOME,
            image=_image(f"creative-{index}.png", (728, 90)),
            review_status=AdCreative.REVIEW_APPROVED,
        )
        for index in range(2)
    ]

    form = AdCampaignForm(
        data={
            "name": "A/B",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/",
            "pricing_model": AdCampaign.PRICING_CPC,
            "bid_usd": "1",
            "creative_ids": [
                str(creatives[0].pk),
                str(creatives[1].pk),
            ],
        },
        advertiser=user,
    )
    assert form.is_valid(), form.errors
    campaign = form.save(commit=False)
    campaign.advertiser = user
    campaign.save()
    form.save_creatives(campaign)

    assert campaign.creatives.count() == 2
    assert AdCampaignCreative.objects.filter(
        campaign=campaign,
        enabled=True,
        weight=1,
    ).count() == 2


def test_choose_creative_uses_weights_for_rotation():
    pool = [
        {"id": 10, "url": "/a.png", "weight": 1},
        {"id": 20, "url": "/b.png", "weight": 1},
    ]
    with patch("ads.runtime.secrets.randbelow", return_value=0):
        assert _choose_creative(pool)["id"] == 10
    with patch("ads.runtime.secrets.randbelow", return_value=1):
        assert _choose_creative(pool)["id"] == 20
