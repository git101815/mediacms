from io import BytesIO
from unittest.mock import patch

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import override_settings
from PIL import Image

from ads.forms import AdCampaignForm, AdCreativeForm
from ads.models import AdCampaign, AdCreative
from ads.providers import PROVIDER_INTERNAL
from ads.runtime import campaign_ecpm_microtokens


def _raster(name, size, fmt="PNG", content_type="image/png"):
    buf = BytesIO()
    Image.new("RGB", size).save(buf, format=fmt)
    return SimpleUploadedFile(
        name,
        buf.getvalue(),
        content_type=content_type,
    )


def _svg(name, width, height):
    body = (
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">'
        '<rect width="100%" height="100%"/>'
        '</svg>'
    )
    return SimpleUploadedFile(
        name,
        body.encode(),
        content_type="image/svg+xml",
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("placement", "file"),
    [
        (
            AdCreative.PLACEMENT_HOME,
            _raster("banner.png", (728, 90)),
        ),
        (
            AdCreative.PLACEMENT_HOME,
            _raster("banner.gif", (728, 90), "GIF", "image/gif"),
        ),
        (
            AdCreative.PLACEMENT_SIDEBAR,
            _raster("banner.jpg", (300, 250), "JPEG", "image/jpeg"),
        ),
        (
            AdCreative.PLACEMENT_HOME,
            _svg("banner.svg", 728, 90),
        ),
    ],
)
def test_banner_formats_are_accepted(placement, file):
    form = AdCreativeForm(
        data={
            "name": "Banner",
            "placement": placement,
        },
        files={"image": file},
    )
    assert form.is_valid(), form.errors


@pytest.mark.django_db
def test_svg_banner_rejects_active_content():
    payload = (
        '<svg xmlns="http://www.w3.org/2000/svg" '
        'width="728" height="90">'
        '<script>alert(1)</script></svg>'
    )
    form = AdCreativeForm(
        data={
            "name": "Bad SVG",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={
            "image": SimpleUploadedFile(
                "bad.svg",
                payload.encode(),
                content_type="image/svg+xml",
            )
        },
    )
    assert not form.is_valid()
    assert "not allowed" in str(form.errors)


@pytest.mark.django_db
def test_in_video_vast_creative_is_reusable_across_roll_positions(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="in-video-formats",
        advertiserUser=True,
    )
    creative_form = AdCreativeForm(
        data={
            "name": "VAST",
            "placement": AdCreative.PLACEMENT_IN_VIDEO,
            "vast_url": "https://ads.example/vast.xml",
        }
    )
    assert creative_form.is_valid(), creative_form.errors
    creative = creative_form.save(commit=False)
    creative.advertiser = user
    creative.review_status = AdCreative.REVIEW_APPROVED
    creative.save()

    for placement in (
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
    ):
        form = AdCampaignForm(
            data={
                "name": placement,
                "placement": placement,
                "pricing_model": AdCampaign.PRICING_CPM,
                "bid_usd": "10",
                "creative_ids": [str(creative.pk)],
            },
            advertiser=user,
        )
        assert form.is_valid(), form.errors


@pytest.mark.django_db
def test_popunder_is_url_creative(django_user_model):
    user = django_user_model.objects.create_user(
        username="popunder-format",
        advertiserUser=True,
    )
    creative_form = AdCreativeForm(
        data={
            "name": "Popunder",
            "placement": AdCreative.PLACEMENT_POPUNDER,
            "destination_url": "https://example.com/landing",
        }
    )
    assert creative_form.is_valid(), creative_form.errors
    creative = creative_form.save(commit=False)
    creative.advertiser = user
    creative.review_status = AdCreative.REVIEW_APPROVED
    creative.save()

    campaign = AdCampaignForm(
        data={
            "name": "Popunder campaign",
            "placement": AdCampaign.PLACEMENT_POPUNDER,
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_usd": "10",
            "creative_ids": [str(creative.pk)],
        },
        advertiser=user,
    )
    assert campaign.is_valid(), campaign.errors


def test_popunder_cpc_ecpm_uses_structural_full_ctr():
    campaign = AdCampaign(
        placement=AdCampaign.PLACEMENT_POPUNDER,
        pricing_model=AdCampaign.PRICING_CPC,
        bid_microtokens=2000,
    )
    assert campaign_ecpm_microtokens(campaign) == 2_000_000


def test_vmap_contains_pre_mid_post(client):
    with patch("ads.views.has_eligible_provider", return_value=True):
        response = client.get("/api/v1/ads/vmap/")
    assert response.status_code == 200
    body = response.content.decode()
    assert 'timeOffset="start"' in body
    assert 'breakId="preroll"' in body
    assert 'timeOffset="50%"' in body
    assert 'breakId="midroll"' in body
    assert 'timeOffset="end"' in body
    assert 'breakId="postroll"' in body


@override_settings(ADS_MIDROLL_TIME_OFFSET="33%")
def test_vmap_midroll_offset_is_configurable(client):
    with patch("ads.views.has_eligible_provider", return_value=True):
        response = client.get("/api/v1/ads/vmap/")
    assert response.status_code == 200
    assert 'timeOffset="33%"' in response.content.decode()


def test_vast_wrapper_tracks_direct_impression_and_click(client):
    candidate = {
        "campaign_id": 10,
        "creative_id": 20,
        "event_token": "signed-event",
        "vast_url": "https://ads.example/external-vast.xml",
        "creative_url": "",
    }
    with (
        patch(
            "ads.views.weighted_provider_order",
            return_value=[PROVIDER_INTERNAL],
        ),
        patch("ads.views.reserve", return_value=candidate),
    ):
        response = client.get(
            "/api/v1/ads/vast/video_preroll/"
        )
    assert response.status_code == 200
    body = response.content.decode()
    assert "external-vast.xml" in body
    assert "/ads/impression/signed-event/" in body
    assert "/ads/track-click/signed-event/" in body
