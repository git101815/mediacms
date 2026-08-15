from io import BytesIO

import pytest
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import override_settings
from PIL import Image

from ads.forms import AdCampaignForm, AdCreativeForm
from ads.models import AdCampaign, AdCampaignCreative, AdCreative


def make_banner_file(name="banner.png", size=(728, 90), fmt="PNG"):
    buffer = BytesIO()
    image = Image.new("RGB", size)
    if fmt == "GIF":
        image.save(buffer, format="GIF")
        content_type = "image/gif"
    elif fmt == "JPEG":
        image.save(buffer, format="JPEG")
        content_type = "image/jpeg"
    elif fmt == "WEBP":
        image.save(buffer, format="WEBP")
        content_type = "image/webp"
    else:
        image.save(buffer, format="PNG")
        content_type = "image/png"
    return SimpleUploadedFile(
        name,
        buffer.getvalue(),
        content_type=content_type,
    )


def _svg(body, name="banner.svg"):
    return SimpleUploadedFile(
        name,
        body.encode(),
        content_type="image/svg+xml",
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("placement", "size", "name", "fmt"),
    [
        (AdCreative.PLACEMENT_HOME, (728, 90), "a.png", "PNG"),
        (AdCreative.PLACEMENT_HOME, (728, 90), "a.jpg", "JPEG"),
        (AdCreative.PLACEMENT_HOME, (728, 90), "a.gif", "GIF"),
        (AdCreative.PLACEMENT_SIDEBAR, (300, 250), "b.png", "PNG"),
        (AdCreative.PLACEMENT_SIDEBAR, (300, 250), "b.jpg", "JPEG"),
        (AdCreative.PLACEMENT_SIDEBAR, (300, 250), "b.gif", "GIF"),
    ],
)
def test_every_supported_raster_banner_format_is_accepted(
    placement,
    size,
    name,
    fmt,
):
    form = AdCreativeForm(
        data={"name": "Banner", "placement": placement},
        files={"image": make_banner_file(name, size, fmt)},
    )
    assert form.is_valid(), form.errors


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("placement", "wrong_size"),
    [
        (AdCreative.PLACEMENT_HOME, (300, 250)),
        (AdCreative.PLACEMENT_HOME, (727, 90)),
        (AdCreative.PLACEMENT_SIDEBAR, (728, 90)),
        (AdCreative.PLACEMENT_SIDEBAR, (300, 249)),
    ],
)
def test_banner_dimensions_are_exact(placement, wrong_size):
    form = AdCreativeForm(
        data={"name": "Wrong", "placement": placement},
        files={
            "image": make_banner_file(
                "wrong.png",
                wrong_size,
                "PNG",
            )
        },
    )
    assert not form.is_valid()
    assert "requires exactly" in str(form.errors)


@pytest.mark.django_db
def test_banner_rejects_missing_corrupt_and_unsupported_files():
    missing = AdCreativeForm(
        data={
            "name": "Missing",
            "placement": AdCreative.PLACEMENT_HOME,
        }
    )
    assert not missing.is_valid()
    assert "banner file is required" in str(missing.errors).lower()

    corrupt = AdCreativeForm(
        data={
            "name": "Corrupt",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={
            "image": SimpleUploadedFile(
                "corrupt.png",
                b"not-an-image",
                content_type="image/png",
            )
        },
    )
    assert not corrupt.is_valid()

    unsupported = AdCreativeForm(
        data={
            "name": "WebP",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={
            "image": make_banner_file(
                "banner.webp",
                (728, 90),
                "WEBP",
            )
        },
    )
    assert not unsupported.is_valid()
    assert "PNG, JPG, SVG or GIF" in str(unsupported.errors)


@pytest.mark.django_db
def test_svg_accepts_exact_dimensions_and_viewbox():
    explicit = AdCreativeForm(
        data={
            "name": "SVG explicit",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={
            "image": _svg(
                '<svg xmlns="http://www.w3.org/2000/svg" '
                'width="728" height="90">'
                '<rect width="728" height="90"/>'
                "</svg>"
            )
        },
    )
    assert explicit.is_valid(), explicit.errors

    viewbox = AdCreativeForm(
        data={
            "name": "SVG viewBox",
            "placement": AdCreative.PLACEMENT_SIDEBAR,
        },
        files={
            "image": _svg(
                '<svg xmlns="http://www.w3.org/2000/svg" '
                'viewBox="0 0 300 250">'
                '<rect width="300" height="250"/>'
                "</svg>",
                "viewbox.svg",
            )
        },
    )
    assert viewbox.is_valid(), viewbox.errors


@pytest.mark.django_db
@pytest.mark.parametrize(
    "payload",
    [
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90">'
        "<script>alert(1)</script></svg>",
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90">'
        '<foreignObject><div>bad</div></foreignObject></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90" '
        'onclick="alert(1)"></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90">'
        '<image href="https://evil.example/x.png"/></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90" '
        'style="background:url(javascript:alert(1))"></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90" '
        'style="background:url(/external.svg)"></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90" '
        'style="background:url(../external.svg)"></svg>',
        '<!DOCTYPE svg [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        '<svg xmlns="http://www.w3.org/2000/svg" width="728" height="90">'
        "&xxe;</svg>",
    ],
)
def test_svg_rejects_active_external_or_entity_content(payload):
    form = AdCreativeForm(
        data={
            "name": "Unsafe SVG",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={"image": _svg(payload, "unsafe.svg")},
    )
    assert not form.is_valid()


@pytest.mark.django_db
@pytest.mark.parametrize(
    "payload",
    [
        '<html xmlns="http://www.w3.org/1999/xhtml"></html>',
        '<svg xmlns="http://www.w3.org/2000/svg" width="100%" height="90"></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg"></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 nope 90"></svg>',
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 727 90"></svg>',
        "<svg",
    ],
)
def test_svg_rejects_malformed_or_non_exact_geometry(payload):
    form = AdCreativeForm(
        data={
            "name": "Bad SVG",
            "placement": AdCreative.PLACEMENT_HOME,
        },
        files={"image": _svg(payload, "bad.svg")},
    )
    assert not form.is_valid()


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("placement", "data", "error_field"),
    [
        (
            AdCreative.PLACEMENT_IN_VIDEO,
            {},
            "vast_url",
        ),
        (
            AdCreative.PLACEMENT_POPUNDER,
            {},
            "destination_url",
        ),
    ],
)
def test_non_banner_creatives_require_their_real_delivery_source(
    placement,
    data,
    error_field,
):
    form = AdCreativeForm(
        data={"name": "Creative", "placement": placement, **data}
    )
    assert not form.is_valid()
    assert error_field in form.errors


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("placement", "field"),
    [
        (AdCreative.PLACEMENT_IN_VIDEO, "vast_url"),
        (AdCreative.PLACEMENT_POPUNDER, "destination_url"),
    ],
)
def test_ad_destinations_reject_non_http_schemes(placement, field):
    form = AdCreativeForm(
        data={
            "name": "Bad URL",
            "placement": placement,
            field: "ftp://example.com/file",
        }
    )
    assert not form.is_valid()
    assert "http:// or https://" in str(form.errors)


@pytest.mark.django_db
def test_creative_form_strips_fields_that_do_not_belong_to_format():
    vast = AdCreativeForm(
        data={
            "name": "VAST",
            "placement": AdCreative.PLACEMENT_IN_VIDEO,
            "vast_url": "https://ads.example/vast.xml",
            "destination_url": "https://should-not-survive.example/",
        }
    )
    assert vast.is_valid(), vast.errors
    vast_obj = vast.save(commit=False)
    assert vast_obj.vast_url == "https://ads.example/vast.xml"
    assert not vast_obj.image
    assert vast_obj.destination_url == ""

    pop = AdCreativeForm(
        data={
            "name": "Pop",
            "placement": AdCreative.PLACEMENT_POPUNDER,
            "vast_url": "https://should-not-survive.example/vast",
            "destination_url": "https://example.com/pop",
        }
    )
    assert pop.is_valid(), pop.errors
    pop_obj = pop.save(commit=False)
    assert pop_obj.vast_url == ""
    assert not pop_obj.image
    assert pop_obj.destination_url == "https://example.com/pop"


MINIMUMS = {
    "banner": {"cpm": "2.5", "cpc": "0.25"},
    "preroll": {"cpm": "4", "cpc": "0.5"},
    "popunder": {"cpm": "3", "cpc": "0.4"},
}


@pytest.mark.django_db
@override_settings(ADS_MIN_BID_USD_BY_AD_TYPE=MINIMUMS)
@pytest.mark.parametrize(
    ("placement", "pricing", "minimum", "creative_placement"),
    [
        (
            AdCampaign.PLACEMENT_HOME,
            AdCampaign.PRICING_CPM,
            "2.5",
            AdCreative.PLACEMENT_HOME,
        ),
        (
            AdCampaign.PLACEMENT_SIDEBAR,
            AdCampaign.PRICING_CPC,
            "0.25",
            AdCreative.PLACEMENT_SIDEBAR,
        ),
        (
            AdCampaign.PLACEMENT_PREROLL,
            AdCampaign.PRICING_CPM,
            "4",
            AdCreative.PLACEMENT_IN_VIDEO,
        ),
        (
            AdCampaign.PLACEMENT_MIDROLL,
            AdCampaign.PRICING_CPC,
            "0.5",
            AdCreative.PLACEMENT_IN_VIDEO,
        ),
        (
            AdCampaign.PLACEMENT_POSTROLL,
            AdCampaign.PRICING_CPM,
            "4",
            AdCreative.PLACEMENT_IN_VIDEO,
        ),
        (
            AdCampaign.PLACEMENT_POPUNDER,
            AdCampaign.PRICING_CPC,
            "0.4",
            AdCreative.PLACEMENT_POPUNDER,
        ),
    ],
)
def test_every_campaign_inventory_uses_its_configured_minimum(
    advertiser_factory,
    creative_factory,
    placement,
    pricing,
    minimum,
    creative_placement,
):
    user = advertiser_factory()
    creative = creative_factory(
        advertiser=user,
        placement=creative_placement,
    )
    minimum_value = float(minimum)
    below = f"{minimum_value - 0.000001:.6f}"

    base = {
        "name": "Campaign",
        "placement": placement,
        "target_url": (
            "https://example.com/"
            if placement
            in {
                AdCampaign.PLACEMENT_HOME,
                AdCampaign.PLACEMENT_SIDEBAR,
            }
            else ""
        ),
        "pricing_model": pricing,
        "creative_ids": [str(creative.pk)],
    }

    invalid = AdCampaignForm(
        data={**base, "bid_usd": below},
        advertiser=user,
    )
    assert not invalid.is_valid()
    assert "Minimum" in str(invalid.errors)

    valid = AdCampaignForm(
        data={**base, "bid_usd": minimum},
        advertiser=user,
    )
    assert valid.is_valid(), valid.errors


@pytest.mark.django_db
def test_banner_campaign_requires_http_destination_and_non_banner_drops_it(
    advertiser_factory,
    creative_factory,
):
    user = advertiser_factory()
    banner = creative_factory(
        advertiser=user,
        placement=AdCreative.PLACEMENT_HOME,
    )
    missing = AdCampaignForm(
        data={
            "name": "Missing target",
            "placement": AdCampaign.PLACEMENT_HOME,
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_usd": "10",
            "creative_ids": [str(banner.pk)],
        },
        advertiser=user,
    )
    assert not missing.is_valid()

    ftp = AdCampaignForm(
        data={
            "name": "FTP",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "ftp://example.com/file",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_usd": "10",
            "creative_ids": [str(banner.pk)],
        },
        advertiser=user,
    )
    assert not ftp.is_valid()
    assert "http:// or https://" in str(ftp.errors)

    vast = creative_factory(
        advertiser=user,
        placement=AdCreative.PLACEMENT_IN_VIDEO,
    )
    preroll = AdCampaignForm(
        data={
            "name": "Pre",
            "placement": AdCampaign.PLACEMENT_PREROLL,
            "target_url": "https://ignored.example/",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_usd": "10",
            "creative_ids": [str(vast.pk)],
        },
        advertiser=user,
    )
    assert preroll.is_valid(), preroll.errors
    obj = preroll.save(commit=False)
    assert obj.target_url == ""


@pytest.mark.django_db
def test_campaign_creative_queryset_is_isolated_and_rejected_is_unselectable(
    advertiser_factory,
    creative_factory,
):
    owner = advertiser_factory()
    other = advertiser_factory()
    approved = creative_factory(advertiser=owner)
    pending = creative_factory(advertiser=owner, approved=False)
    rejected = creative_factory(
        advertiser=owner,
        review_status=AdCreative.REVIEW_REJECTED,
    )
    foreign = creative_factory(advertiser=other)

    form = AdCampaignForm(advertiser=owner)
    ids = set(form.fields["creative_ids"].queryset.values_list("pk", flat=True))
    assert approved.pk in ids
    assert pending.pk in ids
    assert rejected.pk not in ids
    assert foreign.pk not in ids


@pytest.mark.django_db
def test_campaign_save_creatives_adds_removes_and_reenables_links(
    advertiser_factory,
    creative_factory,
    campaign_factory,
):
    user = advertiser_factory()
    first = creative_factory(advertiser=user)
    second = creative_factory(advertiser=user)
    campaign = campaign_factory(
        advertiser=user,
        creative=first,
    )
    link = AdCampaignCreative.objects.get(
        campaign=campaign,
        creative=first,
    )
    link.enabled = False
    link.save(update_fields=["enabled"])

    form = AdCampaignForm(
        data={
            "name": campaign.name,
            "placement": campaign.placement,
            "target_url": campaign.target_url,
            "pricing_model": campaign.pricing_model,
            "bid_usd": "10",
            "creative_ids": [str(first.pk), str(second.pk)],
        },
        instance=campaign,
        advertiser=user,
    )
    assert form.is_valid(), form.errors
    form.save()
    assert AdCampaignCreative.objects.filter(
        campaign=campaign,
        creative=first,
        enabled=True,
    ).exists()
    assert AdCampaignCreative.objects.filter(
        campaign=campaign,
        creative=second,
        enabled=True,
    ).exists()

    form = AdCampaignForm(
        data={
            "name": campaign.name,
            "placement": campaign.placement,
            "target_url": campaign.target_url,
            "pricing_model": campaign.pricing_model,
            "bid_usd": "10",
            "creative_ids": [str(second.pk)],
        },
        instance=campaign,
        advertiser=user,
    )
    assert form.is_valid(), form.errors
    form.save()
    assert not AdCampaignCreative.objects.filter(
        campaign=campaign,
        creative=first,
    ).exists()
    assert AdCampaignCreative.objects.filter(
        campaign=campaign,
        creative=second,
        enabled=True,
    ).exists()
