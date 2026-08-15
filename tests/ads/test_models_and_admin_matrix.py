import pytest
from django.core.exceptions import ValidationError
from django.db import IntegrityError

from ads import admin as ads_admin
from ads.models import AdCampaign, AdCampaignCreative, AdCreative, AdSettlementBatch


def test_campaign_and_creative_format_helpers_cover_every_inventory():
    assert AdCampaign(
        placement=AdCampaign.PLACEMENT_HOME
    ).placement_dimensions == (728, 90)
    assert AdCampaign(
        placement=AdCampaign.PLACEMENT_SIDEBAR
    ).placement_dimensions == (300, 250)
    assert AdCampaign(
        placement=AdCampaign.PLACEMENT_PREROLL
    ).placement_dimensions is None

    for placement in (
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
    ):
        assert AdCampaign(
            placement=placement
        ).creative_format == AdCreative.PLACEMENT_IN_VIDEO

    assert AdCampaign(
        placement=AdCampaign.PLACEMENT_POPUNDER
    ).creative_format == AdCreative.PLACEMENT_POPUNDER

    assert set(AdCampaign.delivery_slots()) == {
        AdCampaign.PLACEMENT_HOME,
        AdCampaign.PLACEMENT_SIDEBAR,
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
        AdCampaign.PLACEMENT_POPUNDER,
    }


def test_visible_status_priority():
    campaign = AdCampaign(
        review_status=AdCampaign.REVIEW_PENDING,
        delivery_status=AdCampaign.DELIVERY_ACTIVE,
    )
    assert campaign.visible_status == "pending_review"

    campaign.review_status = AdCampaign.REVIEW_REJECTED
    assert campaign.visible_status == "rejected"

    campaign.review_status = AdCampaign.REVIEW_APPROVED
    campaign.delivery_status = AdCampaign.DELIVERY_PAUSED_USER
    assert campaign.visible_status == AdCampaign.DELIVERY_PAUSED_USER


def test_creative_kind_properties():
    home = AdCreative(placement=AdCreative.PLACEMENT_HOME)
    side = AdCreative(placement=AdCreative.PLACEMENT_SIDEBAR)
    vast = AdCreative(placement=AdCreative.PLACEMENT_IN_VIDEO)
    pop = AdCreative(placement=AdCreative.PLACEMENT_POPUNDER)

    assert home.is_banner and side.is_banner
    assert home.source_kind == "banner"
    assert vast.is_in_video and vast.source_kind == "vast"
    assert pop.is_popunder and pop.source_kind == "url"
    assert home.placement_dimensions == (728, 90)
    assert side.placement_dimensions == (300, 250)
    assert vast.placement_dimensions is None


@pytest.mark.django_db
def test_campaign_creative_rejects_cross_advertiser_link(
    advertiser_factory,
    campaign_factory,
    creative_factory,
):
    owner = advertiser_factory()
    other = advertiser_factory()
    campaign = campaign_factory(
        advertiser=owner,
        with_creative=False,
    )
    creative = creative_factory(advertiser=other)

    link = AdCampaignCreative(
        campaign=campaign,
        creative=creative,
    )
    with pytest.raises(ValidationError):
        link.full_clean()


@pytest.mark.django_db
def test_campaign_creative_rejects_wrong_format_link(
    advertiser_factory,
    campaign_factory,
    creative_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_HOME,
        with_creative=False,
    )
    creative = creative_factory(
        advertiser=user,
        placement=AdCreative.PLACEMENT_SIDEBAR,
    )

    with pytest.raises(ValidationError):
        AdCampaignCreative(
            campaign=campaign,
            creative=creative,
        ).save()


@pytest.mark.django_db
def test_campaign_creative_enforces_unique_pair(
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    creative = campaign.creatives.get()
    with pytest.raises(ValidationError):
        AdCampaignCreative(
            campaign=campaign,
            creative=creative,
        ).save()


@pytest.mark.django_db
def test_admin_approve_and_reject_actions(
    advertiser_factory,
    campaign_factory,
    creative_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        review_status=AdCampaign.REVIEW_PENDING,
    )
    campaign.review_note = "review"
    campaign.save(update_fields=["review_note", "updated_at"])

    creative = creative_factory(
        advertiser=user,
        review_status=AdCreative.REVIEW_PENDING,
    )
    creative.review_note = "review"
    creative.save(update_fields=["review_note", "updated_at"])

    ads_admin.approve_campaigns(
        None,
        None,
        AdCampaign.objects.filter(pk=campaign.pk),
    )
    ads_admin.approve_creatives(
        None,
        None,
        AdCreative.objects.filter(pk=creative.pk),
    )
    campaign.refresh_from_db()
    creative.refresh_from_db()
    assert campaign.review_status == AdCampaign.REVIEW_APPROVED
    assert campaign.review_note == ""
    assert creative.review_status == AdCreative.REVIEW_APPROVED
    assert creative.review_note == ""

    ads_admin.reject_campaigns(
        None,
        None,
        AdCampaign.objects.filter(pk=campaign.pk),
    )
    ads_admin.reject_creatives(
        None,
        None,
        AdCreative.objects.filter(pk=creative.pk),
    )
    campaign.refresh_from_db()
    creative.refresh_from_db()
    assert campaign.review_status == AdCampaign.REVIEW_REJECTED
    assert creative.review_status == AdCreative.REVIEW_REJECTED


@pytest.mark.django_db
def test_campaign_admin_disables_delete_permission(
    admin_user,
):
    model_admin = ads_admin.AdCampaignAdmin(
        AdCampaign,
        ads_admin.admin.site,
    )
    assert model_admin.has_delete_permission(None) is False


@pytest.mark.django_db
def test_settlement_batch_string_contains_id_and_amount(
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    batch = AdSettlementBatch.objects.create(
        campaign=campaign,
        advertiser=user,
        amount_microtokens=42,
    )
    assert str(batch.id) in str(batch)
    assert "42" in str(batch)
