from django.core import signing
import pytest

import ads.runtime as runtime
from ads.models import AdCampaign, AdCreative


def _payload_from_token(token):
    return signing.loads(
        token,
        salt="ads.click.v1",
    )


@pytest.mark.django_db
def test_wallet_runtime_uses_balance_minus_holds(
    advertiser_factory,
    ads_redis,
):
    user = advertiser_factory(
        balance=10_000,
        held_balance=2_500,
    )
    assert int(
        ads_redis.get(runtime.wallet_funded_key(user.pk)) or 0
    ) == runtime.microtokens_to_nanos(7_500)
    assert runtime.get_effective_balance_nanos(user.pk) == (
        runtime.microtokens_to_nanos(7_500)
    )


def test_cost_and_ctr_math():
    assert runtime.event_cost_nanos(
        AdCampaign.PRICING_CPM,
        123,
    ) == 123
    assert runtime.event_cost_nanos(
        AdCampaign.PRICING_CPC,
        123,
    ) == 123_000

    assert runtime.predicted_ctr_ppm(
        impressions=0,
        clicks=0,
    ) == 10_000
    assert runtime.predicted_ctr_ppm(
        impressions=1_000,
        clicks=20,
    ) == 15_000

    cpm = AdCampaign(
        pricing_model=AdCampaign.PRICING_CPM,
        placement=AdCampaign.PLACEMENT_HOME,
        bid_microtokens=500,
    )
    assert runtime.campaign_ecpm_microtokens(cpm) == 500

    pop = AdCampaign(
        pricing_model=AdCampaign.PRICING_CPC,
        placement=AdCampaign.PLACEMENT_POPUNDER,
        bid_microtokens=500,
    )
    assert runtime.campaign_ecpm_microtokens(pop) == 500_000

    cpc = AdCampaign(
        pricing_model=AdCampaign.PRICING_CPC,
        placement=AdCampaign.PLACEMENT_HOME,
        bid_microtokens=500,
        impressions=0,
        clicks=0,
    )
    assert runtime.campaign_ecpm_microtokens(cpc) == 5_000


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("advertiser_allowed", "review", "delivery"),
    [
        (
            False,
            AdCampaign.REVIEW_APPROVED,
            AdCampaign.DELIVERY_ACTIVE,
        ),
        (
            True,
            AdCampaign.REVIEW_PENDING,
            AdCampaign.DELIVERY_ACTIVE,
        ),
        (
            True,
            AdCampaign.REVIEW_REJECTED,
            AdCampaign.DELIVERY_ACTIVE,
        ),
        (
            True,
            AdCampaign.REVIEW_APPROVED,
            AdCampaign.DELIVERY_PAUSED_USER,
        ),
        (
            True,
            AdCampaign.REVIEW_APPROVED,
            AdCampaign.DELIVERY_PAUSED_FUNDS,
        ),
    ],
)
def test_ineligible_campaign_never_enters_runtime_auction(
    advertiser_factory,
    campaign_factory,
    ads_redis,
    advertiser_allowed,
    review,
    delivery,
):
    user = advertiser_factory(advertiser=advertiser_allowed)
    campaign = campaign_factory(
        advertiser=user,
        review_status=review,
        delivery_status=delivery,
    )
    runtime.sync_campaign_runtime(campaign)
    assert not ads_redis.exists(
        runtime.campaign_config_key(campaign.pk)
    )
    assert ads_redis.zscore(
        runtime.slot_key(campaign.placement),
        campaign.pk,
    ) is None


@pytest.mark.django_db
def test_campaign_without_approved_usable_creative_is_not_served(
    advertiser_factory,
    campaign_factory,
    creative_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        with_creative=False,
    )
    runtime.sync_campaign_runtime(campaign)
    assert not ads_redis.exists(
        runtime.campaign_config_key(campaign.pk)
    )

    rejected = creative_factory(
        advertiser=user,
        review_status=AdCreative.REVIEW_REJECTED,
    )
    campaign.creatives.add(
        rejected,
        through_defaults={"enabled": True, "weight": 1},
    )
    runtime.sync_campaign_runtime(campaign)
    assert not ads_redis.exists(
        runtime.campaign_config_key(campaign.pk)
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    ("placement", "creative_format"),
    [
        (
            AdCampaign.PLACEMENT_HOME,
            AdCreative.PLACEMENT_HOME,
        ),
        (
            AdCampaign.PLACEMENT_SIDEBAR,
            AdCreative.PLACEMENT_SIDEBAR,
        ),
        (
            AdCampaign.PLACEMENT_PREROLL,
            AdCreative.PLACEMENT_IN_VIDEO,
        ),
        (
            AdCampaign.PLACEMENT_MIDROLL,
            AdCreative.PLACEMENT_IN_VIDEO,
        ),
        (
            AdCampaign.PLACEMENT_POSTROLL,
            AdCreative.PLACEMENT_IN_VIDEO,
        ),
        (
            AdCampaign.PLACEMENT_POPUNDER,
            AdCreative.PLACEMENT_POPUNDER,
        ),
    ],
)
def test_every_inventory_type_can_enter_runtime(
    advertiser_factory,
    creative_factory,
    campaign_factory,
    ads_redis,
    placement,
    creative_format,
):
    user = advertiser_factory()
    creative = creative_factory(
        advertiser=user,
        placement=creative_format,
    )
    campaign = campaign_factory(
        advertiser=user,
        placement=placement,
        creative=creative,
    )
    runtime.sync_campaign_runtime(campaign)

    cfg = runtime._decode_hash(
        ads_redis.hgetall(
            runtime.campaign_config_key(campaign.pk)
        )
    )
    assert cfg["slot"] == placement
    assert cfg["funded"] == "1"
    assert ads_redis.zscore(
        runtime.slot_key(placement),
        campaign.pk,
    ) is not None


@pytest.mark.django_db
def test_unfunded_campaign_has_config_but_not_auction_membership(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=0)
    campaign = campaign_factory(
        advertiser=user,
        bid_microtokens=1_000,
    )
    runtime.sync_campaign_runtime(campaign)

    cfg = runtime._decode_hash(
        ads_redis.hgetall(
            runtime.campaign_config_key(campaign.pk)
        )
    )
    assert cfg["funded"] == "0"
    assert ads_redis.zscore(
        runtime.slot_key(campaign.placement),
        campaign.pk,
    ) is None


@pytest.mark.django_db
def test_banner_cpm_impression_is_charged_once(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        pricing=AdCampaign.PRICING_CPM,
        bid_microtokens=2_500,
    )
    runtime.sync_campaign_runtime(campaign)

    ad = runtime.serve(campaign.placement)
    assert ad is not None
    payload = _payload_from_token(ad["event_token"])

    assert int(
        ads_redis.get(
            runtime.campaign_impressions_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 2_500

    assert runtime.record_impression(payload) == 2
    assert int(
        ads_redis.get(
            runtime.campaign_impressions_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 2_500


@pytest.mark.django_db
def test_banner_cpc_charges_click_once_but_not_impression(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        pricing=AdCampaign.PRICING_CPC,
        bid_microtokens=7_000,
    )
    runtime.sync_campaign_runtime(campaign)

    ad = runtime.serve(campaign.placement)
    payload = _payload_from_token(ad["event_token"])

    assert int(
        ads_redis.get(
            runtime.campaign_impressions_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 0

    assert runtime.record_click(payload) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 7_000 * runtime.NANOS_PER_MICROTOKEN

    assert runtime.record_click(payload) == 2
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 1


@pytest.mark.django_db
def test_click_cannot_be_billed_before_impression(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_PREROLL,
        pricing=AdCampaign.PRICING_CPC,
        bid_microtokens=10_000,
    )
    runtime.sync_campaign_runtime(campaign)

    ad = runtime.reserve(campaign.placement)
    payload = _payload_from_token(ad["event_token"])

    assert runtime.record_click(payload) == 0
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 0
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 0

    assert runtime.record_impression(payload) == 1
    assert runtime.record_click(payload) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 1


@pytest.mark.django_db
def test_race_to_insufficient_funds_removes_campaign_and_queues_pause(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=1_000_000)
    campaign = campaign_factory(
        advertiser=user,
        pricing=AdCampaign.PRICING_CPM,
        bid_microtokens=10_000,
    )
    runtime.sync_campaign_runtime(campaign)

    ads_redis.set(
        runtime.wallet_funded_key(user.pk),
        9_999,
    )

    assert runtime.serve(campaign.placement) is None

    cfg = runtime._decode_hash(
        ads_redis.hgetall(
            runtime.campaign_config_key(campaign.pk)
        )
    )
    assert cfg["funded"] == "0"
    assert ads_redis.zscore(
        runtime.slot_key(campaign.placement),
        campaign.pk,
    ) is None
    assert ads_redis.sismember(
        runtime.pause_queue_key(),
        campaign.pk,
    )


@pytest.mark.django_db
def test_auction_skips_more_than_twenty_stale_members(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        pricing=AdCampaign.PRICING_CPM,
        bid_microtokens=1_000,
    )
    runtime.sync_campaign_runtime(campaign)

    slot = runtime.slot_key(campaign.placement)
    for index in range(25):
        ads_redis.zadd(
            slot,
            {str(90_000 + index): 10**18 + index},
        )

    ad = runtime.serve(campaign.placement)
    assert ad is not None
    assert ad["campaign_id"] == campaign.pk

    for index in range(25):
        assert ads_redis.zscore(
            slot,
            90_000 + index,
        ) is None


@pytest.mark.django_db
def test_serve_and_reserve_are_strictly_separated_by_inventory(
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    banner = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_HOME,
    )
    vast = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_PREROLL,
    )
    pop = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_POPUNDER,
    )

    for campaign in (banner, vast, pop):
        runtime.sync_campaign_runtime(campaign)

    assert runtime.serve(banner.placement) is not None
    assert runtime.reserve(banner.placement) is None

    assert runtime.serve(vast.placement) is None
    assert runtime.reserve(vast.placement)["vast_url"]

    assert runtime.serve(pop.placement) is None
    assert runtime.reserve(pop.placement)[
        "destination_url"
    ] == "https://example.com/landing"


@pytest.mark.django_db
def test_higher_ecpm_campaign_wins_auction(
    advertiser_factory,
    campaign_factory,
):
    low_user = advertiser_factory()
    high_user = advertiser_factory()
    low = campaign_factory(
        advertiser=low_user,
        bid_microtokens=1_000,
    )
    high = campaign_factory(
        advertiser=high_user,
        bid_microtokens=2_000,
    )
    runtime.sync_campaign_runtime(low)
    runtime.sync_campaign_runtime(high)

    ad = runtime.serve(AdCampaign.PLACEMENT_HOME)
    assert ad["campaign_id"] == high.pk


@pytest.mark.django_db
def test_live_metrics_include_pending_redis_values(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    campaign.impressions = 10
    campaign.clicks = 2
    campaign.spend_microtokens = 50
    campaign.save(
        update_fields=[
            "impressions",
            "clicks",
            "spend_microtokens",
            "updated_at",
        ]
    )

    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        3,
    )
    ads_redis.set(
        runtime.campaign_clicks_key(campaign.pk),
        1,
    )
    ads_redis.set(
        runtime.campaign_accrued_key(campaign.pk),
        2_500,
    )

    metrics = runtime.get_campaign_live_metrics(campaign)
    assert metrics["impressions"] == 13
    assert metrics["clicks"] == 3
    assert metrics["pending_impressions"] == 3
    assert metrics["pending_clicks"] == 1
    assert metrics["pending_spend_nanos"] == 2_500
    assert metrics["spend_nanos"] == (
        50 * runtime.NANOS_PER_MICROTOKEN + 2_500
    )


@pytest.mark.django_db
def test_cpm_click_is_counted_but_never_adds_spend(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        pricing=AdCampaign.PRICING_CPM,
        bid_microtokens=3_000,
    )
    runtime.sync_campaign_runtime(campaign)
    ad = runtime.serve(campaign.placement)
    payload = _payload_from_token(ad["event_token"])
    spend_before = int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    )
    assert runtime.record_click(payload) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == spend_before


@pytest.mark.django_db
def test_popunder_reservation_is_free_until_open_then_counts_impression_and_click(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_POPUNDER,
        pricing=AdCampaign.PRICING_CPC,
        bid_microtokens=2_000,
    )
    runtime.sync_campaign_runtime(campaign)
    ad = runtime.reserve(campaign.placement)
    payload = _payload_from_token(ad["event_token"])

    assert int(
        ads_redis.get(
            runtime.campaign_impressions_key(campaign.pk)
        )
        or 0
    ) == 0
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 0
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 0

    assert runtime.record_impression(payload) == 1
    assert runtime.record_click(payload) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_impressions_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 2_000 * runtime.NANOS_PER_MICROTOKEN


@pytest.mark.django_db
def test_banner_and_reservation_hot_paths_do_zero_sql_queries(
    advertiser_factory,
    campaign_factory,
    django_assert_num_queries,
):
    banner_user = advertiser_factory()
    vast_user = advertiser_factory()
    banner = campaign_factory(
        advertiser=banner_user,
        placement=AdCampaign.PLACEMENT_HOME,
    )
    vast = campaign_factory(
        advertiser=vast_user,
        placement=AdCampaign.PLACEMENT_PREROLL,
    )
    runtime.sync_campaign_runtime(banner)
    runtime.sync_campaign_runtime(vast)

    with django_assert_num_queries(0):
        assert runtime.serve(banner.placement) is not None

    with django_assert_num_queries(0):
        assert runtime.reserve(vast.placement) is not None


@pytest.mark.django_db
def test_unsettled_balance_rounds_up_to_protect_wallet_outflows(
    advertiser_factory,
    ads_redis,
):
    user = advertiser_factory()
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        1,
    )
    assert runtime.get_account_unsettled_microtokens(user.pk) == 1

    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        runtime.NANOS_PER_MICROTOKEN,
    )
    assert runtime.get_account_unsettled_microtokens(user.pk) == 1

    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        runtime.NANOS_PER_MICROTOKEN + 1,
    )
    assert runtime.get_account_unsettled_microtokens(user.pk) == 2


def test_wallet_runtime_suppression_context_restores_previous_state():
    assert runtime.wallet_sync_suppressed() is False
    with runtime.suppress_wallet_runtime_sync():
        assert runtime.wallet_sync_suppressed() is True
        with runtime.suppress_wallet_runtime_sync():
            assert runtime.wallet_sync_suppressed() is True
        assert runtime.wallet_sync_suppressed() is True
    assert runtime.wallet_sync_suppressed() is False


@pytest.mark.django_db
@pytest.mark.parametrize(
    "placement",
    [
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
        AdCampaign.PLACEMENT_POPUNDER,
    ],
)
@pytest.mark.parametrize(
    "pricing",
    [
        AdCampaign.PRICING_CPM,
        AdCampaign.PRICING_CPC,
    ],
)
def test_every_reserved_inventory_pricing_combination_bills_at_actual_events(
    advertiser_factory,
    campaign_factory,
    ads_redis,
    placement,
    pricing,
):
    user = advertiser_factory()
    bid = 4_000
    campaign = campaign_factory(
        advertiser=user,
        placement=placement,
        pricing=pricing,
        bid_microtokens=bid,
    )
    runtime.sync_campaign_runtime(campaign)

    ad = runtime.reserve(placement)
    assert ad is not None
    payload = _payload_from_token(ad["event_token"])

    assert int(
        ads_redis.get(
            runtime.campaign_impressions_key(campaign.pk)
        )
        or 0
    ) == 0
    assert int(
        ads_redis.get(
            runtime.campaign_clicks_key(campaign.pk)
        )
        or 0
    ) == 0
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 0

    assert runtime.record_impression(payload) == 1
    after_impression = int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    )
    assert after_impression == (
        bid if pricing == AdCampaign.PRICING_CPM else 0
    )

    assert runtime.record_click(payload) == 1
    after_click = int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    )
    assert after_click == (
        bid
        if pricing == AdCampaign.PRICING_CPM
        else bid * runtime.NANOS_PER_MICROTOKEN
    )
