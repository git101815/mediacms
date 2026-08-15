from unittest.mock import Mock

import pytest

import ads.runtime as runtime
import ads.signals as signals
import ads.tasks as tasks
from ads.models import AdCampaign, AdCampaignCreative, AdCreative, AdSettlementBatch
from ledger.models import TokenWallet


@pytest.mark.django_db
def test_pause_queue_drops_missing_campaign_runtime(
    ads_redis,
    monkeypatch,
):
    campaign_id = 987654
    ads_redis.sadd(runtime.pause_queue_key(), campaign_id)
    dropped = []
    monkeypatch.setattr(
        tasks,
        "drop_campaign_runtime",
        lambda value: dropped.append(value),
    )

    tasks._reconcile_pause_queue()

    assert dropped == [campaign_id]
    assert not ads_redis.sismember(
        runtime.pause_queue_key(),
        campaign_id,
    )


@pytest.mark.django_db
def test_pause_queue_moves_active_approved_campaign_to_paused_funds(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=0)
    campaign = campaign_factory(advertiser=user)
    ads_redis.sadd(runtime.pause_queue_key(), campaign.pk)

    tasks._reconcile_pause_queue()

    campaign.refresh_from_db()
    assert (
        campaign.delivery_status
        == AdCampaign.DELIVERY_PAUSED_FUNDS
    )
    assert not ads_redis.sismember(
        runtime.pause_queue_key(),
        campaign.pk,
    )


@pytest.mark.django_db
def test_pause_queue_does_not_override_user_pause(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=0)
    campaign = campaign_factory(
        advertiser=user,
        delivery_status=AdCampaign.DELIVERY_PAUSED_USER,
    )
    ads_redis.sadd(runtime.pause_queue_key(), campaign.pk)

    tasks._reconcile_pause_queue()

    campaign.refresh_from_db()
    assert (
        campaign.delivery_status
        == AdCampaign.DELIVERY_PAUSED_USER
    )


@pytest.mark.django_db
def test_funded_campaign_auto_resumes_but_unfunded_one_does_not(
    advertiser_factory,
    campaign_factory,
):
    funded_user = advertiser_factory(balance=1_000_000)
    empty_user = advertiser_factory(balance=0)
    funded = campaign_factory(
        advertiser=funded_user,
        delivery_status=AdCampaign.DELIVERY_PAUSED_FUNDS,
        bid_microtokens=1_000,
    )
    empty = campaign_factory(
        advertiser=empty_user,
        delivery_status=AdCampaign.DELIVERY_PAUSED_FUNDS,
        bid_microtokens=1_000,
    )

    tasks._resume_funded_campaigns()

    funded.refresh_from_db()
    empty.refresh_from_db()
    assert funded.delivery_status == AdCampaign.DELIVERY_ACTIVE
    assert empty.delivery_status == AdCampaign.DELIVERY_PAUSED_FUNDS


@pytest.mark.django_db
def test_refresh_runtime_state_syncs_live_campaigns_and_removes_stale_members(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=1_000_000)
    campaign = campaign_factory(advertiser=user)
    stale_id = 999999
    ads_redis.zadd(
        runtime.slot_key(campaign.placement),
        {str(stale_id): 10**18},
    )

    tasks.refresh_runtime_state()

    assert ads_redis.zscore(
        runtime.slot_key(campaign.placement),
        campaign.pk,
    ) is not None
    assert ads_redis.zscore(
        runtime.slot_key(campaign.placement),
        stale_id,
    ) is None


@pytest.mark.django_db
def test_settle_runtime_processes_pending_redis_metrics(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=100_000)
    campaign = campaign_factory(advertiser=user)

    ads_redis.set(
        runtime.campaign_accrued_key(campaign.pk),
        4_000,
    )
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        4_000,
    )
    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        2,
    )

    tasks.settle_runtime()

    batch = AdSettlementBatch.objects.get(campaign=campaign)
    assert batch.status == AdSettlementBatch.STATUS_POSTED
    assert batch.redis_acked_at is not None
    campaign.refresh_from_db()
    assert campaign.impressions == 2
    assert campaign.spend_microtokens == 4


@pytest.mark.django_db
def test_campaign_save_signal_synchronizes_after_commit(
    advertiser_factory,
    campaign_factory,
    monkeypatch,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    called = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "_sync_campaign_id",
        lambda pk: called.append(pk),
    )

    signals.ad_campaign_saved(
        sender=AdCampaign,
        instance=campaign,
    )
    assert called == [campaign.pk]


@pytest.mark.django_db
def test_campaign_delete_signal_drops_runtime_after_commit(
    advertiser_factory,
    campaign_factory,
    monkeypatch,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    called = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "drop_campaign_runtime",
        lambda pk: called.append(pk),
    )

    signals.ad_campaign_deleted(
        sender=AdCampaign,
        instance=campaign,
    )
    assert called == [campaign.pk]


@pytest.mark.django_db
def test_campaign_creative_change_signal_resyncs_campaign(
    advertiser_factory,
    campaign_factory,
    monkeypatch,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    link = campaign.creative_links.get()
    called = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "_sync_campaign_id",
        lambda pk: called.append(pk),
    )

    signals.ad_campaign_creative_changed(
        sender=AdCampaignCreative,
        instance=link,
    )
    assert called == [campaign.pk]


@pytest.mark.django_db
def test_creative_save_signal_resyncs_every_linked_campaign(
    advertiser_factory,
    creative_factory,
    campaign_factory,
    monkeypatch,
):
    user = advertiser_factory()
    creative = creative_factory(advertiser=user)
    first = campaign_factory(
        advertiser=user,
        creative=creative,
        name="one",
    )
    second = campaign_factory(
        advertiser=user,
        creative=creative,
        name="two",
    )
    called = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "_sync_campaign_id",
        lambda pk: called.append(pk),
    )

    signals.ad_creative_saved(
        sender=AdCreative,
        instance=creative,
    )
    assert set(called) == {first.pk, second.pk}


@pytest.mark.django_db
def test_wallet_signal_only_syncs_advertiser_wallet(
    django_user_model,
    advertiser_factory,
    monkeypatch,
):
    advertiser = advertiser_factory()
    advertiser_wallet = TokenWallet.objects.get(user=advertiser)

    normal = django_user_model.objects.create_user(
        username="not-advertiser-wallet",
        advertiserUser=False,
    )
    normal_wallet = normal.token_wallet

    called = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "wallet_sync_suppressed",
        lambda: False,
    )
    monkeypatch.setattr(
        signals,
        "sync_wallet_runtime",
        lambda wallet: called.append(wallet.user_id),
    )

    signals.advertiser_wallet_saved(
        sender=TokenWallet,
        instance=normal_wallet,
    )
    signals.advertiser_wallet_saved(
        sender=TokenWallet,
        instance=advertiser_wallet,
    )
    assert called == [advertiser.pk]


@pytest.mark.django_db
def test_wallet_signal_respects_suppression(
    advertiser_factory,
    monkeypatch,
):
    user = advertiser_factory()
    wallet = TokenWallet.objects.get(user=user)
    sync = Mock()
    monkeypatch.setattr(
        signals,
        "wallet_sync_suppressed",
        lambda: True,
    )
    monkeypatch.setattr(signals, "sync_wallet_runtime", sync)

    signals.advertiser_wallet_saved(
        sender=TokenWallet,
        instance=wallet,
    )
    sync.assert_not_called()


@pytest.mark.django_db
def test_disabling_advertiser_user_drops_all_campaign_runtime(
    advertiser_factory,
    campaign_factory,
    monkeypatch,
):
    user = advertiser_factory()
    first = campaign_factory(advertiser=user)
    second = campaign_factory(advertiser=user)
    user.advertiserUser = False
    user.save(update_fields=["advertiserUser"])

    dropped = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "drop_campaign_runtime",
        lambda pk: dropped.append(pk),
    )

    signals.advertiser_user_saved(
        sender=type(user),
        instance=user,
    )
    assert set(dropped) == {first.pk, second.pk}


@pytest.mark.django_db
def test_enabling_advertiser_user_syncs_wallet_and_campaigns(
    django_user_model,
    campaign_factory,
    monkeypatch,
    ads_redis,
):
    user = django_user_model.objects.create_user(
        username="enable-advertiser",
        advertiserUser=True,
    )
    wallet = user.token_wallet
    campaign = campaign_factory(advertiser=user)

    wallet_calls = []
    campaign_calls = []
    monkeypatch.setattr(
        signals.transaction,
        "on_commit",
        lambda callback: callback(),
    )
    monkeypatch.setattr(
        signals,
        "sync_wallet_runtime",
        lambda value: wallet_calls.append(value.user_id),
    )
    monkeypatch.setattr(
        signals,
        "sync_campaign_runtime",
        lambda value: campaign_calls.append(value.pk),
    )

    signals.advertiser_user_saved(
        sender=type(user),
        instance=user,
    )
    assert wallet_calls == [user.pk]
    assert campaign_calls == [campaign.pk]


@pytest.mark.django_db
def test_settle_runtime_recovers_posted_but_unacked_batch_before_new_work(
    advertiser_factory,
    campaign_factory,
    ads_redis,
    monkeypatch,
):
    user = advertiser_factory(balance=100_000)
    campaign = campaign_factory(advertiser=user)
    batch = AdSettlementBatch.objects.create(
        campaign=campaign,
        advertiser=user,
        amount_microtokens=0,
        impressions=0,
        clicks=0,
        status=AdSettlementBatch.STATUS_POSTED,
    )
    calls = []
    real_process = tasks.process_settlement_batch

    def spy(value):
        calls.append(value.pk)
        return real_process(value)

    monkeypatch.setattr(
        tasks,
        "process_settlement_batch",
        spy,
    )
    tasks.settle_runtime()

    batch.refresh_from_db()
    assert batch.pk in calls
    assert batch.redis_acked_at is not None
