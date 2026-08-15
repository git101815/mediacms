import pytest

import ads.runtime as runtime
import ads.services as services
from ads.models import AdCampaign, AdSettlementBatch
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from ledger.services import (
    get_system_wallet,
    get_wallet_available_balance,
)


@pytest.mark.django_db
def test_create_settlement_batch_returns_none_when_nothing_pending(
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    assert services.create_settlement_batch(campaign) is None


@pytest.mark.django_db
def test_create_settlement_batch_reuses_existing_unacked_batch(
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    existing = AdSettlementBatch.objects.create(
        campaign=campaign,
        advertiser=user,
        amount_microtokens=3,
        impressions=1,
        clicks=0,
    )
    assert services.create_settlement_batch(campaign).pk == existing.pk


@pytest.mark.django_db
def test_full_settlement_debits_advertiser_credits_platform_and_acks_redis(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=100_000)
    campaign = campaign_factory(advertiser=user)

    platform = get_system_wallet(
        TokenWallet.SYSTEM_PLATFORM_FEES,
        allow_negative=False,
    )
    platform_start = int(platform.balance)
    wallet = TokenWallet.objects.get(user=user)
    wallet_start = int(wallet.balance)

    ads_redis.set(
        runtime.campaign_accrued_key(campaign.pk),
        5_000,
    )
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        5_000,
    )
    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        7,
    )
    ads_redis.set(
        runtime.campaign_clicks_key(campaign.pk),
        2,
    )

    batch = services.create_settlement_batch(campaign)
    assert batch.amount_microtokens == 5
    assert batch.impressions == 7
    assert batch.clicks == 2

    assert services.process_settlement_batch(batch) is True

    batch.refresh_from_db()
    campaign.refresh_from_db()
    wallet.refresh_from_db()
    platform.refresh_from_db()

    assert batch.status == AdSettlementBatch.STATUS_POSTED
    assert batch.redis_acked_at is not None
    assert batch.ledger_txn_id is not None
    assert wallet.balance == wallet_start - 5
    assert platform.balance == platform_start + 5
    assert campaign.impressions == 7
    assert campaign.clicks == 2
    assert campaign.spend_microtokens == 5

    txn = LedgerTransaction.objects.get(pk=batch.ledger_txn_id)
    assert txn.kind == "ad_spend"
    assert txn.external_id == f"ads:settlement:{batch.id}"
    assert LedgerEntry.objects.filter(txn=txn).count() == 2

    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 0
    assert int(
        ads_redis.get(
            runtime.account_accrued_key(user.pk)
        )
        or 0
    ) == 0
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


@pytest.mark.django_db
def test_unsettled_ads_spend_stays_reserved_after_advertiser_flag_is_removed(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=100)
    campaign = campaign_factory(advertiser=user)
    wallet = TokenWallet.objects.select_related("user").get(user=user)

    # 30_000 nanos = 30 microtokens already consumed by Ads but not settled.
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        30_000,
    )
    assert get_wallet_available_balance(wallet) == 70

    user.advertiserUser = False
    user.save(update_fields=["advertiserUser"])

    wallet = TokenWallet.objects.select_related("user").get(pk=wallet.pk)
    assert wallet.user.ad_campaigns.filter(pk=campaign.pk).exists()
    assert not wallet.user.advertiserUser

    # Removing the role must not make already-consumed Ads money spendable.
    assert get_wallet_available_balance(wallet) == 70


@pytest.mark.django_db
def test_sub_micro_remainder_is_preserved_until_it_reaches_one_microtoken(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=100_000)
    campaign = campaign_factory(advertiser=user)

    ads_redis.set(
        runtime.campaign_accrued_key(campaign.pk),
        999,
    )
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        999,
    )
    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        1,
    )

    first = services.create_settlement_batch(campaign)
    assert first.amount_microtokens == 0
    assert first.impressions == 1
    assert services.process_settlement_batch(first) is True

    campaign.refresh_from_db()
    assert campaign.impressions == 1
    assert campaign.spend_microtokens == 0
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 999

    ads_redis.incrby(
        runtime.campaign_accrued_key(campaign.pk),
        1,
    )
    ads_redis.incrby(
        runtime.account_accrued_key(user.pk),
        1,
    )
    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        1,
    )

    second = services.create_settlement_batch(campaign)
    assert second.amount_microtokens == 1
    assert services.process_settlement_batch(second) is True

    campaign.refresh_from_db()
    assert campaign.impressions == 2
    assert campaign.spend_microtokens == 1
    assert int(
        ads_redis.get(
            runtime.campaign_accrued_key(campaign.pk)
        )
        or 0
    ) == 0


@pytest.mark.django_db
def test_settlement_pauses_campaign_if_database_wallet_no_longer_covers_spend(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory(balance=5)
    campaign = campaign_factory(advertiser=user)

    ads_redis.set(
        runtime.campaign_accrued_key(campaign.pk),
        10_000,
    )
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        10_000,
    )
    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        1,
    )

    batch = services.create_settlement_batch(campaign)
    assert batch.amount_microtokens == 10
    assert services.process_settlement_batch(batch) is False

    batch.refresh_from_db()
    campaign.refresh_from_db()
    assert batch.status == AdSettlementBatch.STATUS_PENDING
    assert batch.redis_acked_at is None
    assert "no longer covers" in batch.last_error
    assert (
        campaign.delivery_status
        == AdCampaign.DELIVERY_PAUSED_FUNDS
    )


@pytest.mark.django_db
def test_crash_after_ledger_post_retries_without_double_debit(
    advertiser_factory,
    campaign_factory,
    ads_redis,
    monkeypatch,
):
    user = advertiser_factory(balance=100_000)
    campaign = campaign_factory(advertiser=user)
    wallet = TokenWallet.objects.get(user=user)
    start_balance = int(wallet.balance)

    ads_redis.set(
        runtime.campaign_accrued_key(campaign.pk),
        9_000,
    )
    ads_redis.set(
        runtime.account_accrued_key(user.pk),
        9_000,
    )
    ads_redis.set(
        runtime.campaign_impressions_key(campaign.pk),
        3,
    )

    batch = services.create_settlement_batch(campaign)
    real_ack = services.ack_settlement

    def crash(*, batch):
        raise RuntimeError("simulated crash before redis ack")

    monkeypatch.setattr(services, "ack_settlement", crash)
    with pytest.raises(RuntimeError):
        services.process_settlement_batch(batch)

    batch.refresh_from_db()
    wallet.refresh_from_db()
    assert batch.status == AdSettlementBatch.STATUS_POSTED
    assert batch.redis_acked_at is None
    assert wallet.balance == start_balance - 9
    assert LedgerTransaction.objects.filter(
        external_id=f"ads:settlement:{batch.id}"
    ).count() == 1

    monkeypatch.setattr(services, "ack_settlement", real_ack)
    assert services.process_settlement_batch(batch) is True

    batch.refresh_from_db()
    wallet.refresh_from_db()
    assert batch.redis_acked_at is not None
    assert wallet.balance == start_balance - 9
    assert LedgerTransaction.objects.filter(
        external_id=f"ads:settlement:{batch.id}"
    ).count() == 1


@pytest.mark.django_db
def test_process_settlement_returns_false_when_account_lock_is_busy(
    advertiser_factory,
    campaign_factory,
    monkeypatch,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    batch = AdSettlementBatch.objects.create(
        campaign=campaign,
        advertiser=user,
        amount_microtokens=1,
        impressions=1,
    )

    class BusyLock:
        def acquire(self, blocking=True):
            return False

    monkeypatch.setattr(
        services,
        "acquire_account_sync_lock",
        lambda *args, **kwargs: BusyLock(),
    )
    assert services.process_settlement_batch(batch) is False
    batch.refresh_from_db()
    assert batch.status == AdSettlementBatch.STATUS_PENDING


@pytest.mark.django_db
def test_ack_settlement_clamps_redis_counters_at_zero(
    advertiser_factory,
    campaign_factory,
    ads_redis,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    batch = AdSettlementBatch.objects.create(
        campaign=campaign,
        advertiser=user,
        amount_microtokens=10,
        impressions=5,
        clicks=4,
    )

    ads_redis.set(runtime.wallet_funded_key(user.pk), 1)
    ads_redis.set(runtime.account_accrued_key(user.pk), 1)
    ads_redis.set(runtime.campaign_accrued_key(campaign.pk), 1)
    ads_redis.set(runtime.campaign_impressions_key(campaign.pk), 1)
    ads_redis.set(runtime.campaign_clicks_key(campaign.pk), 1)

    runtime.ack_settlement(batch=batch)

    for key in (
        runtime.wallet_funded_key(user.pk),
        runtime.account_accrued_key(user.pk),
        runtime.campaign_accrued_key(campaign.pk),
        runtime.campaign_impressions_key(campaign.pk),
        runtime.campaign_clicks_key(campaign.pk),
    ):
        assert int(ads_redis.get(key) or 0) == 0
