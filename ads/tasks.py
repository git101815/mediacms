import logging

from celery import shared_task
from django.db import transaction

from ledger.models import TokenWallet

from .models import AdCampaign, AdSettlementBatch
from .runtime import (
    acquire_account_sync_lock,
    drop_campaign_runtime,
    event_cost_nanos,
    get_effective_balance_nanos,
    pause_queue_key,
    redis_connection,
    settlement_lock_name,
    slot_key,
    sync_campaign_runtime,
    sync_wallet_runtime,
)
from .services import create_settlement_batch, process_settlement_batch

logger = logging.getLogger(__name__)


def _campaign_can_afford(campaign):
    return get_effective_balance_nanos(campaign.advertiser_id) >= event_cost_nanos(
        campaign.pricing_model,
        campaign.bid_microtokens,
    )


def _reconcile_pause_queue():
    redis = redis_connection()
    raw_ids = redis.smembers(pause_queue_key())
    for raw_id in raw_ids:
        campaign_id = int(raw_id)
        campaign = AdCampaign.objects.filter(pk=campaign_id).first()
        if campaign is None:
            redis.srem(pause_queue_key(), campaign_id)
            drop_campaign_runtime(campaign_id)
            continue

        if (
            campaign.review_status == AdCampaign.REVIEW_APPROVED
            and campaign.delivery_status == AdCampaign.DELIVERY_ACTIVE
            and not _campaign_can_afford(campaign)
        ):
            AdCampaign.objects.filter(pk=campaign.pk).update(
                delivery_status=AdCampaign.DELIVERY_PAUSED_FUNDS
            )
            campaign.delivery_status = AdCampaign.DELIVERY_PAUSED_FUNDS
            sync_campaign_runtime(campaign)
        redis.srem(pause_queue_key(), campaign_id)


def _resume_funded_campaigns():
    campaigns = list(
        AdCampaign.objects.filter(
            review_status=AdCampaign.REVIEW_APPROVED,
            delivery_status=AdCampaign.DELIVERY_PAUSED_FUNDS,
        ).select_related("advertiser")
    )
    for campaign in campaigns:
        if _campaign_can_afford(campaign):
            AdCampaign.objects.filter(pk=campaign.pk).update(
                delivery_status=AdCampaign.DELIVERY_ACTIVE
            )
            campaign.delivery_status = AdCampaign.DELIVERY_ACTIVE
            sync_campaign_runtime(campaign)


@shared_task(name="ads.refresh_runtime_state")
def refresh_runtime_state():
    advertiser_ids = list(
        AdCampaign.objects.values_list("advertiser_id", flat=True).distinct()
    )
    wallets = TokenWallet.objects.filter(
        wallet_type=TokenWallet.TYPE_USER,
        user_id__in=advertiser_ids,
    ).select_related("user")
    for wallet in wallets:
        try:
            sync_wallet_runtime(wallet)
        except Exception:
            logger.exception("Failed to refresh Ads wallet %s", wallet.pk)

    _reconcile_pause_queue()
    _resume_funded_campaigns()

    campaigns = list(
        AdCampaign.objects.filter(
            review_status=AdCampaign.REVIEW_APPROVED,
            delivery_status=AdCampaign.DELIVERY_ACTIVE,
        ).select_related("advertiser")
    )
    live_ids = set()
    for campaign in campaigns:
        try:
            sync_campaign_runtime(campaign)
            live_ids.add(str(campaign.pk))
        except Exception:
            logger.exception("Failed to refresh Ads campaign %s", campaign.pk)

    # Remove stale members without querying during actual ad serving.
    redis = redis_connection()
    for slot in AdCampaign.delivery_slots():
        for raw_id in redis.zrange(slot_key(slot), 0, -1):
            campaign_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            if campaign_id not in live_ids:
                redis.zrem(slot_key(slot), campaign_id)


@shared_task(name="ads.settle_runtime")
def settle_runtime():
    # First complete any DB-posted batch that crashed before Redis ack.
    stranded = list(
        AdSettlementBatch.objects.filter(redis_acked_at__isnull=True)
        .select_related("campaign", "advertiser")
        .order_by("created_at")[:200]
    )
    for batch in stranded:
        try:
            process_settlement_batch(batch)
        except Exception:
            logger.exception("Failed Ads settlement batch %s", batch.pk)

    campaigns = list(
        AdCampaign.objects.select_related("advertiser").order_by("pk")
    )
    redis = redis_connection()
    for campaign in campaigns:
        lock = redis.lock(
            settlement_lock_name(campaign.pk),
            timeout=30,
            blocking_timeout=0.05,
        )
        if not lock.acquire(blocking=True):
            continue
        try:
            if AdSettlementBatch.objects.filter(
                campaign=campaign,
                redis_acked_at__isnull=True,
            ).exists():
                continue
            batch = create_settlement_batch(campaign)
            if batch:
                process_settlement_batch(batch)
        except Exception:
            logger.exception("Failed to settle Ads campaign %s", campaign.pk)
        finally:
            try:
                lock.release()
            except Exception:
                pass
