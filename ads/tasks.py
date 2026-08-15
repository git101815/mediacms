import logging
import os
import uuid

import requests
from celery import shared_task
from django.conf import settings
from django.core.files import File
from django.db import transaction
from django.utils.module_loading import import_string

from ledger.models import TokenWallet

from .models import AdCampaign, AdSettlementBatch
from .public_urls import banner_public_url
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


def _admin_url(path):
    base = str(getattr(settings, "FRONTEND_HOST", "") or "").rstrip("/")
    return f"{base}{path}" if base else path


def _development_mode_enabled():
    return (
        str(os.environ.get("DEVELOPMENT_MODE", "") or "")
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )


def _ensure_banner_available_on_storj(creative):
    if (
        creative is None
        or not creative.is_banner
        or not creative.image
        or _development_mode_enabled()
    ):
        return

    name = str(creative.image.name or "").lstrip("/")
    if not name:
        return

    StorageClass = import_string(settings.STORJ_STORAGE)
    storj_storage = StorageClass()

    if storj_storage.exists(name):
        return

    with open(creative.image.path, "rb") as handle:
        storj_storage.save(name, File(handle))

    if not storj_storage.exists(name):
        raise RuntimeError(
            f"Banner creative was not persisted to Storj: {name}"
        )


def _review_webhook_payload(kind, object_id, event_id):
    if kind == "campaign":
        obj = (
            AdCampaign.objects
            .select_related("advertiser")
            .filter(pk=object_id)
            .first()
        )
        if obj is None or obj.review_status != AdCampaign.REVIEW_PENDING:
            return None

        return {
            "event": "ads.review_requested",
            "event_id": event_id,
            "kind": "campaign",
            "id": obj.pk,
            "name": obj.name,
            "review_status": obj.review_status,
            "advertiser": {
                "id": obj.advertiser_id,
                "username": obj.advertiser.username,
                "email": obj.advertiser.email,
            },
            "placement": obj.placement,
            "pricing_model": obj.pricing_model,
            "bid_microtokens": int(obj.bid_microtokens),
            "target_url": obj.target_url,
            "creative_ids": list(
                obj.creative_links
                .filter(enabled=True)
                .values_list("creative_id", flat=True)
            ),
            "admin_url": _admin_url(
                f"/admin/ads/adcampaign/{obj.pk}/change/"
            ),
        }

    if kind == "creative":
        from .models import AdCreative

        obj = (
            AdCreative.objects
            .select_related("advertiser")
            .filter(pk=object_id)
            .first()
        )
        if obj is None or obj.review_status != AdCreative.REVIEW_PENDING:
            return None

        _ensure_banner_available_on_storj(obj)
        banner_url = banner_public_url(obj)

        return {
            "event": "ads.review_requested",
            "event_id": event_id,
            "kind": "creative",
            "id": obj.pk,
            "name": obj.name,
            "review_status": obj.review_status,
            "advertiser": {
                "id": obj.advertiser_id,
                "username": obj.advertiser.username,
                "email": obj.advertiser.email,
            },
            "placement": obj.placement,
            "source_kind": obj.source_kind,
            "banner_url": banner_url,
            "vast_url": obj.vast_url,
            "destination_url": obj.destination_url,
            "admin_url": _admin_url(
                f"/admin/ads/adcreative/{obj.pk}/change/"
            ),
        }

    return None


@shared_task(
    bind=True,
    name="ads.notify_admin_review",
    queue="short_tasks",
    autoretry_for=(requests.RequestException,),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def notify_admin_review(self, kind, object_id, event_id=""):
    # The production test command may run against a production-shaped stack.
    # Never let tests contact n8n/Telegram even if the prod worker environment
    # contains the webhook variables.
    if getattr(settings, "TESTING", False):
        return False

    webhook_url = str(
        os.environ.get("NOTIFICATION_WEBHOOK_URL", "")
    ).strip()
    if not webhook_url:
        logger.info(
            "Ads review webhook not configured; skipping %s #%s",
            kind,
            object_id,
        )
        return False

    event_id = str(event_id or uuid.uuid4().hex)
    try:
        payload = _review_webhook_payload(
            str(kind),
            int(object_id),
            event_id,
        )
    except Exception as exc:
        raise self.retry(exc=exc, countdown=5)
    if payload is None:
        # Object disappeared or is no longer pending by the time the task ran.
        return False

    headers = {
        "Content-Type": "application/json",
        "X-Ads-Review-Event": event_id,
    }
    secret = str(
        os.environ.get("NOTIFICATION_WEBHOOK_SECRET", "")
    ).strip()
    if secret:
        headers["X-Ads-Review-Secret"] = secret

    timeout = 5.0

    response = requests.post(
        webhook_url,
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return True


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
