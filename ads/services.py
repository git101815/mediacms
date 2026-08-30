from __future__ import annotations

import hashlib
import json

from django.db import transaction
from django.utils import timezone

from ledger.models import (
    LEDGER_METADATA_VERSION,
    LedgerEntry,
    LedgerTransaction,
    TokenWallet,
)
from ledger.services import (
    consume_promotional_tokens_for_internal_spend,
    get_system_wallet,
)

from .models import AdCampaign, AdSettlementBatch
from .runtime import (
    NANOS_PER_MICROTOKEN,
    ack_settlement,
    acquire_account_sync_lock,
    campaign_accrued_key,
    campaign_clicks_key,
    campaign_impressions_key,
    redis_connection,
    suppress_wallet_runtime_sync,
    sync_campaign_runtime,
)


def _batch_request_hash(batch):
    payload = {
        "batch_id": str(batch.id),
        "campaign_id": batch.campaign_id,
        "advertiser_id": batch.advertiser_id,
        "amount_microtokens": int(batch.amount_microtokens),
        "impressions": int(batch.impressions),
        "clicks": int(batch.clicks),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def create_settlement_batch(campaign):
    redis = redis_connection()

    existing = (
        AdSettlementBatch.objects.filter(
            campaign=campaign,
            redis_acked_at__isnull=True,
        )
        .order_by("created_at")
        .first()
    )
    if existing:
        return existing

    values = redis.mget(
        campaign_accrued_key(campaign.pk),
        campaign_impressions_key(campaign.pk),
        campaign_clicks_key(campaign.pk),
    )
    accrued_nanos = int(values[0] or 0)
    impressions = int(values[1] or 0)
    clicks = int(values[2] or 0)

    # Financial settlement is in ledger micro-tokens. Keep the sub-micro
    # remainder in Redis for a future batch.
    amount_microtokens = max(0, accrued_nanos // NANOS_PER_MICROTOKEN)
    if amount_microtokens == 0 and impressions == 0 and clicks == 0:
        return None

    return AdSettlementBatch.objects.create(
        campaign=campaign,
        advertiser=campaign.advertiser,
        amount_microtokens=amount_microtokens,
        impressions=impressions,
        clicks=clicks,
    )


def _post_batch_to_ledger(batch):
    if batch.status == AdSettlementBatch.STATUS_POSTED:
        return batch

    amount = int(batch.amount_microtokens)

    with transaction.atomic():
        batch = (
            AdSettlementBatch.objects.select_for_update()
            .select_related("campaign", "advertiser")
            .get(pk=batch.pk)
        )
        if batch.status == AdSettlementBatch.STATUS_POSTED:
            return batch

        campaign = AdCampaign.objects.select_for_update().get(pk=batch.campaign_id)
        wallet = TokenWallet.objects.select_for_update().get(
            wallet_type=TokenWallet.TYPE_USER,
            user_id=batch.advertiser_id,
        )

        # Deliberately use raw balance-held here: this batch itself is part of
        # the Redis unsettled amount, so calling get_wallet_available_balance()
        # would subtract it twice.
        raw_available = int(wallet.balance) - int(wallet.held_balance)
        if amount > raw_available:
            campaign.delivery_status = AdCampaign.DELIVERY_PAUSED_FUNDS
            campaign.save(update_fields=["delivery_status", "updated_at"])
            batch.last_error = "Advertiser wallet no longer covers delivered ad spend"
            batch.save(update_fields=["last_error"])
            return batch

        ledger_txn = None
        if amount > 0:
            platform_wallet = get_system_wallet(
                TokenWallet.SYSTEM_PLATFORM_FEES,
                allow_negative=False,
            )
            platform_wallet = TokenWallet.objects.select_for_update().get(
                pk=platform_wallet.pk
            )

            external_id = f"ads:settlement:{batch.id}"
            existing_txn = LedgerTransaction.objects.filter(
                external_id=external_id
            ).first()
            if existing_txn:
                ledger_txn = existing_txn
            else:
                promotional_spent = consume_promotional_tokens_for_internal_spend(
                    wallet,
                    amount,
                    reserve_unsettled_ads=False,
                )
                withdrawable_spent = amount - promotional_spent
                with suppress_wallet_runtime_sync():
                    wallet.balance = int(wallet.balance) - amount
                    wallet.save(
                        update_fields=[
                            "balance",
                            "promotional_balance",
                            "updated_at",
                        ]
                    )

                platform_wallet.balance = int(platform_wallet.balance) + amount
                platform_wallet.save(update_fields=["balance", "updated_at"])

                ledger_txn = LedgerTransaction.objects.create(
                    kind="ad_spend",
                    external_id=external_id,
                    request_hash=_batch_request_hash(batch),
                    created_by=batch.advertiser,
                    memo=f"Direct Ads spend · campaign #{campaign.pk}",
                    metadata={
                        "campaign_id": campaign.pk,
                        "settlement_batch_id": str(batch.id),
                        "impressions": int(batch.impressions),
                        "clicks": int(batch.clicks),
                        "amount_microtokens": amount,
                        "promotional_spent_units": promotional_spent,
                        "withdrawable_spent_units": withdrawable_spent,
                    },
                    metadata_version=LEDGER_METADATA_VERSION,
                )
                LedgerEntry.objects.create(
                    txn=ledger_txn,
                    wallet=wallet,
                    delta=-amount,
                    promotional_delta=-promotional_spent,
                    balance_after=wallet.balance,
                )
                LedgerEntry.objects.create(
                    txn=ledger_txn,
                    wallet=platform_wallet,
                    delta=amount,
                    balance_after=platform_wallet.balance,
                )

        campaign.impressions = int(campaign.impressions) + int(batch.impressions)
        campaign.clicks = int(campaign.clicks) + int(batch.clicks)
        campaign.spend_microtokens = int(campaign.spend_microtokens) + amount
        campaign.save(
            update_fields=[
                "impressions",
                "clicks",
                "spend_microtokens",
                "updated_at",
            ]
        )

        batch.ledger_txn = ledger_txn
        batch.status = AdSettlementBatch.STATUS_POSTED
        batch.posted_at = timezone.now()
        batch.last_error = ""
        batch.save(
            update_fields=[
                "ledger_txn",
                "status",
                "posted_at",
                "last_error",
            ]
        )
    return batch


def process_settlement_batch(batch):
    lock = acquire_account_sync_lock(batch.advertiser_id, blocking_timeout=5)
    if not lock.acquire(blocking=True):
        return False
    try:
        batch = AdSettlementBatch.objects.get(pk=batch.pk)

        if batch.status != AdSettlementBatch.STATUS_POSTED:
            batch = _post_batch_to_ledger(batch)
            if batch.status != AdSettlementBatch.STATUS_POSTED:
                return False

        if batch.redis_acked_at is None:
            ack_settlement(batch=batch)
            batch.redis_acked_at = timezone.now()
            batch.save(update_fields=["redis_acked_at"])
            campaign = AdCampaign.objects.select_related("advertiser").get(pk=batch.campaign_id)
            sync_campaign_runtime(campaign)
        return True
    finally:
        try:
            lock.release()
        except Exception:
            pass
