import logging

from django.db import transaction
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from ledger.models import TokenWallet
from users.models import User

from .models import AdCampaign, AdCampaignCreative, AdCreative
from .runtime import (
    drop_campaign_runtime,
    sync_campaign_runtime,
    sync_wallet_runtime,
    wallet_sync_suppressed,
)

logger = logging.getLogger(__name__)


def _safe(callback):
    try:
        callback()
    except Exception:
        logger.exception("Ads runtime synchronization failed")


def _sync_campaign_id(campaign_id):
    campaign = (
        AdCampaign.objects
        .select_related("advertiser")
        .filter(pk=campaign_id)
        .first()
    )
    if campaign:
        sync_campaign_runtime(campaign)


@receiver(post_save, sender=AdCampaign)
def ad_campaign_saved(sender, instance, **kwargs):
    campaign_id = instance.pk
    transaction.on_commit(
        lambda: _safe(lambda: _sync_campaign_id(campaign_id))
    )


@receiver(post_delete, sender=AdCampaign)
def ad_campaign_deleted(sender, instance, **kwargs):
    campaign_id = instance.pk
    transaction.on_commit(
        lambda: _safe(lambda: drop_campaign_runtime(campaign_id))
    )


@receiver(post_save, sender=AdCampaignCreative)
@receiver(post_delete, sender=AdCampaignCreative)
def ad_campaign_creative_changed(sender, instance, **kwargs):
    campaign_id = instance.campaign_id
    transaction.on_commit(
        lambda: _safe(lambda: _sync_campaign_id(campaign_id))
    )


@receiver(post_save, sender=AdCreative)
def ad_creative_saved(sender, instance, **kwargs):
    creative_id = instance.pk

    def sync():
        campaign_ids = list(
            AdCampaignCreative.objects
            .filter(creative_id=creative_id)
            .values_list("campaign_id", flat=True)
        )
        for campaign_id in campaign_ids:
            _sync_campaign_id(campaign_id)

    transaction.on_commit(lambda: _safe(sync))


@receiver(post_save, sender=TokenWallet)
def advertiser_wallet_saved(sender, instance, **kwargs):
    if wallet_sync_suppressed() or instance.user_id is None:
        return
    user_id = instance.user_id

    def sync():
        wallet = (
            TokenWallet.objects
            .select_related("user")
            .filter(user_id=user_id)
            .first()
        )
        if wallet and getattr(wallet.user, "advertiserUser", False):
            sync_wallet_runtime(wallet)

    transaction.on_commit(lambda: _safe(sync))


@receiver(post_save, sender=User)
def advertiser_user_saved(sender, instance, **kwargs):
    user_id = instance.pk

    def sync():
        user = User.objects.filter(pk=user_id).first()
        if user is None:
            return
        allowed = bool(
            getattr(user, "advertiserUser", False)
            or getattr(user, "is_superuser", False)
        )
        campaigns = list(
            AdCampaign.objects
            .filter(advertiser_id=user_id)
            .select_related("advertiser")
        )
        if not allowed:
            for campaign in campaigns:
                drop_campaign_runtime(campaign.pk)
            return

        wallet = TokenWallet.objects.filter(user_id=user_id).first()
        if wallet:
            sync_wallet_runtime(wallet)
        for campaign in campaigns:
            sync_campaign_runtime(campaign)

    transaction.on_commit(lambda: _safe(sync))
