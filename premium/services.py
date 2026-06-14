import hashlib
import json
from datetime import timedelta
from decimal import Decimal
from botocore.config import Config

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.urls import reverse
from django.utils import timezone

from files.models import Media
from ledger.models import (
    LEDGER_ACTION_PURCHASE,
    LEDGER_METADATA_VERSION,
    LedgerEntry,
    LedgerTransaction,
    TokenWallet,
)
from ledger.services import (
    _create_outbox_event,
    _require_wallet_not_blocked,
    enforce_wallet_velocity_limits,
    get_system_wallet,
    get_wallet_available_balance,
    record_wallet_velocity,
)

from .models import (
    CreatorSubscription,
    CreatorSubscriptionPlan,
    MediaPurchase,
    PremiumCollectionMedia,
    PremiumMediaAsset,
    PremiumMediaUnlock,
)
from .storage import upload_premium_file_to_private_s3

PLATFORM_TOKEN_DECIMALS = 6
DEFAULT_PREMIUM_CREATOR_SHARE_BPS = 8000
DEFAULT_PREMIUM_MEDIA_PRICE_TOKENS = 500 * (10 ** PLATFORM_TOKEN_DECIMALS)

def format_token_amount(value: int) -> str:
    scaled = int(value) / (10 ** PLATFORM_TOKEN_DECIMALS)
    text = f"{scaled:.2f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def get_premium_creator_share_bps() -> int:
    value = int(getattr(settings, "PREMIUM_CREATOR_SHARE_BPS", DEFAULT_PREMIUM_CREATOR_SHARE_BPS))
    if value < 0 or value > 10000:
        raise ValidationError("PREMIUM_CREATOR_SHARE_BPS must be between 0 and 10000")
    return value


def get_premium_signed_url_ttl_seconds() -> int:
    return int(getattr(settings, "PREMIUM_SIGNED_URL_TTL_SECONDS", 15 * 60))


def get_user_wallet(user) -> TokenWallet:
    wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=user,
        defaults={
            "allow_negative": False,
        },
    )
    return wallet


def get_ready_premium_asset(media: Media) -> PremiumMediaAsset | None:
    try:
        asset = media.premium_asset
    except PremiumMediaAsset.DoesNotExist:
        return None

    if asset.status != PremiumMediaAsset.STATUS_READY:
        return None

    return asset


def user_can_access_premium_media(*, user, media: Media) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False

    if getattr(user, "is_superuser", False):
        return True

    if media.user_id == user.id:
        return True

    return PremiumMediaUnlock.objects.filter(
        user=user,
        media=media,
        revoked_at__isnull=True,
    ).exists()


def grant_premium_unlock(
    *,
    user,
    media: Media,
    source_type: str,
    source_txn=None,
    source_subscription=None,
    metadata: dict | None = None,
) -> PremiumMediaUnlock:
    unlock, created = PremiumMediaUnlock.objects.get_or_create(
        user=user,
        media=media,
        defaults={
            "source_type": source_type,
            "source_txn": source_txn,
            "source_subscription": source_subscription,
            "metadata": metadata or {},
        },
    )

    if not created and unlock.revoked_at is not None:
        unlock.revoked_at = None
        unlock.source_type = source_type
        unlock.source_txn = source_txn
        unlock.source_subscription = source_subscription
        unlock.metadata = metadata or {}
        unlock.save(
            update_fields=[
                "revoked_at",
                "source_type",
                "source_txn",
                "source_subscription",
                "metadata",
            ]
        )

    return unlock


def build_s3_presigned_url(asset: PremiumMediaAsset) -> str:
    try:
        import boto3
    except ImportError as exc:
        raise ValidationError("boto3 is required for S3 premium playback") from exc

    bucket = asset.storage_bucket or getattr(settings, "PREMIUM_S3_BUCKET", "")
    if not bucket:
        raise ValidationError("Premium S3 bucket is not configured")
    if not asset.storage_key:
        raise ValidationError("Premium S3 storage key is missing")

    endpoint_url = getattr(settings, "PREMIUM_S3_ENDPOINT_URL", None)
    region_name = getattr(settings, "PREMIUM_S3_REGION_NAME", None)
    access_key = getattr(settings, "PREMIUM_S3_ACCESS_KEY_ID", None)
    secret_key = getattr(settings, "PREMIUM_S3_SECRET_ACCESS_KEY", None)

    client_kwargs = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url
    if region_name:
        client_kwargs["region_name"] = region_name
    if access_key:
        client_kwargs["aws_access_key_id"] = access_key
    if secret_key:
        client_kwargs["aws_secret_access_key"] = secret_key

    signature_version = getattr(
        settings,
        "PREMIUM_S3_SIGNATURE_VERSION",
        getattr(settings, "AWS_S3_SIGNATURE_VERSION", "s3v4"),
    )

    addressing_style = getattr(
        settings,
        "PREMIUM_S3_ADDRESSING_STYLE",
        getattr(settings, "AWS_S3_ADDRESSING_STYLE", "path"),
    )

    client_kwargs["config"] = Config(
        signature_version=signature_version,
        s3={
            "addressing_style": addressing_style,
        },
    )

    client = boto3.client("s3", **client_kwargs)

    return client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket,
            "Key": asset.storage_key,
            "ResponseContentType": asset.content_type or "video/mp4",
        },
        ExpiresIn=get_premium_signed_url_ttl_seconds(),
    )


def build_premium_source_url(asset: PremiumMediaAsset) -> str:
    if asset.storage_backend == PremiumMediaAsset.STORAGE_DIRECT_URL:
        if not asset.direct_url:
            raise ValidationError("Premium direct URL is missing")
        return asset.direct_url

    if asset.storage_backend == PremiumMediaAsset.STORAGE_S3:
        return build_s3_presigned_url(asset)

    raise ValidationError("Unsupported premium storage backend")


def build_premium_playback_payload(*, user, media: Media, request=None) -> dict:
    if not user_can_access_premium_media(user=user, media=media):
        raise ValidationError("Premium media is not unlocked")

    asset = get_ready_premium_asset(media)
    if asset is None:
        raise ValidationError("Premium media is not available")

    source_url = build_premium_source_url(asset)

    if asset.playback_format == PremiumMediaAsset.PLAYBACK_HLS:
        hls_info = {
            "master_file": source_url,
        }
        encodings_info = {}
    else:
        resolution = str(asset.video_height or media.video_height or 1080)
        codec = asset.codec or "h264"
        hls_info = {}
        encodings_info = {
            resolution: {
                codec: {
                    "url": source_url,
                    "status": "success",
                    "progress": 100,
                }
            }
        }

    return {
        "playback_type": "premium",
        "encodings_info": encodings_info,
        "hls_info": hls_info,
        "expires_in": get_premium_signed_url_ttl_seconds(),
    }


def build_premium_media_state(*, user, media: Media, request=None) -> dict:
    asset = get_ready_premium_asset(media)
    enabled = asset is not None
    has_unlock = enabled and user_can_access_premium_media(user=user, media=media)

    purchase_url = ""
    premium_playback_url = ""
    manage_url = ""

    if request is not None and user_can_manage_premium_media(user=user, media=media):
        manage_url = request.build_absolute_uri(
            reverse("premium_media_asset_edit", kwargs={"friendly_token": media.friendly_token})
        )

    if request is not None and enabled:
        purchase_url = request.build_absolute_uri(
            reverse("premium_media_purchase", kwargs={"friendly_token": media.friendly_token})
        )

        if has_unlock:
            premium_playback_url = request.build_absolute_uri(
                reverse("premium_media_playback", kwargs={"friendly_token": media.friendly_token})
            )

    price_tokens = int(asset.price_tokens) if asset else 0

    return {
        "enabled": enabled,
        "viewer_has_unlock": has_unlock,
        "default_mode": "premium" if has_unlock else "preview",
        "price_tokens": price_tokens,
        "price_display": format_token_amount(price_tokens),
        "purchase_url": purchase_url,
        "premium_playback_url": premium_playback_url,
        "manage_url": manage_url,
    }


@transaction.atomic
def purchase_premium_media_with_tokens(*, actor, media: Media) -> dict:
    if not getattr(actor, "is_authenticated", False):
        raise ValidationError("Authentication required")

    user_model = actor.__class__
    user = user_model.objects.select_for_update().get(pk=actor.pk)
    media = Media.objects.select_for_update().get(pk=media.pk)

    asset = get_ready_premium_asset(media)
    if asset is None:
        raise ValidationError("Premium media is not available")

    if media.user_id == user.id:
        unlock = grant_premium_unlock(
            user=user,
            media=media,
            source_type=PremiumMediaUnlock.SOURCE_ADMIN,
            metadata={"reason": "creator_owner"},
        )
        return {
            "purchased": False,
            "already_unlocked": True,
            "unlock_id": unlock.id,
            "price_tokens": 0,
        }

    existing_unlock = PremiumMediaUnlock.objects.filter(
        user=user,
        media=media,
        revoked_at__isnull=True,
    ).first()
    if existing_unlock:
        return {
            "purchased": False,
            "already_unlocked": True,
            "unlock_id": existing_unlock.id,
            "price_tokens": 0,
        }

    price_tokens = int(asset.price_tokens)
    if price_tokens <= 0:
        raise ValidationError("Premium media price is invalid")

    external_id = f"purchase:premium_media:{media.pk}:user:{user.pk}"

    existing_purchase = MediaPurchase.objects.select_related("txn").filter(
        user=user,
        media=media,
    ).first()
    if existing_purchase:
        unlock = grant_premium_unlock(
            user=user,
            media=media,
            source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
            source_txn=existing_purchase.txn,
            metadata={
                "repaired_from_existing_purchase": True,
                "price_tokens": existing_purchase.price_tokens,
            },
        )
        return {
            "purchased": False,
            "already_unlocked": True,
            "unlock_id": unlock.id,
            "price_tokens": 0,
        }

    buyer_wallet = TokenWallet.objects.select_for_update().get(pk=get_user_wallet(user).pk)
    creator_wallet = TokenWallet.objects.select_for_update().get(pk=get_user_wallet(media.user).pk)
    platform_wallet = TokenWallet.objects.select_for_update().get(
        pk=get_system_wallet(TokenWallet.SYSTEM_PLATFORM_FEES, allow_negative=False).pk
    )

    _require_wallet_not_blocked(buyer_wallet)

    if get_wallet_available_balance(buyer_wallet) < price_tokens:
        raise ValidationError("Insufficient token balance")

    enforce_wallet_velocity_limits(
        wallet=buyer_wallet,
        action=LEDGER_ACTION_PURCHASE,
        amount=price_tokens,
    )

    creator_share_bps = get_premium_creator_share_bps()
    creator_amount = (price_tokens * creator_share_bps) // 10000
    platform_amount = price_tokens - creator_amount

    request_hash = hashlib.sha256(
        json.dumps(
            {
                "external_id": external_id,
                "media_id": media.pk,
                "buyer_user_id": user.pk,
                "creator_user_id": media.user_id,
                "price_tokens": price_tokens,
                "creator_amount": creator_amount,
                "platform_amount": platform_amount,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    buyer_wallet.balance = int(buyer_wallet.balance) - price_tokens
    creator_wallet.balance = int(creator_wallet.balance) + creator_amount
    platform_wallet.balance = int(platform_wallet.balance) + platform_amount

    buyer_wallet.save(update_fields=["balance", "updated_at"])
    creator_wallet.save(update_fields=["balance", "updated_at"])
    platform_wallet.save(update_fields=["balance", "updated_at"])

    txn = LedgerTransaction.objects.create(
        kind=LEDGER_ACTION_PURCHASE,
        external_id=external_id,
        request_hash=request_hash,
        created_by=user,
        memo=f"Premium media purchase #{media.pk}",
        metadata={
            "product": "premium_media",
            "media_id": media.pk,
            "buyer_user_id": user.pk,
            "creator_user_id": media.user_id,
            "price_tokens": price_tokens,
            "creator_amount": creator_amount,
            "platform_amount": platform_amount,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    LedgerEntry.objects.create(
        txn=txn,
        wallet=buyer_wallet,
        delta=-price_tokens,
        balance_after=buyer_wallet.balance,
    )

    if creator_amount > 0:
        LedgerEntry.objects.create(
            txn=txn,
            wallet=creator_wallet,
            delta=creator_amount,
            balance_after=creator_wallet.balance,
        )

    if platform_amount > 0:
        LedgerEntry.objects.create(
            txn=txn,
            wallet=platform_wallet,
            delta=platform_amount,
            balance_after=platform_wallet.balance,
        )

    record_wallet_velocity(
        wallet=buyer_wallet,
        action=LEDGER_ACTION_PURCHASE,
        amount=price_tokens,
    )

    purchase = MediaPurchase.objects.create(
        user=user,
        media=media,
        txn=txn,
        price_tokens=price_tokens,
    )

    unlock = grant_premium_unlock(
        user=user,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_PURCHASE,
        source_txn=txn,
        metadata={
            "purchase_id": purchase.id,
            "price_tokens": price_tokens,
        },
    )

    _create_outbox_event(
        txn=txn,
        topic="ledger.purchase",
        payload={
            "product": "premium_media",
            "media_id": media.pk,
            "buyer_user_id": user.pk,
            "creator_user_id": media.user_id,
            "price_tokens": price_tokens,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    return {
        "purchased": True,
        "already_unlocked": False,
        "unlock_id": unlock.id,
        "price_tokens": price_tokens,
    }


def subscription_grants_media_access(*, subscription: CreatorSubscription, media: Media) -> bool:
    plan = subscription.plan

    if plan.access_policy == CreatorSubscriptionPlan.POLICY_ACTIVE_FULL_CATALOG:
        return media.user_id == subscription.creator_id

    if plan.access_policy == CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES:
        asset = get_ready_premium_asset(media)
        if asset is None or asset.premium_published_at is None:
            return False

        return (
            asset.premium_published_at >= subscription.current_period_start
            and asset.premium_published_at < subscription.current_period_end
        )

    if plan.access_policy == CreatorSubscriptionPlan.POLICY_SELECTED_COLLECTIONS:
        collection_ids = list(plan.included_collections.values_list("id", flat=True))
        if not collection_ids:
            return False

        return PremiumCollectionMedia.objects.filter(
            collection_id__in=collection_ids,
            media=media,
        ).exists()

    return False


def grant_subscription_unlocks_for_media(*, media: Media) -> int:
    asset = get_ready_premium_asset(media)
    if asset is None:
        return 0

    now = timezone.now()
    subscriptions = CreatorSubscription.objects.select_related("plan").filter(
        creator=media.user,
        status=CreatorSubscription.STATUS_ACTIVE,
        current_period_start__lte=now,
        current_period_end__gt=now,
    )

    created_count = 0

    for subscription in subscriptions:
        if not subscription_grants_media_access(subscription=subscription, media=media):
            continue

        unlock, created = PremiumMediaUnlock.objects.get_or_create(
            user=subscription.user,
            media=media,
            defaults={
                "source_type": PremiumMediaUnlock.SOURCE_SUBSCRIPTION,
                "source_subscription": subscription,
                "metadata": {
                    "subscription_id": subscription.id,
                    "plan_id": subscription.plan_id,
                    "period_start": subscription.current_period_start.isoformat(),
                    "period_end": subscription.current_period_end.isoformat(),
                },
            },
        )

        if created:
            created_count += 1

    return created_count

def user_can_manage_premium_media(*, user, media: Media) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False

    if getattr(user, "is_superuser", False):
        return True

    return media.user_id == user.id


def parse_token_display_amount(raw_value) -> int:
    text = str(raw_value or "").strip().replace(",", ".")
    if not text:
        raise ValidationError("Price is required")

    try:
        value = Decimal(text)
    except Exception as exc:
        raise ValidationError("Price must be a valid token amount") from exc

    if value <= 0:
        raise ValidationError("Price must be greater than zero")

    return int(value * (10 ** PLATFORM_TOKEN_DECIMALS))


def create_or_update_creator_premium_asset(
    *,
    actor,
    media: Media,
    uploaded_file,
    price_display,
    publish_now: bool,
) -> PremiumMediaAsset:
    if not user_can_manage_premium_media(user=actor, media=media):
        raise ValidationError("You cannot manage this premium media")

    price_tokens = parse_token_display_amount(price_display)
    upload_result = upload_premium_file_to_private_s3(
        media=media,
        uploaded_file=uploaded_file,
    )

    status = (
        PremiumMediaAsset.STATUS_READY
        if publish_now
        else PremiumMediaAsset.STATUS_DRAFT
    )

    premium_published_at = timezone.now() if publish_now else None

    asset, _created = PremiumMediaAsset.objects.update_or_create(
        media=media,
        defaults={
            "price_tokens": price_tokens,
            "status": status,
            "storage_backend": PremiumMediaAsset.STORAGE_S3,
            "playback_format": PremiumMediaAsset.PLAYBACK_MP4,
            "direct_url": "",
            "storage_bucket": upload_result["storage_bucket"],
            "storage_key": upload_result["storage_key"],
            "file_name": upload_result["file_name"],
            "content_type": upload_result["content_type"],
            "codec": "h264",
            "video_height": 1080,
            "size_bytes": upload_result["size_bytes"],
            "premium_published_at": premium_published_at,
        },
    )

    return asset


def update_creator_premium_asset_settings(
    *,
    actor,
    media: Media,
    price_display,
    publish_now: bool,
) -> PremiumMediaAsset:
    if not user_can_manage_premium_media(user=actor, media=media):
        raise ValidationError("You cannot manage this premium media")

    asset = get_ready_or_draft_premium_asset(media)
    if asset is None:
        raise ValidationError("Premium asset does not exist yet")

    asset.price_tokens = parse_token_display_amount(price_display)

    if publish_now:
        asset.status = PremiumMediaAsset.STATUS_READY
        if asset.premium_published_at is None:
            asset.premium_published_at = timezone.now()
    else:
        asset.status = PremiumMediaAsset.STATUS_DRAFT

    asset.save(
        update_fields=[
            "price_tokens",
            "status",
            "premium_published_at",
            "updated_at",
        ]
    )

    return asset


def get_ready_or_draft_premium_asset(media: Media) -> PremiumMediaAsset | None:
    try:
        return media.premium_asset
    except PremiumMediaAsset.DoesNotExist:
        return None

def get_default_premium_media_price_tokens() -> int:
    return int(
        getattr(
            settings,
            "PREMIUM_DEFAULT_MEDIA_PRICE_TOKENS",
            DEFAULT_PREMIUM_MEDIA_PRICE_TOKENS,
        )
    )


def replace_creator_premium_asset_file(
    *,
    actor,
    media: Media,
    uploaded_file,
) -> PremiumMediaAsset:
    if not user_can_manage_premium_media(user=actor, media=media):
        raise ValidationError("You cannot manage this premium media")

    existing_asset = get_ready_or_draft_premium_asset(media)

    upload_result = upload_premium_file_to_private_s3(
        media=media,
        uploaded_file=uploaded_file,
    )

    if existing_asset is not None:
        price_tokens = existing_asset.price_tokens
        status = existing_asset.status
        premium_published_at = existing_asset.premium_published_at
    else:
        price_tokens = get_default_premium_media_price_tokens()
        status = PremiumMediaAsset.STATUS_DRAFT
        premium_published_at = None

    asset, _created = PremiumMediaAsset.objects.update_or_create(
        media=media,
        defaults={
            "price_tokens": price_tokens,
            "status": status,
            "storage_backend": PremiumMediaAsset.STORAGE_S3,
            "playback_format": PremiumMediaAsset.PLAYBACK_MP4,
            "direct_url": "",
            "storage_bucket": upload_result["storage_bucket"],
            "storage_key": upload_result["storage_key"],
            "file_name": upload_result["file_name"],
            "content_type": upload_result["content_type"],
            "codec": "h264",
            "video_height": 1080,
            "size_bytes": upload_result["size_bytes"],
            "premium_published_at": premium_published_at,
        },
    )

    return asset