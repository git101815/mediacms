import os

from django.conf import settings
from django.core.exceptions import SuspiciousFileOperation, ValidationError
from django.utils.text import get_valid_filename

from .models import PremiumMediaAsset
from .services import (
    get_premium_signed_url_ttl_seconds,
    get_ready_premium_asset,
    user_can_access_premium_media,
)
from .storage import get_premium_s3_client


def get_premium_download_asset(*, user, media) -> PremiumMediaAsset:
    if not user_can_access_premium_media(user=user, media=media):
        raise ValidationError("Premium media is not unlocked")

    asset = get_ready_premium_asset(media)
    if asset is None:
        raise ValidationError("Premium media is not available")

    if asset.storage_backend != PremiumMediaAsset.STORAGE_S3:
        raise ValidationError(
            "Premium downloads require private S3 storage"
        )

    if asset.playback_format != PremiumMediaAsset.PLAYBACK_MP4:
        raise ValidationError(
            "Premium downloads are only available for MP4 assets"
        )

    bucket = str(
        asset.storage_bucket
        or getattr(settings, "PREMIUM_S3_BUCKET", "")
        or ""
    ).strip()

    if not bucket:
        raise ValidationError("Premium S3 bucket is not configured")

    if not asset.storage_key:
        raise ValidationError("Premium S3 storage key is missing")

    return asset


def build_premium_download_filename(*, asset, media) -> str:
    raw_name = os.path.basename(asset.file_name or "").strip()

    if not raw_name:
        raw_name = f"{media.friendly_token}.mp4"

    try:
        safe_name = get_valid_filename(raw_name)
    except SuspiciousFileOperation:
        safe_name = ""

    if not safe_name:
        safe_name = f"{media.friendly_token}.mp4"

    stem, extension = os.path.splitext(safe_name)
    if extension.lower() != ".mp4":
        safe_name = f"{stem or media.friendly_token}.mp4"

    ascii_name = safe_name.encode("ascii", "ignore").decode("ascii")
    if not ascii_name:
        ascii_name = f"{media.friendly_token}.mp4"

    return ascii_name


def build_premium_download_url(*, asset, media) -> str:
    bucket = str(
        asset.storage_bucket
        or getattr(settings, "PREMIUM_S3_BUCKET", "")
        or ""
    ).strip()

    if not bucket:
        raise ValidationError("Premium S3 bucket is not configured")

    if not asset.storage_key:
        raise ValidationError("Premium S3 storage key is missing")

    filename = build_premium_download_filename(
        asset=asset,
        media=media,
    )

    client = get_premium_s3_client()

    return client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": bucket,
            "Key": asset.storage_key,
            "ResponseContentType": "video/mp4",
            "ResponseContentDisposition": (
                f'attachment; filename="{filename}"'
            ),
        },
        ExpiresIn=get_premium_signed_url_ttl_seconds(),
    )
