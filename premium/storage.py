import os
import uuid
from botocore.config import Config

from django.conf import settings
from django.core.exceptions import ValidationError


PREMIUM_ALLOWED_EXTENSIONS = {".mp4"}
DEFAULT_PREMIUM_MAX_UPLOAD_SIZE_BYTES = 20 * 1024 * 1024 * 1024


def get_premium_max_upload_size_bytes() -> int:
    return int(
        getattr(
            settings,
            "PREMIUM_MAX_UPLOAD_SIZE_BYTES",
            DEFAULT_PREMIUM_MAX_UPLOAD_SIZE_BYTES,
        )
    )


def validate_premium_upload_file(uploaded_file) -> None:
    if uploaded_file is None:
        raise ValidationError("Premium file is required")

    original_name = uploaded_file.name or ""
    extension = os.path.splitext(original_name)[1].lower()

    if extension not in PREMIUM_ALLOWED_EXTENSIONS:
        raise ValidationError("Premium upload must be an MP4 file")

    max_size = get_premium_max_upload_size_bytes()
    file_size = int(getattr(uploaded_file, "size", 0) or 0)

    if file_size <= 0:
        raise ValidationError("Premium file is empty")

    if file_size > max_size:
        raise ValidationError("Premium file is too large")


def build_premium_storage_key(*, media, uploaded_file) -> str:
    extension = os.path.splitext(uploaded_file.name or "")[1].lower() or ".mp4"
    prefix = str(getattr(settings, "PREMIUM_S3_UPLOAD_PREFIX", "premium-media")).strip("/")
    token = uuid.uuid4().hex

    return f"{prefix}/users/{media.user_id}/media/{media.friendly_token}/{token}{extension}"


def get_premium_s3_client():
    try:
        import boto3
    except ImportError as exc:
        raise ValidationError("boto3 is required for premium uploads") from exc

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

    return boto3.client("s3", **client_kwargs)


def upload_premium_file_to_private_s3(*, media, uploaded_file) -> dict:
    validate_premium_upload_file(uploaded_file)

    bucket = str(getattr(settings, "PREMIUM_S3_BUCKET", "") or "").strip()
    if not bucket:
        raise ValidationError("PREMIUM_S3_BUCKET is not configured")

    storage_key = build_premium_storage_key(media=media, uploaded_file=uploaded_file)
    content_type = getattr(uploaded_file, "content_type", "") or "video/mp4"

    client = get_premium_s3_client()
    uploaded_file.seek(0)

    client.upload_fileobj(
        uploaded_file,
        bucket,
        storage_key,
        ExtraArgs={
            "ContentType": content_type,
        },
    )

    return {
        "storage_bucket": bucket,
        "storage_key": storage_key,
        "content_type": content_type,
        "file_name": uploaded_file.name or "",
        "size_bytes": int(getattr(uploaded_file, "size", 0) or 0),
    }