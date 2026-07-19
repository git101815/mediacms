import os
import tempfile
from dataclasses import dataclass

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import transaction
from django.utils import timezone

from . import helpers
from .models import DailyVideoUploadQuota


DAILY_VIDEO_UPLOAD_LIMIT_CODE = "daily_video_upload_limit_reached"


class DailyVideoUploadLimitReached(Exception):
    def __init__(self, *, limit, used):
        self.limit = int(limit)
        self.used = int(used)
        super().__init__(
            "You have reached the daily limit of "
            f"{self.limit} video uploads."
        )

    def as_payload(self):
        return {
            "success": False,
            "preventRetry": True,
            "code": DAILY_VIDEO_UPLOAD_LIMIT_CODE,
            "error": str(self),
            "limit": self.limit,
            "used": self.used,
            "remaining": max(0, self.limit - self.used),
        }


@dataclass(frozen=True)
class DailyVideoUploadReservation:
    quota_id: int


def get_daily_video_upload_limit():
    try:
        value = int(
            getattr(
                settings,
                "MAX_VIDEO_UPLOADS_PER_DAY",
                0,
            )
        )
    except (TypeError, ValueError):
        return 0

    return max(0, value)


def get_daily_video_upload_status(user):
    """Return the current user's quota state for the upload page."""
    limit = get_daily_video_upload_limit()
    authenticated = bool(
        getattr(user, "is_authenticated", False)
    )
    enabled = bool(
        limit > 0
        and authenticated
        and getattr(user, "pk", None)
        and not getattr(user, "is_superuser", False)
    )
    day = timezone.localdate()
    used = 0

    if enabled:
        used = int(
            DailyVideoUploadQuota.objects.filter(
                user_id=user.pk,
                day=day,
            ).values_list(
                "used",
                flat=True,
            ).first()
            or 0
        )

    return {
        "enabled": enabled,
        "day": day.isoformat(),
        "timezone": settings.TIME_ZONE,
        "limit": limit,
        "used": used,
        "remaining": max(0, limit - used) if enabled else None,
    }


def media_path_is_video(media_file_path):
    """Use the same file inspection path as Media.media_init()."""
    kind = helpers.get_file_type(media_file_path)

    if kind == "video":
        return True

    if kind in {"audio", "image", "pdf"}:
        return False

    # filetype cannot identify every valid container. Fall back to ffprobe,
    # matching the fallback used by Media.set_media_type().
    media_info = helpers.media_file_info(media_file_path)
    return bool(media_info.get("is_video"))


def uploaded_file_is_video(uploaded_file):
    """Inspect a Django UploadedFile without consuming its stream."""
    temporary_file_path = getattr(
        uploaded_file,
        "temporary_file_path",
        None,
    )

    if callable(temporary_file_path):
        try:
            path = temporary_file_path()
        except (OSError, ValueError):
            path = ""

        if path and os.path.isfile(path):
            return media_path_is_video(path)

    original_position = None
    temporary_path = ""

    try:
        original_position = uploaded_file.tell()
    except (AttributeError, OSError):
        pass

    try:
        try:
            uploaded_file.seek(0)
        except (AttributeError, OSError):
            pass

        suffix = os.path.splitext(
            getattr(uploaded_file, "name", "") or ""
        )[1]

        with tempfile.NamedTemporaryFile(
            delete=False,
            dir=settings.TEMP_DIRECTORY,
            suffix=suffix,
        ) as temporary_file:
            temporary_path = temporary_file.name

            chunks = getattr(uploaded_file, "chunks", None)
            if callable(chunks):
                for chunk in chunks():
                    temporary_file.write(chunk)
            else:
                while True:
                    chunk = uploaded_file.read(1024 * 1024)
                    if not chunk:
                        break
                    temporary_file.write(chunk)

        return media_path_is_video(temporary_path)
    finally:
        if temporary_path:
            helpers.rm_file(temporary_path)

        if original_position is not None:
            try:
                uploaded_file.seek(original_position)
            except (AttributeError, OSError):
                pass


def reserve_daily_video_upload(user):
    """Atomically consume one daily slot, returning a releasable token."""
    limit = get_daily_video_upload_limit()

    if limit <= 0 or getattr(user, "is_superuser", False):
        return None

    if not getattr(user, "is_authenticated", False) or not user.pk:
        return None

    day = timezone.localdate()
    user_model = get_user_model()

    with transaction.atomic():
        # The user row exists before any quota row. Locking it serializes the
        # first and all subsequent reservations for this user and day.
        user_model.objects.select_for_update().only("pk").get(
            pk=user.pk
        )
        quota, _created = DailyVideoUploadQuota.objects.get_or_create(
            user_id=user.pk,
            day=day,
        )
        used = int(quota.used)

        if used >= limit:
            raise DailyVideoUploadLimitReached(
                limit=limit,
                used=used,
            )

        quota.used = used + 1
        quota.save(update_fields=["used"])

    return DailyVideoUploadReservation(quota_id=quota.pk)


def release_daily_video_upload(reservation):
    """Return a slot when Media creation failed after reservation."""
    if reservation is None:
        return

    with transaction.atomic():
        quota = (
            DailyVideoUploadQuota.objects.select_for_update()
            .filter(pk=reservation.quota_id)
            .first()
        )

        if quota is None or quota.used <= 0:
            return

        quota.used -= 1
        quota.save(update_fields=["used"])
