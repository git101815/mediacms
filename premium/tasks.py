import logging
import os
import shutil
from datetime import timedelta

from celery import shared_task
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.files import File
from django.utils import timezone

from files.models import Media
from ledger.models import LedgerOutbox

from .notifications import (
    deliver_creator_email_outbox_event,
    get_recoverable_creator_email_event_ids,
    record_creator_email_delivery_failure,
)
from .services import replace_creator_premium_asset_file


logger = logging.getLogger(__name__)

PREMIUM_UPLOAD_STATUS_CACHE_TIMEOUT_SECONDS = 24 * 60 * 60
PREMIUM_UPLOAD_MAX_RETRIES = 3


def premium_upload_status_cache_key(upload_uuid):
    return f"premium-upload-status:{str(upload_uuid)}"


def store_premium_upload_status(upload_uuid, payload):
    cache.set(
        premium_upload_status_cache_key(upload_uuid),
        payload,
        timeout=PREMIUM_UPLOAD_STATUS_CACHE_TIMEOUT_SECONDS,
    )


def cleanup_premium_upload_files(
    premium_file_path,
    upload_directory,
):
    try:
        if premium_file_path and os.path.isfile(premium_file_path):
            os.remove(premium_file_path)
    except OSError:
        logger.exception(
            "Could not remove premium temporary file path=%s",
            premium_file_path,
        )

    if upload_directory:
        shutil.rmtree(
            upload_directory,
            ignore_errors=True,
        )


def build_processing_status(
    *,
    upload_uuid,
    media_id,
    actor_id,
    message="Finalizing premium upload.",
    retry_count=0,
):
    return {
        "success": True,
        "state": "processing",
        "processing": True,
        "upload_uuid": str(upload_uuid),
        "media_id": int(media_id),
        "actor_id": int(actor_id),
        "message": message,
        "retry_count": int(retry_count),
    }


def build_error_status(
    *,
    upload_uuid,
    media_id,
    actor_id,
    error,
):
    return {
        "success": False,
        "state": "error",
        "processing": False,
        "upload_uuid": str(upload_uuid),
        "media_id": int(media_id),
        "actor_id": int(actor_id),
        "error": str(error),
    }


@shared_task(
    bind=True,
    acks_late=True,
    reject_on_worker_lost=True,
    max_retries=PREMIUM_UPLOAD_MAX_RETRIES,
)
def finalize_premium_upload(
    self,
    *,
    media_id,
    actor_id,
    premium_file_path,
    upload_directory,
    original_filename,
    upload_uuid,
):
    status_key = premium_upload_status_cache_key(upload_uuid)
    existing_status = cache.get(status_key)

    if (
        existing_status
        and existing_status.get("state") == "success"
        and int(existing_status.get("media_id", 0)) == int(media_id)
        and int(existing_status.get("actor_id", 0)) == int(actor_id)
    ):
        return existing_status

    processing_status = build_processing_status(
        upload_uuid=upload_uuid,
        media_id=media_id,
        actor_id=actor_id,
        retry_count=self.request.retries,
    )
    store_premium_upload_status(
        upload_uuid,
        processing_status,
    )

    try:
        if not os.path.isfile(premium_file_path):
            raise FileNotFoundError(
                "Premium temporary file does not exist"
            )

        media = Media.objects.select_related("user").get(
            pk=media_id,
        )
        actor = get_user_model().objects.get(
            pk=actor_id,
        )

        with open(premium_file_path, "rb") as source:
            premium_file = File(
                source,
                name=original_filename,
            )

            asset = replace_creator_premium_asset_file(
                actor=actor,
                media=media,
                uploaded_file=premium_file,
            )

    except ValidationError as exc:
        error_message = (
            exc.messages[0]
            if hasattr(exc, "messages") and exc.messages
            else str(exc)
        )

        logger.warning(
            "Premium upload validation failed "
            "media_id=%s actor_id=%s upload_uuid=%s error=%s",
            media_id,
            actor_id,
            upload_uuid,
            error_message,
        )

        error_status = build_error_status(
            upload_uuid=upload_uuid,
            media_id=media_id,
            actor_id=actor_id,
            error=error_message,
        )
        store_premium_upload_status(
            upload_uuid,
            error_status,
        )

        cleanup_premium_upload_files(
            premium_file_path,
            upload_directory,
        )

        return error_status

    except FileNotFoundError:
        logger.exception(
            "Premium temporary file is missing "
            "media_id=%s actor_id=%s upload_uuid=%s path=%s",
            media_id,
            actor_id,
            upload_uuid,
            premium_file_path,
        )

        error_status = build_error_status(
            upload_uuid=upload_uuid,
            media_id=media_id,
            actor_id=actor_id,
            error="Premium temporary file is missing.",
        )
        store_premium_upload_status(
            upload_uuid,
            error_status,
        )

        cleanup_premium_upload_files(
            premium_file_path,
            upload_directory,
        )

        return error_status

    except Exception as exc:
        retry_count = int(self.request.retries)

        if retry_count < int(self.max_retries):
            countdown = min(
                60 * (2 ** retry_count),
                15 * 60,
            )

            retry_status = build_processing_status(
                upload_uuid=upload_uuid,
                media_id=media_id,
                actor_id=actor_id,
                message="Premium upload finalization is being retried.",
                retry_count=retry_count + 1,
            )
            store_premium_upload_status(
                upload_uuid,
                retry_status,
            )

            logger.warning(
                "Retrying premium upload finalization "
                "media_id=%s actor_id=%s upload_uuid=%s "
                "retry=%s countdown=%s",
                media_id,
                actor_id,
                upload_uuid,
                retry_count + 1,
                countdown,
            )

            raise self.retry(
                exc=exc,
                countdown=countdown,
            )

        logger.exception(
            "Premium upload finalization failed "
            "media_id=%s actor_id=%s upload_uuid=%s",
            media_id,
            actor_id,
            upload_uuid,
        )

        error_status = build_error_status(
            upload_uuid=upload_uuid,
            media_id=media_id,
            actor_id=actor_id,
            error=(
                "Premium upload finalization failed after several "
                "attempts. Upload the file again."
            ),
        )
        store_premium_upload_status(
            upload_uuid,
            error_status,
        )

        cleanup_premium_upload_files(
            premium_file_path,
            upload_directory,
        )

        return error_status

    success_status = {
        "success": True,
        "state": "success",
        "processing": False,
        "upload_uuid": str(upload_uuid),
        "media_id": int(media_id),
        "actor_id": int(actor_id),
        "premium_asset": {
            "id": asset.id,
            "status": asset.status,
            "file_name": asset.file_name,
            "size_bytes": asset.size_bytes,
        },
    }

    store_premium_upload_status(
        upload_uuid,
        success_status,
    )

    cleanup_premium_upload_files(
        premium_file_path,
        upload_directory,
    )

    logger.info(
        "Premium upload finalized "
        "media_id=%s actor_id=%s upload_uuid=%s asset_id=%s",
        media_id,
        actor_id,
        upload_uuid,
        asset.id,
    )

    return success_status

@shared_task(
    bind=True,
    name="premium.tasks.dispatch_creator_email_outbox_event",
    queue="short_tasks",
    acks_late=True,
    reject_on_worker_lost=True,
    max_retries=5,
)
def dispatch_creator_email_outbox_event(self, event_id: int):
    try:
        return deliver_creator_email_outbox_event(int(event_id))
    except LedgerOutbox.DoesNotExist:
        logger.warning(
            "Creator email outbox event no longer exists event_id=%s",
            event_id,
        )
        return {"sent": False, "reason": "missing", "event_id": int(event_id)}
    except Exception as exc:
        retry_count = int(self.request.retries)
        max_retries = int(self.max_retries or 0)
        final_failure = retry_count >= max_retries
        now = timezone.now()
        countdown = min(60 * (2**retry_count), 15 * 60)
        retry_at = (
            None
            if final_failure
            else now + timedelta(seconds=countdown)
        )

        try:
            event = record_creator_email_delivery_failure(
                event_id=int(event_id),
                error_message=str(exc),
                final_failure=final_failure,
                retry_at=retry_at,
            )
        except LedgerOutbox.DoesNotExist:
            logger.warning(
                "Creator email outbox event disappeared while recording "
                "failure event_id=%s",
                event_id,
            )
            return {
                "sent": False,
                "reason": "missing",
                "event_id": int(event_id),
            }

        # A duplicate/recovery task may have completed or terminally failed the
        # row while this task was waiting for its DB lock. Never downgrade it.
        if event.status == LedgerOutbox.STATUS_DISPATCHED:
            return {
                "sent": False,
                "reason": "already_dispatched",
                "event_id": int(event_id),
            }
        if event.status == LedgerOutbox.STATUS_DEAD_LETTERED:
            logger.exception(
                "Creator transactional email dead-lettered event_id=%s",
                event_id,
            )
            return {
                "sent": False,
                "reason": "dead_lettered",
                "event_id": int(event_id),
            }

        logger.warning(
            "Retrying creator transactional email event_id=%s retry=%s countdown=%s",
            event_id,
            retry_count + 1,
            countdown,
        )
        raise self.retry(exc=exc, countdown=countdown)


@shared_task(
    name="premium.tasks.recover_creator_email_outbox",
    queue="short_tasks",
)
def recover_creator_email_outbox():
    event_ids = get_recoverable_creator_email_event_ids()
    enqueued = 0
    enqueue_failures = 0

    for event_id in event_ids:
        try:
            dispatch_creator_email_outbox_event.delay(int(event_id))
            enqueued += 1
        except Exception:
            # Keep the durable row untouched. The next beat run will find it
            # again after the broker is healthy.
            enqueue_failures += 1
            logger.exception(
                "Failed to recover creator email outbox event_id=%s",
                event_id,
            )

    return {
        "candidates": len(event_ids),
        "enqueued": enqueued,
        "enqueue_failures": enqueue_failures,
    }

