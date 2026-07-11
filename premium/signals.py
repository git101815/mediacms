import logging

from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver

from files.models import Media

from .subscription_tasks import grant_premium_release_unlocks
from .subscriptions import record_media_release


logger = logging.getLogger(__name__)


@receiver(
    post_save,
    sender=Media,
    dispatch_uid="premium.capture_first_public_video_release",
)
def capture_first_public_video_release(
    sender,
    instance,
    raw=False,
    **kwargs,
):
    if raw:
        return
    if instance.media_type != "video" or not instance.listable:
        return

    media_id = instance.pk

    def create_release_after_commit():
        media = Media.objects.select_related("user").get(pk=media_id)
        release, created = record_media_release(media=media)

        if release is None or not created:
            return

        try:
            grant_premium_release_unlocks.apply_async(
                args=[release.id],
                queue="short_tasks",
            )
        except Exception:
            logger.exception(
                "Could not enqueue premium release grants release_id=%s",
                release.id,
            )

    transaction.on_commit(create_release_after_commit)
