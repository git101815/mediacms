import logging

from celery import current_app, shared_task
from celery.schedules import crontab

from .subscriptions import (
    backfill_missing_media_releases,
    get_due_subscription_ids,
    grant_release_subscription_unlocks,
    process_pending_releases,
    renew_creator_subscription_with_tokens,
)


logger = logging.getLogger(__name__)


def install_subscription_beat_schedule() -> None:
    schedule = dict(current_app.conf.beat_schedule or {})
    schedule.setdefault(
        "premium-process-pending-releases",
        {
            "task": "premium.tasks.process_pending_premium_releases",
            "schedule": crontab(minute="*/5"),
        },
    )
    schedule.setdefault(
        "premium-backfill-missing-releases",
        {
            "task": "premium.tasks.backfill_missing_premium_releases",
            "schedule": crontab(minute="*/10"),
        },
    )
    schedule.setdefault(
        "premium-renew-creator-subscriptions",
        {
            "task": "premium.tasks.renew_due_creator_subscriptions",
            "schedule": crontab(minute="*/5"),
        },
    )
    current_app.conf.beat_schedule = schedule


@shared_task(name="premium.tasks.grant_premium_release_unlocks")
def grant_premium_release_unlocks(release_id):
    return grant_release_subscription_unlocks(
        release_id=int(release_id),
    )


@shared_task(name="premium.tasks.process_pending_premium_releases")
def process_pending_premium_releases(limit=500):
    return process_pending_releases(limit=int(limit))


@shared_task(name="premium.tasks.backfill_missing_premium_releases")
def backfill_missing_premium_releases(limit=500):
    return {
        "created": backfill_missing_media_releases(limit=int(limit)),
    }


@shared_task(name="premium.tasks.renew_due_creator_subscriptions")
def renew_due_creator_subscriptions(limit=500):
    subscription_ids = get_due_subscription_ids(limit=int(limit))
    summary = {
        "checked": 0,
        "renewed": 0,
        "not_renewed": 0,
        "errors": 0,
        "reasons": {},
    }

    for subscription_id in subscription_ids:
        summary["checked"] += 1
        try:
            result = renew_creator_subscription_with_tokens(
                subscription_id=subscription_id,
            )
        except Exception:
            summary["errors"] += 1
            logger.exception(
                "Creator subscription renewal crashed subscription_id=%s",
                subscription_id,
            )
            continue

        reason = result.get("reason", "unknown")
        summary["reasons"][reason] = summary["reasons"].get(reason, 0) + 1

        if result.get("renewed"):
            summary["renewed"] += 1
        else:
            summary["not_renewed"] += 1

    return summary


install_subscription_beat_schedule()
