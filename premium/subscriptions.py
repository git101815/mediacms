import hashlib
import json
from datetime import timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.models import Q
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
    CreatorSubscriptionPeriod,
    CreatorSubscriptionPlan,
    PremiumMediaRelease,
    PremiumMediaUnlock,
)
from .services import format_token_amount, get_user_wallet


DEFAULT_CREATOR_SUBSCRIPTION_PLAN_CODE = "default"
DEFAULT_CREATOR_SUBSCRIPTION_PLAN_NAME = "Membership"
DEFAULT_CREATOR_SUBSCRIPTION_PERIOD_DAYS = 30
DEFAULT_SUBSCRIPTION_CREATOR_SHARE_BPS = 8000
DEFAULT_SUBSCRIPTION_RENEWAL_GRACE_DAYS = 3
DEFAULT_SUBSCRIPTION_RETRY_MINUTES = 60


def get_subscription_creator_share_bps() -> int:
    value = int(
        getattr(
            settings,
            "PREMIUM_SUBSCRIPTION_CREATOR_SHARE_BPS",
            DEFAULT_SUBSCRIPTION_CREATOR_SHARE_BPS,
        )
    )
    if value < 0 or value > 10000:
        raise ValidationError(
            "PREMIUM_SUBSCRIPTION_CREATOR_SHARE_BPS must be between 0 and 10000"
        )
    return value


def get_subscription_renewal_grace_days() -> int:
    value = int(
        getattr(
            settings,
            "PREMIUM_SUBSCRIPTION_RENEWAL_GRACE_DAYS",
            DEFAULT_SUBSCRIPTION_RENEWAL_GRACE_DAYS,
        )
    )
    if value < 0:
        raise ValidationError(
            "PREMIUM_SUBSCRIPTION_RENEWAL_GRACE_DAYS must be non-negative"
        )
    return value


def get_subscription_retry_minutes() -> int:
    value = int(
        getattr(
            settings,
            "PREMIUM_SUBSCRIPTION_RETRY_MINUTES",
            DEFAULT_SUBSCRIPTION_RETRY_MINUTES,
        )
    )
    return max(value, 1)


def _subscription_unlock_metadata(
    *,
    period: CreatorSubscriptionPeriod,
    release: PremiumMediaRelease,
) -> dict:
    return {
        "release_id": release.id,
        "subscription_period_id": period.id,
        "plan_id": period.plan_id,
        "released_at": release.released_at.isoformat(),
        "period_start": period.period_start.isoformat(),
        "period_end": period.period_end.isoformat(),
    }


def _grant_subscription_unlock(
    *,
    period: CreatorSubscriptionPeriod,
    release: PremiumMediaRelease,
) -> bool:
    _, created = PremiumMediaUnlock.objects.get_or_create(
        user_id=period.subscription.user_id,
        media_id=release.media_id,
        defaults={
            "source_type": PremiumMediaUnlock.SOURCE_SUBSCRIPTION,
            "source_subscription_id": period.subscription_id,
            "metadata": _subscription_unlock_metadata(
                period=period,
                release=release,
            ),
        },
    )

    # A revoked row remains revoked. An explicit administrative revocation must
    # not be undone by an asynchronous replay of the release grant.
    return created


@transaction.atomic
def grant_release_subscription_unlocks(*, release_id: int) -> int:
    release = (
        PremiumMediaRelease.objects.select_for_update()
        .select_related("media", "creator")
        .get(pk=release_id)
    )

    if release.processed_at is not None:
        return 0

    periods = (
        CreatorSubscriptionPeriod.objects.select_related(
            "subscription",
            "plan",
        )
        .filter(
            creator_id=release.creator_id,
            plan__access_policy=CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES,
            period_start__lte=release.released_at,
            period_end__gt=release.released_at,
        )
        .order_by("id")
    )

    created_count = 0
    for period in periods.iterator():
        if _grant_subscription_unlock(period=period, release=release):
            created_count += 1

    release.processed_at = timezone.now()
    release.save(update_fields=["processed_at"])
    return created_count


def grant_period_release_unlocks(*, period: CreatorSubscriptionPeriod) -> int:
    if period.plan.access_policy != CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES:
        return 0

    releases = PremiumMediaRelease.objects.filter(
        creator_id=period.creator_id,
        released_at__gte=period.period_start,
        released_at__lt=period.period_end,
    ).order_by("id")

    created_count = 0
    for release in releases.iterator():
        if _grant_subscription_unlock(period=period, release=release):
            created_count += 1

    return created_count


def process_pending_releases(*, limit: int = 500) -> dict:
    release_ids = list(
        PremiumMediaRelease.objects.filter(processed_at__isnull=True)
        .order_by("id")
        .values_list("id", flat=True)[: max(int(limit), 1)]
    )

    processed = 0
    unlocks_created = 0

    for release_id in release_ids:
        unlocks_created += grant_release_subscription_unlocks(
            release_id=release_id,
        )
        processed += 1

    return {
        "processed": processed,
        "unlocks_created": unlocks_created,
    }


def record_media_release(
    *,
    media: Media,
    released_at=None,
) -> tuple[PremiumMediaRelease | None, bool]:
    if media.media_type != "video" or not media.listable:
        return None, False
    if not bool(getattr(media.user, "advancedUser", False)):
        return None, False

    release, created = PremiumMediaRelease.objects.get_or_create(
        media=media,
        defaults={
            "creator_id": media.user_id,
            "released_at": released_at or timezone.now(),
        },
    )
    return release, created


def _create_subscription_payment(
    *,
    subscription: CreatorSubscription,
    plan: CreatorSubscriptionPlan,
    period_start,
) -> CreatorSubscriptionPeriod:
    if plan.creator_id != subscription.creator_id:
        raise ValidationError(
            "Subscription plan creator does not match subscription creator"
        )
    if plan.access_policy != CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES:
        raise ValidationError("This subscription access policy is not supported")

    price_tokens = int(plan.price_tokens)
    billing_period_days = int(plan.billing_period_days)

    if price_tokens <= 0:
        raise ValidationError("Subscription price is invalid")
    if billing_period_days <= 0:
        raise ValidationError("Subscription billing period is invalid")

    period_end = period_start + timedelta(days=billing_period_days)

    existing_period = CreatorSubscriptionPeriod.objects.filter(
        subscription=subscription,
        period_start=period_start,
    ).first()
    if existing_period is not None:
        return existing_period

    user = subscription.user
    creator = subscription.creator

    buyer_wallet = TokenWallet.objects.select_for_update().get(
        pk=get_user_wallet(user).pk
    )
    creator_wallet = TokenWallet.objects.select_for_update().get(
        pk=get_user_wallet(creator).pk
    )
    platform_wallet = TokenWallet.objects.select_for_update().get(
        pk=get_system_wallet(
            TokenWallet.SYSTEM_PLATFORM_FEES,
            allow_negative=False,
        ).pk
    )

    _require_wallet_not_blocked(buyer_wallet)

    if get_wallet_available_balance(buyer_wallet) < price_tokens:
        raise ValidationError("Insufficient token balance")

    enforce_wallet_velocity_limits(
        wallet=buyer_wallet,
        action=LEDGER_ACTION_PURCHASE,
        amount=price_tokens,
    )

    creator_share_bps = get_subscription_creator_share_bps()
    creator_amount = (price_tokens * creator_share_bps) // 10000
    platform_amount = price_tokens - creator_amount

    external_id = (
        f"purchase:creator_subscription:{subscription.pk}:"
        f"period:{period_start.isoformat()}"
    )
    request_hash = hashlib.sha256(
        json.dumps(
            {
                "external_id": external_id,
                "subscription_id": subscription.pk,
                "plan_id": plan.pk,
                "subscriber_user_id": user.pk,
                "creator_user_id": creator.pk,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
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
        memo=f"Creator subscription #{subscription.pk}",
        metadata={
            "product": "creator_subscription",
            "subscription_id": subscription.pk,
            "plan_id": plan.pk,
            "subscriber_user_id": user.pk,
            "creator_user_id": creator.pk,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
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

    period = CreatorSubscriptionPeriod.objects.create(
        subscription=subscription,
        plan=plan,
        creator=creator,
        txn=txn,
        period_start=period_start,
        period_end=period_end,
        price_tokens=price_tokens,
    )

    subscription.plan = plan
    subscription.status = CreatorSubscription.STATUS_ACTIVE
    subscription.current_period_start = period_start
    subscription.current_period_end = period_end
    subscription.last_txn = txn
    subscription.past_due_since = None
    subscription.renewal_attempted_at = timezone.now()
    subscription.cancel_at_period_end = False
    subscription.save(
        update_fields=[
            "plan",
            "status",
            "current_period_start",
            "current_period_end",
            "last_txn",
            "past_due_since",
            "renewal_attempted_at",
            "cancel_at_period_end",
        ]
    )

    _create_outbox_event(
        txn=txn,
        topic="ledger.purchase",
        payload={
            "product": "creator_subscription",
            "subscription_id": subscription.pk,
            "plan_id": plan.pk,
            "subscriber_user_id": user.pk,
            "creator_user_id": creator.pk,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "price_tokens": price_tokens,
        },
        metadata_version=LEDGER_METADATA_VERSION,
    )

    grant_period_release_unlocks(period=period)
    return period


@transaction.atomic
def subscribe_to_creator_with_tokens(
    *,
    actor,
    plan: CreatorSubscriptionPlan,
) -> dict:
    if not getattr(actor, "is_authenticated", False):
        raise ValidationError("Authentication required")

    user_model = actor.__class__
    user = user_model.objects.select_for_update().get(pk=actor.pk)
    plan = (
        CreatorSubscriptionPlan.objects.select_for_update()
        .select_related("creator")
        .get(pk=plan.pk)
    )

    if not bool(getattr(plan.creator, "advancedUser", False)):
        raise ValidationError("Creator subscriptions are not available")
    if not plan.is_active:
        raise ValidationError("Subscription plan is not active")
    if plan.access_policy != CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES:
        raise ValidationError("This subscription access policy is not supported")
    if plan.creator_id == user.id:
        raise ValidationError("You cannot subscribe to yourself")

    subscription = (
        CreatorSubscription.objects.select_for_update()
        .select_related("plan", "creator")
        .filter(user=user, creator=plan.creator)
        .first()
    )

    now = timezone.now()
    if (
        subscription is not None
        and subscription.status == CreatorSubscription.STATUS_ACTIVE
        and subscription.current_period_end > now
    ):
        if subscription.plan_id != plan.id:
            raise ValidationError(
                "You already have an active subscription to this creator"
            )

        if subscription.cancel_at_period_end:
            subscription.cancel_at_period_end = False
            subscription.save(update_fields=["cancel_at_period_end"])

        return {
            "charged": False,
            "already_active": True,
            "subscription": subscription,
            "period": None,
        }

    if subscription is None:
        subscription = CreatorSubscription.objects.create(
            user=user,
            creator=plan.creator,
            plan=plan,
            status=CreatorSubscription.STATUS_EXPIRED,
            current_period_start=now,
            current_period_end=now,
        )
    else:
        subscription.plan = plan
        subscription.cancel_at_period_end = False
        subscription.save(
            update_fields=[
                "plan",
                "cancel_at_period_end",
            ]
        )

    period_start = now
    if (
        subscription.status == CreatorSubscription.STATUS_ACTIVE
        and subscription.current_period_end <= now
        and now < subscription.current_period_end + timedelta(
            days=get_subscription_renewal_grace_days()
        )
    ):
        period_start = subscription.current_period_end

    period = _create_subscription_payment(
        subscription=subscription,
        plan=plan,
        period_start=period_start,
    )

    return {
        "charged": True,
        "already_active": False,
        "subscription": subscription,
        "period": period,
    }


@transaction.atomic
def renew_creator_subscription_with_tokens(*, subscription_id: int) -> dict:
    subscription = (
        CreatorSubscription.objects.select_for_update()
        .select_related("user", "creator", "plan")
        .get(pk=subscription_id)
    )
    now = timezone.now()

    if subscription.current_period_end > now:
        return {
            "renewed": False,
            "reason": "not_due",
            "subscription": subscription,
        }

    if subscription.cancel_at_period_end:
        subscription.status = CreatorSubscription.STATUS_EXPIRED
        subscription.renewal_attempted_at = now
        subscription.save(
            update_fields=["status", "renewal_attempted_at"]
        )
        return {
            "renewed": False,
            "reason": "canceled",
            "subscription": subscription,
        }

    if not bool(getattr(subscription.creator, "advancedUser", False)):
        subscription.status = CreatorSubscription.STATUS_EXPIRED
        subscription.renewal_attempted_at = now
        subscription.save(
            update_fields=["status", "renewal_attempted_at"]
        )
        return {
            "renewed": False,
            "reason": "creator_not_eligible",
            "subscription": subscription,
        }

    if subscription.plan.creator_id != subscription.creator_id:
        subscription.status = CreatorSubscription.STATUS_EXPIRED
        subscription.renewal_attempted_at = now
        subscription.save(
            update_fields=["status", "renewal_attempted_at"]
        )
        return {
            "renewed": False,
            "reason": "plan_creator_mismatch",
            "subscription": subscription,
        }

    if (
        subscription.plan.access_policy
        != CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES
    ):
        subscription.status = CreatorSubscription.STATUS_EXPIRED
        subscription.renewal_attempted_at = now
        subscription.save(
            update_fields=["status", "renewal_attempted_at"]
        )
        return {
            "renewed": False,
            "reason": "unsupported_policy",
            "subscription": subscription,
        }

    if not subscription.plan.is_active:
        subscription.status = CreatorSubscription.STATUS_EXPIRED
        subscription.renewal_attempted_at = now
        subscription.save(
            update_fields=["status", "renewal_attempted_at"]
        )
        return {
            "renewed": False,
            "reason": "plan_inactive",
            "subscription": subscription,
        }

    grace_deadline = subscription.current_period_end + timedelta(
        days=get_subscription_renewal_grace_days()
    )

    if (
        subscription.status == CreatorSubscription.STATUS_PAST_DUE
        and now >= grace_deadline
    ):
        subscription.status = CreatorSubscription.STATUS_EXPIRED
        subscription.renewal_attempted_at = now
        subscription.save(
            update_fields=["status", "renewal_attempted_at"]
        )
        return {
            "renewed": False,
            "reason": "grace_expired",
            "subscription": subscription,
        }

    period_start = (
        subscription.current_period_end
        if subscription.status == CreatorSubscription.STATUS_ACTIVE
        else now
    )

    try:
        period = _create_subscription_payment(
            subscription=subscription,
            plan=subscription.plan,
            period_start=period_start,
        )
    except ValidationError as exc:
        subscription.status = CreatorSubscription.STATUS_PAST_DUE
        subscription.renewal_attempted_at = now
        if subscription.past_due_since is None:
            subscription.past_due_since = now

        if now >= grace_deadline:
            subscription.status = CreatorSubscription.STATUS_EXPIRED

        subscription.save(
            update_fields=[
                "status",
                "past_due_since",
                "renewal_attempted_at",
            ]
        )
        return {
            "renewed": False,
            "reason": "payment_failed",
            "error": (
                exc.messages[0]
                if getattr(exc, "messages", None)
                else str(exc)
            ),
            "subscription": subscription,
        }

    return {
        "renewed": True,
        "reason": "renewed",
        "subscription": subscription,
        "period": period,
    }


@transaction.atomic
def cancel_creator_subscription(
    *,
    actor,
    subscription_id: int,
) -> CreatorSubscription:
    subscription = CreatorSubscription.objects.select_for_update().get(
        pk=subscription_id,
        user=actor,
    )
    now = timezone.now()

    if subscription.current_period_end <= now:
        subscription.status = CreatorSubscription.STATUS_EXPIRED

    subscription.cancel_at_period_end = True
    subscription.save(
        update_fields=["status", "cancel_at_period_end"]
    )
    return subscription


@transaction.atomic
def resume_creator_subscription(
    *,
    actor,
    subscription_id: int,
) -> CreatorSubscription:
    subscription = CreatorSubscription.objects.select_for_update().get(
        pk=subscription_id,
        user=actor,
    )
    now = timezone.now()

    if subscription.current_period_end <= now:
        raise ValidationError("This subscription period has already ended")
    if subscription.status != CreatorSubscription.STATUS_ACTIVE:
        raise ValidationError("This subscription cannot be resumed")

    subscription.cancel_at_period_end = False
    subscription.save(update_fields=["cancel_at_period_end"])
    return subscription


def serialize_subscription(subscription: CreatorSubscription) -> dict:
    now = timezone.now()
    is_active = (
        subscription.status == CreatorSubscription.STATUS_ACTIVE
        and subscription.current_period_end > now
    )

    return {
        "id": subscription.id,
        "creator": subscription.creator.username,
        "plan_id": subscription.plan_id,
        "plan_name": subscription.plan.name,
        "price_tokens": int(subscription.plan.price_tokens),
        "price_display": format_token_amount(subscription.plan.price_tokens),
        "billing_period_days": int(subscription.plan.billing_period_days),
        "status": subscription.status,
        "active": is_active,
        "current_period_start": subscription.current_period_start.isoformat(),
        "current_period_end": subscription.current_period_end.isoformat(),
        "cancel_at_period_end": subscription.cancel_at_period_end,
        "cancel_url": reverse(
            "premium_subscription_cancel",
            kwargs={"subscription_id": subscription.id},
        ),
        "resume_url": reverse(
            "premium_subscription_resume",
            kwargs={"subscription_id": subscription.id},
        ),
    }


def build_creator_subscription_offer(*, creator, viewer) -> dict:
    if not bool(getattr(creator, "advancedUser", False)):
        return {
            "plans": [],
            "subscription": None,
        }

    plans = list(
        CreatorSubscriptionPlan.objects.filter(
            creator=creator,
            code=DEFAULT_CREATOR_SUBSCRIPTION_PLAN_CODE,
            is_active=True,
            access_policy=CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES,
        ).order_by("price_tokens", "id")
    )

    subscription = None
    if getattr(viewer, "is_authenticated", False):
        subscription = (
            CreatorSubscription.objects.filter(
                user=viewer,
                creator=creator,
            )
            .select_related("creator", "plan")
            .first()
        )

    current_subscription_payload = (
        serialize_subscription(subscription)
        if subscription is not None
        else None
    )
    has_active_subscription = bool(
        current_subscription_payload
        and current_subscription_payload["active"]
    )

    plan_payloads = []
    for plan in plans:
        plan_payloads.append(
            {
                "id": plan.id,
                "code": plan.code,
                "name": plan.name,
                "price_tokens": int(plan.price_tokens),
                "price_display": format_token_amount(plan.price_tokens),
                "billing_period_days": int(plan.billing_period_days),
                "access_policy": plan.access_policy,
                "subscribe_url": reverse(
                    "premium_subscription_subscribe",
                    kwargs={"plan_id": plan.id},
                ),
                "can_subscribe": bool(
                    getattr(viewer, "is_authenticated", False)
                    and viewer.id != creator.id
                    and (
                        not has_active_subscription
                        or subscription.plan_id == plan.id
                    )
                ),
            }
        )

    return {
        "plans": plan_payloads,
        "subscription": current_subscription_payload,
    }


def build_user_subscription_payloads(*, user) -> list[dict]:
    subscriptions = (
        CreatorSubscription.objects.filter(user=user)
        .select_related("creator", "plan")
        .order_by("-created_at")
    )
    return [serialize_subscription(item) for item in subscriptions]


def get_due_subscription_ids(*, limit: int = 500) -> list[int]:
    now = timezone.now()
    retry_before = now - timedelta(minutes=get_subscription_retry_minutes())

    return list(
        CreatorSubscription.objects.filter(
            status__in=[
                CreatorSubscription.STATUS_ACTIVE,
                CreatorSubscription.STATUS_PAST_DUE,
            ],
            current_period_end__lte=now,
        )
        .filter(
            Q(renewal_attempted_at__isnull=True)
            | Q(renewal_attempted_at__lte=retry_before)
        )
        .order_by("current_period_end", "id")
        .values_list("id", flat=True)[: max(int(limit), 1)]
    )
