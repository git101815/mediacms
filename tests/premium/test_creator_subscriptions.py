from datetime import timedelta
from unittest.mock import patch

import pytest
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from django.urls import reverse
from django.utils import timezone

from files.models import Media
from ledger.models import LedgerEntry, LedgerTransaction, TokenWallet
from premium.models import (
    CreatorSubscription,
    CreatorSubscriptionPeriod,
    CreatorSubscriptionPlan,
    PremiumMediaAsset,
    PremiumMediaRelease,
    PremiumMediaUnlock,
)
from premium.services import user_can_access_premium_media
from premium.subscription_tasks import renew_due_creator_subscriptions
from premium.subscriptions import (
    build_creator_subscription_offer,
    cancel_creator_subscription,
    get_due_subscription_ids,
    grant_period_release_unlocks,
    grant_release_subscription_unlocks,
    record_media_release,
    renew_creator_subscription_with_tokens,
    resume_creator_subscription,
    subscribe_to_creator_with_tokens,
)
from users.forms import UserForm


TOKEN_SCALE = 10**6
DEFAULT_PRICE = 10 * TOKEN_SCALE


def create_user(django_user_model, username, *, creator=False, **kwargs):
    return django_user_model.objects.create_user(
        username=username,
        password="pass12345",
        name=kwargs.pop("name", username),
        advancedUser=creator,
        **kwargs,
    )


def fund_wallet(user, amount):
    wallet, _created = TokenWallet.objects.get_or_create(
        wallet_type=TokenWallet.TYPE_USER,
        user=user,
        defaults={"allow_negative": False},
    )
    wallet.balance = int(amount)
    wallet.held_balance = 0
    wallet.save(update_fields=["balance", "held_balance", "updated_at"])
    return wallet


def create_plan(
    creator,
    *,
    price_tokens=DEFAULT_PRICE,
    active=True,
    code="default",
):
    return CreatorSubscriptionPlan.objects.create(
        creator=creator,
        code=code,
        name="Membership",
        price_tokens=price_tokens,
        billing_period_days=30,
        access_policy=CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES,
        is_active=active,
    )


def create_media(
    user,
    token,
    *,
    listable=True,
    state="public",
    media_type="video",
):
    Media.objects.bulk_create(
        [
            Media(
                user=user,
                friendly_token=token,
                title=token,
                media_file=f"tests/subscriptions/{token}.mp4",
                media_type=media_type,
                state=state,
                encoding_status="success",
                is_reviewed=True,
                listable=listable,
                add_date=timezone.now(),
            )
        ]
    )
    return Media.objects.get(friendly_token=token)


def create_ready_asset(media, *, price_tokens=500 * TOKEN_SCALE):
    return PremiumMediaAsset.objects.create(
        media=media,
        status=PremiumMediaAsset.STATUS_READY,
        price_tokens=price_tokens,
        storage_backend=PremiumMediaAsset.STORAGE_DIRECT_URL,
        playback_format=PremiumMediaAsset.PLAYBACK_MP4,
        direct_url="https://example.com/full.mp4",
        file_name="full.mp4",
        content_type="video/mp4",
        codec="h264",
        video_height=1080,
        size_bytes=12345,
        premium_published_at=timezone.now(),
    )


def subscribe_at(*, actor, plan, when):
    with patch("premium.subscriptions.timezone.now", return_value=when):
        return subscribe_to_creator_with_tokens(actor=actor, plan=plan)


def renew_at(*, subscription, when):
    with patch("premium.subscriptions.timezone.now", return_value=when):
        return renew_creator_subscription_with_tokens(
            subscription_id=subscription.id,
        )


def form_payload(user, **overrides):
    payload = {
        "name": user.name,
        "description": user.description,
        "global_media_description": user.global_media_description,
        "notification_on_comments": "on",
    }
    payload.update(overrides)
    return payload


@pytest.mark.django_db
def test_initial_subscription_payment_creates_balanced_ledger_and_paid_period(
    django_user_model,
    settings,
):
    settings.PREMIUM_SUBSCRIPTION_CREATOR_SHARE_BPS = 8000

    creator = create_user(django_user_model, "sub_creator_payment", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_payment")
    plan = create_plan(creator, price_tokens=10 * TOKEN_SCALE)

    buyer_wallet = fund_wallet(subscriber, 100 * TOKEN_SCALE)
    creator_wallet = fund_wallet(creator, 0)
    period_start = timezone.now().replace(microsecond=0)

    result = subscribe_at(actor=subscriber, plan=plan, when=period_start)

    buyer_wallet.refresh_from_db()
    creator_wallet.refresh_from_db()
    subscription = CreatorSubscription.objects.get(
        user=subscriber,
        creator=creator,
    )
    period = CreatorSubscriptionPeriod.objects.get(subscription=subscription)
    platform_wallet = TokenWallet.objects.get(
        wallet_type=TokenWallet.TYPE_SYSTEM,
        system_key=TokenWallet.SYSTEM_PLATFORM_FEES,
    )

    assert result["charged"] is True
    assert result["already_active"] is False
    assert subscription.status == CreatorSubscription.STATUS_ACTIVE
    assert subscription.current_period_start == period_start
    assert subscription.current_period_end == period_start + timedelta(days=30)
    assert period.price_tokens == 10 * TOKEN_SCALE
    assert period.plan_id == plan.id
    assert period.creator_id == creator.id

    assert buyer_wallet.balance == 90 * TOKEN_SCALE
    assert creator_wallet.balance == 8 * TOKEN_SCALE
    assert platform_wallet.balance == 2 * TOKEN_SCALE

    txn = period.txn
    assert txn.kind == "purchase"
    assert txn.metadata["product"] == "creator_subscription"
    assert txn.metadata["subscription_id"] == subscription.id
    assert txn.metadata["price_tokens"] == 10 * TOKEN_SCALE

    entries = list(LedgerEntry.objects.filter(txn=txn))
    assert len(entries) == 3
    assert sum(entry.delta for entry in entries) == 0


@pytest.mark.django_db
def test_subscription_payment_is_atomic_when_balance_is_insufficient(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_insufficient", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_insufficient")
    plan = create_plan(creator, price_tokens=20 * TOKEN_SCALE)
    fund_wallet(subscriber, 5 * TOKEN_SCALE)

    initial_transactions = LedgerTransaction.objects.count()

    with pytest.raises(ValidationError, match="Insufficient token balance"):
        subscribe_at(
            actor=subscriber,
            plan=plan,
            when=timezone.now().replace(microsecond=0),
        )

    assert CreatorSubscription.objects.filter(
        user=subscriber,
        creator=creator,
    ).exists() is False
    assert CreatorSubscriptionPeriod.objects.exists() is False
    assert LedgerTransaction.objects.count() == initial_transactions


@pytest.mark.django_db
def test_repeated_subscribe_does_not_charge_and_resumes_renewal(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_repeat", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_repeat")
    plan = create_plan(creator)
    buyer_wallet = fund_wallet(subscriber, 100 * TOKEN_SCALE)

    started_at = timezone.now().replace(microsecond=0)
    subscribe_at(actor=subscriber, plan=plan, when=started_at)

    subscription = CreatorSubscription.objects.get(user=subscriber, creator=creator)
    subscription.cancel_at_period_end = True
    subscription.save(update_fields=["cancel_at_period_end"])

    buyer_wallet.refresh_from_db()
    balance_after_first_charge = buyer_wallet.balance
    transaction_count = LedgerTransaction.objects.count()

    result = subscribe_at(
        actor=subscriber,
        plan=plan,
        when=started_at + timedelta(days=1),
    )

    subscription.refresh_from_db()
    buyer_wallet.refresh_from_db()

    assert result["charged"] is False
    assert result["already_active"] is True
    assert subscription.cancel_at_period_end is False
    assert buyer_wallet.balance == balance_after_first_charge
    assert CreatorSubscriptionPeriod.objects.filter(subscription=subscription).count() == 1
    assert LedgerTransaction.objects.count() == transaction_count


@pytest.mark.django_db
def test_subscription_rejects_self_subscription(django_user_model):
    creator = create_user(django_user_model, "sub_creator_self", creator=True)
    plan = create_plan(creator)
    fund_wallet(creator, 100 * TOKEN_SCALE)

    with pytest.raises(ValidationError, match="cannot subscribe to yourself"):
        subscribe_at(
            actor=creator,
            plan=plan,
            when=timezone.now().replace(microsecond=0),
        )


@pytest.mark.django_db
def test_subscription_rejects_non_creator_and_inactive_plan(django_user_model):
    non_creator = create_user(django_user_model, "sub_not_creator")
    subscriber = create_user(django_user_model, "sub_buyer_invalid")
    plan = create_plan(non_creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    with pytest.raises(
        ValidationError,
        match="Creator subscriptions are not available",
    ):
        subscribe_at(
            actor=subscriber,
            plan=plan,
            when=timezone.now().replace(microsecond=0),
        )

    non_creator.advancedUser = True
    non_creator.save(update_fields=["advancedUser"])
    plan.is_active = False
    plan.save(update_fields=["is_active"])

    with pytest.raises(ValidationError, match="plan is not active"):
        subscribe_at(
            actor=subscriber,
            plan=plan,
            when=timezone.now().replace(microsecond=0),
        )


@pytest.mark.django_db
def test_release_unlock_boundaries_and_access_remain_permanent(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_boundaries", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_boundaries")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    result = subscribe_at(actor=subscriber, plan=plan, when=start)
    period = result["period"]
    end = period.period_end

    before_media = create_media(creator, "sub_release_before")
    start_media = create_media(creator, "sub_release_start")
    inside_media = create_media(creator, "sub_release_inside")
    end_media = create_media(creator, "sub_release_end")

    PremiumMediaRelease.objects.create(
        media=before_media,
        creator=creator,
        released_at=start - timedelta(microseconds=1),
    )
    PremiumMediaRelease.objects.create(
        media=start_media,
        creator=creator,
        released_at=start,
    )
    PremiumMediaRelease.objects.create(
        media=inside_media,
        creator=creator,
        released_at=end - timedelta(microseconds=1),
    )
    PremiumMediaRelease.objects.create(
        media=end_media,
        creator=creator,
        released_at=end,
    )

    assert grant_period_release_unlocks(period=period) == 2

    unlocked_media_ids = set(
        PremiumMediaUnlock.objects.filter(user=subscriber).values_list(
            "media_id",
            flat=True,
        )
    )
    assert unlocked_media_ids == {start_media.id, inside_media.id}

    start_unlock = PremiumMediaUnlock.objects.get(
        user=subscriber,
        media=start_media,
    )
    assert start_unlock.source_type == PremiumMediaUnlock.SOURCE_SUBSCRIPTION
    assert start_unlock.source_subscription_id == period.subscription_id

    # The unlock may be granted before the premium asset is attached. Once the
    # asset exists, access remains valid even after the subscription expires.
    create_ready_asset(start_media)
    subscription = period.subscription
    subscription.status = CreatorSubscription.STATUS_EXPIRED
    subscription.current_period_end = end
    subscription.save(update_fields=["status", "current_period_end"])

    assert user_can_access_premium_media(
        user=subscriber,
        media=start_media,
    ) is True


@pytest.mark.django_db
def test_revoked_subscription_unlock_is_not_reactivated(django_user_model):
    creator = create_user(django_user_model, "sub_creator_revoked", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_revoked")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    period = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    media = create_media(creator, "sub_release_revoked")
    release = PremiumMediaRelease.objects.create(
        media=media,
        creator=creator,
        released_at=start + timedelta(days=1),
    )
    revoked_at = start + timedelta(days=2)
    unlock = PremiumMediaUnlock.objects.create(
        user=subscriber,
        media=media,
        source_type=PremiumMediaUnlock.SOURCE_SUBSCRIPTION,
        source_subscription=period.subscription,
        revoked_at=revoked_at,
    )

    assert grant_period_release_unlocks(period=period) == 0

    unlock.refresh_from_db()
    assert unlock.revoked_at == revoked_at
    assert PremiumMediaUnlock.objects.filter(user=subscriber, media=media).count() == 1
    assert release.processed_at is None


@pytest.mark.django_db
def test_release_processing_is_idempotent(django_user_model):
    creator = create_user(django_user_model, "sub_creator_idempotent", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_idempotent")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    subscribe_at(actor=subscriber, plan=plan, when=start)
    media = create_media(creator, "sub_release_idempotent")
    release = PremiumMediaRelease.objects.create(
        media=media,
        creator=creator,
        released_at=start + timedelta(days=1),
    )

    assert grant_release_subscription_unlocks(release_id=release.id) == 1
    assert grant_release_subscription_unlocks(release_id=release.id) == 0

    release.refresh_from_db()
    assert release.processed_at is not None
    assert PremiumMediaUnlock.objects.filter(user=subscriber, media=media).count() == 1


@pytest.mark.django_db
def test_record_media_release_only_accepts_listable_creator_videos(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_release", creator=True)
    normal_user = create_user(django_user_model, "sub_normal_release")

    eligible = create_media(creator, "sub_release_eligible")
    not_listable = create_media(
        creator,
        "sub_release_hidden",
        listable=False,
        state="private",
    )
    not_video = create_media(
        creator,
        "sub_release_image",
        media_type="image",
    )
    not_creator = create_media(normal_user, "sub_release_normal")

    release, created = record_media_release(media=eligible)
    assert release is not None
    assert created is True

    same_release, created_again = record_media_release(media=eligible)
    assert same_release.id == release.id
    assert created_again is False

    assert record_media_release(media=not_listable) == (None, False)
    assert record_media_release(media=not_video) == (None, False)
    assert record_media_release(media=not_creator) == (None, False)


@pytest.mark.django_db
def test_signal_records_only_first_public_transition_and_queues_short_task(
    django_user_model,
    django_capture_on_commit_callbacks,
):
    creator = create_user(django_user_model, "sub_creator_signal", creator=True)
    media = create_media(
        creator,
        "sub_release_signal",
        listable=False,
        state="private",
    )

    with patch(
        "premium.signals.grant_premium_release_unlocks.apply_async"
    ) as enqueue:
        with django_capture_on_commit_callbacks(execute=True):
            media.state = "public"
            media.save(update_fields=["state", "listable"])

        release = PremiumMediaRelease.objects.get(media=media)
        enqueue.assert_called_once_with(
            args=[release.id],
            queue="short_tasks",
        )

        with django_capture_on_commit_callbacks(execute=True):
            media.title = "Edited after publication"
            media.save(update_fields=["title", "listable"])

    assert PremiumMediaRelease.objects.filter(media=media).count() == 1
    assert enqueue.call_count == 1


@pytest.mark.django_db
def test_renewal_uses_latest_price_and_contiguous_period(django_user_model):
    creator = create_user(django_user_model, "sub_creator_renew", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_renew")
    plan = create_plan(creator, price_tokens=10 * TOKEN_SCALE)
    buyer_wallet = fund_wallet(subscriber, 100 * TOKEN_SCALE)
    fund_wallet(creator, 0)

    start = timezone.now().replace(microsecond=0)
    first = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = first.subscription

    plan.price_tokens = 20 * TOKEN_SCALE
    plan.save(update_fields=["price_tokens"])

    result = renew_at(subscription=subscription, when=first.period_end)
    second = result["period"]

    buyer_wallet.refresh_from_db()
    subscription.refresh_from_db()

    assert result["renewed"] is True
    assert result["reason"] == "renewed"
    assert second.period_start == first.period_end
    assert second.period_end == first.period_end + timedelta(days=30)
    assert second.price_tokens == 20 * TOKEN_SCALE
    assert subscription.current_period_start == second.period_start
    assert subscription.current_period_end == second.period_end
    assert buyer_wallet.balance == 70 * TOKEN_SCALE
    assert CreatorSubscriptionPeriod.objects.filter(subscription=subscription).count() == 2


@pytest.mark.django_db
def test_failed_renewal_becomes_past_due_and_retry_starts_at_retry_time(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_retry", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_retry")
    plan = create_plan(creator)
    buyer_wallet = fund_wallet(subscriber, DEFAULT_PRICE)

    start = timezone.now().replace(microsecond=0)
    first = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = first.subscription

    failed = renew_at(subscription=subscription, when=first.period_end)
    subscription.refresh_from_db()

    assert failed["renewed"] is False
    assert failed["reason"] == "payment_failed"
    assert failed["error"] == "Insufficient token balance"
    assert subscription.status == CreatorSubscription.STATUS_PAST_DUE
    assert subscription.past_due_since == first.period_end
    assert CreatorSubscriptionPeriod.objects.filter(subscription=subscription).count() == 1

    fund_wallet(subscriber, DEFAULT_PRICE)
    retry_at = first.period_end + timedelta(days=1)
    retried = renew_at(subscription=subscription, when=retry_at)
    second = retried["period"]

    subscription.refresh_from_db()
    buyer_wallet.refresh_from_db()

    assert retried["renewed"] is True
    assert second.period_start == retry_at
    assert second.period_start > first.period_end
    assert subscription.status == CreatorSubscription.STATUS_ACTIVE
    assert subscription.past_due_since is None
    assert buyer_wallet.balance == 0


@pytest.mark.django_db
def test_past_due_subscription_expires_after_grace_without_charging(
    django_user_model,
    settings,
):
    settings.PREMIUM_SUBSCRIPTION_RENEWAL_GRACE_DAYS = 3

    creator = create_user(django_user_model, "sub_creator_grace", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_grace")
    plan = create_plan(creator)
    fund_wallet(subscriber, DEFAULT_PRICE)

    start = timezone.now().replace(microsecond=0)
    first = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = first.subscription

    renew_at(subscription=subscription, when=first.period_end)
    buyer_wallet = fund_wallet(subscriber, 100 * TOKEN_SCALE)
    balance_before_expiration = buyer_wallet.balance

    result = renew_at(
        subscription=subscription,
        when=first.period_end + timedelta(days=4),
    )

    subscription.refresh_from_db()
    buyer_wallet.refresh_from_db()

    assert result["renewed"] is False
    assert result["reason"] == "grace_expired"
    assert subscription.status == CreatorSubscription.STATUS_EXPIRED
    assert buyer_wallet.balance == balance_before_expiration
    assert CreatorSubscriptionPeriod.objects.filter(subscription=subscription).count() == 1


@pytest.mark.django_db
def test_cancelled_subscription_expires_without_renewal(django_user_model):
    creator = create_user(django_user_model, "sub_creator_cancel", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_cancel")
    plan = create_plan(creator)
    buyer_wallet = fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    first = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = first.subscription
    cancel_creator_subscription(actor=subscriber, subscription_id=subscription.id)

    buyer_wallet.refresh_from_db()
    balance_before_due = buyer_wallet.balance
    result = renew_at(subscription=subscription, when=first.period_end)

    subscription.refresh_from_db()
    buyer_wallet.refresh_from_db()

    assert result["renewed"] is False
    assert result["reason"] == "canceled"
    assert subscription.status == CreatorSubscription.STATUS_EXPIRED
    assert buyer_wallet.balance == balance_before_due
    assert CreatorSubscriptionPeriod.objects.filter(subscription=subscription).count() == 1


@pytest.mark.django_db
def test_resume_rejects_an_ended_subscription(django_user_model):
    creator = create_user(django_user_model, "sub_creator_resume_end", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_resume_end")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    period = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = period.subscription
    subscription.cancel_at_period_end = True
    subscription.current_period_end = timezone.now() - timedelta(seconds=1)
    subscription.save(
        update_fields=["cancel_at_period_end", "current_period_end"]
    )

    with pytest.raises(ValidationError, match="already ended"):
        resume_creator_subscription(
            actor=subscriber,
            subscription_id=subscription.id,
        )


@pytest.mark.django_db
def test_due_subscription_selection_respects_retry_window(
    django_user_model,
    settings,
):
    settings.PREMIUM_SUBSCRIPTION_RETRY_MINUTES = 60

    creator = create_user(django_user_model, "sub_creator_due", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_due")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    period = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = period.subscription
    now = period.period_end + timedelta(hours=2)

    subscription.current_period_end = period.period_end
    subscription.renewal_attempted_at = now - timedelta(minutes=30)
    subscription.save(
        update_fields=["current_period_end", "renewal_attempted_at"]
    )

    with patch("premium.subscriptions.timezone.now", return_value=now):
        assert subscription.id not in get_due_subscription_ids()

    subscription.renewal_attempted_at = now - timedelta(minutes=61)
    subscription.save(update_fields=["renewal_attempted_at"])

    with patch("premium.subscriptions.timezone.now", return_value=now):
        assert subscription.id in get_due_subscription_ids()


@pytest.mark.django_db
def test_renewal_expires_when_creator_is_no_longer_eligible(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_disabled", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_disabled")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    start = timezone.now().replace(microsecond=0)
    period = subscribe_at(actor=subscriber, plan=plan, when=start)["period"]
    subscription = period.subscription

    creator.advancedUser = False
    creator.save(update_fields=["advancedUser"])

    result = renew_at(subscription=subscription, when=period.period_end)
    subscription.refresh_from_db()

    assert result["renewed"] is False
    assert result["reason"] == "creator_not_eligible"
    assert subscription.status == CreatorSubscription.STATUS_EXPIRED
    assert CreatorSubscriptionPeriod.objects.filter(subscription=subscription).count() == 1


@pytest.mark.django_db
def test_offer_only_exposes_default_plan_and_current_subscription(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_offer", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_offer")
    default_plan = create_plan(creator, price_tokens=12_500_000)
    create_plan(
        creator,
        price_tokens=50 * TOKEN_SCALE,
        code="hidden-secondary-plan",
    )

    anonymous_offer = build_creator_subscription_offer(
        creator=creator,
        viewer=AnonymousUser(),
    )
    assert len(anonymous_offer["plans"]) == 1
    assert anonymous_offer["plans"][0]["id"] == default_plan.id
    assert anonymous_offer["plans"][0]["price_display"] == "12.5"
    assert anonymous_offer["plans"][0]["can_subscribe"] is False

    fund_wallet(subscriber, 100 * TOKEN_SCALE)
    subscribe_at(
        actor=subscriber,
        plan=default_plan,
        when=timezone.now().replace(microsecond=0),
    )
    authenticated_offer = build_creator_subscription_offer(
        creator=creator,
        viewer=subscriber,
    )

    assert authenticated_offer["subscription"]["active"] is True
    assert authenticated_offer["subscription"]["plan_id"] == default_plan.id
    assert authenticated_offer["plans"][0]["can_subscribe"] is True


@pytest.mark.django_db
def test_offer_api_and_subscribe_api(django_user_model, client):
    creator = create_user(django_user_model, "sub_creator_api", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_api")
    plan = create_plan(creator, price_tokens=7 * TOKEN_SCALE)
    fund_wallet(subscriber, 50 * TOKEN_SCALE)

    offer_url = reverse(
        "premium_creator_subscription_plans",
        kwargs={"username": creator.username},
    )
    anonymous_response = client.get(offer_url)

    assert anonymous_response.status_code == 200
    assert anonymous_response.json()["ok"] is True
    assert anonymous_response.json()["plans"][0]["can_subscribe"] is False

    client.force_login(subscriber)
    subscribe_url = reverse(
        "premium_subscription_subscribe",
        kwargs={"plan_id": plan.id},
    )
    first_response = client.post(subscribe_url)
    second_response = client.post(subscribe_url)

    assert first_response.status_code == 200
    assert first_response.json()["charged"] is True
    assert first_response.json()["already_active"] is False
    assert first_response.json()["subscription"]["active"] is True

    assert second_response.status_code == 200
    assert second_response.json()["charged"] is False
    assert second_response.json()["already_active"] is True
    assert CreatorSubscriptionPeriod.objects.count() == 1


@pytest.mark.django_db
def test_subscription_write_apis_require_authentication(
    django_user_model,
    client,
):
    creator = create_user(django_user_model, "sub_creator_auth", creator=True)
    plan = create_plan(creator)

    subscribe_url = reverse(
        "premium_subscription_subscribe",
        kwargs={"plan_id": plan.id},
    )
    my_subscriptions_url = reverse("premium_my_subscriptions")

    assert client.post(subscribe_url).status_code == 302
    assert client.get(my_subscriptions_url).status_code == 302


@pytest.mark.django_db
def test_cancel_and_resume_api_enforce_ownership(django_user_model, client):
    creator = create_user(django_user_model, "sub_creator_api_manage", creator=True)
    subscriber = create_user(django_user_model, "sub_buyer_api_manage")
    other_user = create_user(django_user_model, "sub_other_api_manage")
    plan = create_plan(creator)
    fund_wallet(subscriber, 100 * TOKEN_SCALE)

    subscription = subscribe_at(
        actor=subscriber,
        plan=plan,
        when=timezone.now().replace(microsecond=0),
    )["subscription"]
    cancel_url = reverse(
        "premium_subscription_cancel",
        kwargs={"subscription_id": subscription.id},
    )
    resume_url = reverse(
        "premium_subscription_resume",
        kwargs={"subscription_id": subscription.id},
    )

    client.force_login(other_user)
    assert client.post(cancel_url).status_code == 404
    assert client.post(resume_url).status_code == 404

    client.force_login(subscriber)
    cancel_response = client.post(cancel_url)
    assert cancel_response.status_code == 200
    assert cancel_response.json()["subscription"]["cancel_at_period_end"] is True

    resume_response = client.post(resume_url)
    assert resume_response.status_code == 200
    assert resume_response.json()["subscription"]["cancel_at_period_end"] is False


@pytest.mark.django_db
def test_creator_form_exposes_settings_and_creates_default_plan(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_form", creator=True)

    form = UserForm(
        creator,
        data=form_payload(
            creator,
            dfans_url="https://dfans.example/creator",
            subscriptions_enabled="on",
            subscription_price="12.50",
        ),
        instance=creator,
    )

    assert "dfans_url" in form.fields
    assert "subscriptions_enabled" in form.fields
    assert "subscription_price" in form.fields
    assert form.is_valid(), form.errors

    # Mirrors users.views.edit_user(), which saves with commit=False first.
    saved_user = form.save(commit=False)
    saved_user.save()

    plan = CreatorSubscriptionPlan.objects.get(
        creator=creator,
        code="default",
    )
    creator.refresh_from_db()

    assert creator.dfans_url == "https://dfans.example/creator"
    assert plan.name == "Membership"
    assert plan.price_tokens == 12_500_000
    assert plan.billing_period_days == 30
    assert plan.access_policy == CreatorSubscriptionPlan.POLICY_FUTURE_RELEASES
    assert plan.is_active is True


@pytest.mark.django_db
def test_creator_form_requires_price_when_enabling(django_user_model):
    creator = create_user(django_user_model, "sub_creator_form_price", creator=True)
    form = UserForm(
        creator,
        data=form_payload(
            creator,
            subscriptions_enabled="on",
            subscription_price="",
        ),
        instance=creator,
    )

    assert form.is_valid() is False
    assert "subscription_price" in form.errors
    assert CreatorSubscriptionPlan.objects.filter(creator=creator).exists() is False


@pytest.mark.django_db
def test_creator_form_disables_plan_without_erasing_price(django_user_model):
    creator = create_user(django_user_model, "sub_creator_form_disable", creator=True)
    plan = create_plan(creator, price_tokens=25 * TOKEN_SCALE)

    form = UserForm(
        creator,
        data=form_payload(
            creator,
            subscription_price="",
        ),
        instance=creator,
    )

    assert form.is_valid(), form.errors
    form.save()

    plan.refresh_from_db()
    assert plan.is_active is False
    assert plan.price_tokens == 25 * TOKEN_SCALE


@pytest.mark.django_db
def test_creator_settings_are_hidden_and_ignored_for_normal_users(
    django_user_model,
):
    user = create_user(django_user_model, "sub_normal_form")
    user.dfans_url = "https://dfans.example/original"
    user.save(update_fields=["dfans_url"])

    form = UserForm(
        user,
        data=form_payload(
            user,
            dfans_url="https://dfans.example/injected",
            subscriptions_enabled="on",
            subscription_price="99.99",
        ),
        instance=user,
    )

    assert "dfans_url" not in form.fields
    assert "subscriptions_enabled" not in form.fields
    assert "subscription_price" not in form.fields
    assert form.is_valid(), form.errors
    form.save()

    user.refresh_from_db()
    assert user.dfans_url == "https://dfans.example/original"
    assert CreatorSubscriptionPlan.objects.filter(creator=user).exists() is False


@pytest.mark.django_db
def test_manager_editing_creator_cannot_change_creator_financial_settings(
    django_user_model,
):
    creator = create_user(django_user_model, "sub_creator_manager_edit", creator=True)
    manager = create_user(
        django_user_model,
        "sub_manager_edit",
        is_manager=True,
    )
    plan = create_plan(creator, price_tokens=30 * TOKEN_SCALE)

    form = UserForm(
        manager,
        data=form_payload(
            creator,
            dfans_url="https://dfans.example/injected",
            subscriptions_enabled="on",
            subscription_price="1.00",
            advancedUser="on",
        ),
        instance=creator,
    )

    assert "subscriptions_enabled" not in form.fields
    assert "subscription_price" not in form.fields
    assert "dfans_url" not in form.fields
    assert form.is_valid(), form.errors
    form.save()

    plan.refresh_from_db()
    assert plan.price_tokens == 30 * TOKEN_SCALE
    assert plan.is_active is True


@pytest.mark.django_db
def test_renewal_task_continues_after_one_subscription_crashes():
    with patch(
        "premium.subscription_tasks.get_due_subscription_ids",
        return_value=[11, 22, 33],
    ), patch(
        "premium.subscription_tasks.renew_creator_subscription_with_tokens",
        side_effect=[
            {"renewed": True, "reason": "renewed"},
            RuntimeError("broken row"),
            {"renewed": False, "reason": "payment_failed"},
        ],
    ):
        result = renew_due_creator_subscriptions.run(limit=3)

    assert result == {
        "checked": 3,
        "renewed": 1,
        "not_renewed": 1,
        "errors": 1,
        "reasons": {
            "renewed": 1,
            "payment_failed": 1,
        },
    }
