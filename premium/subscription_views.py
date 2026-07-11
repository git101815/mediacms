from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from .models import CreatorSubscription, CreatorSubscriptionPlan
from .subscriptions import (
    build_creator_subscription_offer,
    build_user_subscription_payloads,
    cancel_creator_subscription,
    resume_creator_subscription,
    serialize_subscription,
    subscribe_to_creator_with_tokens,
)


def _validation_error_response(exc: ValidationError) -> JsonResponse:
    message = (
        exc.messages[0]
        if getattr(exc, "messages", None)
        else str(exc)
    )
    return JsonResponse(
        {
            "ok": False,
            "error": message,
        },
        status=400,
    )


@require_GET
@never_cache
def creator_subscription_plans(request, username):
    creator = get_object_or_404(
        get_user_model(),
        username=username,
    )
    offer = build_creator_subscription_offer(
        creator=creator,
        viewer=request.user,
    )

    return JsonResponse(
        {
            "ok": True,
            "creator": creator.username,
            **offer,
        }
    )


@login_required
@require_POST
@never_cache
def subscribe_creator_plan(request, plan_id):
    plan = get_object_or_404(
        CreatorSubscriptionPlan.objects.select_related("creator"),
        pk=plan_id,
        is_active=True,
    )

    try:
        result = subscribe_to_creator_with_tokens(
            actor=request.user,
            plan=plan,
        )
    except ValidationError as exc:
        return _validation_error_response(exc)

    subscription = CreatorSubscription.objects.select_related(
        "creator",
        "plan",
    ).get(pk=result["subscription"].pk)

    return JsonResponse(
        {
            "ok": True,
            "charged": result["charged"],
            "already_active": result["already_active"],
            "subscription": serialize_subscription(subscription),
        }
    )


@login_required
@require_GET
@never_cache
def my_creator_subscriptions(request):
    return JsonResponse(
        {
            "ok": True,
            "subscriptions": build_user_subscription_payloads(
                user=request.user,
            ),
        }
    )


@login_required
@require_POST
@never_cache
def cancel_subscription(request, subscription_id):
    try:
        subscription = cancel_creator_subscription(
            actor=request.user,
            subscription_id=subscription_id,
        )
    except CreatorSubscription.DoesNotExist:
        return JsonResponse(
            {
                "ok": False,
                "error": "Subscription was not found",
            },
            status=404,
        )

    subscription = CreatorSubscription.objects.select_related(
        "creator",
        "plan",
    ).get(pk=subscription.pk)

    return JsonResponse(
        {
            "ok": True,
            "subscription": serialize_subscription(subscription),
        }
    )


@login_required
@require_POST
@never_cache
def resume_subscription(request, subscription_id):
    try:
        subscription = resume_creator_subscription(
            actor=request.user,
            subscription_id=subscription_id,
        )
    except CreatorSubscription.DoesNotExist:
        return JsonResponse(
            {
                "ok": False,
                "error": "Subscription was not found",
            },
            status=404,
        )
    except ValidationError as exc:
        return _validation_error_response(exc)

    subscription = CreatorSubscription.objects.select_related(
        "creator",
        "plan",
    ).get(pk=subscription.pk)

    return JsonResponse(
        {
            "ok": True,
            "subscription": serialize_subscription(subscription),
        }
    )
