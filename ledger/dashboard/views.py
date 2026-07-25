from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.shortcuts import redirect
from django.views.decorators.http import require_POST

from .bonus_vault import open_bonus_vault
from .daily_rewards import claim_daily_reward
from .quests import claim_quest_reward


@login_required
@require_POST
def wallet_claim_quest(request, quest_key):
    try:
        result = claim_quest_reward(
            user=request.user,
            quest_key=quest_key,
        )
    except (PermissionDenied, ValidationError) as exc:
        messages.error(
            request,
            exc.messages[0] if hasattr(exc, "messages") else str(exc),
        )
        return redirect("wallet")

    if result["claimed"]:
        messages.success(
            request,
            (
                f"Quest completed: {result['quest_title']} "
                f"(+{result['amount_tokens']:,} tokens)."
            ),
        )
    else:
        messages.info(request, "This quest reward was already claimed.")
    return redirect("wallet")


@login_required
@require_POST
def wallet_open_bonus_vault(request):
    try:
        result = open_bonus_vault(user=request.user)
    except (PermissionDenied, ValidationError) as exc:
        messages.error(
            request,
            exc.messages[0] if hasattr(exc, "messages") else str(exc),
        )
        return redirect("wallet")

    if result["opened"]:
        messages.success(
            request,
            (
                f"Bonus Vault opened: {result['amount_tokens']:,} tokens "
                f"({result['rarity']})."
            ),
        )
    else:
        messages.info(request, "This Bonus Vault was already opened.")
    return redirect("wallet")


@login_required
@require_POST
def wallet_claim_daily_reward(request):
    try:
        result = claim_daily_reward(user=request.user)
    except (PermissionDenied, ValidationError) as exc:
        messages.error(
            request,
            exc.messages[0] if hasattr(exc, "messages") else str(exc),
        )
        return redirect("wallet")

    if result["claimed"] and result["reward_kind"] == "chest":
        messages.success(
            request,
            f"Reward Chest opened: {result['amount_tokens']:,} tokens.",
        )
    elif result["claimed"]:
        messages.success(request, "Daily reward claimed.")
    else:
        messages.info(request, "Today's daily reward was already claimed.")
    return redirect("wallet")
