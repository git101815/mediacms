from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.shortcuts import redirect
from django.views.decorators.http import require_POST

from .daily_rewards import claim_daily_reward


@login_required
@require_POST
def wallet_claim_daily_reward(request):
    try:
        result = claim_daily_reward(user=request.user)
    except (PermissionDenied, ValidationError) as exc:
        messages.error(request, exc.messages[0] if hasattr(exc, "messages") else str(exc))
        return redirect("wallet")

    if result["claimed"]:
        messages.success(request, "Daily reward claimed.")
    else:
        messages.info(request, "Today's daily reward was already claimed.")
    return redirect("wallet")
