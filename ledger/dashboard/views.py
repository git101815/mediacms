import json

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.http import JsonResponse
from django.templatetags.static import static
from django.shortcuts import redirect
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from . import config
from .bonus_vault import open_bonus_vault
from .daily_rewards import claim_daily_reward
from .quests import claim_quest_reward
from .weekly_quests import (
    ATTRIBUTION_COOKIE_NAME,
    build_share_redirect_response,
    build_weekly_quest_status,
    create_site_share_link,
    create_video_share_link,
    open_weekly_quest_reward,
    record_navigation,
    set_visitor_cookie,
)


# interactive-chest-opening-v1

def _wants_wallet_json(request) -> bool:
    return (
        request.headers.get("x-requested-with") == "XMLHttpRequest"
        or "application/json" in request.headers.get("accept", "")
    )


def _wallet_error_text(exc) -> str:
    if hasattr(exc, "messages") and exc.messages:
        return str(exc.messages[0])
    return str(exc)


def _build_chest_opening_payload(*, result: dict) -> dict:
    amount_tokens = int(result.get("amount_tokens") or 0)
    closed_image_path = str(result.get("closed_image_path") or "")
    opened_image_path = str(result.get("opened_image_path") or "")
    drop_key = str(result.get("drop_key") or "").strip()
    grant = (
        result.get("grant")
        or result.get("reward_chest_grant")
    )
    chest_key = str(getattr(grant, "chest_key", "") or "").strip()
    rarity = str(result.get("rarity") or "reward").strip().lower()

    if (
        not closed_image_path
        or not opened_image_path
        or not chest_key
        or not drop_key
    ):
        raise ValidationError(
            "Reward Chest result is missing its visual assets"
        )

    return {
        "chest_label": str(
            result.get("chest_label")
            or result.get("chest_name")
            or "Reward Chest"
        ),
        "amount_tokens": amount_tokens,
        "amount_display": f"{amount_tokens:,}",
        "rarity": rarity,
        "rarity_label": (
            "LEGENDARY"
            if rarity == "jackpot"
            else rarity.replace("_", " ").upper()
        ),
        "closed_image_url": static(closed_image_path),
        "opened_image_url": static(opened_image_path),
        "drop_image_url": static(
            config.get_reward_chest_drop_image_path(
                chest_key=chest_key,
                drop_key=drop_key,
            )
        ),
        "token_icon_url": static(
            config.get_wallet_asset_paths()["token_icon"]
        ),
    }


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
def wallet_open_weekly_quest(request, cycle_key, quest_key):
    wants_json = _wants_wallet_json(request)
    try:
        result = open_weekly_quest_reward(
            user=request.user,
            cycle_key=cycle_key,
            quest_key=quest_key,
        )
    except (PermissionDenied, ValidationError) as exc:
        error_text = _wallet_error_text(exc)
        if wants_json:
            return JsonResponse({"ok": False, "error": error_text}, status=400)
        messages.error(request, error_text)
        return redirect("wallet")

    if wants_json:
        return JsonResponse(
            {
                "ok": True,
                "already_opened": not bool(result["opened"]),
                "opening": _build_chest_opening_payload(result=result),
            }
        )
    messages.success(
        request,
        f"Weekly quest chest opened: {result['amount_tokens']:,} tokens.",
    )
    return redirect("wallet")


@login_required
@require_POST
def weekly_quest_site_link(request):
    try:
        payload = json.loads(request.body or b"{}")
        result = create_site_share_link(
            request=request,
            user=request.user,
            fingerprint=payload.get("fingerprint"),
        )
    except (json.JSONDecodeError, PermissionDenied, ValidationError) as exc:
        return JsonResponse({"ok": False, "error": _wallet_error_text(exc)}, status=400)
    response = JsonResponse({"ok": True, "url": result["url"]})
    return set_visitor_cookie(
        response,
        request=request,
        token=result["visitor_token"],
    )


@login_required
@require_POST
def weekly_quest_video_link(request):
    try:
        payload = json.loads(request.body or b"{}")
        result = create_video_share_link(
            request=request,
            user=request.user,
            fingerprint=payload.get("fingerprint"),
            media_token=payload.get("media"),
            platform=payload.get("platform"),
        )
    except (json.JSONDecodeError, PermissionDenied, ValidationError) as exc:
        return JsonResponse({"ok": False, "error": _wallet_error_text(exc)}, status=400)
    response = JsonResponse(
        {
            "ok": True,
            "url": result["url"],
            "tracked": bool(result.get("tracked")),
        }
    )
    if result.get("visitor_token"):
        set_visitor_cookie(
            response,
            request=request,
            token=result["visitor_token"],
        )
    return response


@csrf_exempt
@require_POST
def weekly_quest_navigation(request):
    try:
        payload = json.loads(request.body or b"{}")
        result = record_navigation(
            request=request,
            fingerprint=payload.get("fingerprint"),
            page=payload.get("page"),
        )
    except (json.JSONDecodeError, ValidationError) as exc:
        return JsonResponse({"ok": False, "error": _wallet_error_text(exc)}, status=400)
    response = JsonResponse(
        {
            "ok": True,
            "qualified": bool(result.get("qualified")),
        }
    )
    set_visitor_cookie(
        response,
        request=request,
        token=result["visitor_token"],
    )
    if result.get("clear_attribution"):
        response.delete_cookie(ATTRIBUTION_COOKIE_NAME, samesite="Lax")
    return response


def weekly_quest_redirect(request, public_id):
    return build_share_redirect_response(request=request, public_id=public_id)


@login_required
def weekly_quest_status(request):
    return JsonResponse(build_weekly_quest_status(user=request.user))


@login_required
@require_POST
def wallet_open_bonus_vault(request):
    wants_json = _wants_wallet_json(request)

    try:
        result = open_bonus_vault(user=request.user)
    except (PermissionDenied, ValidationError) as exc:
        error_text = _wallet_error_text(exc)
        if wants_json:
            return JsonResponse(
                {"ok": False, "error": error_text},
                status=400,
            )
        messages.error(request, error_text)
        return redirect("wallet")

    if wants_json:
        return JsonResponse(
            {
                "ok": True,
                "already_opened": not bool(result["opened"]),
                "opening": _build_chest_opening_payload(
                    result=result,
                ),
            }
        )

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
    wants_json = _wants_wallet_json(request)

    try:
        result = claim_daily_reward(user=request.user)
    except (PermissionDenied, ValidationError) as exc:
        error_text = _wallet_error_text(exc)
        if wants_json:
            return JsonResponse(
                {"ok": False, "error": error_text},
                status=400,
            )
        messages.error(request, error_text)
        return redirect("wallet")

    if wants_json:
        if result["reward_kind"] == "chest":
            return JsonResponse(
                {
                    "ok": True,
                    "already_claimed": bool(
                        result["already_claimed"]
                    ),
                    "opening": _build_chest_opening_payload(
                        result=result,
                    ),
                }
            )
        return JsonResponse(
            {
                "ok": True,
                "kind": "fixed",
                "reload": True,
            }
        )

    if result["claimed"] and result["reward_kind"] == "chest":
        messages.success(
            request,
            f"Reward Chest opened: {result['amount_tokens']:,} tokens.",
        )
    elif result["claimed"]:
        messages.success(request, "Daily reward claimed.")
    else:
        messages.info(
            request,
            "Today's daily reward was already claimed.",
        )
    return redirect("wallet")
