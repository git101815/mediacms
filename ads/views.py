from __future__ import annotations

import secrets
from decimal import Decimal
from urllib.parse import quote

from django.conf import settings
from django.contrib.auth import get_user_model, login, logout
from django.contrib.auth.forms import AuthenticationForm
from django.core import signing
from django.core.cache import cache
from django.core.signing import BadSignature, SignatureExpired
from django.db import transaction
from django.http import Http404, HttpResponse, HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from ledger.models import DepositSession, TokenWallet
from ledger.services import (
    PLATFORM_TOKEN_DECIMALS,
    PLATFORM_TOKENS_PER_STABLECOIN,
    get_wallet_available_balance,
)

from .forms import AdCampaignForm, TOKEN_SCALE
from .models import AdCampaign
from .runtime import (
    NANOS_PER_MICROTOKEN,
    get_campaign_live_metrics,
    get_effective_balance_nanos,
    nanos_to_token_decimal,
    record_click,
    serve,
    sync_campaign_runtime,
)

SSO_SALT = "ads.sso.v1"


def _is_advertiser(user):
    return bool(
        getattr(user, "is_authenticated", False)
        and (
            getattr(user, "advertiserUser", False)
            or getattr(user, "is_superuser", False)
        )
    )


def _safe_next(value):
    value = str(value or "/")
    if not value.startswith("/") or value.startswith("//"):
        return "/"
    return value


def _ads_base_url():
    scheme = str(getattr(settings, "ADS_SCHEME", "https") or "https")
    return f"{scheme}://{settings.ADS_HOST}"


def _format_tokens_from_micro(value):
    number = Decimal(int(value)) / Decimal(TOKEN_SCALE)
    text = f"{number:,.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _format_tokens_from_nanos(value):
    number = nanos_to_token_decimal(value)
    text = f"{number:,.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _format_ads_balance_usd(available_micro):
    tokens = (
        Decimal(int(available_micro))
        / (Decimal(10) ** PLATFORM_TOKEN_DECIMALS)
    )
    usd_value = tokens / Decimal(PLATFORM_TOKENS_PER_STABLECOIN)
    text = f"{usd_value:,.2f}".rstrip("0").rstrip(".")
    return f"{text or '0'}$"


def _get_user_wallet(user):
    wallet, _created = TokenWallet.objects.get_or_create(
        user=user,
        defaults={
            "wallet_type": TokenWallet.TYPE_USER,
            "allow_negative": False,
        },
    )
    return wallet


def _ads_nav_context(user):
    try:
        wallet = _get_user_wallet(user)
        available_micro = get_wallet_available_balance(wallet)
        balance = _format_tokens_from_micro(available_micro)
        balance_usd = _format_ads_balance_usd(available_micro)
    except Exception:
        balance = "Unavailable"
        balance_usd = "—"
    return {
        "balance": balance,
        "balance_usd": balance_usd,
        "finance_url": "/finance/",
        "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
    }


@never_cache
@require_http_methods(["GET", "POST"])
def ads_login(request):
    next_path = _safe_next(request.POST.get("next") or request.GET.get("next"))
    if _is_advertiser(request.user):
        return redirect(next_path)

    form = AuthenticationForm(
        request=request,
        data=request.POST if request.method == "POST" else None,
    )
    if request.method == "POST" and form.is_valid():
        user = form.get_user()
        if not _is_advertiser(user):
            form.add_error(None, "This account does not have advertiser access.")
        else:
            login(request, user)
            return redirect(next_path)

    return render(
        request,
        "ads/login.html",
        {
            "form": form,
            "next_path": next_path,
            "denied": request.GET.get("denied") == "1",
            "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
        },
    )


@require_GET
def sso_start(request):
    next_path = _safe_next(request.GET.get("next"))
    if not request.user.is_authenticated:
        login_url = reverse("account_login")
        return redirect(f"{login_url}?next={quote(request.get_full_path(), safe='')}")

    if not _is_advertiser(request.user):
        return HttpResponseForbidden(
            "This account does not have advertiser access."
        )

    nonce = secrets.token_urlsafe(24)
    max_age = int(getattr(settings, "ADS_SSO_TICKET_MAX_AGE_SECONDS", 60))
    cache_key = f"ads:sso:issued:{nonce}"
    cache.set(
        cache_key,
        {"user_id": request.user.pk, "next": next_path},
        timeout=max_age,
    )
    ticket = signing.dumps(
        {"u": request.user.pk, "n": nonce},
        salt=SSO_SALT,
        compress=True,
    )
    return redirect(
        f"{_ads_base_url()}/auth/callback/?ticket={quote(ticket, safe='')}"
    )


@require_GET
def sso_callback(request):
    ticket = request.GET.get("ticket", "")
    max_age = int(getattr(settings, "ADS_SSO_TICKET_MAX_AGE_SECONDS", 60))
    try:
        payload = signing.loads(ticket, salt=SSO_SALT, max_age=max_age)
    except (BadSignature, SignatureExpired):
        return HttpResponseForbidden("Invalid or expired advertiser sign-in.")

    nonce = str(payload.get("n") or "")
    user_id = int(payload.get("u") or 0)
    issued_key = f"ads:sso:issued:{nonce}"
    issued = cache.get(issued_key)
    if not issued or int(issued.get("user_id") or 0) != user_id:
        return HttpResponseForbidden("Advertiser sign-in ticket is no longer valid.")

    used_key = f"ads:sso:used:{nonce}"
    if not cache.add(used_key, "1", timeout=max_age):
        return HttpResponseForbidden("Advertiser sign-in ticket was already used.")
    cache.delete(issued_key)

    User = get_user_model()
    user = get_object_or_404(User, pk=user_id, is_active=True)
    if not (
        getattr(user, "advertiserUser", False)
        or getattr(user, "is_superuser", False)
    ):
        return HttpResponseForbidden(
            "This account does not have advertiser access."
        )

    login(request, user, backend="django.contrib.auth.backends.ModelBackend")
    return redirect(_safe_next(issued.get("next")))


@require_GET
def ads_logout(request):
    logout(request)
    return redirect("/login/")


def _campaigns_for_user(user):
    qs = AdCampaign.objects.all()
    if not getattr(user, "is_superuser", False):
        qs = qs.filter(advertiser=user)
    return qs


def _status_view(campaign):
    status = campaign.visible_status
    labels = {
        "pending_review": ("Pending review", "pending"),
        "rejected": ("Rejected", "rejected"),
        AdCampaign.DELIVERY_ACTIVE: ("Active", "active"),
        AdCampaign.DELIVERY_PAUSED_USER: ("Paused", "paused"),
        AdCampaign.DELIVERY_PAUSED_FUNDS: (
            "Paused · insufficient funds",
            "funds",
        ),
    }
    return labels.get(status, (status, "paused"))


@require_GET
def dashboard(request):
    campaigns = list(_campaigns_for_user(request.user).select_related("advertiser"))
    rows = []
    totals = {
        "impressions": 0,
        "clicks": 0,
        "spend_nanos": 0,
        "active": 0,
    }

    for campaign in campaigns:
        metrics = get_campaign_live_metrics(campaign)
        status_label, status_class = _status_view(campaign)
        totals["impressions"] += metrics["impressions"]
        totals["clicks"] += metrics["clicks"]
        totals["spend_nanos"] += metrics["spend_nanos"]
        if (
            campaign.review_status == AdCampaign.REVIEW_APPROVED
            and campaign.delivery_status == AdCampaign.DELIVERY_ACTIVE
        ):
            totals["active"] += 1

        rows.append(
            {
                "campaign": campaign,
                "status_label": status_label,
                "status_class": status_class,
                "impressions": metrics["impressions"],
                "clicks": metrics["clicks"],
                "ctr": f"{metrics['ctr']:.2f}",
                "spend": _format_tokens_from_nanos(metrics["spend_nanos"]),
                "bid": _format_tokens_from_micro(campaign.bid_microtokens),
            }
        )

    nav_context = _ads_nav_context(request.user)

    total_ctr = (
        Decimal(totals["clicks"]) * Decimal(100) / Decimal(totals["impressions"])
        if totals["impressions"]
        else Decimal(0)
    )
    context = {
        "rows": rows,
        **nav_context,
        "total_impressions": totals["impressions"],
        "total_clicks": totals["clicks"],
        "total_ctr": f"{total_ctr:.2f}",
        "total_spend": _format_tokens_from_nanos(totals["spend_nanos"]),
        "active_campaigns": totals["active"],
    }
    return render(request, "ads/dashboard.html", context)


@require_http_methods(["GET", "POST"])
def campaign_create(request):
    if request.method == "POST":
        form = AdCampaignForm(request.POST, request.FILES)
        if form.is_valid():
            campaign = form.save(commit=False)
            campaign.advertiser = request.user
            campaign.review_status = AdCampaign.REVIEW_PENDING
            campaign.delivery_status = AdCampaign.DELIVERY_ACTIVE
            campaign.save()
            return redirect("/")
    else:
        form = AdCampaignForm()
    return render(
        request,
        "ads/campaign_form.html",
        {
            "form": form,
            "campaign": None,
            "title": "Create campaign",
            **_ads_nav_context(request.user),
            "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
        },
    )


@require_http_methods(["GET", "POST"])
def campaign_edit(request, campaign_id):
    campaign = get_object_or_404(_campaigns_for_user(request.user), pk=campaign_id)
    previous = {
        "placement": campaign.placement,
        "target_url": campaign.target_url,
        "creative_name": campaign.creative.name,
        "pricing_model": campaign.pricing_model,
    }

    if request.method == "POST":
        form = AdCampaignForm(request.POST, request.FILES, instance=campaign)
        if form.is_valid():
            edited = form.save(commit=False)
            moderation_sensitive = (
                edited.placement != previous["placement"]
                or edited.target_url != previous["target_url"]
                or edited.pricing_model != previous["pricing_model"]
                or bool(request.FILES.get("creative"))
            )
            if moderation_sensitive:
                edited.review_status = AdCampaign.REVIEW_PENDING
                edited.review_note = ""
            edited.save()
            return redirect("/")
    else:
        form = AdCampaignForm(instance=campaign)

    return render(
        request,
        "ads/campaign_form.html",
        {
            "form": form,
            "campaign": campaign,
            "title": f"Edit · {campaign.name}",
            **_ads_nav_context(request.user),
            "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
        },
    )


@require_POST
def campaign_toggle(request, campaign_id):
    campaign = get_object_or_404(_campaigns_for_user(request.user), pk=campaign_id)

    if campaign.delivery_status == AdCampaign.DELIVERY_ACTIVE:
        campaign.delivery_status = AdCampaign.DELIVERY_PAUSED_USER
        campaign.save(update_fields=["delivery_status", "updated_at"])
    elif campaign.delivery_status == AdCampaign.DELIVERY_PAUSED_USER:
        campaign.delivery_status = AdCampaign.DELIVERY_ACTIVE
        campaign.save(update_fields=["delivery_status", "updated_at"])
    # PAUSED_FUNDS is intentionally not a manual resume state. It will resume
    # automatically when a wallet refresh sees enough funds.
    return redirect("/")


# ads-independent-subdomain-v1
def _wallet_views():
    from files import views as wallet_views
    return wallet_views


@require_GET
def creatives(request):
    rows = []
    campaigns = _campaigns_for_user(request.user).order_by("-updated_at", "-id")
    for campaign in campaigns:
        status_label, status_class = _status_view(campaign)
        rows.append({
            "campaign": campaign,
            "status_label": status_label,
            "status_class": status_class,
        })
    return render(
        request,
        "ads/creatives.html",
        {"rows": rows, **_ads_nav_context(request.user)},
    )


@never_cache
@require_GET
def finance(request):
    wallet = _get_user_wallet(request.user)
    wallet_views = _wallet_views()
    return render(
        request,
        "ads/finance.html",
        {
            **_ads_nav_context(request.user),
            "deposit_options": wallet_views._build_wallet_deposit_options(),
            "token_pack_rows": wallet_views._build_wallet_token_pack_rows(),
            "recent_deposit_sessions": wallet_views._build_recent_deposit_session_rows(wallet),
        },
    )


@require_POST
def finance_deposit_request(request):
    return _wallet_views().wallet_deposit_request(request)


@never_cache
@require_GET
def finance_deposit_session(request, public_id):
    wallet_views = _wallet_views()
    session = get_object_or_404(
        DepositSession.objects.select_related("wallet"),
        public_id=public_id,
        user=request.user,
    )
    return render(
        request,
        "ads/deposit_session.html",
        {
            **_ads_nav_context(request.user),
            "deposit_session": wallet_views._build_deposit_session_payload(session),
            "wallet_deposit_session_status_url": reverse(
                "wallet_deposit_session_status",
                kwargs={"public_id": session.public_id},
            ),
            "cancel_url": reverse(
                "wallet_deposit_session_cancel",
                kwargs={"public_id": session.public_id},
            ),
        },
    )


@never_cache
@require_GET
def finance_deposit_session_status(request, public_id):
    session = get_object_or_404(
        DepositSession.objects.only(
            "public_id", "user_id", "status", "chain", "asset_code",
            "deposit_address", "required_confirmations", "confirmations",
            "min_amount", "observed_txid", "observed_amount", "expires_at",
            "metadata",
        ),
        public_id=public_id,
        user=request.user,
    )
    return JsonResponse(_wallet_views()._build_deposit_session_payload(session))


@require_POST
def finance_deposit_session_cancel(request, public_id):
    return _wallet_views().wallet_deposit_session_cancel(request, public_id)


@never_cache
@require_GET
def finance_dfx_launch(request, public_id):
    return _wallet_views().wallet_dfx_launch(request, public_id)


@require_GET
def finance_dfx_return(request, public_id):
    return _wallet_views().wallet_dfx_return(request, public_id)


@never_cache
@require_GET
def finance_mtpelerin_launch(request, public_id):
    return _wallet_views().wallet_mtpelerin_launch(request, public_id)


@never_cache
@require_GET
def finance_banxa_launch(request, public_id):
    return _wallet_views().wallet_banxa_launch(request, public_id)


def _no_store(response):
    response["Cache-Control"] = "no-store, private, max-age=0"
    response["Pragma"] = "no-cache"
    return response


@require_GET
def serve_direct_ad(request, slot):
    if getattr(request, "is_googlebot_verified", False):
        return _no_store(HttpResponse(status=204))
    # Client-side config suppresses advanced/ad-free accounts without turning
    # this hot path into a database-backed authorization check.
    try:
        payload = serve(slot)
    except Exception:
        return _no_store(HttpResponse(status=204))
    if not payload:
        return _no_store(HttpResponse(status=204))
    return _no_store(JsonResponse(payload))


@require_GET
def ad_click(request, token):
    max_age = int(getattr(settings, "ADS_CLICK_TOKEN_MAX_AGE_SECONDS", 7 * 24 * 60 * 60))
    try:
        payload = signing.loads(token, salt="ads.click.v1", max_age=max_age)
    except (BadSignature, SignatureExpired):
        raise Http404

    target = str(payload.get("u") or "")
    if not target.startswith(("http://", "https://")):
        raise Http404

    try:
        record_click(payload)
    except Exception:
        # A click destination must never fail because accounting is unavailable.
        # Serving fails closed separately; the already-rendered ad still redirects.
        pass
    return redirect(target)
