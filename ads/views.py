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
from django.db.models import Count
from django.http import Http404, HttpResponse, HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from ledger.models import DepositSession, TokenWallet
from ledger.services import (
    PLATFORM_TOKEN_DECIMALS,
    PLATFORM_TOKENS_PER_STABLECOIN,
    STABLECOIN_CANONICAL_DECIMALS,
    get_wallet_available_balance,
)

from .forms import AdCampaignForm, AdCreativeForm
from .models import AdCampaign, AdCampaignCreative, AdCreative
from .providers import (
    FORMAT_IN_VIDEO,
    FORMAT_POPUNDER,
    PROVIDER_CLICKAINE,
    PROVIDER_INTERNAL,
    PROVIDER_PARTNER,
    clickaine_popunder_script_url,
    clickaine_vast_url,
    weighted_provider_order,
)
from .runtime import (
    NANOS_PER_MICROTOKEN,
    get_campaign_live_metrics,
    get_effective_balance_nanos,
    nanos_to_token_decimal,
    record_click,
    record_impression,
    reserve,
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


def _format_ads_usd(value, *, decimal_places=6):
    number = Decimal(value)
    text = f"{number:,.{decimal_places}f}".rstrip("0").rstrip(".")
    return f"${text or '0'}"


def _format_usd_from_microtokens(value):
    token_value = (
        Decimal(int(value))
        / (Decimal(10) ** PLATFORM_TOKEN_DECIMALS)
    )
    usd_value = token_value / Decimal(PLATFORM_TOKENS_PER_STABLECOIN)
    return _format_ads_usd(usd_value)


def _format_usd_from_nanos(value):
    token_value = nanos_to_token_decimal(value)
    usd_value = token_value / Decimal(PLATFORM_TOKENS_PER_STABLECOIN)
    return _format_ads_usd(usd_value)


def _format_ads_balance_usd(available_micro):
    token_value = (
        Decimal(int(available_micro))
        / (Decimal(10) ** PLATFORM_TOKEN_DECIMALS)
    )
    usd_value = token_value / Decimal(PLATFORM_TOKENS_PER_STABLECOIN)
    return _format_ads_usd(usd_value, decimal_places=2)


def _format_ads_pack_usd(metadata):
    token_pack = (metadata or {}).get("token_pack") or {}
    try:
        gross = int(token_pack.get("gross_stable_amount") or 0)
    except (TypeError, ValueError):
        return ""
    if gross <= 0:
        return ""
    usd_value = (
        Decimal(gross)
        / (Decimal(10) ** STABLECOIN_CANONICAL_DECIMALS)
    )
    return _format_ads_usd(usd_value, decimal_places=2)


def _build_ads_deposit_session_payload(session, wallet_views):
    payload = dict(
        wallet_views._build_deposit_session_payload(session)
    )
    payload["ads_pack_usd"] = _format_ads_pack_usd(
        session.metadata
    )
    payload.pop("token_pack_name", None)
    payload.pop("token_pack_label", None)
    return payload


def _build_ads_recent_deposit_rows(wallet, wallet_views):
    rows = [
        dict(row)
        for row in wallet_views._build_recent_deposit_session_rows(wallet)
    ]
    public_ids = [row.get("public_id") for row in rows if row.get("public_id")]
    if not public_ids:
        return rows

    sessions = DepositSession.objects.filter(
        wallet=wallet,
        public_id__in=public_ids,
    ).only("public_id", "metadata")
    session_by_public_id = {
        str(session.public_id): session
        for session in sessions
    }
    for row in rows:
        session = session_by_public_id.get(str(row.get("public_id") or ""))
        row["amount_usd"] = (
            _format_ads_pack_usd(session.metadata)
            if session is not None
            else ""
        )
    return rows


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
        balance_usd = _format_ads_balance_usd(available_micro)
    except Exception:
        balance_usd = "—"
    return {
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


def _creatives_for_user(user):
    qs = AdCreative.objects.all()
    if not getattr(user, "is_superuser", False):
        qs = qs.filter(advertiser=user)
    return qs


def _creative_status_view(creative):
    labels = {
        AdCreative.REVIEW_PENDING: ("Pending review", "pending"),
        AdCreative.REVIEW_APPROVED: ("Approved", "active"),
        AdCreative.REVIEW_REJECTED: ("Rejected", "rejected"),
    }
    return labels.get(
        creative.review_status,
        (creative.review_status, "paused"),
    )


def _campaign_creative_library_context(form):
    selected_ids = set()
    if form.is_bound:
        selected_ids = {
            str(value)
            for value in form.data.getlist("creative_ids")
        }
    else:
        for value in form.initial.get("creative_ids", []) or []:
            selected_ids.add(str(getattr(value, "pk", value)))

    rows = []
    for creative in form.fields["creative_ids"].queryset:
        status_label, status_class = _creative_status_view(creative)
        rows.append(
            {
                "creative": creative,
                "selected": str(creative.pk) in selected_ids,
                "status_label": status_label,
                "status_class": status_class,
            }
        )
    return rows


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
                "spend": _format_usd_from_nanos(metrics["spend_nanos"]),
                "bid": _format_usd_from_microtokens(campaign.bid_microtokens),
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
        "total_spend": _format_usd_from_nanos(totals["spend_nanos"]),
        "active_campaigns": totals["active"],
    }
    return render(request, "ads/dashboard.html", context)


@require_http_methods(["GET", "POST"])
def campaign_create(request):
    advertiser = request.user
    if request.method == "POST":
        form = AdCampaignForm(
            request.POST,
            advertiser=advertiser,
        )
        if form.is_valid():
            campaign = form.save(commit=False)
            campaign.advertiser = advertiser
            campaign.review_status = AdCampaign.REVIEW_PENDING
            campaign.delivery_status = AdCampaign.DELIVERY_ACTIVE
            campaign.save()
            form.save_creatives(campaign)
            return redirect("/")
    else:
        form = AdCampaignForm(advertiser=advertiser)

    return render(
        request,
        "ads/campaign_form.html",
        {
            "form": form,
            "campaign": None,
            "creative_library": _campaign_creative_library_context(form),
            "title": "Create campaign",
            **_ads_nav_context(request.user),
            "portal_name": getattr(settings, "PORTAL_NAME", "MediaCMS"),
        },
    )


@require_http_methods(["GET", "POST"])
def campaign_edit(request, campaign_id):
    campaign = get_object_or_404(
        _campaigns_for_user(request.user),
        pk=campaign_id,
    )
    advertiser = campaign.advertiser
    previous = {
        "placement": campaign.placement,
        "target_url": campaign.target_url,
        "pricing_model": campaign.pricing_model,
    }

    if request.method == "POST":
        form = AdCampaignForm(
            request.POST,
            instance=campaign,
            advertiser=advertiser,
        )
        if form.is_valid():
            edited = form.save(commit=False)
            moderation_sensitive = (
                edited.placement != previous["placement"]
                or edited.target_url != previous["target_url"]
                or edited.pricing_model != previous["pricing_model"]
            )
            if moderation_sensitive:
                edited.review_status = AdCampaign.REVIEW_PENDING
                edited.review_note = ""
            edited.save()
            form.save_creatives(edited)
            return redirect("/")
    else:
        form = AdCampaignForm(
            instance=campaign,
            advertiser=advertiser,
        )

    return render(
        request,
        "ads/campaign_form.html",
        {
            "form": form,
            "campaign": campaign,
            "creative_library": _campaign_creative_library_context(form),
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
    queryset = (
        _creatives_for_user(request.user)
        .annotate(campaign_count=Count("campaigns", distinct=True))
        .order_by("-updated_at", "-id")
    )
    rows = []
    for creative in queryset:
        status_label, status_class = _creative_status_view(creative)
        rows.append(
            {
                "creative": creative,
                "status_label": status_label,
                "status_class": status_class,
                "campaign_count": creative.campaign_count,
            }
        )
    return render(
        request,
        "ads/creatives.html",
        {"rows": rows, **_ads_nav_context(request.user)},
    )


@require_http_methods(["GET", "POST"])
def creative_create(request):
    if request.method == "POST":
        form = AdCreativeForm(request.POST, request.FILES)
        if form.is_valid():
            creative = form.save(commit=False)
            creative.advertiser = request.user
            creative.review_status = AdCreative.REVIEW_PENDING
            creative.review_note = ""
            creative.save()
            return redirect("/creatives/")
    else:
        form = AdCreativeForm()

    return render(
        request,
        "ads/creative_form.html",
        {
            "form": form,
            "creative": None,
            "title": "Add creative",
            **_ads_nav_context(request.user),
        },
    )


@require_http_methods(["GET", "POST"])
def creative_edit(request, creative_id):
    creative = get_object_or_404(
        _creatives_for_user(request.user),
        pk=creative_id,
    )
    previous_placement = creative.placement
    previous_image = creative.image.name
    previous_vast_url = creative.vast_url
    previous_destination_url = creative.destination_url

    if request.method == "POST":
        form = AdCreativeForm(
            request.POST,
            request.FILES,
            instance=creative,
        )
        if form.is_valid():
            edited = form.save(commit=False)
            moderation_sensitive = (
                edited.placement != previous_placement
                or edited.image.name != previous_image
                or edited.vast_url != previous_vast_url
                or edited.destination_url != previous_destination_url
                or bool(request.FILES.get("image"))
            )
            if moderation_sensitive:
                edited.review_status = AdCreative.REVIEW_PENDING
                edited.review_note = ""
            edited.save()

            if edited.placement != previous_placement:
                (
                    AdCampaignCreative.objects
                    .filter(creative=edited)
                    .exclude(
                        campaign__placement__in=(
                            (
                                AdCampaign.PLACEMENT_PREROLL,
                                AdCampaign.PLACEMENT_MIDROLL,
                                AdCampaign.PLACEMENT_POSTROLL,
                            )
                            if edited.placement == AdCreative.PLACEMENT_IN_VIDEO
                            else (edited.placement,)
                        )
                    )
                    .delete()
                )
            return redirect("/creatives/")
    else:
        form = AdCreativeForm(instance=creative)

    return render(
        request,
        "ads/creative_form.html",
        {
            "form": form,
            "creative": creative,
            "title": f"Edit · {creative.name}",
            **_ads_nav_context(request.user),
        },
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
            "recent_deposit_sessions": _build_ads_recent_deposit_rows(
                wallet,
                wallet_views,
            ),
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
    deposit_payload = _build_ads_deposit_session_payload(
        session,
        wallet_views,
    )
    return render(
        request,
        "ads/deposit_session.html",
        {
            **_ads_nav_context(request.user),
            "deposit_session": deposit_payload,
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
    wallet_views = _wallet_views()
    return JsonResponse(
        _build_ads_deposit_session_payload(
            session,
            wallet_views,
        )
    )


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


def _load_ad_event_token(token):
    max_age = int(
        getattr(
            settings,
            "ADS_CLICK_TOKEN_MAX_AGE_SECONDS",
            7 * 24 * 60 * 60,
        )
    )
    try:
        return signing.loads(
            token,
            salt="ads.click.v1",
            max_age=max_age,
        )
    except (BadSignature, SignatureExpired):
        raise Http404


def _cdata(value):
    return str(value or "").replace("]]>", "]]]]><![CDATA[>")


def _empty_vast():
    return _no_store(
        HttpResponse(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<VAST version="3.0"></VAST>',
            content_type="application/xml",
        )
    )


@never_cache
@require_GET
def ads_vmap(request):
    midroll_offset = str(
        getattr(settings, "ADS_MIDROLL_TIME_OFFSET", "50%")
        or "50%"
    )
    breaks = (
        (
            "start",
            "preroll",
            AdCampaign.PLACEMENT_PREROLL,
        ),
        (
            midroll_offset,
            "midroll",
            AdCampaign.PLACEMENT_MIDROLL,
        ),
        (
            "end",
            "postroll",
            AdCampaign.PLACEMENT_POSTROLL,
        ),
    )

    chunks = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<vmap:VMAP version="1.0" '
        'xmlns:vmap="http://www.iab.net/videosuite/vmap">',
    ]

    for time_offset, break_id, slot in breaks:
        tag_url = request.build_absolute_uri(
            reverse(
                "ads_vast",
                kwargs={"slot": slot},
            )
        )
        chunks.extend(
            [
                (
                    f'<vmap:AdBreak timeOffset="{time_offset}" '
                    f'breakType="linear" breakId="{break_id}">'
                ),
                (
                    f'<vmap:AdSource id="{break_id}" '
                    'allowMultipleAds="false" followRedirects="true">'
                ),
                '<vmap:AdTagURI templateType="vast3"><![CDATA['
                + _cdata(tag_url)
                + ']]></vmap:AdTagURI>',
                '</vmap:AdSource>',
                '</vmap:AdBreak>',
            ]
        )

    chunks.append("</vmap:VMAP>")
    return _no_store(
        HttpResponse(
            "".join(chunks),
            content_type="application/xml",
        )
    )


def _internal_vast_material(slot):
    try:
        candidate = reserve(slot)
    except Exception:
        return None
    if not candidate:
        return None
    vast_url = str(candidate.get("vast_url") or "").strip()
    if not vast_url.startswith(("http://", "https://")):
        return None
    return candidate


def _vast_wrapper_attributes(has_fallback):
    attributes = [
        'followAdditionalWrappers="true"',
        'allowMultipleAds="false"',
    ]
    if has_fallback:
        attributes.append('fallbackOnNoAd="true"')
    return " ".join(attributes)


def _internal_vast_ad(request, candidate, has_fallback):
    token = candidate["event_token"]
    impression_url = request.build_absolute_uri(
        reverse(
            "direct_ad_impression",
            kwargs={"token": token},
        )
    )
    click_tracking_url = request.build_absolute_uri(
        reverse(
            "direct_ad_track_click",
            kwargs={"token": token},
        )
    )
    vast_url = str(candidate["vast_url"])
    return (
        '<Ad id="internal-'
        + str(candidate["campaign_id"])
        + '-'
        + str(candidate["creative_id"])
        + '"><Wrapper '
        + _vast_wrapper_attributes(has_fallback)
        + '>'
        '<AdSystem version="1.0">MediaCMS Internal Ads</AdSystem>'
        '<VASTAdTagURI><![CDATA['
        + _cdata(vast_url)
        + ']]></VASTAdTagURI>'
        '<Impression><![CDATA['
        + _cdata(impression_url)
        + ']]></Impression>'
        '<Creatives><Creative><Linear>'
        '<VideoClicks><ClickTracking><![CDATA['
        + _cdata(click_tracking_url)
        + ']]></ClickTracking></VideoClicks>'
        '</Linear></Creative></Creatives>'
        '</Wrapper></Ad>'
    )


def _clickaine_vast_ad(request, vast_url, has_fallback):
    impression_url = request.build_absolute_uri(
        reverse("clickaine_vast_impression")
    )
    return (
        '<Ad id="clickaine"><Wrapper '
        + _vast_wrapper_attributes(has_fallback)
        + '>'
        '<AdSystem version="1.0">Clickaine</AdSystem>'
        '<VASTAdTagURI><![CDATA['
        + _cdata(vast_url)
        + ']]></VASTAdTagURI>'
        '<Impression><![CDATA['
        + _cdata(impression_url)
        + ']]></Impression>'
        '<Creatives></Creatives>'
        '</Wrapper></Ad>'
    )


@never_cache
@require_GET
def ads_vast(request, slot):
    if slot not in {
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
    }:
        raise Http404

    if getattr(request, "is_googlebot_verified", False):
        return _empty_vast()

    materials = []
    for provider in weighted_provider_order(FORMAT_IN_VIDEO):
        if provider == PROVIDER_INTERNAL:
            candidate = _internal_vast_material(slot)
            if candidate is not None:
                materials.append((provider, candidate))
        elif provider == PROVIDER_CLICKAINE:
            materials.append((provider, clickaine_vast_url()))

    if not materials:
        return _empty_vast()

    ads = []
    for index, (provider, material) in enumerate(materials):
        has_fallback = index < len(materials) - 1
        if provider == PROVIDER_INTERNAL:
            ads.append(
                _internal_vast_ad(
                    request,
                    material,
                    has_fallback,
                )
            )
        elif provider == PROVIDER_CLICKAINE:
            ads.append(
                _clickaine_vast_ad(
                    request,
                    material,
                    has_fallback,
                )
            )

    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<VAST version="3.0">'
        + "".join(ads)
        + '</VAST>'
    )
    return _no_store(
        HttpResponse(body, content_type="application/xml")
    )


@never_cache
@require_GET
def ads_popunder(request):
    if getattr(request, "is_googlebot_verified", False):
        return _no_store(HttpResponse(status=204))

    providers = []
    for provider in weighted_provider_order(FORMAT_POPUNDER):
        if provider == PROVIDER_INTERNAL:
            try:
                payload = reserve(AdCampaign.PLACEMENT_POPUNDER)
            except Exception:
                payload = None
            if not payload:
                continue
            providers.append(
                {
                    "name": PROVIDER_INTERNAL,
                    "campaign_id": payload["campaign_id"],
                    "creative_id": payload["creative_id"],
                    "open_url": reverse(
                        "direct_ad_open",
                        kwargs={"token": payload["event_token"]},
                    ),
                }
            )
        elif provider == PROVIDER_CLICKAINE:
            providers.append(
                {
                    "name": PROVIDER_CLICKAINE,
                    "script_url": clickaine_popunder_script_url(),
                }
            )
        elif provider == PROVIDER_PARTNER:
            providers.append({"name": PROVIDER_PARTNER})

    if not providers:
        return _no_store(HttpResponse(status=204))
    return _no_store(JsonResponse({"providers": providers}))


@never_cache
@require_GET
def clickaine_vast_impression(request):
    return _no_store(HttpResponse(status=204))


@require_GET
def direct_ad_impression(request, token):
    payload = _load_ad_event_token(token)
    try:
        record_impression(payload)
    except Exception:
        pass
    return _no_store(HttpResponse(status=204))


@require_GET
def direct_ad_track_click(request, token):
    payload = _load_ad_event_token(token)
    try:
        record_click(payload)
    except Exception:
        pass
    return _no_store(HttpResponse(status=204))


@require_GET
def direct_ad_open(request, token):
    payload = _load_ad_event_token(token)
    target = str(payload.get("u") or "")
    if not target.startswith(("http://", "https://")):
        raise Http404

    try:
        record_impression(payload)
    except Exception:
        pass

    try:
        record_click(payload)
    except Exception:
        pass

    return redirect(target)


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
    payload = _load_ad_event_token(token)

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
