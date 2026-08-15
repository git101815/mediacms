from urllib.parse import parse_qs, urlparse

import pytest
from django.contrib.auth.models import AnonymousUser
from django.core.cache import cache
from django.http import Http404, HttpResponse
from django.test import RequestFactory, override_settings

from ads.middleware import AdsHostMiddleware
from ads.models import AdCampaign, AdCampaignCreative, AdCreative
from ads import views


ADS_HOST_SETTINGS = override_settings(
    ADS_HOST="ads.localhost",
    ADS_SCHEME="http",
    ALLOWED_HOSTS=[
        "testserver",
        "localhost",
        "ads.localhost",
    ],
)

LOCMEM_CACHE = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "ads-tests",
    }
}


@ADS_HOST_SETTINGS
def test_ads_host_middleware_ignores_other_hosts():
    request = RequestFactory().get(
        "/finance/",
        HTTP_HOST="localhost",
    )
    request.user = AnonymousUser()
    middleware = AdsHostMiddleware(
        lambda req: HttpResponse("main")
    )
    response = middleware(request)
    assert response.status_code == 200
    assert response.content == b"main"
    assert not hasattr(request, "urlconf")


@ADS_HOST_SETTINGS
def test_ads_host_middleware_allows_public_paths():
    request = RequestFactory().get(
        "/login/",
        HTTP_HOST="ads.localhost",
    )
    request.user = AnonymousUser()
    middleware = AdsHostMiddleware(
        lambda req: HttpResponse("public")
    )
    response = middleware(request)
    assert response.status_code == 200
    assert request.urlconf == "ads.host_urls"


@ADS_HOST_SETTINGS
def test_ads_host_middleware_redirects_anonymous_with_exact_next():
    request = RequestFactory().get(
        "/finance/?tab=x",
        HTTP_HOST="ads.localhost",
    )
    request.user = AnonymousUser()
    middleware = AdsHostMiddleware(
        lambda req: HttpResponse("private")
    )
    response = middleware(request)
    assert response.status_code == 302
    assert response.url.startswith("/login/?next=")
    assert "%2Ffinance%2F%3Ftab%3Dx" in response.url
    assert request.urlconf == "ads.host_urls"


@pytest.mark.django_db
@ADS_HOST_SETTINGS
def test_ads_host_middleware_rejects_authenticated_non_advertiser(
    django_user_model,
):
    user = django_user_model.objects.create_user(
        username="ads-host-denied",
        advertiserUser=False,
    )
    request = RequestFactory().get(
        "/",
        HTTP_HOST="ads.localhost",
    )
    request.user = user
    middleware = AdsHostMiddleware(
        lambda req: HttpResponse("private")
    )
    response = middleware(request)
    assert response.status_code == 302
    assert response.url == "/login/?denied=1"


@pytest.mark.django_db
@ADS_HOST_SETTINGS
@pytest.mark.parametrize("superuser", [False, True])
def test_ads_host_middleware_allows_advertiser_or_superuser(
    django_user_model,
    superuser,
):
    user = django_user_model.objects.create_user(
        username=f"ads-host-ok-{superuser}",
        advertiserUser=not superuser,
        is_superuser=superuser,
        is_staff=superuser,
    )
    request = RequestFactory().get(
        "/",
        HTTP_HOST="ads.localhost",
    )
    request.user = user
    middleware = AdsHostMiddleware(
        lambda req: HttpResponse("private")
    )
    response = middleware(request)
    assert response.status_code == 200
    assert response.content == b"private"


@pytest.mark.django_db
@ADS_HOST_SETTINGS
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_ads_login_allows_only_advertiser_accounts(
    client,
    django_user_model,
):
    advertiser = django_user_model.objects.create_user(
        username="ads-login-ok",
        password="pass-12345",
        advertiserUser=True,
    )
    normal = django_user_model.objects.create_user(
        username="ads-login-no",
        password="pass-12345",
        advertiserUser=False,
    )

    denied = client.post(
        "/login/",
        {
            "username": normal.username,
            "password": "pass-12345",
            "next": "/finance/",
        },
    )
    assert denied.status_code == 200
    assert "_auth_user_id" not in client.session

    allowed = client.post(
        "/login/",
        {
            "username": advertiser.username,
            "password": "pass-12345",
            "next": "/finance/",
        },
    )
    assert allowed.status_code == 302
    assert allowed.url == "/finance/"
    assert int(client.session["_auth_user_id"]) == advertiser.pk


@pytest.mark.django_db
@ADS_HOST_SETTINGS
@override_settings(CACHES=LOCMEM_CACHE)
def test_sso_ticket_is_one_time_and_preserves_safe_next(
    client,
    django_user_model,
):
    cache.clear()
    user = django_user_model.objects.create_user(
        username="ads-sso-user",
        advertiserUser=True,
    )
    client.force_login(user)

    start = client.get(
        "/ads/sso/start/?next=/finance/"
    )
    assert start.status_code == 302
    parsed = urlparse(start.url)
    assert parsed.netloc == "ads.localhost"
    ticket = parse_qs(parsed.query)["ticket"][0]

    client.logout()
    with override_settings(ROOT_URLCONF="ads.host_urls"):
        callback = client.get(
            "/auth/callback/",
            {"ticket": ticket},
        )
        assert callback.status_code == 302
        assert callback.url == "/finance/"
        assert int(client.session["_auth_user_id"]) == user.pk

        replay = client.get(
            "/auth/callback/",
            {"ticket": ticket},
        )
        assert replay.status_code == 403


@pytest.mark.django_db
@ADS_HOST_SETTINGS
@override_settings(CACHES=LOCMEM_CACHE)
def test_sso_rejects_non_advertiser_and_tampered_ticket(
    client,
    django_user_model,
):
    cache.clear()
    normal = django_user_model.objects.create_user(
        username="ads-sso-normal",
        advertiserUser=False,
    )
    client.force_login(normal)
    denied = client.get("/ads/sso/start/")
    assert denied.status_code == 403

    client.logout()
    with override_settings(ROOT_URLCONF="ads.host_urls"):
        invalid = client.get(
            "/auth/callback/",
            {"ticket": "tampered"},
        )
        assert invalid.status_code == 403


@pytest.mark.django_db
def test_safe_next_rejects_external_or_protocol_relative_paths():
    assert views._safe_next("/finance/") == "/finance/"
    assert views._safe_next("https://evil.example/") == "/"
    assert views._safe_next("//evil.example/") == "/"
    assert views._safe_next("") == "/"


@pytest.mark.django_db
def test_campaign_and_creative_querysets_are_tenant_isolated(
    advertiser_factory,
    campaign_factory,
    creative_factory,
):
    first = advertiser_factory()
    second = advertiser_factory()
    admin = advertiser_factory(superuser=True)

    first_campaign = campaign_factory(advertiser=first)
    second_campaign = campaign_factory(advertiser=second)
    first_creative = creative_factory(advertiser=first)
    second_creative = creative_factory(advertiser=second)

    assert set(
        views._campaigns_for_user(first).values_list(
            "pk",
            flat=True,
        )
    ) == {first_campaign.pk}
    assert set(
        views._campaigns_for_user(admin).values_list(
            "pk",
            flat=True,
        )
    ) == {first_campaign.pk, second_campaign.pk}

    assert set(
        views._creatives_for_user(first).values_list(
            "pk",
            flat=True,
        )
    ) >= {first_creative.pk}
    assert second_creative.pk not in set(
        views._creatives_for_user(first).values_list(
            "pk",
            flat=True,
        )
    )


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_campaign_create_sets_pending_review_and_links_creative(
    client,
    advertiser_factory,
    creative_factory,
):
    user = advertiser_factory()
    creative = creative_factory(advertiser=user)
    client.force_login(user)

    response = client.post(
        "/campaigns/new/",
        {
            "name": "Created",
            "placement": AdCampaign.PLACEMENT_HOME,
            "target_url": "https://example.com/landing",
            "pricing_model": AdCampaign.PRICING_CPM,
            "bid_usd": "10",
            "creative_ids": [str(creative.pk)],
        },
    )
    assert response.status_code == 302
    campaign = AdCampaign.objects.get(name="Created")
    assert campaign.advertiser_id == user.pk
    assert campaign.review_status == AdCampaign.REVIEW_PENDING
    assert campaign.delivery_status == AdCampaign.DELIVERY_ACTIVE
    assert campaign.creatives.filter(pk=creative.pk).exists()


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_campaign_bid_change_does_not_require_moderation_but_content_change_does(
    client,
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    creative = campaign.creatives.get()
    client.force_login(user)

    bid_change = client.post(
        f"/campaigns/{campaign.pk}/edit/",
        {
            "name": campaign.name,
            "placement": campaign.placement,
            "target_url": campaign.target_url,
            "pricing_model": campaign.pricing_model,
            "bid_usd": "11",
            "creative_ids": [str(creative.pk)],
        },
    )
    assert bid_change.status_code == 302
    campaign.refresh_from_db()
    assert campaign.review_status == AdCampaign.REVIEW_APPROVED

    content_change = client.post(
        f"/campaigns/{campaign.pk}/edit/",
        {
            "name": campaign.name,
            "placement": campaign.placement,
            "target_url": "https://example.com/new-target",
            "pricing_model": campaign.pricing_model,
            "bid_usd": "11",
            "creative_ids": [str(creative.pk)],
        },
    )
    assert content_change.status_code == 302
    campaign.refresh_from_db()
    assert campaign.review_status == AdCampaign.REVIEW_PENDING
    assert campaign.review_note == ""


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_campaign_toggle_only_toggles_user_pause_state(
    client,
    advertiser_factory,
    campaign_factory,
):
    user = advertiser_factory()
    campaign = campaign_factory(advertiser=user)
    client.force_login(user)

    response = client.post(
        f"/campaigns/{campaign.pk}/toggle/"
    )
    assert response.status_code == 302
    campaign.refresh_from_db()
    assert campaign.delivery_status == AdCampaign.DELIVERY_PAUSED_USER

    client.post(f"/campaigns/{campaign.pk}/toggle/")
    campaign.refresh_from_db()
    assert campaign.delivery_status == AdCampaign.DELIVERY_ACTIVE

    campaign.delivery_status = AdCampaign.DELIVERY_PAUSED_FUNDS
    campaign.save(update_fields=["delivery_status", "updated_at"])
    client.post(f"/campaigns/{campaign.pk}/toggle/")
    campaign.refresh_from_db()
    assert campaign.delivery_status == AdCampaign.DELIVERY_PAUSED_FUNDS


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_other_advertiser_cannot_edit_campaign_or_creative(
    client,
    advertiser_factory,
    campaign_factory,
):
    owner = advertiser_factory()
    attacker = advertiser_factory()
    campaign = campaign_factory(advertiser=owner)
    creative = campaign.creatives.get()
    client.force_login(attacker)

    request = RequestFactory().get(
        f"/campaigns/{campaign.pk}/edit/"
    )
    request.user = attacker
    with pytest.raises(Http404):
        views.campaign_edit(request, campaign.pk)

    request = RequestFactory().get(
        f"/creatives/{creative.pk}/edit/"
    )
    request.user = attacker
    with pytest.raises(Http404):
        views.creative_edit(request, creative.pk)


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_creative_create_is_pending_and_source_change_returns_approved_creative_to_review(
    client,
    advertiser_factory,
    creative_factory,
):
    user = advertiser_factory()
    client.force_login(user)

    created = client.post(
        "/creatives/new/",
        {
            "name": "VAST created",
            "placement": AdCreative.PLACEMENT_IN_VIDEO,
            "vast_url": "https://ads.example/vast.xml",
        },
    )
    assert created.status_code == 302
    creative = AdCreative.objects.get(name="VAST created")
    assert creative.advertiser_id == user.pk
    assert creative.review_status == AdCreative.REVIEW_PENDING

    creative.review_status = AdCreative.REVIEW_APPROVED
    creative.review_note = "old"
    creative.save(
        update_fields=[
            "review_status",
            "review_note",
            "updated_at",
        ]
    )

    renamed = client.post(
        f"/creatives/{creative.pk}/edit/",
        {
            "name": "Renamed only",
            "placement": AdCreative.PLACEMENT_IN_VIDEO,
            "vast_url": "https://ads.example/vast.xml",
        },
    )
    assert renamed.status_code == 302
    creative.refresh_from_db()
    assert creative.review_status == AdCreative.REVIEW_APPROVED
    assert creative.review_note == "old"

    changed = client.post(
        f"/creatives/{creative.pk}/edit/",
        {
            "name": "Renamed only",
            "placement": AdCreative.PLACEMENT_IN_VIDEO,
            "vast_url": "https://ads.example/other-vast.xml",
        },
    )
    assert changed.status_code == 302
    creative.refresh_from_db()
    assert creative.review_status == AdCreative.REVIEW_PENDING
    assert creative.review_note == ""


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_creative_format_change_removes_only_incompatible_campaign_links(
    client,
    advertiser_factory,
    creative_factory,
    campaign_factory,
):
    user = advertiser_factory()
    vast = creative_factory(
        advertiser=user,
        placement=AdCreative.PLACEMENT_IN_VIDEO,
    )
    pre = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_PREROLL,
        creative=vast,
    )
    mid = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_MIDROLL,
        creative=vast,
    )
    client.force_login(user)

    response = client.post(
        f"/creatives/{vast.pk}/edit/",
        {
            "name": vast.name,
            "placement": AdCreative.PLACEMENT_POPUNDER,
            "destination_url": "https://example.com/pop",
        },
    )
    assert response.status_code == 302
    assert not AdCampaignCreative.objects.filter(
        creative=vast,
        campaign__in=[pre, mid],
    ).exists()


@pytest.mark.django_db
@ADS_HOST_SETTINGS
@override_settings(CACHES=LOCMEM_CACHE)
def test_anonymous_sso_start_redirects_to_main_login(
    client,
):
    cache.clear()
    response = client.get(
        "/ads/sso/start/?next=/finance/"
    )
    assert response.status_code == 302
    assert "login" in response.url.lower()
    assert "next=" in response.url


@pytest.mark.django_db
@ADS_HOST_SETTINGS
@override_settings(CACHES=LOCMEM_CACHE)
def test_sso_callback_rejects_ticket_whose_issued_cache_entry_is_missing(
    client,
    django_user_model,
):
    cache.clear()
    user = django_user_model.objects.create_user(
        username="ads-sso-stale",
        advertiserUser=True,
    )
    ticket = views.signing.dumps(
        {"u": user.pk, "n": "missing-nonce"},
        salt=views.SSO_SALT,
        compress=True,
    )
    with override_settings(ROOT_URLCONF="ads.host_urls"):
        response = client.get(
            "/auth/callback/",
            {"ticket": ticket},
        )
    assert response.status_code == 403


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_ads_logout_clears_session_and_stays_on_ads_login(
    client,
    advertiser_factory,
):
    user = advertiser_factory()
    client.force_login(user)
    response = client.get("/logout/")
    assert response.status_code == 302
    assert response.url == "/login/"
    assert "_auth_user_id" not in client.session


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_dashboard_and_creative_library_only_render_current_advertiser_rows(
    client,
    advertiser_factory,
    campaign_factory,
    creative_factory,
):
    owner = advertiser_factory()
    other = advertiser_factory()
    own_campaign = campaign_factory(
        advertiser=owner,
        name="OWN-CAMPAIGN-UNIQUE",
    )
    other_campaign = campaign_factory(
        advertiser=other,
        name="OTHER-CAMPAIGN-UNIQUE",
    )
    own_creative = creative_factory(
        advertiser=owner,
        name="OWN-CREATIVE-UNIQUE",
    )
    other_creative = creative_factory(
        advertiser=other,
        name="OTHER-CREATIVE-UNIQUE",
    )
    client.force_login(owner)

    dashboard = client.get("/")
    assert dashboard.status_code == 200
    dashboard_body = dashboard.content.decode()
    assert own_campaign.name in dashboard_body
    assert other_campaign.name not in dashboard_body

    library = client.get("/creatives/")
    assert library.status_code == 200
    library_body = library.content.decode()
    assert own_creative.name in library_body
    assert other_creative.name not in library_body


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("pricing_model", AdCampaign.PRICING_CPC),
        ("placement", AdCampaign.PLACEMENT_SIDEBAR),
    ],
)
def test_campaign_pricing_or_placement_change_returns_to_pending_review(
    client,
    advertiser_factory,
    creative_factory,
    campaign_factory,
    changed_field,
    changed_value,
):
    user = advertiser_factory()
    home = creative_factory(
        advertiser=user,
        placement=AdCreative.PLACEMENT_HOME,
    )
    side = creative_factory(
        advertiser=user,
        placement=AdCreative.PLACEMENT_SIDEBAR,
    )
    campaign = campaign_factory(
        advertiser=user,
        placement=AdCampaign.PLACEMENT_HOME,
        creative=home,
    )
    client.force_login(user)

    placement = (
        changed_value
        if changed_field == "placement"
        else campaign.placement
    )
    pricing = (
        changed_value
        if changed_field == "pricing_model"
        else campaign.pricing_model
    )
    creative = (
        side
        if placement == AdCampaign.PLACEMENT_SIDEBAR
        else home
    )
    target = campaign.target_url

    response = client.post(
        f"/campaigns/{campaign.pk}/edit/",
        {
            "name": campaign.name,
            "placement": placement,
            "target_url": target,
            "pricing_model": pricing,
            "bid_usd": "10",
            "creative_ids": [str(creative.pk)],
        },
    )
    assert response.status_code == 302
    campaign.refresh_from_db()
    assert campaign.review_status == AdCampaign.REVIEW_PENDING


@pytest.mark.django_db
@override_settings(ROOT_URLCONF="ads.host_urls")
def test_switching_between_approved_creatives_does_not_re_review_campaign(
    client,
    advertiser_factory,
    creative_factory,
    campaign_factory,
):
    user = advertiser_factory()
    first = creative_factory(advertiser=user)
    second = creative_factory(advertiser=user)
    campaign = campaign_factory(
        advertiser=user,
        creative=first,
    )
    client.force_login(user)

    response = client.post(
        f"/campaigns/{campaign.pk}/edit/",
        {
            "name": campaign.name,
            "placement": campaign.placement,
            "target_url": campaign.target_url,
            "pricing_model": campaign.pricing_model,
            "bid_usd": "10",
            "creative_ids": [str(second.pk)],
        },
    )
    assert response.status_code == 302
    campaign.refresh_from_db()
    assert campaign.review_status == AdCampaign.REVIEW_APPROVED
    assert list(campaign.creatives.values_list("pk", flat=True)) == [
        second.pk
    ]
