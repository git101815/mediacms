from xml.etree import ElementTree
from unittest.mock import Mock

import pytest
from django.core import signing
from django.core.signing import SignatureExpired
from django.http import Http404
from django.test import RequestFactory, override_settings

from ads import views
from ads.models import AdCampaign


def _signed_payload(**overrides):
    payload = {
        "c": 10,
        "r": 20,
        "a": 30,
        "p": AdCampaign.PRICING_CPC,
        "b": 1_000,
        "s": AdCampaign.PLACEMENT_POPUNDER,
        "i": "impression-1",
        "u": "https://example.com/landing",
    }
    payload.update(overrides)
    return signing.dumps(
        payload,
        salt="ads.click.v1",
        compress=True,
    )


def _assert_no_store(response):
    directives = {
        part.strip().lower()
        for part in response["Cache-Control"].split(",")
        if part.strip()
    }
    assert {
        "no-store",
        "private",
        "max-age=0",
    }.issubset(directives)
    assert response["Pragma"].lower() == "no-cache"


@override_settings(
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 100, "clickaine": 0, "partner": 0},
        "in_video": {"internal": 100, "clickaine": 0, "partner": 0},
    },
)
def test_vmap_is_valid_xml_and_contains_all_three_breaks(client):
    response = client.get("/api/v1/ads/vmap/")
    assert response.status_code == 200
    _assert_no_store(response)
    root = ElementTree.fromstring(response.content)
    assert root.tag.endswith("VMAP")
    body = response.content.decode()
    assert 'timeOffset="start"' in body
    assert 'breakId="preroll"' in body
    assert 'timeOffset="50%"' in body
    assert 'breakId="midroll"' in body
    assert 'timeOffset="end"' in body
    assert 'breakId="postroll"' in body
    assert "/api/v1/ads/vast/video_preroll/" in body
    assert "/api/v1/ads/vast/video_midroll/" in body
    assert "/api/v1/ads/vast/video_postroll/" in body


@override_settings(
    ADS_MIDROLL_TIME_OFFSET="33%",
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 100, "clickaine": 0, "partner": 0},
        "in_video": {"internal": 100, "clickaine": 0, "partner": 0},
    },
)
def test_vmap_uses_configured_midroll_offset(client):
    response = client.get("/api/v1/ads/vmap/")
    assert 'timeOffset="33%"' in response.content.decode()


@override_settings(IN_VIDEO_ADS_ENABLED=False)
def test_vmap_is_empty_when_in_video_is_disabled(client):
    response = client.get("/api/v1/ads/vmap/")
    assert response.status_code == 200
    body = response.content.decode()
    assert "<vmap:AdBreak" not in body
    _assert_no_store(response)


def test_vast_rejects_non_video_slot(client):
    response = client.get(
        "/api/v1/ads/vast/home_leaderboard/"
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    "candidate",
    [
        None,
        {
            "campaign_id": 1,
            "creative_id": 2,
            "event_token": "event",
            "vast_url": "ftp://example.com/vast.xml",
        },
        {
            "campaign_id": 1,
            "creative_id": 2,
            "event_token": "event",
            "vast_url": "",
        },
    ],
)
@override_settings(
    POPUNDER_ADS_ENABLED=True,
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 50, "clickaine": 50, "partner": 0},
        "in_video": {"internal": 100, "clickaine": 0, "partner": 0},
    },
    CLICKAINE_VAST_ENABLED=False,
    CLICKAINE_VAST_URL="https://clickaine.example/vast",
)
def test_vast_returns_empty_document_when_internal_has_no_deliverable_vast(
    client,
    monkeypatch,
    candidate,
):
    monkeypatch.setattr(views, "reserve", lambda slot: candidate)
    response = client.get(
        "/api/v1/ads/vast/video_preroll/"
    )
    assert response.status_code == 200
    assert response.content.decode().endswith(
        '<VAST version="3.0"></VAST>'
    )
    _assert_no_store(response)


@override_settings(
    POPUNDER_ADS_ENABLED=True,
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 50, "clickaine": 50, "partner": 0},
        "in_video": {"internal": 50, "clickaine": 50, "partner": 0},
    },
    CLICKAINE_VAST_ENABLED=True,
    CLICKAINE_VAST_URL="https://clickaine.example/vast",
)
def test_vast_skips_internal_runtime_failure_to_clickaine(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        views,
        "weighted_provider_order",
        lambda ad_format: ["internal", "clickaine"],
    )

    def fail(slot):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "reserve", fail)
    response = client.get(
        "/api/v1/ads/vast/video_preroll/"
    )
    body = response.content.decode()
    assert response.status_code == 200
    assert "https://clickaine.example/vast" in body
    assert "internal-" not in body


def test_verified_googlebot_gets_empty_vast(monkeypatch):
    reserve = Mock()
    order = Mock()
    monkeypatch.setattr(views, "reserve", reserve)
    monkeypatch.setattr(views, "weighted_provider_order", order)
    request = RequestFactory().get(
        "/api/v1/ads/vast/video_preroll/"
    )
    request.is_googlebot_verified = True
    response = views.ads_vast(
        request,
        AdCampaign.PLACEMENT_PREROLL,
    )
    assert response.status_code == 200
    assert '<VAST version="3.0"></VAST>' in response.content.decode()
    reserve.assert_not_called()
    order.assert_not_called()


@override_settings(
    POPUNDER_ADS_ENABLED=True,
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 50, "clickaine": 50, "partner": 0},
        "in_video": {"internal": 100, "clickaine": 0, "partner": 0},
    },
    CLICKAINE_VAST_ENABLED=False,
    CLICKAINE_VAST_URL="https://clickaine.example/vast",
)
def test_internal_vast_wrapper_contains_downstream_tag_and_tracking(
    client,
    monkeypatch,
):
    candidate = {
        "campaign_id": 10,
        "creative_id": 20,
        "event_token": "signed-event",
        "vast_url": "https://ads.example/external-vast.xml",
    }
    monkeypatch.setattr(
        views,
        "reserve",
        lambda slot: candidate,
    )

    response = client.get(
        "/api/v1/ads/vast/video_midroll/"
    )
    assert response.status_code == 200
    body = response.content.decode()
    ElementTree.fromstring(response.content)
    assert "https://ads.example/external-vast.xml" in body
    assert "/ads/impression/signed-event/" in body
    assert "/ads/track-click/signed-event/" in body
    assert "MediaCMS Internal Ads" in body
    assert 'fallbackOnNoAd="true"' not in body
    _assert_no_store(response)


@override_settings(
    POPUNDER_ADS_ENABLED=True,
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 50, "clickaine": 50, "partner": 0},
        "in_video": {"internal": 50, "clickaine": 50, "partner": 0},
    },
    CLICKAINE_VAST_ENABLED=True,
    CLICKAINE_VAST_URL="https://clickaine.example/vast",
)
def test_vast_waterfall_preserves_weighted_provider_order(
    client,
    monkeypatch,
):
    candidate = {
        "campaign_id": 10,
        "creative_id": 20,
        "event_token": "signed-event",
        "vast_url": "https://internal.example/vast",
    }
    monkeypatch.setattr(views, "reserve", lambda slot: candidate)
    monkeypatch.setattr(
        views,
        "weighted_provider_order",
        lambda ad_format: ["clickaine", "internal"],
    )

    response = client.get(
        "/api/v1/ads/vast/video_preroll/"
    )
    body = response.content.decode()
    ElementTree.fromstring(response.content)
    assert body.index('id="clickaine"') < body.index('id="internal-10-20"')
    assert body.count('fallbackOnNoAd="true"') == 1
    assert "https://clickaine.example/vast" in body
    assert "https://internal.example/vast" in body


def test_cdata_terminator_is_safely_split():
    assert views._cdata("a]]>b") == "a]]]]><![CDATA[>b"


@override_settings(
    POPUNDER_ADS_ENABLED=True,
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 50, "clickaine": 50, "partner": 0},
        "in_video": {"internal": 50, "clickaine": 50, "partner": 0},
    },
    ADS_PARTNER_POPUNDER_OFFERS=[
        {"weight": 100, "url_template": "https://partner.example/?click=CLICKID"}
    ],
    CLICKAINE_POPUNDER_ENABLED=True,
    CLICKAINE_POPUNDER_SCRIPT_URL="https://clickaine.example/pop.js",
)
def test_popunder_returns_weighted_provider_queue(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        views,
        "weighted_provider_order",
        lambda ad_format: ["clickaine", "internal"],
    )
    monkeypatch.setattr(
        views,
        "reserve",
        lambda slot: {
            "campaign_id": 1,
            "creative_id": 2,
            "event_token": "signed-event",
        },
    )
    response = client.get("/api/v1/ads/popunder/")
    assert response.status_code == 200
    assert response.json() == {
        "providers": [
            {
                "name": "clickaine",
                "delivery": "script",
                "script_url": "https://clickaine.example/pop.js",
            },
            {
                "name": "internal",
                "delivery": "url",
                "open_url": "/ads/open/signed-event/",
            },
        ]
    }
    _assert_no_store(response)


@override_settings(
    POPUNDER_ADS_ENABLED=True,
    IN_VIDEO_ADS_ENABLED=True,
    ADS_PROVIDER_WEIGHTS={
        "popunder": {"internal": 50, "clickaine": 50, "partner": 0},
        "in_video": {"internal": 50, "clickaine": 50, "partner": 0},
    },
    ADS_PARTNER_POPUNDER_OFFERS=[
        {"weight": 100, "url_template": "https://partner.example/?click=CLICKID"}
    ],
    CLICKAINE_POPUNDER_ENABLED=True,
    CLICKAINE_POPUNDER_SCRIPT_URL="https://clickaine.example/pop.js",
)
def test_popunder_skips_internal_no_fill_and_keeps_next_provider(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        views,
        "weighted_provider_order",
        lambda ad_format: ["internal", "clickaine"],
    )
    monkeypatch.setattr(views, "reserve", lambda slot: None)
    response = client.get("/api/v1/ads/popunder/")
    assert response.status_code == 200
    assert response.json() == {
        "providers": [
            {
                "name": "clickaine",
                "delivery": "script",
                "script_url": "https://clickaine.example/pop.js",
            }
        ]
    }


def test_verified_googlebot_gets_no_popunder(monkeypatch):
    reserve = Mock()
    order = Mock()
    monkeypatch.setattr(views, "reserve", reserve)
    monkeypatch.setattr(views, "weighted_provider_order", order)
    request = RequestFactory().get("/api/v1/ads/popunder/")
    request.is_googlebot_verified = True
    response = views.ads_popunder(request)
    assert response.status_code == 204
    reserve.assert_not_called()
    order.assert_not_called()


def test_internal_preroll_impression_consumes_cooldown(client, monkeypatch):
    token = _signed_payload(s=AdCampaign.PLACEMENT_PREROLL)
    marker = Mock()
    monkeypatch.setattr(views, "mark_cooldown", marker)
    monkeypatch.setattr(views, "record_impression", lambda payload: None)
    response = client.get(f"/ads/impression/{token}/")
    assert response.status_code == 204
    marker.assert_called_once()


def test_clickaine_vast_impression_consumes_preroll_cooldown(client, monkeypatch):
    monkeypatch.setattr(views, "mark_cooldown", Mock())
    response = client.get(
        "/ads/clickaine-vast-impression/?slot=video_preroll"
    )
    assert response.status_code == 204
    _assert_no_store(response)
    views.mark_cooldown.assert_called_once()


def test_popunder_consume_endpoint_marks_session_cooldown(client, monkeypatch):
    marker = Mock()
    monkeypatch.setattr(views, "mark_cooldown", marker)
    response = client.post("/api/v1/ads/popunder/consume/", data="1", content_type="text/plain")
    assert response.status_code == 204
    _assert_no_store(response)
    marker.assert_called_once()


def test_banner_serve_contract_and_fail_closed_behavior(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        views,
        "serve",
        lambda slot: {
            "campaign_id": 1,
            "creative_id": 2,
            "creative_url": "/media/banner.gif",
            "click_url": "/ads/click/token/",
        },
    )
    response = client.get(
        "/api/v1/direct-ads/serve/home_leaderboard/"
    )
    assert response.status_code == 200
    assert response.json()["creative_url"] == "/media/banner.gif"
    _assert_no_store(response)

    monkeypatch.setattr(views, "serve", lambda slot: None)
    assert client.get(
        "/api/v1/direct-ads/serve/home_leaderboard/"
    ).status_code == 204

    def fail(slot):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "serve", fail)
    assert client.get(
        "/api/v1/direct-ads/serve/home_leaderboard/"
    ).status_code == 204


def test_verified_googlebot_gets_no_banner(monkeypatch):
    serve = Mock()
    monkeypatch.setattr(views, "serve", serve)
    request = RequestFactory().get(
        "/api/v1/direct-ads/serve/home_leaderboard/"
    )
    request.is_googlebot_verified = True
    response = views.serve_direct_ad(
        request,
        AdCampaign.PLACEMENT_HOME,
    )
    assert response.status_code == 204
    serve.assert_not_called()


def test_impression_endpoint_is_idempotency_transport_and_swallows_accounting_errors(
    client,
    monkeypatch,
):
    token = _signed_payload()
    calls = []
    monkeypatch.setattr(
        views,
        "record_impression",
        lambda payload: calls.append(payload) or 1,
    )
    response = client.get(f"/ads/impression/{token}/")
    assert response.status_code == 204
    assert len(calls) == 1
    _assert_no_store(response)

    def fail(payload):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "record_impression", fail)
    response = client.get(f"/ads/impression/{token}/")
    assert response.status_code == 204


def test_click_tracking_endpoint_swallows_accounting_errors(
    client,
    monkeypatch,
):
    token = _signed_payload()
    calls = []
    monkeypatch.setattr(
        views,
        "record_click",
        lambda payload: calls.append(payload) or 1,
    )
    response = client.get(f"/ads/track-click/{token}/")
    assert response.status_code == 204
    assert len(calls) == 1

    def fail(payload):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "record_click", fail)
    assert client.get(
        f"/ads/track-click/{token}/"
    ).status_code == 204


def test_popunder_open_records_impression_then_click_and_redirects(
    client,
    monkeypatch,
):
    token = _signed_payload()
    order = []
    monkeypatch.setattr(
        views,
        "record_impression",
        lambda payload: order.append("impression"),
    )
    monkeypatch.setattr(
        views,
        "record_click",
        lambda payload: order.append("click"),
    )
    response = client.get(f"/ads/open/{token}/")
    assert response.status_code == 302
    assert response.url == "https://example.com/landing"
    assert order == ["impression", "click"]
    assert "mediacms_ads_popunder_cd" in response.cookies
    assert "sessionid" not in response.cookies


def test_popunder_open_still_redirects_if_accounting_is_down(
    client,
    monkeypatch,
):
    token = _signed_payload()

    def fail(payload):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "record_impression", fail)
    monkeypatch.setattr(views, "record_click", fail)
    response = client.get(f"/ads/open/{token}/")
    assert response.status_code == 302
    assert response.url == "https://example.com/landing"


def test_banner_click_redirects_even_if_accounting_is_down(
    client,
    monkeypatch,
):
    token = _signed_payload(
        s=AdCampaign.PLACEMENT_HOME,
        u="https://example.com/banner",
    )

    def fail(payload):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "record_click", fail)
    response = client.get(f"/ads/click/{token}/")
    assert response.status_code == 302
    assert response.url == "https://example.com/banner"


@pytest.mark.parametrize(
    "path",
    [
        "/ads/open/{token}/",
        "/ads/click/{token}/",
    ],
)
def test_redirect_endpoints_reject_non_http_targets(
    client,
    path,
):
    token = _signed_payload(u="ftp://example.com/file")
    response = client.get(path.format(token=token))
    assert response.status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/ads/impression/not-signed/",
        "/ads/track-click/not-signed/",
        "/ads/open/not-signed/",
        "/ads/click/not-signed/",
    ],
)
def test_all_event_endpoints_reject_bad_signatures(client, path):
    assert client.get(path).status_code == 404


def test_expired_signed_token_is_rejected(monkeypatch):
    request = RequestFactory().get("/")
    monkeypatch.setattr(
        views.signing,
        "loads",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            SignatureExpired("expired")
        ),
    )
    with pytest.raises(Http404):
        views._load_ad_event_token("token")
