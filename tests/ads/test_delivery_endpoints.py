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


def test_vmap_is_valid_xml_and_contains_all_three_breaks(client):
    response = client.get("/api/v1/direct-ads/vmap/")
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
    assert "/api/v1/direct-ads/vast/video_preroll/" in body
    assert "/api/v1/direct-ads/vast/video_midroll/" in body
    assert "/api/v1/direct-ads/vast/video_postroll/" in body


@override_settings(ADS_MIDROLL_TIME_OFFSET="33%")
def test_vmap_uses_configured_midroll_offset(client):
    response = client.get("/api/v1/direct-ads/vmap/")
    assert 'timeOffset="33%"' in response.content.decode()


def test_vast_rejects_non_video_slot(client):
    response = client.get(
        "/api/v1/direct-ads/vast/home_leaderboard/"
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
def test_vast_returns_empty_document_when_no_deliverable_vast(
    client,
    monkeypatch,
    candidate,
):
    monkeypatch.setattr(views, "reserve", lambda slot: candidate)
    response = client.get(
        "/api/v1/direct-ads/vast/video_preroll/"
    )
    assert response.status_code == 200
    assert response.content.decode().endswith(
        '<VAST version="3.0"></VAST>'
    )
    _assert_no_store(response)


def test_vast_returns_empty_document_when_runtime_fails(
    client,
    monkeypatch,
):
    def fail(slot):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "reserve", fail)
    response = client.get(
        "/api/v1/direct-ads/vast/video_preroll/"
    )
    assert response.status_code == 200
    assert '<VAST version="3.0"></VAST>' in response.content.decode()


def test_verified_googlebot_gets_empty_vast(monkeypatch):
    reserve = Mock()
    monkeypatch.setattr(views, "reserve", reserve)
    request = RequestFactory().get(
        "/api/v1/direct-ads/vast/video_preroll/"
    )
    request.is_googlebot_verified = True
    response = views.direct_ads_vast(
        request,
        AdCampaign.PLACEMENT_PREROLL,
    )
    assert response.status_code == 200
    assert '<VAST version="3.0"></VAST>' in response.content.decode()
    reserve.assert_not_called()


def test_vast_wrapper_contains_downstream_tag_and_direct_tracking(
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
        "/api/v1/direct-ads/vast/video_midroll/"
    )
    assert response.status_code == 200
    body = response.content.decode()
    ElementTree.fromstring(response.content)
    assert "https://ads.example/external-vast.xml" in body
    assert "/ads/impression/signed-event/" in body
    assert "/ads/track-click/signed-event/" in body
    assert "<Wrapper>" in body
    _assert_no_store(response)


def test_cdata_terminator_is_safely_split():
    assert views._cdata("a]]>b") == "a]]]]><![CDATA[>b"


@pytest.mark.parametrize(
    "slot",
    [
        AdCampaign.PLACEMENT_HOME,
        AdCampaign.PLACEMENT_PREROLL,
        AdCampaign.PLACEMENT_MIDROLL,
        AdCampaign.PLACEMENT_POSTROLL,
        "unknown",
    ],
)
def test_popunder_reservation_rejects_every_non_popunder_slot(
    client,
    slot,
):
    response = client.get(
        f"/api/v1/direct-ads/reserve/{slot}/"
    )
    assert response.status_code == 204
    _assert_no_store(response)


def test_popunder_reservation_returns_204_on_empty_or_runtime_failure(
    client,
    monkeypatch,
):
    monkeypatch.setattr(views, "reserve", lambda slot: None)
    empty = client.get(
        "/api/v1/direct-ads/reserve/popunder/"
    )
    assert empty.status_code == 204

    def fail(slot):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(views, "reserve", fail)
    failed = client.get(
        "/api/v1/direct-ads/reserve/popunder/"
    )
    assert failed.status_code == 204


def test_popunder_reservation_returns_only_open_contract(
    client,
    monkeypatch,
):
    monkeypatch.setattr(
        views,
        "reserve",
        lambda slot: {
            "campaign_id": 1,
            "creative_id": 2,
            "event_token": "signed-event",
            "destination_url": "https://example.com/",
        },
    )
    response = client.get(
        "/api/v1/direct-ads/reserve/popunder/"
    )
    assert response.status_code == 200
    assert response.json() == {
        "campaign_id": 1,
        "creative_id": 2,
        "open_url": "/ads/open/signed-event/",
    }
    _assert_no_store(response)


def test_verified_googlebot_gets_no_popunder(monkeypatch):
    reserve = Mock()
    monkeypatch.setattr(views, "reserve", reserve)
    request = RequestFactory().get(
        "/api/v1/direct-ads/reserve/popunder/"
    )
    request.is_googlebot_verified = True
    response = views.reserve_direct_ad(
        request,
        AdCampaign.PLACEMENT_POPUNDER,
    )
    assert response.status_code == 204
    reserve.assert_not_called()


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
