from urllib.parse import quote

import pytest
from django.core import signing

from ads import views as ads_views
from ads.models import AdCampaign


pytestmark = pytest.mark.django_db


def _token(payload):
    return quote(signing.dumps(payload, salt="ads.click.v1", compress=True), safe="")


def test_direct_ad_open_rejects_invalid_token(client):
    response = client.get("/ads/open/not-a-valid-token/")

    assert response.status_code == 404


def test_direct_ad_click_rejects_invalid_token(client):
    response = client.get("/ads/click/not-a-valid-token/")

    assert response.status_code == 404


@pytest.mark.parametrize(
    "target",
    [
        "",
        "/relative/path",
        "//evil.example/path",
        "javascript:alert(1)",
        "data:text/html,<script>alert(1)</script>",
    ],
)
def test_direct_ad_open_rejects_non_http_targets(client, target):
    response = client.get(f"/ads/open/{_token({'u': target})}/")

    assert response.status_code == 404


@pytest.mark.parametrize(
    "target",
    [
        "",
        "/relative/path",
        "//evil.example/path",
        "javascript:alert(1)",
        "data:text/html,<script>alert(1)</script>",
    ],
)
def test_direct_ad_click_rejects_non_http_targets(client, target):
    response = client.get(f"/ads/click/{_token({'u': target})}/")

    assert response.status_code == 404


def test_click_tracking_failure_does_not_block_valid_click_redirect(client, monkeypatch):
    monkeypatch.setattr(
        ads_views,
        "record_click",
        lambda payload: (_ for _ in ()).throw(RuntimeError("redis down")),
    )

    response = client.get(f"/ads/click/{_token({'u': 'https://example.test/landing'})}/")

    assert response.status_code == 302
    assert response["Location"] == "https://example.test/landing"


def test_open_tracking_failures_do_not_block_valid_open_redirect(client, monkeypatch):
    monkeypatch.setattr(
        ads_views,
        "record_impression",
        lambda payload: (_ for _ in ()).throw(RuntimeError("redis down")),
    )
    monkeypatch.setattr(
        ads_views,
        "record_click",
        lambda payload: (_ for _ in ()).throw(RuntimeError("redis down")),
    )

    response = client.get(f"/ads/open/{_token({'u': 'https://example.test/open'})}/")

    assert response.status_code == 302
    assert response["Location"] == "https://example.test/open"


def test_direct_ad_serve_runtime_failure_fails_closed_without_500(client, monkeypatch):
    monkeypatch.setattr(
        ads_views,
        "serve",
        lambda slot: (_ for _ in ()).throw(RuntimeError("redis down")),
    )

    response = client.get(f"/api/v1/direct-ads/serve/{AdCampaign.PLACEMENT_HOME}/")

    assert response.status_code == 204
    assert "no-store" in response["Cache-Control"]


def test_direct_ad_reserve_non_popunder_slot_is_no_content(client):
    response = client.get(f"/api/v1/direct-ads/reserve/{AdCampaign.PLACEMENT_HOME}/")

    assert response.status_code == 204
    assert "no-store" in response["Cache-Control"]


def test_direct_ad_vast_rejects_non_video_slot(client):
    response = client.get(f"/api/v1/direct-ads/vast/{AdCampaign.PLACEMENT_HOME}/")

    assert response.status_code == 404


def test_direct_ad_vmap_has_no_store_cache_header(client):
    response = client.get("/api/v1/direct-ads/vmap/")

    assert response.status_code == 200
    assert response["Content-Type"].startswith("application/xml")
    assert "no-store" in response["Cache-Control"]

