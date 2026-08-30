
from pathlib import Path

from django.conf import settings


def _source(rel):
    return (Path(settings.BASE_DIR) / rel).read_text(encoding="utf-8")


def test_banner_inventory_is_wired_to_both_real_frontend_slots():
    home = _source("frontend/src/static/js/pages/HomePage.tsx")
    sidebar = _source(
        "frontend/src/static/js/components/media-page/ViewerSidebar.js"
    )
    direct_ad = _source("frontend/src/static/js/components/ads/DirectAd.js")

    assert '<DirectAd slot="home_leaderboard" />' in home
    assert '<DirectAd slot="media_sidebar_rectangle" />' in sidebar
    assert "/api/v1/direct-ads/serve/" in direct_ad
    assert "flags.adFree || flags.advanced" in direct_ad
    assert "ad.creative_url && ad.click_url" in direct_ad


def test_existing_videojs_ima_pipeline_uses_generic_ads_vmap():
    viewer = _source(
        "frontend/src/static/js/components/media-viewer/VideoViewer/index.js"
    )
    prerolls = _source("templates/ads/prerolls.html")

    assert "window.__vjsPluginsLoadedPromise" in viewer
    assert "videoJsPlayer.ima({" in viewer
    assert "adTagUrl: vmapUrl" in viewer
    assert "initializeAdDisplayContainer" in viewer
    assert "initInVideoAds" in viewer
    assert 'window.__mcAdsVmapUrl = "/api/v1/ads/vmap/";' in prerolls
    assert "https://imasdk.googleapis.com/js/sdkloader/ima3.js" in prerolls


def test_popunder_frontend_is_adapter_based_not_provider_based():
    rotation = _source("templates/ads/popunder_rotation.html")
    url_adapter = _source("templates/ads/popunder_url.html")
    script_adapter = _source("templates/ads/popunder_script.html")

    assert '"/api/v1/ads/popunder/"' in rotation
    assert '"/api/v1/ads/popunder/consume/"' in rotation
    assert "providerQueue" in rotation
    assert "advanceProvider" in rotation

    assert 'data.delivery !== "url"' in url_adapter
    assert 'data.delivery !== "script"' in script_adapter
    assert 'runtime.chosenProvider === "internal"' not in url_adapter
    assert 'provider !== "clickaine"' not in script_adapter


def test_popunder_never_installs_invisible_click_overlays():
    rotation = _source("templates/ads/popunder_rotation.html")
    url_adapter = _source("templates/ads/popunder_url.html")

    assert "mc-popunder-overlay" not in rotation
    assert "mc-popunder-overlay" not in url_adapter
    assert "coverSelectors" not in rotation
    assert "createOverlayFor" not in url_adapter


def test_popunder_does_not_consume_original_click_when_popup_is_blocked():
    url_adapter = _source("templates/ads/popunder_url.html")
    popup_check = url_adapter.index("if (!looksUsable(cloneHandle))")
    consume = url_adapter.index("consumeOriginalEvent(event, triggerEventName)")
    assert popup_check < consume


def test_media_template_keeps_popunder_adapters_behind_one_gate():
    media = _source("templates/cms/media.html")
    gated_block = media.split("{% if SHOW_TABUNDER %}", 1)[1].split(
        "{% endif %}", 1
    )[0]
    assert 'ads/popunder_rotation.html' in gated_block
    assert 'ads/popunder_tracking.html' in gated_block
    assert 'ads/popunder_url.html' in gated_block
    assert 'ads/popunder_script.html' in gated_block
    assert 'ads/partner_popads.html' not in gated_block
    assert 'ads/popunder_clickaine.html' not in gated_block
