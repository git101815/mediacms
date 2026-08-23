from pathlib import Path

from django.conf import settings


def _source(rel):
    return (
        Path(settings.BASE_DIR) / rel
    ).read_text(encoding="utf-8")


def test_banner_inventory_is_wired_to_both_real_frontend_slots():
    home = _source(
        "frontend/src/static/js/pages/HomePage.tsx"
    )
    sidebar = _source(
        "frontend/src/static/js/components/media-page/ViewerSidebar.js"
    )
    direct_ad = _source(
        "frontend/src/static/js/components/ads/DirectAd.js"
    )

    assert '<DirectAd slot="home_leaderboard" />' in home
    assert (
        '<DirectAd slot="media_sidebar_rectangle" />'
        in sidebar
    )
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

    assert (
        'window.__mcAdsVmapUrl = "/api/v1/ads/vmap/";'
        in prerolls
    )
    assert (
        "https://imasdk.googleapis.com/js/sdkloader/ima3.js"
        in prerolls
    )


def test_popunder_uses_generic_provider_queue_and_internal_name():
    rotation = _source("templates/ads/popunder_rotation.html")
    clickaine = _source("templates/ads/popunder_clickaine.html")
    partner = _source("templates/ads/partner_popads.html")

    assert '"/api/v1/ads/popunder/"' in rotation
    assert "providerQueue" in rotation
    assert "advanceProvider" in rotation
    assert "fallbackProvider" not in rotation
    assert "/api/v1/direct-ads/reserve/popunder/" not in rotation

    assert "runtime.providerData" in clickaine
    assert "providerData.script_url" in clickaine
    assert "36707.phidonatome.com" not in clickaine
    assert 'runtime.advanceProvider("clickaine")' in clickaine

    assert 'runtime.chosenProvider === "internal"' in partner
    assert 'runtime.chosenProvider !== "internal"' in partner
    assert "runtime.markOpen" not in partner
    assert "runtime.canRun" not in partner


def test_popunder_runtime_has_no_browser_frequency_state():
    rotation = _source("templates/ads/popunder_rotation.html")
    partner = _source("templates/ads/partner_popads.html")

    assert "localStorage" not in rotation
    assert "sessionStorage" not in rotation
    assert "providerQueue" in rotation
    assert "advanceProvider" in rotation
    assert "markOpen" not in partner
    assert "canRun" not in partner


def test_media_template_keeps_all_popunder_providers_behind_one_gate():
    media = _source("templates/cms/media.html")
    gated_block = media.split("{% if SHOW_TABUNDER %}", 1)[1].split(
        "{% endif %}", 1
    )[0]
    assert 'ads/popunder_rotation.html' in gated_block
    assert 'ads/popunder_tracking.html' in gated_block
    assert 'ads/popunder_clickaine.html' in gated_block
    assert 'ads/partner_popads.html' in gated_block


