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


def test_existing_videojs_ima_pipeline_is_the_direct_in_video_delivery_path():
    viewer = _source(
        "frontend/src/static/js/components/media-viewer/VideoViewer/index.js"
    )
    prerolls = _source("templates/ads/prerolls.html")

    assert "window.__vjsPluginsLoadedPromise" in viewer
    assert "videoJsPlayer.ima({" in viewer
    assert "adTagUrl: vmapUrl" in viewer
    assert "initializeAdDisplayContainer" in viewer

    assert (
        'window.__mcDirectAdsVmapUrl = '
        '"/api/v1/direct-ads/vmap/";'
        in prerolls
    )
    assert (
        "https://imasdk.googleapis.com/js/sdkloader/ima3.js"
        in prerolls
    )


def test_direct_popunder_reuses_existing_rotation_and_tabunder_engine():
    rotation = _source("templates/ads/popunder_rotation.html")
    clickaine = _source("templates/ads/popunder_clickaine.html")
    partner = _source("templates/ads/partner_popads.html")

    assert (
        '"/api/v1/direct-ads/reserve/popunder/"'
        in rotation
    )
    assert (
        'window.mcPopAdsRuntime.chosenProvider = "direct"'
        in rotation
    )
    assert "fallbackProvider" in rotation

    assert "window.mcPopAdsRuntime.ready" in clickaine
    assert 'provider !== "clickaine"' in clickaine

    assert 'runtime.chosenProvider === "direct"' in partner
    assert 'runtime.chosenProvider !== "direct"' in partner
    assert "runtime.markOpen({" in partner
