
from types import SimpleNamespace

from django.test import override_settings

from files import context_processors


ADS_SETTINGS = {
    "POPUNDER_ADS_ENABLED": True,
    "IN_VIDEO_ADS_ENABLED": True,
    "ADS_PROVIDER_WEIGHTS": {
        "popunder": {
            "internal": 100,
            "clickaine": 0,
            "partner": 0,
        },
        "in_video": {
            "internal": 100,
            "clickaine": 0,
            "partner": 0,
        },
    },
    "CLICKAINE_POPUNDER_ENABLED": False,
    "CLICKAINE_POPUNDER_SCRIPT_URL": "https://clickaine.example/pop.js",
    "CLICKAINE_VAST_ENABLED": False,
    "CLICKAINE_VAST_URL": "https://clickaine.example/vast",
    "ADS_PARTNER_POPUNDER_OFFERS": [
        {
            "weight": 100,
            "url_template": "https://partner.example/?click=CLICKID",
        }
    ],
    "TABUNDER_COOLDOWN_SECONDS": 60,
    "PREROLLS_COOLDOWN_SECONDS": 10,
}


def _request(*, session, media_page=True, preroll_eligible=True):
    return SimpleNamespace(
        user=SimpleNamespace(is_authenticated=False),
        session=session,
        media_page=media_page,
        preroll_eligible=preroll_eligible,
        is_googlebot_verified=False,
    )


@override_settings(**ADS_SETTINGS)
def test_context_gates_do_not_consume_cooldown(monkeypatch):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    session = {}
    flags = context_processors.ads_flags(_request(session=session))
    assert flags["SHOW_TABUNDER"] is True
    assert flags["SHOW_PREROLL"] is True
    assert session == {}


@override_settings(**ADS_SETTINGS)
def test_existing_delivery_cooldowns_block_format(monkeypatch):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    session = {
        "tabunder_last_ts": 950,
        "preroll_last_ts": 995,
    }
    flags = context_processors.ads_flags(_request(session=session))
    assert flags["SHOW_TABUNDER"] is False
    assert flags["SHOW_PREROLL"] is False


@override_settings(**{**ADS_SETTINGS, "POPUNDER_ADS_ENABLED": False})
def test_popunder_kill_switch_prevents_frontend_pipeline(monkeypatch):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    flags = context_processors.ads_flags(_request(session={}))
    assert flags["SHOW_TABUNDER"] is False
    assert flags["SHOW_PREROLL"] is True


@override_settings(**{**ADS_SETTINGS, "IN_VIDEO_ADS_ENABLED": False})
def test_in_video_kill_switch_prevents_ima_pipeline(monkeypatch):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    flags = context_processors.ads_flags(_request(session={}))
    assert flags["SHOW_TABUNDER"] is True
    assert flags["SHOW_PREROLL"] is False


@override_settings(**ADS_SETTINGS)
def test_non_video_media_never_enables_preroll(monkeypatch):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    flags = context_processors.ads_flags(
        _request(session={}, media_page=True, preroll_eligible=False)
    )
    assert flags["SHOW_TABUNDER"] is True
    assert flags["SHOW_PREROLL"] is False
