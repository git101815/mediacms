from types import SimpleNamespace

from django.test import override_settings

from files import context_processors


def _request(*, session, media_page=True, preroll_eligible=True):
    return SimpleNamespace(
        user=SimpleNamespace(is_authenticated=False),
        session=session,
        media_page=media_page,
        preroll_eligible=preroll_eligible,
        is_googlebot_verified=False,
    )


@override_settings(
    TABUNDER_COOLDOWN_SECONDS=60,
    PREROLLS_COOLDOWN_SECONDS=10,
)
def test_server_cooldowns_gate_entire_popunder_and_in_video_pipelines(
    monkeypatch,
):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    session = {}
    first = context_processors.ads_flags(
        _request(session=session)
    )
    assert first["SHOW_TABUNDER"] is True
    assert first["SHOW_PREROLL"] is True
    assert session == {
        "tabunder_last_ts": 1000,
        "preroll_last_ts": 1000,
    }

    second = context_processors.ads_flags(
        _request(session=session)
    )
    assert second["SHOW_TABUNDER"] is False
    assert second["SHOW_PREROLL"] is False


@override_settings(
    TABUNDER_COOLDOWN_SECONDS=60,
    PREROLLS_COOLDOWN_SECONDS=10,
)
def test_non_video_media_does_not_consume_preroll_cooldown(monkeypatch):
    monkeypatch.setattr(context_processors.time, "time", lambda: 1000)
    session = {}
    flags = context_processors.ads_flags(
        _request(
            session=session,
            media_page=True,
            preroll_eligible=False,
        )
    )
    assert flags["SHOW_TABUNDER"] is True
    assert flags["SHOW_PREROLL"] is False
    assert session == {"tabunder_last_ts": 1000}
