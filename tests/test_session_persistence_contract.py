from django.conf import settings


def test_sessions_use_persistent_backend():
    assert settings.SESSION_ENGINE == "cms.session_backend"
    assert "legacy_sessions" in settings.CACHES
