from unittest.mock import Mock, patch

from django.contrib.sessions.backends.base import CreateError
from django.contrib.sessions.backends.cached_db import SessionStore as CachedDBSessionStore
from django.conf import settings

from cms.session_backend import SessionStore


def test_sessions_use_persistent_backend():
    assert settings.SESSION_ENGINE == "cms.session_backend"
    assert "legacy_sessions" in settings.CACHES


def test_legacy_session_is_deleted_after_successful_import():
    store = SessionStore(session_key="legacy-key")
    legacy = Mock()
    legacy.load.return_value = {"user": 1}
    with patch("cms.session_backend.Session.objects.filter") as rows, patch(
        "cms.session_backend.CacheSessionStore", return_value=legacy
    ), patch.object(SessionStore, "save") as save:
        rows.return_value.exists.return_value = False
        assert store.load() == {"user": 1}
    save.assert_called_once_with(must_create=True)
    legacy.delete.assert_called_once_with("legacy-key")


def test_concurrent_legacy_import_never_returns_stale_redis_value():
    store = SessionStore(session_key="legacy-key")
    legacy = Mock()
    legacy.load.return_value = {"stale": True}
    with patch("cms.session_backend.Session.objects.filter") as rows, patch(
        "cms.session_backend.CacheSessionStore", return_value=legacy
    ), patch.object(SessionStore, "save", side_effect=CreateError), patch.object(
        CachedDBSessionStore, "load", return_value={"authoritative": True}
    ):
        rows.return_value.exists.return_value = False
        assert store.load() == {"authoritative": True}
    legacy.delete.assert_called_once_with("legacy-key")
