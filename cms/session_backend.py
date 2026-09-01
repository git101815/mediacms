"""Persistent session backend with one-time cache-only migration support."""

import logging

from django.contrib.sessions.backends.cache import SessionStore as CacheSessionStore
from django.contrib.sessions.backends.cached_db import SessionStore as CachedDBSessionStore
from django.contrib.sessions.backends.base import CreateError
from django.contrib.sessions.models import Session
from django.core.cache import caches
from django.db import IntegrityError

logger = logging.getLogger(__name__)


class SessionStore(CachedDBSessionStore):
    """Use cached_db, but import an old cache-only session on first access.

    Before this hardening MediaCMS used the cache backend directly. Existing
    cookies therefore point to Redis entries with no django_session row. The
    fallback below preserves those sessions while all new/updated sessions are
    persisted in PostgreSQL by cached_db.
    """

    def load(self):
        session_key = self.session_key
        if session_key and not Session.objects.filter(session_key=session_key).exists():
            legacy = CacheSessionStore(session_key=session_key)
            legacy._cache = caches["legacy_sessions"]
            data = legacy.load()
            if data:
                self._session_key = session_key
                self._session_cache = data
                try:
                    self.save(must_create=True)
                except (CreateError, IntegrityError):
                    # Another request can migrate the same session concurrently.
                    logger.debug(
                        "Session %s was migrated concurrently",
                        session_key,
                    )
                return data
        return super().load()
