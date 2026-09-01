"""Logging handlers used by MediaCMS production processes."""

from __future__ import annotations

import fcntl
import logging
from logging.handlers import RotatingFileHandler


class ProcessSafeRotatingFileHandler(RotatingFileHandler):
    """Rotate a shared log file safely across local processes/containers.

    MediaCMS production containers bind-mount the same repository, so multiple
    uWSGI/Celery processes can have the same log file open at once. The stdlib
    RotatingFileHandler is not process-safe: two processes may race a rollover,
    and a process can keep writing to a file another process already renamed.

    A sibling ``.lock`` file serializes rollover/write operations with flock.
    The stream is deliberately reopened while holding that lock before every
    emitted record so a process never keeps writing into a rotated backup.
    This handler is only attached at ERROR level, making the extra open/close
    negligible while preserving bounded on-disk logs.
    """

    def __init__(self, filename, *args, **kwargs):
        super().__init__(filename, *args, **kwargs)
        self._process_lock_path = f"{self.baseFilename}.lock"

    def _close_stream_for_reopen(self) -> None:
        if self.stream is not None:
            self.stream.flush()
            self.stream.close()
            self.stream = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            with open(self._process_lock_path, "a", encoding="utf-8") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    # Another process may have rotated the path since this
                    # handler last emitted a record. Reopen the canonical path
                    # before testing its current size and writing.
                    self._close_stream_for_reopen()
                    if self.shouldRollover(record):
                        self.doRollover()
                    logging.FileHandler.emit(self, record)
                    self.flush()
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            self.handleError(record)
