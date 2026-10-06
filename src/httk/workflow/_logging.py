"""Logging configuration for the :mod:`httk.workflow` logger hierarchy.

The implementation lives in :mod:`httk.core.report`. Library code only ever
calls :func:`logging.getLogger` and never installs a handler. A manager
process opts into diagnostics by calling :func:`configure_logging` once and,
when the manager identity is known, :func:`add_log_file`.

When this module's handlers are installed, ``httk.workflow`` has
``propagate = False``, so records do not reach a ``"httk"``-level collector
(:func:`httk.core.report.collect_reports`). That is fine for CLI processes and
is intentional.
"""

import logging
import logging.handlers
import os
from pathlib import Path

from httk.core.report import (
    DEFAULT_BACKUP_COUNT,
    DEFAULT_MAXIMUM_BYTES,
    LOG_LEVELS,
    JsonFormatter,
    add_report_file,
    configure_reporting,
    reset_reporting,
    resolve_level,
)

__all__ = [
    "DEFAULT_BACKUP_COUNT",
    "DEFAULT_MAXIMUM_BYTES",
    "LOG_LEVELS",
    "PACKAGE_LOGGER",
    "JsonFormatter",
    "add_log_file",
    "configure_logging",
    "reset_logging",
    "resolve_level",
]

PACKAGE_LOGGER = "httk.workflow"
_LOGGER = logging.getLogger(__name__)
#: A per-manager log is rotated once it exceeds this size.
MANAGER_LOG_ROTATION_BYTES = 16 * 1024 * 1024
MANAGER_LOG_ROTATION_RECORDS = 1000
_rotation_warnings: set[Path] = set()


def _rotate_manager_log(path: Path) -> None:
    """Move an oversized manager log to its one ``.1`` backup.

    Only the owning manager writes the file, so no lock is needed
    (ponytail: the file must never be shared between managers).
    """

    try:
        if path.stat().st_size > MANAGER_LOG_ROTATION_BYTES:
            os.replace(path, path.with_name(f"{path.name}.1"))
    except FileNotFoundError:
        return
    except OSError as exc:
        _warn_rotation_error(path, exc)


def _warn_rotation_error(path: Path, error: OSError) -> None:
    """Warn once when a best-effort manager-log rotation cannot proceed."""

    if path in _rotation_warnings:
        return
    _rotation_warnings.add(path)
    _LOGGER.warning("could not rotate manager log %s: %s", path, error)


def configure_logging(*, level: str | int = "warning", json_logs: bool = False) -> None:
    """Install one console handler for the workflow logger hierarchy."""

    configure_reporting(level=level, json_logs=json_logs, logger=PACKAGE_LOGGER)


def add_log_file(
    path: Path,
    *,
    level: str | int = "info",
    json_logs: bool = False,
    manager_id: str | None = None,
    maximum_bytes: int = DEFAULT_MAXIMUM_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> Path:
    """Add a file handler and return the path it writes.

    When *manager_id* is supplied, the handler is an append-only manager log:
    text records begin with the id and JSON records carry it as
    ``manager_id``. The manager log is rotated to one ``.1`` backup
    when it exceeds 16 MiB. The legacy rotating handler remains available to
    callers that do not identify a manager.
    """

    if manager_id is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        _rotate_manager_log(path)
        handler = _ManagerLogHandler(path)
        handler.setLevel(resolve_level(level))
        handler.addFilter(_ManagerLogFilter(manager_id))
        handler.setFormatter(_ManagerJsonFormatter() if json_logs else _ManagerTextFormatter())
        setattr(handler, "_httk_report_handler", True)  # noqa: B010 - matches the shared reset marker
        target = logging.getLogger(PACKAGE_LOGGER)
        target.addHandler(handler)
        target.propagate = False
        if target.level == logging.NOTSET or handler.level < target.level:
            target.setLevel(handler.level)
        return path

    return add_report_file(
        path,
        level=level,
        json_logs=json_logs,
        maximum_bytes=maximum_bytes,
        backup_count=backup_count,
        logger=PACKAGE_LOGGER,
    )


class _ManagerLogFilter(logging.Filter):
    """Attach the owning manager id to every record handled by its file."""

    def __init__(self, manager_id: str) -> None:
        super().__init__()
        self.manager_id = manager_id

    def filter(self, record: logging.LogRecord) -> bool:
        """Set the manager identity and accept the record."""

        record.manager_id = self.manager_id
        return True


class _ManagerTextFormatter(logging.Formatter):
    """Render text manager records with the manager id as a line prefix."""

    def __init__(self) -> None:
        super().__init__("%(manager_id)s %(asctime)s %(levelname)-7s %(name)s: %(message)s")


class _ManagerJsonFormatter(JsonFormatter):
    """Render JSON manager records using the shared report representation."""


class _ManagerLogHandler(logging.handlers.WatchedFileHandler):
    """Write and periodically rotate this manager's own log."""

    def __init__(self, path: Path) -> None:
        super().__init__(path, mode="a", encoding="utf-8")
        self._records = 0

    def _open(self):
        """Open the log in append mode and secure the opened stream."""

        stream = super()._open()
        os.fchmod(stream.fileno(), 0o600)
        return stream

    def emit(self, record: logging.LogRecord) -> None:
        """Rotate an oversized log periodically, then emit the record."""

        self._records += 1
        if self._records >= MANAGER_LOG_ROTATION_RECORDS:
            self._records = 0
            if self.stream is not None:
                self.stream.close()
                self.stream = None
            _rotate_manager_log(Path(self.baseFilename))
            self.stream = self._open()
            self._statstream()  # refresh dev/ino so the next emit does not reopen again
        super().emit(record)


def reset_logging() -> None:
    """Remove the handlers this module installed."""

    reset_reporting(PACKAGE_LOGGER)
