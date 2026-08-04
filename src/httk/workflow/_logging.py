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


def configure_logging(*, level: str | int = "warning", json_logs: bool = False) -> None:
    """Install one console handler for the workflow logger hierarchy."""

    configure_reporting(level=level, json_logs=json_logs, logger=PACKAGE_LOGGER)


def add_log_file(
    path: Path,
    *,
    level: str | int = "info",
    json_logs: bool = False,
    maximum_bytes: int = DEFAULT_MAXIMUM_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> Path:
    """Add one rotating file handler and return the path it writes."""

    return add_report_file(
        path,
        level=level,
        json_logs=json_logs,
        maximum_bytes=maximum_bytes,
        backup_count=backup_count,
        logger=PACKAGE_LOGGER,
    )


def reset_logging() -> None:
    """Remove the handlers this module installed."""

    reset_reporting(PACKAGE_LOGGER)
