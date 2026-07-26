"""Logging configuration for the :mod:`httk.workflow` logger hierarchy.

Library code only ever calls :func:`logging.getLogger` and never installs a
handler. A manager process opts into diagnostics by calling
:func:`configure_logging` once and, when the manager identity is known,
:func:`add_log_file`.
"""

import json
import logging
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

PACKAGE_LOGGER = "httk.workflow"
LOG_LEVELS = ("debug", "info", "warning", "error", "critical")
DEFAULT_MAXIMUM_BYTES = 4 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 3

_TEXT_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
_MARK = "_httk_workflow_handler"
# Attributes every LogRecord carries. Everything else was supplied through the
# ``extra`` argument and belongs in the structured representation.
_STANDARD_MEMBERS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    }
)


class JsonFormatter(logging.Formatter):
    """Render one record as a single line of JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_MEMBERS or key.startswith("_") or key in payload:
                continue
            payload[key] = value
        if record.exc_info is not None:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)


def resolve_level(level: str | int) -> int:
    """Return the numeric level for a protocol log-level name."""

    if isinstance(level, int):
        return level
    try:
        return logging.getLevelNamesMapping()[level.upper()]
    except KeyError:
        raise ValueError(f"unknown log level: {level!r}") from None


def _formatter(*, json_logs: bool) -> logging.Formatter:
    return JsonFormatter() if json_logs else logging.Formatter(_TEXT_FORMAT)


def _install(handler: logging.Handler, level: int) -> None:
    logger = logging.getLogger(PACKAGE_LOGGER)
    handler.setLevel(level)
    setattr(handler, _MARK, True)
    logger.addHandler(handler)
    logger.propagate = False
    if logger.level == logging.NOTSET or level < logger.level:
        logger.setLevel(level)


def configure_logging(*, level: str | int = "warning", json_logs: bool = False) -> None:
    """Install one console handler for the workflow logger hierarchy."""

    reset_logging()
    handler = logging.StreamHandler()
    handler.setFormatter(_formatter(json_logs=json_logs))
    _install(handler, resolve_level(level))


def add_log_file(
    path: Path,
    *,
    level: str | int = "info",
    json_logs: bool = False,
    maximum_bytes: int = DEFAULT_MAXIMUM_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> Path:
    """Add one rotating file handler and return the path it writes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        path,
        maxBytes=maximum_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(_formatter(json_logs=json_logs))
    _install(handler, resolve_level(level))
    return path


def reset_logging() -> None:
    """Remove the handlers this module installed."""

    logger = logging.getLogger(PACKAGE_LOGGER)
    for handler in list(logger.handlers):
        if not getattr(handler, _MARK, False):
            continue
        logger.removeHandler(handler)
        handler.close()
    if not logger.handlers:
        logger.setLevel(logging.NOTSET)
        logger.propagate = True
