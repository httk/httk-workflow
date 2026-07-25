"""Private filesystem and JSON helpers."""

import hashlib
import json
import os
import tempfile
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .errors import FormatError


def utc_now() -> str:
    """Return a UTC timestamp in the protocol's RFC 3339 representation."""

    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def timestamp_seconds(value: str) -> float:
    """Parse a protocol timestamp."""

    from datetime import datetime

    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def json_bytes(value: object) -> bytes:
    """Encode compact, deterministic UTF-8 JSON."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def read_json(path: Path) -> dict[str, Any]:
    """Read a JSON object from *path*."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FormatError(f"cannot read JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise FormatError(f"expected JSON object in {path}")
    return value


def write_json_atomic(path: Path, value: object, *, durable: bool = False) -> None:
    """Atomically replace *path* with JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.tmp.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(json_bytes(value))
            handle.write(b"\n")
            handle.flush()
            if durable:
                os.fsync(handle.fileno())
        os.replace(temporary, path)
        if durable:
            fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def fsync_directory(path: Path) -> None:
    """Synchronize directory entries where the platform permits it."""

    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sha256_file(path: Path) -> str:
    """Return the lowercase SHA-256 digest of one regular file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def tree_digest(path: Path) -> str:
    """Hash a tree without following symlinks."""

    digest = hashlib.sha256()
    for entry in sorted(path.rglob("*"), key=lambda item: item.relative_to(path).as_posix()):
        relative = entry.relative_to(path).as_posix().encode()
        if entry.is_symlink():
            raise FormatError(f"symlink is forbidden in immutable bundle: {entry}")
        if entry.is_dir():
            digest.update(b"D\0" + relative + b"\0")
        elif entry.is_file():
            digest.update(b"F\0" + relative + b"\0")
            with entry.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        else:
            raise FormatError(f"special file is forbidden in immutable bundle: {entry}")
    return digest.hexdigest()


def require_mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise FormatError(f"{name} must be an object")
    return value


def require_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise FormatError(f"{name} must be a nonempty string")
    return value


def require_int(value: object, name: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise FormatError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        limit = f"..{maximum}" if maximum is not None else " or greater"
        raise FormatError(f"{name} must be {minimum}{limit}")
    return value


def retry_delay(attempt: int) -> float:
    """Small bounded delay for metadata visibility retries."""

    return min(0.01 * (2**attempt), 0.25)


def wait_for_path(path: Path, *, attempts: int = 6) -> bool:
    """Reopen and retry a path lookup."""

    for attempt in range(attempts):
        if path.exists():
            return True
        time.sleep(retry_delay(attempt))
    return path.exists()
