"""Private filesystem and JSON helpers."""

import hashlib
import json
import math
import os
import tempfile
import time
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

from .errors import FormatError

#: The visibility deadline used by a caller that has no workspace policy to
#: read. It is a local-filesystem value: a shared filesystem needs the far
#: larger deadline its workspace policy configures.
DEFAULT_VISIBILITY_DEADLINE_SECONDS = 5.0
#: The first backoff step of every visibility retry schedule. The first probes
#: stay short so a local rename or read that is simply not yet settled costs
#: milliseconds rather than the whole deadline.
FIRST_RETRY_DELAY_SECONDS = 0.01
#: The longest single sleep in a visibility retry schedule, so that even a
#: minute-long deadline keeps re-probing instead of blocking on one long sleep.
MAXIMUM_RETRY_DELAY_SECONDS = 5.0


def utc_now() -> str:
    """Return a UTC timestamp in the protocol's RFC 3339 representation."""

    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def timestamp_seconds(value: str) -> float:
    """Parse a protocol timestamp."""

    from datetime import datetime

    return datetime.fromisoformat(value).timestamp()


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


def fsync_file(path: Path) -> None:
    """Synchronize one regular file's contents to storage."""

    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_tree(root: Path) -> None:
    """Synchronize every regular file and directory below *root*, and *root*.

    This is the batched counterpart of :func:`write_json_atomic`'s per-file
    durability: a publication that stages a whole tree — an outcome draft, a
    sealed workdir batch — synchronizes it once, just before the atomic rename
    that makes it authoritative, rather than paying an ``fsync`` per staged
    write. Each directory is synchronized exactly once. Symlinks are skipped:
    the protocol trees this walks never contain them, and a symlink carries no
    file contents of its own to flush.
    """

    directories: list[Path] = [root]
    for entry in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if entry.is_symlink():
            continue
        if entry.is_dir():
            directories.append(entry)
        elif entry.is_file():
            fsync_file(entry)
    for directory in directories:
        fsync_directory(directory)


def sha256_file(path: Path) -> str:
    """Return the lowercase SHA-256 digest of one regular file."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def tree_digest(path: Path, *, skip: Callable[[str], bool] | None = None) -> str:
    """Hash a tree without following symlinks.

    *skip* names top-level entries to leave out of the digest entirely, which is
    how a payload digest ignores the runner-private directories that live inside
    a payload without being part of the job.
    """

    digest = hashlib.sha256()
    for entry in sorted(path.rglob("*"), key=lambda item: item.relative_to(path).as_posix()):
        parts = entry.relative_to(path).parts
        if skip is not None and parts and skip(parts[0]):
            continue
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


def validate_parameters(value: object, name: str = "parameters") -> dict[str, str | None]:
    """Validate a creation-parameter declaration."""

    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    result: dict[str, str | None] = {}
    for parameter, destination in value.items():
        if not isinstance(parameter, str) or not parameter:
            raise ValueError(f"{name} names must be nonempty strings")
        if destination is not None and (not isinstance(destination, str) or not destination):
            raise ValueError(f"{name} destination for {parameter!r} must be a nonempty string or null")
        result[parameter] = destination
    return result


def require_int(value: object, name: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise FormatError(f"{name} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        limit = f"..{maximum}" if maximum is not None else " or greater"
        raise FormatError(f"{name} must be {minimum}{limit}")
    return value


def require_number(value: object, name: str, *, minimum: float = 0.0, maximum: float | None = None) -> float:
    """Validate one JSON number, accepting an integer as the float it denotes."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FormatError(f"{name} must be a number")
    number = float(value)
    if math.isnan(number) or number in (float("inf"), float("-inf")):
        raise FormatError(f"{name} must be a finite number")
    if number < minimum or (maximum is not None and number > maximum):
        limit = f"..{maximum}" if maximum is not None else " or greater"
        raise FormatError(f"{name} must be {minimum}{limit}")
    return number


def retry_delay(attempt: int) -> float:
    """Small bounded delay for metadata visibility retries."""

    return min(0.01 * (2**attempt), 0.25)


def visibility_schedule(deadline_seconds: float | None = None) -> tuple[float, ...]:
    """Return the backoff delays that spend one visibility deadline.

    The schedule doubles from :data:`FIRST_RETRY_DELAY_SECONDS` and is clipped
    so the delays sum to exactly the deadline: fast first probes for the
    ordinary local case, then longer ones for a network filesystem whose
    attribute cache may hold a stale answer for tens of seconds. The returned
    tuple holds the waits *between* attempts, so a caller performs one more
    attempt than there are delays.
    """

    deadline = DEFAULT_VISIBILITY_DEADLINE_SECONDS if deadline_seconds is None else max(0.0, deadline_seconds)
    delays: list[float] = []
    remaining = deadline
    delay = FIRST_RETRY_DELAY_SECONDS
    while remaining > 0.0:
        step = min(delay, remaining)
        delays.append(step)
        remaining -= step
        delay = min(delay * 2.0, MAXIMUM_RETRY_DELAY_SECONDS)
    return tuple(delays)


def visibility_attempts(deadline_seconds: float | None = None) -> Iterator[int]:
    """Yield attempt ordinals, sleeping the visibility backoff between them.

    Leaving the loop early — by returning the value that finally became
    visible — skips the remaining sleeps, so a schedule sized for a network
    filesystem costs nothing when the first probe already succeeds.
    """

    schedule = visibility_schedule(deadline_seconds)
    for index, delay in enumerate(schedule):
        yield index
        time.sleep(delay)
    yield len(schedule)


def wait_for_path(path: Path, *, deadline_seconds: float | None = None) -> bool:
    """Reopen and retry a path lookup until the visibility deadline expires."""

    for _ in visibility_attempts(deadline_seconds):
        if path.exists():
            return True
    return path.exists()
