"""Deterministic signed project manifests."""

import base64
import bz2
import fnmatch
import hashlib
import json
import os
import socket
import stat
import tempfile
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from httk.core import ed25519_public_key, ed25519_sign, ed25519_verify

from ._util import json_bytes, retry_delay, sha256_file, timestamp_seconds, utc_now
from .projects import (
    discover_project,
    project_exclusions,
    read_project,
    require_project,
)
from .workspace import WorkflowWorkspace

_DOMAIN = b"httk-project-manifest-v2\0"

MAINTENANCE_LOCK_FILE = "maintenance.lock"
MAINTENANCE_LOCK_MAX_AGE_SECONDS = 24 * 60 * 60


def _excluded(path: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _records(root: Path, patterns: Sequence[str]) -> Iterator[dict[str, object]]:
    """Yield sorted records without following symlinks."""

    paths: list[Path] = []

    def visit(directory: Path) -> None:
        with os.scandir(directory) as entries:
            for raw in entries:
                entry = Path(raw.path)
                relative = entry.relative_to(root).as_posix()
                if _excluded(relative, patterns):
                    continue
                mode = raw.stat(follow_symlinks=False).st_mode
                paths.append(entry)
                if stat.S_ISDIR(mode):
                    visit(entry)

    visit(root)
    for entry in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        relative = entry.relative_to(root).as_posix()
        mode = entry.lstat().st_mode
        if stat.S_ISLNK(mode):
            yield {"path": relative, "type": "symlink", "target": os.readlink(entry)}
        elif stat.S_ISDIR(mode):
            yield {"path": relative, "type": "directory"}
        elif stat.S_ISREG(mode):
            yield {
                "path": relative,
                "type": "file",
                "size": entry.stat().st_size,
                "sha256": sha256_file(entry),
            }
        else:
            raise ValueError(f"project manifest rejects special filesystem entry: {relative}")


def _seed(project: Path) -> bytes:
    path = project / ".httk-project" / "keys" / "project.seed"
    try:
        seed = base64.b64decode(path.read_text(encoding="ascii").strip(), validate=True)
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read project signing seed: {path}") from exc
    if len(seed) != 32:
        raise ValueError("project private key is not a standard 32-byte Ed25519 seed")
    return seed


@dataclass(frozen=True)
class MaintenanceLock:
    """Recorded holder of one workspace maintenance lock."""

    path: Path
    pid: int | None
    hostname: str | None
    created: str | None

    @property
    def age_seconds(self) -> float | None:
        """Age of the lock, or ``None`` when its timestamp is unusable."""

        if self.created is None:
            return None
        try:
            return max(0.0, time.time() - timestamp_seconds(self.created))
        except ValueError:
            return None

    @property
    def local(self) -> bool:
        """Whether the recorded host is the host inspecting the lock."""

        return self.hostname is not None and self.hostname == socket.gethostname()

    @property
    def dead(self) -> bool:
        """Whether a same-host holder process is known to be gone."""

        if not self.local or self.pid is None or self.pid <= 0:
            return False
        try:
            os.kill(self.pid, 0)
        except ProcessLookupError:
            return True
        except OSError:
            # A live process owned by another user still holds the lock.
            return False
        return False

    def is_stale(self, *, max_age_seconds: float = MAINTENANCE_LOCK_MAX_AGE_SECONDS) -> bool:
        """Whether the lock can be reclaimed without operator confirmation."""

        if self.pid is None or self.hostname is None or self.created is None:
            return True
        if self.dead:
            return True
        age = self.age_seconds
        return age is None or age > max_age_seconds

    def describe(self) -> str:
        """Describe the holder for an operator diagnostic."""

        who = "an unrecorded process" if self.pid is None else f"pid {self.pid}"
        where = "an unrecorded host" if self.hostname is None else f"host {self.hostname}"
        when = "an unrecorded time" if self.created is None else self.created
        age = self.age_seconds
        return f"{who} on {where} since {when}" + ("" if age is None else f" (age {age:.0f}s)")


def _read_maintenance_lock(path: Path) -> MaintenanceLock | None:
    """Describe an existing lock, retrying to distinguish races from damage."""

    for attempt in range(4):
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError):
            raw = ""
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = None
        if isinstance(value, dict):
            pid = value.get("pid")
            hostname = value.get("hostname")
            created = value.get("created")
            return MaintenanceLock(
                path=path,
                pid=pid if isinstance(pid, int) and not isinstance(pid, bool) else None,
                hostname=hostname if isinstance(hostname, str) and hostname else None,
                created=created if isinstance(created, str) and created else None,
            )
        time.sleep(retry_delay(attempt))
    # Legacy plain-pid content, truncation, or a foreign writer: reclaimable.
    return MaintenanceLock(path=path, pid=None, hostname=None, created=None)


def read_maintenance_lock(workspace: WorkflowWorkspace) -> MaintenanceLock | None:
    """Describe the workspace maintenance lock, or ``None`` when it is absent."""

    return _read_maintenance_lock(workspace.control / MAINTENANCE_LOCK_FILE)


def _acquire_maintenance_lock(path: Path) -> None:
    """Create the lock exclusively, reclaiming a provably stale predecessor."""

    body = json_bytes({"created": utc_now(), "hostname": socket.gethostname(), "pid": os.getpid()}) + b"\n"
    for _ in range(3):
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            holder = _read_maintenance_lock(path)
            if holder is not None and not holder.is_stale():
                raise ValueError(
                    "project maintenance is already in progress; the maintenance lock is held by "
                    f"{holder.describe()}; release it with 'httk workflow workspace unlock' once that "
                    "operation is known to be finished"
                ) from None
            path.unlink(missing_ok=True)
            continue
        try:
            # One write keeps readers from ever observing a partial record.
            os.write(descriptor, body)
        finally:
            os.close(descriptor)
        return
    raise ValueError(f"cannot acquire the project maintenance lock: {path}")


def release_maintenance_lock(workspace: WorkflowWorkspace, *, force: bool = False) -> str:
    """Remove a stale, or with *force* any, maintenance lock and report it."""

    path = workspace.control / MAINTENANCE_LOCK_FILE
    holder = read_maintenance_lock(workspace)
    if holder is None:
        return f"no maintenance lock is present: {path}"
    stale = holder.is_stale()
    if not stale and not force:
        raise ValueError(f"maintenance lock is held by {holder.describe()}; rerun with --force to remove it anyway")
    path.unlink(missing_ok=True)
    return f"removed {'stale' if stale else 'live'} maintenance lock held by {holder.describe()}: {path}"


@contextmanager
def workspace_maintenance_guard(workspace: WorkflowWorkspace) -> Iterator[None]:
    """Fence manager launches while a project snapshot is inspected."""

    path = workspace.control / MAINTENANCE_LOCK_FILE
    _acquire_maintenance_lock(path)
    try:
        unresolved = list(workspace.scan_markers(("claimed", "running", "committing", "transferring")))
        if unresolved:
            states = ", ".join(f"{item.job_key}:{item.kind}" for item in unresolved)
            raise ValueError(f"manifest requires a quiescent workspace; unresolved work: {states}")
        yield
    finally:
        path.unlink(missing_ok=True)


def create_manifest(
    project: str | os.PathLike[str] | None = None,
    *,
    output: str | os.PathLike[str] | None = None,
) -> Path:
    """Create and atomically publish the signed v2 project manifest."""

    root = require_project(project)
    metadata = read_project(root)
    destination = (
        Path(output).expanduser().resolve() if output is not None else root / ".httk-project" / "manifest.jsonl.bz2"
    )
    exclusions = project_exclusions(metadata)
    if destination.is_relative_to(root):
        exclusions = (*exclusions, destination.relative_to(root).as_posix())
    workspace = WorkflowWorkspace(root)
    with workspace_maintenance_guard(workspace):
        seed = _seed(root)
        header = {
            "format": "httk-project-manifest",
            "format_version": 2,
            "project_id": metadata["project_id"],
            "hash": "sha256",
            "signature": "ed25519",
            "public_key": base64.b64encode(ed25519_public_key(seed)).decode("ascii"),
            "exclusions": list(exclusions),
        }
        body = b"".join(json_bytes(record) + b"\n" for record in (header, *_records(root, exclusions)))
        body_digest = hashlib.sha256(_DOMAIN + body).digest()
        trailer = {
            "body_sha256": body_digest.hex(),
            "signature": base64.b64encode(ed25519_sign(seed, body_digest)).decode("ascii"),
        }
        compressed = bz2.compress(body + json_bytes(trailer) + b"\n", compresslevel=9)
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.tmp.", dir=destination.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(compressed)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
    return destination


def _parse_v2(path: Path) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object], bytes]:
    try:
        raw = bz2.decompress(path.read_bytes())
    except (OSError, EOFError) as exc:
        raise ValueError(f"cannot decompress manifest: {path}") from exc
    lines = raw.splitlines(keepends=True)
    if len(lines) < 2 or any(not line.endswith(b"\n") for line in lines):
        raise ValueError("manifest must contain complete canonical JSON lines")
    values: list[dict[str, object]] = []
    for line in lines:
        try:
            value = json.loads(line)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("manifest contains invalid JSON") from exc
        if not isinstance(value, dict) or json_bytes(value) + b"\n" != line:
            raise ValueError("manifest line is not canonical JSON")
        values.append(value)
    header, trailer = values[0], values[-1]
    if header.get("format") != "httk-project-manifest" or header.get("format_version") != 2:
        raise ValueError("not a v2 httk project manifest")
    return header, values[1:-1], trailer, b"".join(lines[:-1])


def verify_v2_manifest(root: Path, path: Path) -> bool:
    header, records, trailer, body = _parse_v2(path)
    exclusions = header.get("exclusions")
    if not isinstance(exclusions, list) or not all(isinstance(item, str) for item in exclusions):
        raise ValueError("manifest exclusions must be an array of strings")
    digest = hashlib.sha256(_DOMAIN + body).digest()
    if trailer.get("body_sha256") != digest.hex():
        return False
    try:
        public_key = base64.b64decode(str(header["public_key"]), validate=True)
        signature = base64.b64decode(str(trailer["signature"]), validate=True)
    except (KeyError, ValueError):
        return False
    if not ed25519_verify(public_key, digest, signature):
        return False
    actual = list(_records(root, exclusions))
    return records == actual


def _legacy_file_digest(path: Path) -> str:
    return sha256_file(path)


def verify_legacy_manifest(root: Path, path: Path) -> bool:
    """Verify a legacy manifest without modifying its project tree."""

    try:
        raw = bz2.decompress(path.read_bytes())
    except (OSError, EOFError):
        return False
    lines = raw.splitlines(keepends=True)
    try:
        first_blank = lines.index(b"\n")
        second_blank = lines.index(b"\n", first_blank + 1)
    except ValueError:
        return False
    if first_blank == 0 or second_blank + 1 >= len(lines):
        return False
    signed = b"".join(lines[: first_blank + 1] + lines[first_blank + 1 : second_blank])
    try:
        public_key = base64.b64decode(lines[0].strip(), validate=True)
        signature = base64.b64decode(lines[second_blank + 1].strip(), validate=True)
    except ValueError:
        return False
    if not ed25519_verify(public_key, signed, signature):
        return False
    for line in lines[first_blank + 1 : second_blank]:
        try:
            digest, relative_bytes = line.rstrip(b"\n").split(b" ", 1)
            relative = relative_bytes.decode("utf-8")
        except (ValueError, UnicodeError):
            return False
        target = root / relative.rstrip("/")
        manifest_target = target / "ht.manifest.bz2" if relative.endswith("/") else target
        if not manifest_target.is_file() or _legacy_file_digest(manifest_target) != digest.decode("ascii"):
            return False
        if relative.endswith("/") and not verify_legacy_manifest(target, manifest_target):
            return False
    return True


def verify_manifest(
    project: str | os.PathLike[str] | None = None,
    *,
    manifest: str | os.PathLike[str] | None = None,
) -> bool:
    """Auto-detect and verify a v2 or legacy project manifest."""

    supplied = Path(project).expanduser().resolve() if project is not None else Path.cwd().resolve()
    v2_root = discover_project(supplied)
    if v2_root is None:
        start = supplied.parent if supplied.is_file() else supplied
        legacy_root = next(
            (
                candidate
                for candidate in (start, *start.parents)
                if (candidate / "ht.project" / "manifest.bz2").is_file()
            ),
            None,
        )
        if legacy_root is None:
            raise ValueError("no v2 or legacy httk project exists at or above the working directory")
        path = (
            Path(manifest).expanduser().resolve()
            if manifest is not None
            else legacy_root / "ht.project" / "manifest.bz2"
        )
        return verify_legacy_manifest(legacy_root, path)
    if manifest is not None:
        path = Path(manifest).expanduser().resolve()
    else:
        path = v2_root / ".httk-project" / "manifest.jsonl.bz2"
        if not path.is_file():
            legacy = v2_root / "ht.project" / "manifest.bz2"
            if legacy.is_file():
                return verify_legacy_manifest(v2_root, legacy)
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        return verify_v2_manifest(v2_root, path)
    except ValueError as exc:
        if "not a v2" not in str(exc):
            raise
        return verify_legacy_manifest(v2_root, path)
