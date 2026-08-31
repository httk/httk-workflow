"""Deterministic signed project manifests."""

import base64
import bz2
import json
import os
import socket
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from httk.core.crypto import ed25519_verify
from httk.core.digests import sha256_file
from httk.core.project import LegacyProjectError
from httk.core.project.manifests import (
    INVALID,
    VALID_UNKNOWN_KEY,
    ManifestVerification,
    resolve_trusted_keys,
    verdict_for_key,
)
from httk.core.project.manifests import verify_manifest as _core_verify_manifest
from httk.core.records import file_records

from ._util import json_bytes, retry_delay, timestamp_seconds, utc_now
from .models import (
    QUIESCENT_KINDS,
    STATE_KINDS,
    is_payload_private,
)
from .projects import (
    PROJECT_DIRECTORY,
    discover_project,
    format_public_key,
)
from .workspace import Workspace

__all__ = [
    "MAINTENANCE_LOCK_FILE",
    "MAINTENANCE_LOCK_MAX_AGE_SECONDS",
    "MaintenanceLock",
    "ManifestVerification",
    "payload_file_records",
    "read_maintenance_lock",
    "release_maintenance_lock",
    "verify_legacy_manifest",
    "verify_manifest",
    "workspace_maintenance_guard",
]

MAINTENANCE_LOCK_FILE = "maintenance.lock"
MAINTENANCE_LOCK_MAX_AGE_SECONDS = 24 * 60 * 60


def payload_file_records(root: Path) -> list[dict[str, object]]:
    """Return the deterministic records of one job payload, minus runner scratch.

    A payload's runner-private entries — attempt control, logs, and job state —
    are excluded from every seal record exactly as they are from a payload
    digest, so publishing an outcome never changes a sealed payload's records.
    """

    base = Path(root)
    return file_records(base, skip=lambda entry: entry.parent == base and is_payload_private(entry.name))


@dataclass(frozen=True)
class MaintenanceLock:
    """Record the holder of one workspace maintenance lock.

    :param path: Locate the lock file.
    :param pid: Record the holder process identifier, when readable.
    :param hostname: Record the holder host, when readable.
    :param created: Record the holder creation timestamp, when readable.
    :param readable: Mark whether the lock contents could be read.
    """

    path: Path
    pid: int | None
    hostname: str | None
    created: str | None
    readable: bool = True

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

        if not self.readable:
            return False
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
        except PermissionError:
            return MaintenanceLock(path=path, pid=None, hostname=None, created=None, readable=False)
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


def read_maintenance_lock(workspace: Workspace) -> MaintenanceLock | None:
    """Describe the workspace maintenance lock, or ``None`` when it is absent.

    :param workspace: Locate the workspace whose lock to inspect.
    :return: The recorded lock, or ``None`` when no lock exists.
    """

    return _read_maintenance_lock(workspace.control / MAINTENANCE_LOCK_FILE)


def _acquire_maintenance_lock(path: Path) -> None:
    """Create the lock exclusively, reclaiming a provably stale predecessor."""

    body = json_bytes({"created": utc_now(), "hostname": socket.gethostname(), "pid": os.getpid()}) + b"\n"
    for _ in range(3):
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            holder = _read_maintenance_lock(path)
            if holder is not None and not holder.is_stale():
                raise ValueError(
                    "project maintenance is already in progress; the maintenance lock is held by "
                    f"{holder.describe()}; release it with 'httk workspace unlock' once that "
                    "operation is known to be finished"
                ) from None
            path.unlink(missing_ok=True)
            continue
        try:
            # One write keeps readers from ever observing a partial record.
            os.fchmod(descriptor, 0o644)
            os.write(descriptor, body)
        finally:
            os.close(descriptor)
        return
    raise ValueError(f"cannot acquire the project maintenance lock: {path}")


def release_maintenance_lock(workspace: Workspace, *, force: bool = False) -> str:
    """Remove a stale, or with *force* any, maintenance lock and report it.

    :param workspace: Locate the workspace whose lock to remove.
    :param force: Permit removal of a lock that does not appear stale.
    :return: A human-readable removal result.
    :raises ValueError: If a live lock is protected by the default policy.
    """

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
def workspace_maintenance_guard(workspace: Workspace) -> Iterator[None]:
    """Fence manager launches while a project snapshot is inspected.

    :param workspace: Lock and inspect this workspace around the guarded work.
    :return: A context manager that holds the maintenance lock.
    :raises ValueError: If the workspace is already maintained or not quiescent.
    """

    path = workspace.control / MAINTENANCE_LOCK_FILE
    _acquire_maintenance_lock(path)
    try:
        guarded_kinds = tuple(kind for kind in STATE_KINDS if kind not in QUIESCENT_KINDS)
        unresolved = list(workspace.scan_markers(guarded_kinds))
        if unresolved:
            states = ", ".join(f"{item.job_key}:{item.kind}" for item in unresolved)
            raise ValueError(f"manifest requires a quiescent workspace; unresolved work: {states}")
        yield
    finally:
        path.unlink(missing_ok=True)


def _legacy_file_digest(path: Path) -> str:
    return sha256_file(path)


def _legacy_public_key(path: Path) -> str | None:
    """Return the public key one legacy manifest names, when it is readable."""

    try:
        raw = bz2.decompress(path.read_bytes())
        return format_public_key(base64.b64decode(raw.splitlines()[0].strip(), validate=True))
    except (OSError, EOFError, IndexError, ValueError):
        return None


def verify_legacy_manifest(root: Path, path: Path) -> bool:
    """Verify a legacy manifest without modifying its project tree.

    :param root: Locate the tree the manifest should describe.
    :param path: Locate the legacy manifest to verify.
    :return: Whether the legacy tree records and signature verify.
    """

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


def _verify_legacy(root: Path, path: Path, trusted: Sequence[str]) -> ManifestVerification:
    """Verify a legacy manifest and classify the identity that signed it."""

    public_key = _legacy_public_key(path)
    if not verify_legacy_manifest(root, path):
        return ManifestVerification(
            INVALID,
            "the legacy manifest does not verify against this tree",
            path,
            "legacy",
            public_key,
            tuple(trusted),
        )
    if public_key is None:
        return ManifestVerification(
            VALID_UNKNOWN_KEY,
            "the legacy manifest verifies, but its signing key could not be read back",
            path,
            "legacy",
            None,
            tuple(trusted),
        )
    return verdict_for_key(public_key, trusted, manifest=path, manifest_format="legacy")


def verify_manifest(
    project: str | os.PathLike[str] | None = None,
    *,
    manifest: str | os.PathLike[str] | None = None,
    trusted_keys: Sequence[str | os.PathLike[str]] | None = None,
) -> ManifestVerification:
    """Auto-detect a v2 or legacy manifest and verify it against its trust anchors.

    The trust anchor is the key pinned in ``project.json`` — never the key the
    manifest being verified names in its own header — plus any key passed in
    *trusted_keys*, as a recorded value or as the path of a ``*.pub`` file.

    :param project: Locate the project to discover and verify.
    :param manifest: Select a manifest path instead of the project default.
    :param trusted_keys: Add explicit trust anchors to the project keys.
    :return: The detailed verification verdict.
    :raises ValueError: If no project or usable manifest exists.
    """

    supplied = Path(project).expanduser().resolve() if project is not None else Path.cwd().resolve()
    try:
        v2_root = discover_project(supplied)
    except LegacyProjectError as error:
        # A v1 ht.project has a read-only legacy verification path here.
        path = (
            Path(manifest).expanduser().resolve()
            if manifest is not None
            else error.root / "ht.project" / "manifest.bz2"
        )
        if not path.is_file():
            raise ValueError(f"the v1 project at {error.root} has no manifest to verify: {path}") from error
        return _verify_legacy(error.root, path, resolve_trusted_keys(None, trusted_keys=trusted_keys))
    if v2_root is None:
        raise ValueError("no v2 or legacy httk project exists at or above the working directory")
    trusted = resolve_trusted_keys(v2_root, trusted_keys=trusted_keys)
    # A v2 project may still hold only a legacy manifest at the default location.
    if manifest is None and not (v2_root / PROJECT_DIRECTORY / "manifest.jsonl.bz2").is_file():
        legacy = v2_root / "ht.project" / "manifest.bz2"
        if legacy.is_file():
            return _verify_legacy(v2_root, legacy, trusted)
    # The v2 verification is core's; a manifest that is not v2 falls back to legacy.
    try:
        return _core_verify_manifest(v2_root, manifest=manifest, trusted_keys=trusted_keys)
    except ValueError as exc:
        if "not a v2" not in str(exc):
            raise
        path = (
            Path(manifest).expanduser().resolve()
            if manifest is not None
            else v2_root / PROJECT_DIRECTORY / "manifest.jsonl.bz2"
        )
        return _verify_legacy(v2_root, path, trusted)
