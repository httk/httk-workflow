"""Deterministic signed project manifests."""

import base64
import bz2
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
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

from . import _death, _kernel
from .models import TRANSFER_DIRECTORY, is_payload_private
from .projects import (
    PROJECT_DIRECTORY,
    discover_project,
    format_public_key,
)
from .workspace import Workspace

__all__ = [
    "ManifestVerification",
    "payload_file_records",
    "require_quiescent_workspace",
    "verify_legacy_manifest",
    "verify_manifest",
    "workspace_maintenance_guard",
]

#: Owner-written entries at a job's root that no payload record covers (besides the runner-private ones).
_UNRECORDED = frozenset({TRANSFER_DIRECTORY, "state.json", "seal.json"})


def payload_file_records(root: Path) -> list[dict[str, object]]:
    """Return the deterministic records of one job payload, minus runner scratch and owner documents.

    The runner-private entries (``attempts/``, ``logs/``, ``.httk-job/``), the
    transfer envelope, the owner's ``state.json`` and a root ``seal.json`` are
    excluded from every seal record exactly as they are from a transfer payload
    digest, so committing an outcome, or moving the job out of its workspace
    and back, never changes a sealed payload's records.

    :param root: The job directory.
    :return: The file records.
    """

    base = Path(root)
    return file_records(
        base, skip=lambda entry: entry.parent == base and (is_payload_private(entry.name) or entry.name in _UNRECORDED)
    )


def require_quiescent_workspace(workspace: Workspace) -> None:
    """Refuse while any owner of the workspace is not proven dead; read-only, holds nothing.

    A closed owner leaves no ``owners/<id>/``; a dead one is reclaimed by
    ``httk workspace gc``. The check is a snapshot: an owner registering
    afterwards is not fenced, which suits manifests and project snapshots of a
    workspace nobody is running.

    :param workspace: The workspace.
    :raises ValueError: If an owner is alive or its death cannot be proven.
    """

    scheduler, here = _death.SchedulerQueries(), _death.process_identity()
    busy = [
        record.owner_id
        for record in _kernel.list_owners(workspace)
        if _death.probe(record.path, visibility_deadline=0.0, scheduler=scheduler, here=here)[0]
        is not _death.Liveness.DEAD
    ]
    if busy:
        raise ValueError(f"manifest requires a quiescent workspace; owners not proven dead: {', '.join(busy)}")


@contextmanager
def workspace_maintenance_guard(workspace: Workspace) -> Iterator[None]:
    """Run :func:`require_quiescent_workspace` before the guarded work (no lock is held).

    :param workspace: The workspace.
    :return: A context manager.
    :raises ValueError: If an owner is alive or its death cannot be proven.
    """

    require_quiescent_workspace(workspace)
    yield


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
