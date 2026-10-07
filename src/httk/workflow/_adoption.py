"""The adoption chain (plan 4.5): every import of a bundle into a workspace, without any lock.

All state of one adoption lives in one private staging directory, its
*lineage* ``tmp/import.<owner>.<claim_ns>.<L>`` (``S``):

====  ===============================================================  ============================
Step  What happens                                                     Mechanism
====  ===============================================================  ============================
V1    ``S`` is created with ``intent.json``                            ``mkdir``, ``link_new``
V2    the bundle is claimed into ``S/bundle``                          one rename (or a copy, then a rename)
V3    its directories are made writable (after V4, which bounds it)    ``fchmod`` through descriptors
V4    it is verified                                                   :func:`~httk.workflow._bundle.verify_bundle`
V5    presence: replay, wait, refuse, expired, or proceed              name lookups
V6    one per-job claim ``transfers/adopting/<job_id>`` per job        ``link(2)`` of ``S/claims/<id>``
V7    presence again, with the claims held                             name lookups
V8    ``S/bundle/.httk-transfer`` becomes ``S/envelope``               one rename
V9    runners, payloads (members, then the root), frames, markers      single-source renames
V10   refusal: the bundle goes back (or to ``rejected/``)              renames
V11   receipt, source removal, acknowledgement, claim release, trash   idempotent
====  ===============================================================  ============================

Every step after the claim takes its source from inside ``S``, so renaming
``S`` (a takeover by an actor that found its owner gone) fences the previous
owner at its next step: its renames, claims and releases all name paths below
the old name. A per-job claim is a hard link of ``S/claims/<job_id>`` and is
"ours" exactly when ``transfers/adopting/<job_id>`` has the same ``st_ino``;
it is released by renaming it into ``S/released/``, which only the current
holder of ``S``'s name can do.
"""

import errno
import json
import logging
import os
import secrets
import shutil
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from httk.core.identity import identity_seed, sign_document

from . import _bundle, _txn
from ._bundle import (
    EXCHANGE_SOURCES,
    PACE_STRIDE,
    TRANSFER_DIRECTORY,
    TRANSFER_MANIFEST,
    TRANSFER_MARKERS,
    TRANSFER_RUNNERS,
    BundleManifest,
    BundleSource,
    VerifiedJob,
    _refuse_unsafe_entries,
    _runner_digest,
    verified_jobs,
    verify_bundle,
)
from ._receipts import (
    CLOCK_SKEW_NS,
    FRESHNESS_WINDOW_NS,
    read_receipt,
    receipt_document,
    receipt_path,
    record_receipt,
)
from ._util import fsync_directory, json_bytes, visibility_attempts
from .errors import FormatError, WorkflowError, WorkspaceCorruptionError
from .models import (
    STATE_KINDS,
    Marker,
    _read_regular_file,
    check_job_placement,
    normalize_placement,
    parse_placement_text,
    placement_text,
)
from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

INTENT = "intent.json"
BUNDLE = "bundle"
BUNDLE_PARTIAL = "bundle.partial"
ENVELOPE = "envelope"
CLAIMS = "claims"
RELEASED = "released"
#: The frame members a published job never takes from its prior state: the
#: header the import writes itself and the provenance it sets itself.
_NOT_CARRIED = frozenset(
    {
        "format",
        "format_version",
        "workspace_id",
        "job_id",
        "job_key",
        "placement",
        "state_generation",
        "kind",
        "previous_record_ref",
        "created_at",
        "priority",
        "transfer",
        "origin",
        "outgoing",
        "prior_kind",
        "prior_state",
    }
)
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_MANIFEST_LIMIT = 1 << 20
#: The current time in integer UTC nanoseconds; tests replace it to inject clocks.
_now: Callable[[], int] = time.time_ns


class _Fenced(Exception):
    """A step's source vanished: this actor no longer holds the lineage (a takeover)."""


# ---------------------------------------------------------------------------
# Sources and results
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Source:
    """Where one bundle is adopted from.

    :param kind: The verification source: ``"adopt"`` (a free-standing directory),
        ``"incoming"`` (an addressed bundle), ``"export"`` (a held export taken
        back), ``"exchange_inbox"`` (the exchange pass) or ``"exchange_outbox"``
        (an operator taking back an entry of this workspace's exchange outbox;
        a refusal of either exchange source is never put back by path).
    :param path: The bundle's path (for messages, and for putting a refused
        bundle back); with *directory_fd* only its name is used to claim it.
    :param copy: Copy the bundle in (another filesystem, or a bundle another
        workspace still holds) instead of renaming it.
    :param remove_source: Remove the copied source once the import is complete.
    :param directory_fd: Claim ``path.name`` relative to this directory descriptor.
    :param rejected_fd: Put a refused bundle below this directory descriptor (the
        exchange's ``outbox/rejected``) instead of back where it came from.
    """

    kind: BundleSource
    path: Path
    copy: bool = False
    remove_source: bool = False
    directory_fd: int | None = None
    rejected_fd: int | None = None


@dataclass(frozen=True)
class ExchangeDescriptors:
    """The open exchange directories an exchange-inbox adoption needs to be continued by recovery.

    A lineage whose bundle came from the exchange inbox is refused (V10) only
    into a fresh ``outbox/rejected/<unique>/`` reached through these
    descriptors, never by a path rename back into client territory; an actor
    without them leaves such a lineage alone.

    :param inbox_fd: The exchange ``inbox`` directory, opened ``O_NOFOLLOW``.
    :param rejected_fd: The exchange ``outbox/rejected`` directory, opened ``O_NOFOLLOW``.
    """

    inbox_fd: int
    rejected_fd: int


@dataclass(frozen=True)
class AdoptionResult:
    """The outcome of one adoption.

    :param status: ``"imported"``, ``"replay"`` (it had arrived already),
        ``"expired"`` (an addressed bundle past its window, discarded),
        ``"refused"`` (put back, or rejected), ``"waiting"`` (the lineage
        waits in ``tmp/`` for another transfer or adoption to finish) or
        ``"lost"`` (another actor took the bundle first).
    :param transfer_id: The bundle's transfer id, once known.
    :param job_id: The root job's id, once known.
    :param job_key: The root job's key, once known.
    :param acknowledgement: The acknowledgement document, when imported or replayed.
    :param reason: Why it was refused, waits or expired.
    :param error: The refusal, as the exception an API caller raises.
    :param lineage: The lineage directory, while it is left in place.
    """

    status: str
    transfer_id: str | None = None
    job_id: str | None = None
    job_key: str | None = None
    acknowledgement: dict[str, object] | None = None
    reason: str | None = None
    error: Exception | None = field(default=None, compare=False)
    lineage: Path | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON form reported per bundle.

        :return: The result.
        """

        return {
            "status": self.status,
            "transfer_id": self.transfer_id,
            "job_id": self.job_id,
            "job_key": self.job_key,
            "acknowledgement": self.acknowledgement,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class _Plan:
    """What the chain publishes, from the verified manifest and the intent."""

    manifest: BundleManifest
    jobs: tuple[VerifiedJob, ...]
    origin: str | None

    @property
    def transfer_id(self) -> str:
        return self.manifest.transfer_id

    @property
    def addressed(self) -> bool:
        return self.manifest.destination_workspace_id is not None

    @property
    def root(self) -> VerifiedJob:
        return self.jobs[0]

    def by_id(self) -> list[VerifiedJob]:
        return sorted(self.jobs, key=lambda job: job.job_id)


def _plan(manifest: BundleManifest, root: Path, intent: Mapping[str, Any]) -> _Plan:
    # Both exchange directories hold client-written content and get the exchange
    # checks; only a bundle the exchange pass adopts from the inbox gets exchange
    # origin (an operator's adoption of an outbox entry takes a job back for good).
    exchange = intent.get("source") == "exchange_inbox"
    jobs = verified_jobs(manifest, root, exchange=intent.get("source") in EXCHANGE_SOURCES)
    override = intent.get("placement")
    if override is not None:
        placement = parse_placement_text(override)
        if manifest.members:
            raise ValueError("a job tree keeps its placements; adopt it without --placement")
        check_job_placement(placement)
        jobs[0] = VerifiedJob(**{**jobs[0].__dict__, "placement": placement})
    return _Plan(manifest, tuple(jobs), "exchange" if exchange else None)


# ---------------------------------------------------------------------------
# Names and small helpers
# ---------------------------------------------------------------------------


def claims_directory(workspace: Workspace) -> Path:
    """Return ``transfers/adopting``, where every per-job claim lives.

    :param workspace: The adopting workspace.
    :return: The directory.
    """

    return workspace.control / "transfers" / "adopting"


def ack_path(workspace: Workspace, transfer_id: str) -> Path:
    """Return ``transfers/acks/<T>.json``.

    :param workspace: The adopting workspace.
    :param transfer_id: The transfer.
    :return: The acknowledgement path.
    """

    return workspace.control / "transfers" / "acks" / f"{transfer_id}.json"


def _lexists(path: Path) -> bool:
    try:
        os.lstat(path)
    except (FileNotFoundError, NotADirectoryError):
        return False
    return True


def _sync(workspace: Workspace, *directories: Path) -> None:
    if not workspace.durable:
        return
    for directory in dict.fromkeys(directories):
        try:
            fsync_directory(directory)
        except FileNotFoundError:
            pass


def _arrived_by(workspace: Workspace, job_id: str, transfer_id: str) -> Marker | None:
    """Return the job's marker when its current frame carries provenance *transfer_id*."""

    marker = workspace.find_marker_by_id(job_id, kinds=STATE_KINDS)
    if marker is None:
        return None
    provenance = workspace.read_state(marker).get("transfer")
    return marker if isinstance(provenance, Mapping) and provenance.get("transfer_id") == transfer_id else None


def _read_intent(lineage: Path) -> dict[str, Any]:
    value = json.loads(_read_regular_file(lineage / INTENT, 1 << 16))
    if not isinstance(value, dict):
        raise FormatError(f"{lineage / INTENT} is not an object")
    return value


def _read_manifest(envelope: Path) -> BundleManifest:
    return BundleManifest.from_mapping(json.loads(_read_regular_file(envelope / TRANSFER_MANIFEST, _MANIFEST_LIMIT)))


def lineage_name(owner: str, claimed_at: int, lineage: str) -> str:
    """Return the name of one lineage directory below ``tmp/``.

    :param owner: The owner token.
    :param claimed_at: When it was claimed, in integer UTC nanoseconds.
    :param lineage: The lineage id (16 hex digits).
    :return: ``import.<owner>.<claim_ns>.<L>``.
    """

    return f"import.{owner}.{_txn.format_ns(claimed_at)}.{lineage}"


def parse_lineage_name(name: str) -> tuple[str, int, str] | None:
    """Parse ``import.<owner>.<claim_ns>.<L>``.

    :param name: A ``tmp/`` entry name.
    :return: The owner token, claim time and lineage id, or ``None`` for another name.
    """

    parts = name.split(".")
    if len(parts) != 4 or parts[0] != "import" or len(parts[3]) != 16:
        return None
    try:
        _txn.parse_owner_token(parts[1])
        return parts[1], _txn.parse_ns(parts[2]), parts[3]
    except FormatError:
        return None


# ---------------------------------------------------------------------------
# V2, V3: claim and make writable
# ---------------------------------------------------------------------------


def _claim(workspace: Workspace, lineage: Path, source: Source) -> bool:
    """V2: take the bundle into ``S/bundle``; ``False`` when another actor had taken it (or ``S``).

    A copy is built under ``tmp/birth.*`` and moved into ``S/bundle.partial`` by
    one rename, so a previous owner whose ``S`` was renamed by a takeover can
    never re-create it: its rename fails with ``ENOENT``.
    """

    target = lineage / BUNDLE
    if not source.copy:
        _writable_root(source)
        if source.directory_fd is not None:
            return _txn.rename_verified(source.path.name, str(target), src_dir_fd=source.directory_fd)
        return _txn.rename_verified(source.path, target)
    # A copy never follows a symlink and never opens anything the walk refuses.
    _refuse_unsafe_entries(source.path)

    def build(directory: Path) -> None:
        shutil.copytree(source.path, directory, symlinks=True, dirs_exist_ok=True)
        # Moving the copy to another parent needs write permission on its root.
        os.chmod(directory, stat.S_IMODE(os.lstat(directory).st_mode) | stat.S_IWUSR)

    try:
        born = _txn.birth(workspace.control / "tmp", BUNDLE_PARTIAL, build, durable=workspace.durable, parent=lineage)
    except FileNotFoundError:
        if _lexists(lineage):
            raise
        return False  # S was renamed by a takeover meanwhile
    if not born:
        return False
    _txn._hook("V2.copied")
    return _txn.rename_verified(lineage / BUNDLE_PARTIAL, target)


def _writable_root(source: Source) -> None:
    """Before the V2 rename: give the bundle's root its owner's write bit, through a descriptor.

    Moving a directory to another parent needs write permission on the
    directory itself, so a read-only root could never be claimed. The root is
    opened by its name below its directory (never following a symlink, never
    opening a special file), and only ``u+w`` is added, only to an entry this
    user owns; anything else is left for the claim and the verification to refuse.
    """

    flags = _DIRECTORY_FLAGS | os.O_NONBLOCK
    try:
        parent = (
            os.open(source.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            if source.directory_fd is None
            else source.directory_fd
        )
    except OSError:
        return  # the rename reports it
    try:
        try:
            descriptor = os.open(source.path.name, flags, dir_fd=parent)
        except OSError:
            return  # a symlink, a file, a special entry, or gone: the claim or the verification decides
        try:
            information = os.fstat(descriptor)
            if information.st_uid == os.getuid() and not information.st_mode & stat.S_IWUSR:
                os.fchmod(descriptor, stat.S_IMODE(information.st_mode) | stat.S_IWUSR)
        finally:
            os.close(descriptor)
    finally:
        if parent != source.directory_fd:
            os.close(parent)


def _make_writable(root: Path, *, pace: Callable[[], None] | None = None) -> None:
    """V3: give every real directory below the verified bundle its owner's write bit.

    It runs after V4, so the walk bounds were applied before anything in the
    bundle was opened for writing; the same bounds apply again here (the walk
    is a new one), and exceeding them refuses the bundle. Besides the root, one
    directory is open at a time: it is opened by its name below the root
    (``O_NOFOLLOW``), checked to be the directory that was listed (``st_ino``,
    one process), made writable, listed and closed again, and only names are
    kept, so no tree shape can exhaust the descriptors.

    :param root: The bundle directory ``S/bundle``.
    :param pace: Called every :data:`~httk.workflow._bundle.PACE_STRIDE` entries.
    :raises httk.workflow.errors.FormatError: If the bundle exceeds the walk bounds.
    :raises OSError: If a directory cannot be opened, changed or listed.
    """

    try:
        root_fd = os.open(root, _DIRECTORY_FLAGS)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            return  # not a real directory: verification refused it already
        raise
    try:
        device = os.fstat(root_fd).st_dev
        pending: list[tuple[str, int, int | None]] = [(".", 1, None)]
        visited = 0
        while pending:
            relative, depth, inode = pending.pop()
            try:
                descriptor = os.open(relative, _DIRECTORY_FLAGS, dir_fd=root_fd)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR, errno.ENOENT}:
                    continue  # changed since it was listed; a later step refuses or fails on it
                raise
            try:
                information = os.fstat(descriptor)
                if inode is not None and (information.st_ino != inode or information.st_dev != device):
                    continue  # not the directory that was listed
                mode = stat.S_IMODE(information.st_mode)
                if mode & 0o700 != 0o700:
                    os.fchmod(descriptor, mode | 0o700)
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        visited += 1
                        if visited > _bundle._ADOPT_MAX_ENTRIES:
                            raise FormatError(f"transfer bundle holds more than {_bundle._ADOPT_MAX_ENTRIES} entries")
                        if pace is not None and visited % PACE_STRIDE == 0:
                            pace()
                        if not entry.is_dir(follow_symlinks=False):
                            continue
                        name = entry.name if relative == "." else f"{relative}/{entry.name}"
                        if depth >= _bundle._ADOPT_MAX_DEPTH:
                            raise FormatError(
                                f"transfer bundle rejects {name}: it nests deeper than {_bundle._ADOPT_MAX_DEPTH} levels"
                            )
                        pending.append((name, depth + 1, entry.stat(follow_symlinks=False).st_ino))
            finally:
                os.close(descriptor)
    finally:
        os.close(root_fd)


# ---------------------------------------------------------------------------
# V5, V7: presence
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Presence:
    decision: str  # "proceed" | "replay" | "wait" | "refuse" | "expired"
    reason: str | None = None
    error: Exception | None = None
    complete: bool = False  # a replay whose every job is here (or received): the copy is redundant
    retry: bool = False  # a wait on another lineage of this transfer, worth looking again shortly


def _presence(workspace: Workspace, lineage: Path, plan: _Plan, now: int) -> _Presence:
    """V5/V7: decide from fixed names whether this bundle may be published now."""

    transfer_id = plan.transfer_id
    manifest = plan.manifest
    conflict = _Presence(
        "refuse",
        error=WorkspaceCorruptionError(
            f"transfer {transfer_id} arrived here before with another provenance than this copy carries"
        ),
    )
    receipt = receipt_path(workspace, transfer_id)
    if _lexists(receipt):
        document = read_receipt(receipt)
        if (document["payload_sha256"], document["source_workspace_id"]) != (
            manifest.payload_sha256,
            manifest.source_workspace_id,
        ):
            return conflict
        return _Presence("replay", complete=True)
    arrived = 0
    for job in plan.by_id():
        marker = workspace.find_marker_by_id(job.job_id, kinds=STATE_KINDS)
        if marker is None:
            continue
        provenance = workspace.read_state(marker).get("transfer")
        if isinstance(provenance, Mapping) and provenance.get("transfer_id") == transfer_id:
            if provenance.get("payload_sha256") != job.payload_sha256:
                return conflict
            arrived += 1
            continue
        if marker.kind == "transferring":
            return _Presence("wait", f"job {marker.job_key} is still transferring away from this workspace")
        return _Presence(
            "refuse", error=FileExistsError(f"this workspace already holds job {job.job_id}; the bundle was left")
        )
    acknowledged = _lexists(ack_path(workspace, transfer_id))
    if arrived == len(plan.jobs):
        return _Presence("replay", complete=True)
    if arrived:
        if acknowledged and plan.addressed:
            return _Presence("replay")
        if acknowledged:
            return _Presence(
                "refuse",
                error=ValueError(
                    f"transfer {transfer_id} was already adopted into this workspace once and part of its tree "
                    "has since moved on; this is a stale copy and was left"
                ),
            )
        return _Presence("wait", f"another adoption of transfer {transfer_id} is still publishing it", retry=True)
    if acknowledged:
        if plan.addressed:
            return _Presence("replay")
        return _Presence(
            "refuse",
            error=ValueError(
                f"transfer {transfer_id} was already adopted into this workspace once and its job has since "
                "moved on; this is a stale copy and was left"
            ),
        )
    for job in plan.by_id():
        if _claimed_by_another(workspace, lineage, job.job_id):
            return _Presence("wait", f"job {job.job_id} is being adopted by another transfer", retry=True)
    for job in plan.jobs:
        payload = workspace.payload_path(job.placement, job.job_key)
        if _lexists(payload):
            return _Presence("refuse", error=FileExistsError(f"payload path {payload} is already taken"))
    if plan.addressed:
        sealed_at = plan.manifest.sealed_at
        if sealed_at > now + CLOCK_SKEW_NS or now > sealed_at + FRESHNESS_WINDOW_NS:
            return _Presence("expired", f"transfer {transfer_id} is outside its freshness window")
    for runner in plan.manifest.runners:
        stored = workspace.runner_store_path(runner.path)
        if stored.is_file() or stored.is_dir():
            existing = _runner_digest(stored)
            if existing != runner.sha256:
                return _Presence(
                    "refuse",
                    error=WorkspaceCorruptionError(
                        f"destination workspace runner {runner.path.as_posix()} holds digest {existing}, "
                        f"but the transfer carries {runner.sha256}"
                    ),
                )
    return _Presence("proceed")


# ---------------------------------------------------------------------------
# V6: per-job claims, and their release
# ---------------------------------------------------------------------------


def _claimed_by_another(workspace: Workspace, lineage: Path, job_id: str) -> bool:
    """Report whether ``adopting/<job_id>`` exists and is not this lineage's claim."""

    try:
        held = os.lstat(claims_directory(workspace) / job_id)
    except FileNotFoundError:
        return False
    try:
        return os.lstat(lineage / CLAIMS / job_id).st_ino != held.st_ino
    except FileNotFoundError:
        return True


def _claim_jobs(workspace: Workspace, lineage: Path, plan: _Plan) -> str | None:
    """V6: hold ``adopting/<job_id>`` for every job, in job-id order; the blocking job id when another holds one."""

    claims = claims_directory(workspace)
    claims.mkdir(parents=True, exist_ok=True)
    mine = lineage / CLAIMS
    mine.mkdir(exist_ok=True)
    name = lineage.name
    for job in plan.by_id():
        _txn._hook("V6.claim")
        _txn.link_new(
            mine,
            job.job_id,
            json_bytes({"lineage": name.rsplit(".", 1)[-1], "nonce": secrets.token_hex(8)}),
            nonce=True,
            durable=workspace.durable,
        )
        own = os.lstat(mine / job.job_id)
        try:
            os.link(mine / job.job_id, claims / job.job_id, follow_symlinks=False)
        except FileExistsError:
            pass
        except OSError:
            if not _lexists(claims / job.job_id):
                raise
        try:
            held = os.lstat(claims / job.job_id)
        except FileNotFoundError:
            return job.job_id
        if held.st_ino != own.st_ino:
            return job.job_id
    _sync(workspace, claims)
    return None


def _release(workspace: Workspace, lineage: Path) -> None:
    """Release every claim this lineage holds: rename it into ``S/released`` (only ``S``'s holder can)."""

    mine = lineage / CLAIMS
    try:
        names = sorted(os.listdir(mine))
    except FileNotFoundError:
        return
    claims = claims_directory(workspace)
    released = lineage / RELEASED
    released.mkdir(exist_ok=True)
    for job_id in names:
        try:
            own = os.lstat(mine / job_id)
            held = os.lstat(claims / job_id)
        except FileNotFoundError:
            continue
        if held.st_ino != own.st_ino:
            continue
        _txn._hook("V11.release")
        _txn.rename_verified(claims / job_id, released / job_id)
    _sync(workspace, claims)


def stale_claims(workspace: Workspace) -> list[str]:
    """Return the job ids whose claim names no lineage directory below ``tmp/`` (reported, never removed).

    :param workspace: The workspace.
    :return: The job ids.
    """

    claims = claims_directory(workspace)
    try:
        names = sorted(os.listdir(claims))
    except FileNotFoundError:
        return []
    owned: set[int] = set()
    tmp = workspace.control / "tmp"
    for entry in os.listdir(tmp) if tmp.is_dir() else []:
        if parse_lineage_name(entry) is None:
            continue
        try:
            for claim in os.scandir(tmp / entry / CLAIMS):
                owned.add(claim.inode())
        except OSError:
            continue
    result = []
    for job_id in names:
        try:
            if os.lstat(claims / job_id).st_ino not in owned:
                result.append(job_id)
        except FileNotFoundError:
            continue
    return result


# ---------------------------------------------------------------------------
# V9: publication
# ---------------------------------------------------------------------------


def _install_runners(workspace: Workspace, envelope: Path, manifest: BundleManifest) -> None:
    for runner in manifest.runners:
        target = workspace.runner_store_path(runner.path)
        if target.is_file() or target.is_dir():
            if _runner_digest(target) != runner.sha256:
                raise WorkspaceCorruptionError(
                    f"destination workspace runner {runner.path.as_posix()} changed while the transfer was imported"
                )
            continue
        workspace.publish_runner(envelope.joinpath(TRANSFER_RUNNERS, *runner.path.parts), name=runner.path)


def _provenance(plan: _Plan, job: VerifiedJob) -> dict[str, object]:
    manifest = plan.manifest
    return {
        "transfer_id": manifest.transfer_id,
        "source_workspace_id": manifest.source_workspace_id,
        "destination_workspace_id": manifest.destination_workspace_id,
        "payload_sha256": job.payload_sha256,
        "sealed_at": manifest.sealed_at,
    }


def _import_frame(workspace: Workspace, plan: _Plan, job: VerifiedJob) -> dict[str, object]:
    frame: dict[str, object] = {name: value for name, value in job.prior_state.items() if name not in _NOT_CARRIED}
    frame.update(
        {
            "format": "httk-workflow-state",
            "format_version": 3,
            "workspace_id": workspace.workspace_id,
            "job_id": job.job_id,
            "job_key": job.job_key,
            "placement": placement_text(job.placement),
            "state_generation": job.source_generation + 1,
            "kind": job.prior_kind,
            "previous_record_ref": None,
            "created_at": _utc_now(),
            "priority": job.priority,
            "transfer": _provenance(plan, job),
        }
    )
    if plan.origin is not None:
        frame["origin"] = plan.origin
    return frame


def _utc_now() -> str:
    from ._util import utc_now

    return utc_now()


def _publish(workspace: Workspace, lineage: Path, plan: _Plan) -> None:
    """V9: runners, then member payloads, the root payload, and every job's frame and marker."""

    envelope = lineage / ENVELOPE
    if not _lexists(envelope):
        raise _Fenced()
    _install_runners(workspace, envelope, plan.manifest)
    members = list(plan.jobs[1:])
    for job in [*members, plan.root]:
        _txn._hook("V9.payload")
        source = envelope.joinpath(*job.envelope_relative.parts) if job.envelope_relative else lineage / BUNDLE
        home = workspace.payload_path(job.placement, job.job_key)
        if _lexists(source):
            workspace.ensure_directory(home.parent)
            if not _txn.rename_verified(source, home) and not _lexists(home):
                raise _Fenced()
            _sync(workspace, home.parent)
        elif not _lexists(home):
            # S is private and a payload's only destination is its placement:
            # absent from both, another actor (a takeover) has this lineage.
            raise _Fenced()
    state_root = workspace.control / "state"
    for job in [*members, plan.root]:
        if _arrived_by(workspace, job.job_id, plan.transfer_id) is not None:
            continue
        marker = envelope / TRANSFER_MARKERS / job.job_id
        if not _lexists(marker):
            raise _Fenced()
        with workspace.open_journal_writer() as writer:
            record_ref = writer.append(_import_frame(workspace, plan, job))
        destination = workspace.marker_path(
            job.prior_kind, job.placement, job.job_key, job.priority, job.source_generation + 1, record_ref
        )
        workspace.ensure_directory(destination.parent)
        _txn._hook("V9.marker")
        if not _txn.rename_verified(marker, destination):
            if _arrived_by(workspace, job.job_id, plan.transfer_id) is None:
                raise _Fenced()
            continue
        _sync(workspace, destination.parent, workspace.control / "journal")
        workspace._index_note(Marker.from_path(state_root, destination))


# ---------------------------------------------------------------------------
# V10, V11: refusal and finish
# ---------------------------------------------------------------------------


def _acknowledgement(workspace: Workspace, plan: _Plan) -> dict[str, object]:
    """Create ``acks/<T>.json`` (no-replace) and return the acknowledgement document there."""

    manifest = plan.manifest
    root = plan.root
    document = sign_document(
        {
            "format": "httk-workflow-transfer-acknowledgement",
            "format_version": 3,
            "transfer_id": manifest.transfer_id,
            "source_workspace_id": manifest.source_workspace_id,
            "destination_workspace_id": workspace.workspace_id,
            "payload_sha256": manifest.payload_sha256,
            "job_id": manifest.job_id,
            "job_key": manifest.job_key,
            "placement": placement_text(root.placement),
            "state": manifest.prior_kind,
        }
    )
    path = ack_path(workspace, manifest.transfer_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not _txn.link_new(path.parent, path.name, json_bytes(document) + b"\n", durable=workspace.durable):
        existing = json.loads(_read_regular_file(path, 1 << 16))
        if not isinstance(existing, dict) or existing.get("payload_sha256") != manifest.payload_sha256:
            raise WorkspaceCorruptionError(f"acknowledgement {path} names another payload than this transfer")
        return existing
    return document


def _source_holds(source: Path, transfer_id: str) -> bool:
    try:
        manifest = json.loads(_read_regular_file(source / TRANSFER_DIRECTORY / TRANSFER_MANIFEST, _MANIFEST_LIMIT))
    except (OSError, ValueError, FormatError):
        return False
    return isinstance(manifest, dict) and manifest.get("transfer_id") == transfer_id


def _finish(
    workspace: Workspace, lineage: Path, intent: Mapping[str, Any], plan: _Plan, *, published: bool, complete: bool
) -> AdoptionResult:
    """V11: receipt, source removal, acknowledgement, claim release, and the lineage to the trash."""

    manifest = plan.manifest
    if published:
        missing = [job.job_key for job in plan.jobs if _arrived_by(workspace, job.job_id, plan.transfer_id) is None]
        if missing:
            raise _Fenced()
    if plan.addressed:
        record_receipt(
            workspace,
            manifest.transfer_id,
            receipt_document(
                sealed_at=manifest.sealed_at,
                source_workspace_id=manifest.source_workspace_id,
                job_id=manifest.job_id,
                payload_sha256=manifest.payload_sha256,
            ),
        )
    _txn._hook("V11.source")
    source = Path(str(intent.get("path")))
    if intent.get("remove_source") and _source_holds(source, manifest.transfer_id):
        hidden = source.parent / f".{source.name}.httk-adopted.{lineage.name.rsplit('.', 1)[-1]}"
        if _txn.rename_verified(source, hidden):
            _txn.remove_tree(hidden)
        _sync(workspace, source.parent)
    acknowledgement = _acknowledgement(workspace, plan)
    _release(workspace, lineage)
    _txn._hook("V11.trash")
    # A replay's bundle is only redundant once every job is known to be here.
    _txn.trash(
        lineage,
        control=workspace.control,
        holds_payload=None if (complete and not published) else _txn.holds_job_payload,
    )
    if intent.get("source") == "export":
        exports = source.parent
        if _lexists(exports) and not _lexists(source):
            _txn.trash(exports, control=workspace.control, holds_payload=_txn.holds_job_payload)
    return AdoptionResult(
        "imported" if published else "replay",
        manifest.transfer_id,
        manifest.job_id,
        manifest.job_key,
        acknowledgement,
    )


def _reject_by_descriptor(workspace: Workspace, lineage: Path, source: Source, reason: str) -> None:
    """V10 for the exchange: the bundle goes into a fresh ``rejected/<unique>`` reached only by descriptor.

    ``reason.json`` is written only once the bundle positively arrived there; a
    minted directory that cannot be used is removed again (``rmdir``, which
    removes nothing but an empty directory).

    :param workspace: The adopting workspace.
    :param lineage: The lineage directory holding the refused bundle.
    :param source: The exchange source, with its ``rejected_fd``.
    :param reason: Why the bundle was refused.
    :raises _Fenced: If the bundle left ``S`` meanwhile (a takeover).
    :raises httk.workflow.errors.WorkspaceCorruptionError: If no private rejected directory can be minted.
    """

    assert source.rejected_fd is not None
    own = os.fstat(source.rejected_fd)

    def discard(name: str) -> None:
        try:
            os.rmdir(name, dir_fd=source.rejected_fd)
        except OSError:
            pass  # swapped for something else by the client: not ours to remove

    for _attempt in range(16):
        unique = f"{source.path.name}.{secrets.token_hex(6)}"
        try:
            os.mkdir(unique, 0o755, dir_fd=source.rejected_fd)
        except FileExistsError:
            continue
        # The client can swap the fresh directory for anything before it is
        # opened; it is used only through a descriptor that is the directory itself.
        _txn._hook("V10.rejected_dir")
        try:
            descriptor = os.open(unique, _DIRECTORY_FLAGS, dir_fd=source.rejected_fd)
        except OSError:
            discard(unique)
            continue
        try:
            information = os.fstat(descriptor)
            if information.st_uid != os.getuid() or information.st_dev != own.st_dev:
                discard(unique)
                continue
            tmp = os.open(workspace.control / "tmp", _DIRECTORY_FLAGS & ~os.O_NOFOLLOW)
            try:
                moved = _txn.rename_verified(
                    f"{lineage.name}/{BUNDLE}", source.path.name, src_dir_fd=tmp, dst_dir_fd=descriptor
                )
            finally:
                os.close(tmp)
            if not moved:
                discard(unique)
                raise _Fenced()
            _txn.link_new(
                descriptor,
                "reason.json",
                json_bytes(
                    {
                        "format": "httk-workspace-exchange-rejection",
                        "format_version": 1,
                        "name": source.path.name,
                        "reason": reason[:1000],
                        "rejected_at": _utc_now(),
                    }
                ),
                durable=workspace.durable,
            )
            return
        finally:
            os.close(descriptor)
    raise WorkspaceCorruptionError("cannot mint a private rejected directory for the exchange")


def _refuse(workspace: Workspace, lineage: Path, source: Source, error: Exception) -> AdoptionResult:
    """V10: release any claims, put the bundle back (or reject it), and trash the lineage."""

    _release(workspace, lineage)
    bundle = lineage / BUNDLE
    _txn._hook("V10.refuse")
    if _lexists(bundle):
        if source.copy:
            # The source still holds the bundle; the copy is disposable.
            _txn.remove_tree(bundle)
        elif source.rejected_fd is not None:
            _reject_by_descriptor(workspace, lineage, source, str(error))
        elif source.kind in EXCHANGE_SOURCES:
            # Never renamed back by path into client territory: the trash below
            # quarantines a bundle that holds a payload.
            _LOGGER.error(
                "refused exchange bundle %s is not put back into client territory; it is quarantined",
                source.path,
                extra={"event": "adopt_refused_kept", "entry": str(source.path)},
            )
        elif not _lexists(source.path):
            _txn.rename_verified(bundle, source.path)
        else:
            _LOGGER.error(
                "refused bundle %s cannot go back to %s, which is taken; it is quarantined",
                bundle,
                source.path,
                extra={"event": "adopt_refused_kept", "entry": str(source.path)},
            )
    _txn.trash(lineage, control=workspace.control, holds_payload=_txn.holds_job_payload)
    _LOGGER.warning(
        "refused to adopt %s: %s",
        source.path,
        error,
        extra={"event": "adopt_refused", "entry": str(source.path)},
    )
    return AdoptionResult("refused", reason=str(error), error=error)


def _expire(workspace: Workspace, lineage: Path, plan: _Plan, reason: str) -> AdoptionResult:
    _release(workspace, lineage)
    _txn.trash(lineage, control=workspace.control, holds_payload=None)
    _LOGGER.warning(
        "discarded expired transfer %s",
        plan.transfer_id,
        extra={"event": "transfer_expired", "transfer_id": plan.transfer_id},
    )
    return AdoptionResult("expired", plan.transfer_id, plan.manifest.job_id, plan.manifest.job_key, reason=reason)


# ---------------------------------------------------------------------------
# The chain
# ---------------------------------------------------------------------------


def _source_of(intent: Mapping[str, Any], fds: Source | None = None) -> Source:
    if fds is not None:
        return fds
    return Source(
        kind=intent["source"],
        path=Path(str(intent["path"])),
        copy=bool(intent.get("copy")),
        remove_source=bool(intent.get("remove_source")),
    )


#: The sources whose bundles someone else wrote: an entry this workspace cannot
#: read in one of them is a content refusal, not a retry. A held export
#: (``"export"``) is this workspace's own bundle, so an error reading it is retried.
_UNTRUSTED_SOURCES = frozenset({*EXCHANGE_SOURCES, "incoming", "adopt"})


def _within(name: object, bundle: Path) -> bool:
    """Report whether an error's file name is an absolute path at or below *bundle*."""

    if not isinstance(name, (str, bytes, os.PathLike)):
        return False
    path = Path(os.fsdecode(name))
    return path.is_absolute() and (path == bundle or bundle in path.parents)


def _unreadable(bundle: Path, exc: PermissionError) -> FormatError:
    """Describe a permission error inside a bundle as the content refusal it is."""

    name = exc.filename
    entry = str(name) if name is not None else "an entry"
    if name is not None:
        try:
            entry = Path(os.fsdecode(name)).relative_to(bundle).as_posix() or "."
        except ValueError:
            entry = os.fsdecode(name)
    return FormatError(
        f"transfer bundle entry {entry} cannot be read or searched ({exc.strerror}); "
        "give its owner read and search permission and send it again"
    )


def _from_v3(
    workspace: Workspace,
    lineage: Path,
    intent: Mapping[str, Any],
    source: Source,
    pace: Callable[[], None] | None,
) -> AdoptionResult:
    """V3-V11 for a lineage holding ``S/bundle``."""

    bundle = lineage / BUNDLE
    try:
        verified = verify_bundle(bundle, workspace=workspace, source=source.kind, pace=pace)
        plan = _plan(verified.manifest, bundle, intent)
    except (FormatError, ValueError) as exc:
        return _refuse(workspace, lineage, source, exc)
    except PermissionError as exc:
        # An untrusted bundle with an entry its owner cannot read or search (a
        # mode-0 directory or file) would fail verification on every retry: it
        # is the bundle's content, so it is refused. A denied path outside the
        # bundle (this workspace's own state, read by the exchange checks) is
        # not the bundle's fault: the lineage is left for a retry.
        if source.kind not in _UNTRUSTED_SOURCES or not _within(exc.filename, bundle):
            raise
        return _refuse(workspace, lineage, source, _unreadable(bundle, exc))
    _txn._hook("V4.verified")
    try:
        # V3 after V4: the read-only verification bounds the walk before anything is opened for writing.
        # It touches nothing but the bundle (names relative to its root descriptor).
        _make_writable(bundle, pace=pace)
    except FormatError as exc:
        return _refuse(workspace, lineage, source, exc)
    except PermissionError as exc:
        if source.kind not in _UNTRUSTED_SOURCES:
            raise
        return _refuse(workspace, lineage, source, _unreadable(bundle, exc))
    _txn._hook("V3.writable")
    # Another lineage of this very transfer (a second copy) holding the claims
    # is usually about to finish: the owner looks again, for one visibility
    # deadline, before it leaves its lineage waiting.
    reason = "waiting"
    for _attempt in visibility_attempts(workspace.visibility_deadline):
        if not _lexists(lineage):
            raise _Fenced()
        presence = _presence(workspace, lineage, plan, _now())
        if presence.decision == "wait" and presence.retry:
            reason = presence.reason or reason
            continue
        outcome = _settle_presence(workspace, lineage, intent, source, plan, presence)
        if outcome is not None:
            return outcome
        _txn._hook("V5.checked")
        blocker = _claim_jobs(workspace, lineage, plan)
        if blocker is not None:
            _release(workspace, lineage)
            reason = f"job {blocker} is being adopted by another transfer"
            continue
        _txn._hook("V6.claimed")
        presence = _presence(workspace, lineage, plan, _now())
        if presence.decision == "wait" and presence.retry:
            _release(workspace, lineage)
            reason = presence.reason or reason
            continue
        outcome = _settle_presence(workspace, lineage, intent, source, plan, presence)
        if outcome is not None:
            return outcome
        break
    else:
        return _waiting(lineage, plan, reason)
    _txn._hook("V7.rechecked")
    if not _txn.rename_verified(bundle / TRANSFER_DIRECTORY, lineage / ENVELOPE):
        raise _Fenced()
    _sync(workspace, lineage, bundle)
    _txn._hook("V8.envelope")
    return _after_v8(workspace, lineage, intent, plan)


def _settle_presence(
    workspace: Workspace,
    lineage: Path,
    intent: Mapping[str, Any],
    source: Source,
    plan: _Plan,
    presence: _Presence,
) -> AdoptionResult | None:
    if presence.decision == "proceed":
        return None
    if presence.decision == "replay":
        return _finish(workspace, lineage, intent, plan, published=False, complete=presence.complete)
    if presence.decision == "wait":
        _release(workspace, lineage)
        return _waiting(lineage, plan, presence.reason or "waiting")
    if presence.decision == "expired":
        return _expire(workspace, lineage, plan, presence.reason or "expired")
    assert presence.error is not None
    return _refuse(workspace, lineage, source, presence.error)


def _waiting(lineage: Path, plan: _Plan, reason: str) -> AdoptionResult:
    _LOGGER.info(
        "adoption of transfer %s waits in %s: %s",
        plan.transfer_id,
        lineage,
        reason,
        extra={"event": "adopt_waiting", "transfer_id": plan.transfer_id},
    )
    return AdoptionResult(
        "waiting", plan.transfer_id, plan.manifest.job_id, plan.manifest.job_key, reason=reason, lineage=lineage
    )


def _after_v8(workspace: Workspace, lineage: Path, intent: Mapping[str, Any], plan: _Plan) -> AdoptionResult:
    _publish(workspace, lineage, plan)
    _txn._hook("V9.published")
    return _finish(workspace, lineage, intent, plan, published=True, complete=True)


def adopt(
    workspace: Workspace,
    source: Source,
    *,
    placement: str | PurePosixPath | None = None,
    owner: str | None = None,
    pace: Callable[[], None] | None = None,
) -> AdoptionResult:
    """Run the adoption chain V1-V11 for one bundle.

    :param workspace: The adopting workspace.
    :param source: Where the bundle is.
    :param placement: Publish a single (non-tree) root here instead of its recorded placement.
    :param owner: The owner token (this CLI process when omitted).
    :param pace: Called regularly while the bundle is verified (a manager's heartbeat pacer).
    :return: The result; a refusal carries its exception in ``error``.
    :raises httk.workflow.errors.SealedError: If the workspace or its project is sealed.
    :raises OSError: If a step cannot be observed; the lineage is left for recovery.
    """

    workspace._require_unsealed()
    # The acknowledgement is signed at the end: an unusable identity refuses before anything moves.
    identity_seed()
    token = _txn.owner_token() if owner is None else owner
    lineage_id = secrets.token_hex(8)
    lineage = workspace.control / "tmp" / lineage_name(token, _now(), lineage_id)
    intent = {
        "format": "httk-workflow-adoption-intent",
        "format_version": 1,
        "lineage": lineage_id,
        "owner": token,
        "source": source.kind,
        "path": str(source.path),
        "copy": source.copy,
        "remove_source": source.remove_source,
        "placement": None if placement is None else placement_text(normalize_placement(placement)),
    }
    # V1
    os.mkdir(lineage, 0o700)
    _txn.link_new(lineage, INTENT, json_bytes(intent) + b"\n", nonce=True, durable=workspace.durable)
    _sync(workspace, lineage.parent)
    _txn._hook("V1.staged")
    # V2
    try:
        claimed = _claim(workspace, lineage, source)
    except (FormatError, ValueError) as exc:
        # Refused before anything was taken (a copy that would follow a link or open a FIFO).
        _txn.remove_tree(lineage / BUNDLE_PARTIAL)
        _txn.trash(lineage, control=workspace.control, holds_payload=_txn.holds_job_payload)
        return AdoptionResult("refused", reason=str(exc), error=exc)
    if not claimed:
        _txn.trash(lineage, control=workspace.control, holds_payload=_txn.holds_job_payload)
        return AdoptionResult("lost", reason=f"{source.path} was taken by another actor first")
    _sync(workspace, lineage)
    if source.directory_fd is not None:
        # Client territory is touched only through its descriptor.
        if workspace.durable:
            os.fsync(source.directory_fd)
    else:
        _sync(workspace, source.path.parent)
    _txn._hook("V2.claimed")
    try:
        return _from_v3(workspace, lineage, intent, source, pace)
    except _Fenced:
        return AdoptionResult("lost", reason=f"the adoption of {source.path} was taken over by another actor")
    except (WorkflowError, OSError):
        if _lexists(lineage):
            raise
        # A step failed because the lineage was renamed away under it: a takeover.
        return AdoptionResult("lost", reason=f"the adoption of {source.path} was taken over by another actor")


# ---------------------------------------------------------------------------
# Takeover (recovery step 1)
# ---------------------------------------------------------------------------


def resume(
    workspace: Workspace,
    lineage: Path,
    *,
    pace: Callable[[], None] | None = None,
    exchange: ExchangeDescriptors | None = None,
) -> AdoptionResult:
    """Continue a lineage this actor holds, reading its state earliest-first.

    * ``bundle.partial``: the cross-filesystem copy never finished; the source
      still holds the bundle, the partial copy is removed and the lineage trashed.
    * ``bundle`` (with its ``.httk-transfer``): before V8, V3-V7 run again.
    * ``envelope``: after V8, publication continues (a payload absent from ``S``
      and present at its placement was moved there by this lineage).
    * nothing: before V2 (or after V11): the lineage is trashed.

    A lineage of an exchange-inbox bundle is continued only with *exchange*,
    so that a refusal goes into the exchange's ``outbox/rejected`` by
    descriptor; without it the lineage is left waiting, untouched.

    :param workspace: The adopting workspace.
    :param lineage: The lineage directory, already renamed to this actor's name.
    :param pace: Called regularly while the bundle is verified.
    :param exchange: The open exchange directories, for an exchange-inbox lineage.
    :return: The result.
    :raises OSError: If a step cannot be observed.
    """

    try:
        intent = _read_intent(lineage)
    except (OSError, ValueError, FormatError):
        if any(_lexists(lineage / name) for name in (BUNDLE, BUNDLE_PARTIAL, ENVELOPE)):
            raise
        # Killed while writing its intent: nothing was claimed.
        _txn.trash(lineage, control=workspace.control, holds_payload=_txn.holds_job_payload)
        return AdoptionResult("lost", reason="nothing had been claimed")
    source = _source_of(intent)
    if source.kind == "exchange_inbox":
        if exchange is None:
            return AdoptionResult(
                "waiting",
                reason="an exchange-inbox adoption continues only where the exchange directories are open",
                lineage=lineage,
            )
        source = Source("exchange_inbox", source.path, directory_fd=exchange.inbox_fd, rejected_fd=exchange.rejected_fd)
    try:
        if _lexists(lineage / BUNDLE_PARTIAL) and not _lexists(lineage / BUNDLE):
            _txn.remove_tree(lineage / BUNDLE_PARTIAL)
            _txn.trash(lineage, control=workspace.control, holds_payload=_txn.holds_job_payload)
            return AdoptionResult("lost", reason="an unfinished copy was discarded; the source still holds it")
        if _lexists(lineage / ENVELOPE):
            manifest = _read_manifest(lineage / ENVELOPE)
            plan = _plan(manifest, lineage / BUNDLE, intent)
            return _after_v8(workspace, lineage, intent, plan)
        if _lexists(lineage / BUNDLE):
            return _from_v3(workspace, lineage, intent, source, pace)
        _release(workspace, lineage)
        _txn.trash(lineage, control=workspace.control, holds_payload=_txn.holds_job_payload)
        return AdoptionResult("lost", reason="nothing had been claimed")
    except _Fenced:
        return AdoptionResult("lost", reason=f"lineage {lineage.name} was taken over by another actor")


def _from_exchange(lineage: Path) -> bool:
    """Report whether a lineage's intent names the exchange inbox as its source (read only)."""

    try:
        return _read_intent(lineage).get("source") == "exchange_inbox"
    except (OSError, ValueError, FormatError):
        return False


def recover_lineages(
    workspace: Workspace,
    *,
    owner: str | None = None,
    now: int | None = None,
    exchange: ExchangeDescriptors | None = None,
) -> list[dict[str, object]]:
    """Take over every lineage whose owner is evidently gone, and continue it.

    A lineage of an exchange-inbox bundle is taken over only by an actor that
    has the exchange directories open (*exchange*): its refusal must go into
    the exchange's ``outbox/rejected`` by descriptor. Any other actor leaves it
    for an exchange pass.

    :param workspace: The adopting workspace.
    :param owner: The recovering actor's owner token (this CLI process when omitted).
    :param now: The current time in integer UTC nanoseconds.
    :param exchange: The open exchange directories, when the recovering actor serves the exchange.
    :return: One ``{lineage, status}`` record per lineage taken over.
    """

    token = _txn.owner_token() if owner is None else owner
    clock = _now() if now is None else now
    tmp = workspace.control / "tmp"
    try:
        names = sorted(os.listdir(tmp))
    except FileNotFoundError:
        return []
    results: list[dict[str, object]] = []
    for name in names:
        parsed = parse_lineage_name(name)
        if parsed is None:
            continue
        holder, claimed_at, lineage_id = parsed
        try:
            gone = _txn.owner_gone(
                workspace.control, holder, since=claimed_at, now=clock, lease_seconds=workspace.policy.lease_seconds
            )
        except FormatError:
            gone = True
        if not gone:
            continue
        if exchange is None and _from_exchange(tmp / name):
            continue
        mine = tmp / lineage_name(token, clock, lineage_id)
        _txn._hook("takeover.rename")
        try:
            if not _txn.rename_verified(tmp / name, mine):
                continue
            _txn._hook("takeover.renamed")
            result = resume(workspace, mine, exchange=exchange)
        except (WorkflowError, OSError, ValueError) as exc:
            _LOGGER.warning(
                "cannot recover adoption %s: %s", name, exc, extra={"event": "adopt_recovery_failed", "entry": name}
            )
            results.append({"lineage": name, "status": "failed", "reason": str(exc)})
            continue
        results.append({"lineage": name, "status": result.status, "transfer_id": result.transfer_id})
    return results


def resume_owned(
    workspace: Workspace,
    owner: str,
    *,
    exchange: ExchangeDescriptors | None = None,
    pace: Callable[[], None] | None = None,
) -> list[dict[str, object]]:
    """Continue every lineage this actor itself left waiting below ``tmp/``.

    Recovery takes over only lineages whose owner is gone, so a long-lived
    actor (a manager's exchange pass) resumes its own waiting lineages here.
    Only an actor whose owner token is unique to one single-threaded caller may
    call this: it acts on the lineages without renaming them first.

    :param workspace: The adopting workspace.
    :param owner: This actor's owner token.
    :param exchange: The open exchange directories, for exchange-inbox lineages.
    :param pace: Called regularly while a bundle is verified.
    :return: One ``{lineage, status}`` record per lineage continued.
    """

    results: list[dict[str, object]] = []
    for lineage in pending_lineages(workspace):
        parsed = parse_lineage_name(lineage.name)
        if parsed is None or parsed[0] != owner:
            continue
        try:
            result = resume(workspace, lineage, pace=pace, exchange=exchange)
        except (WorkflowError, OSError, ValueError) as exc:
            _LOGGER.warning(
                "cannot continue adoption %s: %s",
                lineage.name,
                exc,
                extra={"event": "adopt_recovery_failed", "entry": lineage.name},
            )
            results.append({"lineage": lineage.name, "status": "failed", "reason": str(exc)})
            continue
        results.append({"lineage": lineage.name, "status": result.status, "transfer_id": result.transfer_id})
    return results


def pending_lineages(workspace: Workspace) -> list[Path]:
    """Return every lineage directory below ``tmp/`` (unfinished or waiting adoptions).

    :param workspace: The workspace.
    :return: The lineage paths.
    """

    tmp = workspace.control / "tmp"
    try:
        return [tmp / name for name in sorted(os.listdir(tmp)) if parse_lineage_name(name) is not None]
    except FileNotFoundError:
        return []
