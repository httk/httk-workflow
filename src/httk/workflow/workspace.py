"""Workflow workspace creation, submission, marker discovery, and transitions."""

import errno
import logging
import os
import re
import shutil
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from ._util import (
    fsync_directory,
    read_json,
    sha256_file,
    tree_digest,
    utc_now,
    visibility_attempts,
    write_json_atomic,
)
from .errors import (
    FormatError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
    WorkspaceCorruptionError,
    WorkspaceUnavailableError,
)
from .journal import JournalWriter, read_record
from .models import (
    CORE_PROFILE,
    CORE_STATE_KINDS,
    READABLE_CORE_PROFILES,
    STATE_KINDS,
    SUPPORTED_EXTENSIONS,
    WITHDRAWN_EXTENSIONS,
    JobDefinition,
    Marker,
    WorkspacePolicy,
    is_payload_private,
    marker_basename,
    normalize_placement,
    parse_job_key,
    validate_runner_path,
)

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .fsck import FsckReport
    from .gc import GcReport

_LOGGER = logging.getLogger(__name__)
# Anything below state/ that cannot possibly be a marker basename is ignored
# silently: NFS silly-renames, editor droppings, and other foreign files are
# not protocol entries and must never stop a scan or reach quarantine.
_MARKER_SHAPE_PATTERN = re.compile(r"\.p[0-9]{3}\.g[0-9a-z]+\.")


@dataclass(frozen=True)
class MarkerFault:
    """One state entry shaped like a marker that cannot be interpreted."""

    path: Path
    reason: str


@dataclass(frozen=True)
class _IndexEntry:
    """Where one job's marker was last observed, as a cache of the state tree.

    The three members are exactly what rebuilds the marker path, and every one
    of them is derived: nothing here is authoritative, and a hit is confirmed
    against the filesystem before it is used.
    """

    kind: str
    placement: PurePosixPath
    basename: str


def _marker_shaped(name: str) -> bool:
    """Report whether *name* is shaped enough like a marker to be validated."""

    return not name.startswith(".") and _MARKER_SHAPE_PATTERN.search(name) is not None


class WorkflowWorkspace:
    """One self-contained httk workflow filesystem workspace."""

    def __init__(self, root: str | os.PathLike[str], *, mutable: bool = True, durable: bool = True) -> None:
        self.root = Path(root).resolve()
        self.control = self.root / ".httk-workflow"
        self.runners = self.control / "runners"
        self.durable = durable
        self._reported_faults: set[Path] = set()
        self.format = read_json(self.control / "format.json")
        # A workspace written before the policy section existed reads as the
        # defaults, so an old workspace attaches without migration.
        self._policy = WorkspacePolicy.from_mapping(self.format.get("policy", {}))
        if self.format.get("format") != "httk-workflow-filesystem" or self.format.get("format_version") != 1:
            raise FormatError("workspace must use httk-workflow-filesystem format version 1")
        self.core_profile = self.format.get("core_profile")
        if self.core_profile not in READABLE_CORE_PROFILES:
            raise UnsupportedExtensionError(f"unsupported core profile: {self.core_profile!r}")
        if mutable and self.core_profile != CORE_PROFILE:
            # An older profile can be read and exported, never written: its
            # spawn sets, join summaries, and runner references predate the
            # shapes this implementation now publishes.
            raise UnsupportedExtensionError(
                f"workspace core profile {self.core_profile!r} cannot be mutated by this "
                f"implementation, which writes {CORE_PROFILE!r}; attach read-only to inspect it"
            )
        extensions_raw = self.format.get("extensions", [])
        if not isinstance(extensions_raw, list) or not all(isinstance(item, str) for item in extensions_raw):
            raise FormatError("workspace extensions must be an array of strings")
        self.extensions = frozenset(extensions_raw)
        withdrawn = self.extensions & WITHDRAWN_EXTENSIONS
        if withdrawn:
            # A withdrawn extension changed the on-disk shapes this
            # implementation reads, so even a read-only attach would misparse
            # the workspace. There is no migration: the state tree would have to
            # be rewritten marker by marker.
            raise UnsupportedExtensionError(
                f"workspace declares withdrawn extensions: {', '.join(sorted(withdrawn))}; "
                "this implementation no longer reads that layout, so the workspace must be "
                "re-initialized (httk workflow workspace init) and its jobs resubmitted"
            )
        unsupported = self.extensions - SUPPORTED_EXTENSIONS
        if mutable and unsupported:
            raise UnsupportedExtensionError(f"unsupported enabled extensions: {', '.join(sorted(unsupported))}")
        if self.format.get("record_ref_encoding") != "hwref-v1":
            raise UnsupportedExtensionError("unsupported record reference encoding")
        self.workspace_id = str(self.format.get("workspace_id"))
        try:
            if str(uuid.UUID(self.workspace_id)) != self.workspace_id:
                raise ValueError
        except ValueError as exc:
            raise FormatError("workspace_id must be a canonical UUID") from exc
        # Job id -> where this instance last saw that job's marker. It is a pure
        # cache of the state tree, built lazily from one scan and maintained by
        # every rename this instance performs; see :meth:`find_marker_by_id`.
        self._marker_index: dict[str, _IndexEntry] | None = None
        # Job ids the last complete scan found more than one marker for. That is
        # workspace corruption, and the lookup that meets one must say so rather
        # than pick a winner.
        self._marker_duplicates: frozenset[str] = frozenset()

    @classmethod
    def initialize(
        cls,
        root: str | os.PathLike[str],
        *,
        extensions: Iterable[str] = (),
        durable: bool = True,
        policy: Mapping[str, object] | None = None,
    ) -> "WorkflowWorkspace":
        """Create and return a new workspace."""

        root_path = Path(root).resolve()
        root_path.mkdir(parents=True, exist_ok=True)
        initial_policy = WorkspacePolicy.from_mapping({} if policy is None else policy)
        extension_set = frozenset(extensions)
        withdrawn = extension_set & WITHDRAWN_EXTENSIONS
        if withdrawn:
            raise UnsupportedExtensionError(
                f"withdrawn extensions cannot be enabled: {', '.join(sorted(withdrawn))}; "
                "priority is encoded in every marker basename and needs no directory layer"
            )
        unsupported = extension_set - SUPPORTED_EXTENSIONS
        if unsupported:
            raise UnsupportedExtensionError(f"unsupported extensions: {', '.join(sorted(unsupported))}")
        name_max = os.pathconf(root_path, "PC_NAME_MAX")
        if name_max < 213:
            raise FormatError(f"filesystem NAME_MAX {name_max} is below the {CORE_PROFILE} requirement of 213")
        control = root_path / ".httk-workflow"
        control.mkdir(exist_ok=False)
        for relative in (
            "tmp",
            "quarantine",
            "journal",
            "managers",
            "runners",
            "requests/tmp",
            "requests/ready",
            "requests/claimed",
            "state/submitted",
        ):
            (control / relative).mkdir(parents=True, exist_ok=True)
        if "detached-transfer-v1" in extension_set:
            for relative in ("transfers/acks", "transfers/imported", "transfers/incoming", "transfers/retired"):
                (control / relative).mkdir(parents=True, exist_ok=True)
        write_json_atomic(
            control / "format.json",
            {
                "format": "httk-workflow-filesystem",
                "format_version": 1,
                "core_profile": CORE_PROFILE,
                "extensions": sorted(extension_set),
                "record_ref_encoding": "hwref-v1",
                "workspace_id": str(uuid.uuid4()),
                "created_at": utc_now(),
                "policy": initial_policy.as_mapping(),
            },
            durable=durable,
        )
        return cls(root_path, durable=durable)

    @property
    def policy(self) -> WorkspacePolicy:
        """Return the shared tunables this workspace publishes to every attacher."""

        return self._policy

    @property
    def visibility_deadline(self) -> float:
        """Return how long a metadata visibility retry may keep probing."""

        return self._policy.visibility_deadline_seconds

    def set_policy(self, changes: Mapping[str, object]) -> WorkspacePolicy:
        """Validate *changes*, merge them into the stored policy, and publish it.

        The write is an ordinary read-modify-write of ``format.json`` through an
        exclusively created temporary file and a rename, so a reader never sees
        a torn object. It is deliberately not serialized against another writer:
        policy is administrative, changes are rare, and last writer wins.
        """

        stored = read_json(self.control / "format.json")
        merged = WorkspacePolicy.from_mapping(stored.get("policy", {})).updated(changes)
        stored["policy"] = merged.as_mapping()
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        self._policy = merged
        _LOGGER.info(
            "workspace %s policy updated: %s",
            self.workspace_id,
            ", ".join(f"{key}={value!r}" for key, value in sorted(changes.items())),
            extra={"event": "policy_updated", "workspace_id": self.workspace_id},
        )
        return merged

    def open_journal_writer(self, *, writer_id: str | None = None) -> JournalWriter:
        """Open one exclusive journal writer configured by workspace policy."""

        return JournalWriter(
            self.control,
            writer_id=writer_id,
            durable=self.durable,
            maximum_segment_bytes=self._policy.journal_segment_bytes,
        )

    def check(
        self,
        *,
        repair: bool = False,
        quarantine_unrepairable: bool = False,
    ) -> "FsckReport":
        """Verify that every marker resolves to its journal frame."""

        from .fsck import check_workspace

        return check_workspace(self, repair=repair, quarantine_unrepairable=quarantine_unrepairable)

    def collect_garbage(self, *, dry_run: bool = False, now: float | None = None) -> "GcReport":
        """Collect the disk this workspace's retention policy permits freeing."""

        from .gc import collect_garbage

        return collect_garbage(self, dry_run=dry_run, now=now)

    def upgrade(self, extensions: Iterable[str]) -> frozenset[str]:
        """Enable extensions that have an implemented in-place migration."""

        requested = frozenset(extensions)
        unknown = requested - SUPPORTED_EXTENSIONS
        if unknown:
            raise UnsupportedExtensionError(f"no implemented migration for extensions: {', '.join(sorted(unknown))}")
        additions = requested - self.extensions
        unsupported_migrations = additions - {"detached-transfer-v1"}
        if unsupported_migrations:
            raise UnsupportedExtensionError(
                "existing workspaces can only enable detached-transfer-v1; "
                f"no implemented migration for: {', '.join(sorted(unsupported_migrations))}"
            )
        if "detached-transfer-v1" in additions:
            for relative in ("transfers/acks", "transfers/imported", "transfers/incoming", "transfers/retired"):
                (self.control / relative).mkdir(parents=True, exist_ok=True)
            updated = dict(self.format)
            updated["extensions"] = sorted(self.extensions | additions)
            write_json_atomic(self.control / "format.json", updated, durable=self.durable)
            self.format = updated
            self.extensions = frozenset(updated["extensions"])
        return self.extensions

    def runner_store_path(self, path: str | PurePosixPath) -> Path:
        """Return the store location of one workspace runner.

        The store is flat and name-keyed below ``.httk-workflow/runners/``.
        Relative subdirectories are permitted so a campaign can group runners,
        but a name can never escape the store.
        """

        relative = validate_runner_path(str(PurePosixPath(path)), "workspace")
        resolved = self.runners.joinpath(*relative.parts)
        root = self.runners.resolve()
        if not Path(os.path.normpath(resolved)).is_relative_to(root):
            raise FormatError(f"runner name must remain below the workspace runner store: {relative}")
        return resolved

    def publish_runner(
        self,
        source: str | os.PathLike[str],
        *,
        name: str | PurePosixPath | None = None,
        replace: bool = False,
    ) -> dict[str, object]:
        """Install one runner in the workspace store and describe the reference.

        Publication is content addressed: republishing identical bytes is an
        idempotent no-op, and replacing a name whose content differs requires
        *replace* so a live campaign referring to the old digest can never be
        changed underneath by accident.
        """

        source_path = Path(source).expanduser()
        if source_path.is_symlink() or not source_path.is_file():
            raise FormatError(f"a published runner must be a regular file: {source_path}")
        digest = sha256_file(source_path)
        target = self.runner_store_path(name if name is not None else source_path.name)
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise FormatError(f"workspace runner store entry is not a regular file: {target}")
            existing = sha256_file(target)
            if existing == digest:
                _LOGGER.debug("workspace runner %s already holds digest %s", target, digest)
            elif not replace:
                raise FileExistsError(
                    f"workspace runner {target.relative_to(self.runners).as_posix()} already holds a "
                    f"different digest {existing}; pass replace to overwrite it"
                )
            else:
                self._install_runner_file(source_path, target)
        else:
            self._install_runner_file(source_path, target)
        relative = target.relative_to(self.runners)
        _LOGGER.info(
            "published workspace runner %s with digest %s",
            relative.as_posix(),
            digest,
            extra={"event": "runner_published", "runner": relative.as_posix(), "sha256": digest},
        )
        return {"source": "workspace", "path": relative.as_posix(), "sha256": digest}

    def _install_runner_file(self, source: Path, target: Path) -> None:
        """Atomically replace one store entry with the bytes of *source*."""

        target.parent.mkdir(parents=True, exist_ok=True)
        staging = self.control / "tmp" / f"runner.{uuid.uuid4()}"
        staging.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, staging)
        staging.chmod(0o555)
        try:
            os.replace(staging, target)
        finally:
            staging.unlink(missing_ok=True)

    def detach(
        self,
        job_id: str,
        *,
        destination_workspace_id: str,
        destination_placement: str | PurePosixPath | None = None,
        transfer_id: str | None = None,
    ) -> Path:
        """Seal one quiescent job as a detached transfer bundle."""

        from .transfers import detach_job

        return detach_job(
            self,
            job_id,
            destination_workspace_id=destination_workspace_id,
            destination_placement=destination_placement,
            transfer_id=transfer_id,
        )

    def import_bundle(self, bundle: str | os.PathLike[str]) -> dict[str, object]:
        """Import a validated detached transfer bundle."""

        from .transfers import import_bundle

        return import_bundle(self, bundle)

    def acknowledge_transfer(self, acknowledgement: Mapping[str, object]) -> Path:
        """Retire a source bundle after destination acknowledgement."""

        from .transfers import acknowledge_transfer

        return acknowledge_transfer(self, acknowledgement)

    def recover_transfers(self) -> list[dict[str, object]]:
        """Recover or report interrupted detached-transfer publications."""

        from .transfers import recover_transfers

        return recover_transfers(self)

    def state_directory(self, kind: str, placement: PurePosixPath) -> Path:
        if kind not in STATE_KINDS:
            raise ValueError(f"unknown state kind: {kind}")
        return (self.control / "state" / kind).joinpath(*placement.parts)

    def marker_path(
        self,
        kind: str,
        placement: PurePosixPath,
        job_key: str,
        priority: int,
        generation: int,
        record_ref: str,
    ) -> Path:
        return self.state_directory(kind, placement) / marker_basename(job_key, priority, generation, record_ref)

    def payload_path(self, placement: PurePosixPath, job_key: str) -> Path:
        return self.root.joinpath(*placement.parts, job_key)

    def scan_marker_entries(self, kinds: Iterable[str] | None = None) -> Iterator[Marker | MarkerFault]:
        """Yield every marker below ``state/``, reporting damage per entry.

        One unusable entry must never hide the rest of the workspace, so a
        marker-shaped basename that fails validation is reported as a
        :class:`MarkerFault` instead of aborting the scan.
        """

        selected = tuple(kinds or CORE_STATE_KINDS)
        state_root = self.control / "state"
        for kind in selected:
            directory = state_root / kind
            if not directory.exists():
                continue
            for path in directory.rglob("*"):
                if not path.is_file():
                    continue
                if not _marker_shaped(path.name):
                    _LOGGER.debug("ignoring foreign file below state: %s", path)
                    continue
                try:
                    yield Marker.from_path(state_root, path)
                except (WorkflowError, ValueError) as exc:
                    yield MarkerFault(path=path, reason=str(exc))

    def scan_markers(self, kinds: Iterable[str] | None = None) -> Iterable[Marker]:
        for entry in self.scan_marker_entries(kinds):
            if isinstance(entry, Marker):
                yield entry
            else:
                self.report_marker_fault(entry)

    def report_marker_fault(self, fault: MarkerFault) -> None:
        """Report an uninterpretable state entry loudly once, then quietly.

        A marker whose basename or placement cannot be parsed is workspace
        corruption rather than a job state: the core profile leaves its repair
        to an explicit workspace tool, so a manager only reports it and never
        schedules or relocates it.
        """

        message = "unusable state entry %s: %s (repair it with a workspace tool)"
        if fault.path in self._reported_faults:
            _LOGGER.debug(message, fault.path, fault.reason)
            return
        self._reported_faults.add(fault.path)
        _LOGGER.error(message, fault.path, fault.reason, extra={"event": "marker_fault", "entry": str(fault.path)})

    # -- the derived job-id index -------------------------------------------
    #
    # ``find_marker_by_id`` and ``find_markers`` are asked one question — where
    # is this job right now — by join evaluation, by every operator request, by
    # ``job show``/``job why``, and by transfers. Answering it by walking the
    # whole state tree costs one rglob per state kind per question, which is what
    # made a waiting parent's per-tick cost grow with its number of children.
    #
    # The answer is cached per workspace instance in memory only. There is
    # deliberately no on-disk index: two managers write one workspace, and a
    # shared file would need a durability and invalidation story that the
    # authoritative state tree already provides for free. The cache is therefore
    # never trusted negatively — a miss rescans before absence is declared — and
    # a hit is confirmed against the filesystem before it is returned, so a
    # marker another manager moved is detected rather than reported stale.
    #
    # It costs a few hundred bytes per job of this workspace, which is the price
    # the specification's "keep an in-memory job-key-to-marker map" advice always
    # implied; a process that must not pay it can drop the whole index at any
    # moment with :meth:`invalidate_marker_index`, at the cost of one rescan.

    def _index_note(self, marker: Marker) -> None:
        """Record where this instance just observed one job's marker.

        The index covers exactly the kinds a lookup answers for, so a job that
        moves into ``relocating`` or ``transferring`` is dropped rather than
        recorded: a lookup must report it as absent from the schedulable tree.
        """

        if self._marker_index is None:
            return
        if marker.kind not in CORE_STATE_KINDS:
            self._marker_index.pop(marker.job_id, None)
            return
        self._marker_index[marker.job_id] = _IndexEntry(
            kind=marker.kind,
            placement=marker.placement,
            basename=marker.path.name,
        )

    def _index_note_path(self, path: Path) -> None:
        """Record one marker this instance published or moved by raw path.

        Marker publication that does not go through :meth:`transition` — a
        submission, a registered child, an imported transfer bundle — still
        renames into ``state/``, so the index is refreshed from the destination
        path rather than from a parsed marker the caller may not have built.
        """

        state_root = self.control / "state"
        try:
            relative = path.relative_to(state_root)
        except ValueError:
            return
        if len(relative.parts) < 3:
            return
        try:
            self._index_note(Marker.from_path(state_root, path))
        except (WorkflowError, ValueError):
            _LOGGER.debug("not indexing an uninterpretable marker publication: %s", path)

    def _index_forget(self, job_id: str) -> None:
        """Drop one job from the index, for instance when it is quarantined."""

        if self._marker_index is not None:
            self._marker_index.pop(job_id, None)

    def invalidate_marker_index(self) -> None:
        """Drop the cached job-id index, so the next lookup rebuilds it."""

        self._marker_index = None
        self._marker_duplicates = frozenset()

    def _rebuild_marker_index(self) -> dict[str, _IndexEntry]:
        """Rebuild the whole index from one complete scan of ``state/``.

        The scan is deliberately the base-class one: a subclass may narrow what
        a manager *scans* for scheduling, but never what the workspace may look
        up, and an index built from a narrowed scan would answer "absent" for a
        job that is plainly there.
        """

        index: dict[str, _IndexEntry] = {}
        duplicates: set[str] = set()
        for entry in WorkflowWorkspace.scan_marker_entries(self, CORE_STATE_KINDS):
            if isinstance(entry, MarkerFault):
                self.report_marker_fault(entry)
                continue
            if entry.job_id in index:
                duplicates.add(entry.job_id)
            index[entry.job_id] = _IndexEntry(
                kind=entry.kind,
                placement=entry.placement,
                basename=entry.path.name,
            )
        self._marker_index = index
        self._marker_duplicates = frozenset(duplicates)
        _LOGGER.debug("rebuilt the marker index of workspace %s with %d jobs", self.workspace_id, len(index))
        return index

    def _index_marker(self, job_id: str, entry: "_IndexEntry") -> Marker | None:
        """Return the marker one index entry names, if it is still there."""

        path = self.state_directory(entry.kind, entry.placement) / entry.basename
        if not path.is_file():
            return None
        try:
            marker = Marker.from_path(self.control / "state", path)
        except (WorkflowError, ValueError):
            return None
        return marker if marker.job_id == job_id else None

    def find_markers(self, job_key: str, kinds: Iterable[str] | None = None) -> list[Marker]:
        selected = tuple(kinds or CORE_STATE_KINDS)
        if set(selected) <= set(CORE_STATE_KINDS):
            try:
                job_id = parse_job_key(job_key)[1]
            except FormatError:
                job_id = None
            if job_id is not None:
                marker = self.find_marker_by_id(job_id)
                if marker is None or marker.job_key != job_key:
                    return []
                return [marker] if marker.kind in selected else []
        return [marker for marker in self.scan_markers(selected) if marker.job_key == job_key]

    def find_marker_by_id(self, job_id: str) -> Marker | None:
        """Return the one current marker of *job_id*, or ``None`` if it has none.

        Resolution follows the specified ladder: the in-memory index, then a
        targeted probe of the finite state set at the placement the index last
        saw, then one complete rescan. Absence is only ever reported after that
        rescan, so a job another actor has just created or moved is never
        mistaken for a job that does not exist.
        """

        index = self._marker_index
        if index is not None:
            entry = index.get(job_id)
            if entry is not None and job_id not in self._marker_duplicates:
                marker = self._index_marker(job_id, entry)
                if marker is not None:
                    return marker
                # The cached location is stale. The job most likely only changed
                # kind, and ordinary transitions never change placement, so the
                # finite state set at that placement is the cheap next probe.
                index.pop(job_id, None)
                probed = self._probe_placement(job_id, entry.placement)
                if probed is not None:
                    self._index_note(probed)
                    return probed
        index = self._rebuild_marker_index()
        if job_id in self._marker_duplicates:
            raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
        entry = index.get(job_id)
        return None if entry is None else self._index_marker(job_id, entry)

    def _probe_placement(self, job_id: str, placement: PurePosixPath) -> Marker | None:
        """Find one job id by checking every state kind at one placement."""

        state_root = self.control / "state"
        matches: list[Marker] = []
        for kind in CORE_STATE_KINDS:
            directory = self.state_directory(kind, placement)
            if not directory.is_dir():
                continue
            for path in directory.glob(f"*{job_id}.p???.g*.*"):
                if not path.is_file():
                    continue
                try:
                    marker = Marker.from_path(state_root, path)
                except (WorkflowError, ValueError) as exc:
                    self.report_marker_fault(MarkerFault(path=path, reason=str(exc)))
                    continue
                if marker.job_id == job_id:
                    matches.append(marker)
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
        return matches[0] if matches else None

    def find_marker_at(self, job_key: str, placement: PurePosixPath) -> Marker | None:
        """Find *job_key* by checking the finite state set at a placement.

        This is the first rung of the resolution ladder: a join child carrying a
        placement hint is resolved here, without the index and without a scan.
        The index is used only as a shortcut when it already names this job at
        exactly this placement, which turns the bounded directory sweep below
        into one confirmed lookup.
        """

        index = self._marker_index
        if index is not None:
            try:
                job_id = parse_job_key(job_key)[1]
            except FormatError:
                job_id = None
            entry = None if job_id is None else index.get(job_id)
            if job_id is not None and entry is not None and entry.placement == placement:
                marker = self._index_marker(job_id, entry)
                if marker is not None and marker.job_key == job_key:
                    return marker
        matches: list[Marker] = []
        state_root = self.control / "state"
        for kind in CORE_STATE_KINDS:
            directory = self.state_directory(kind, placement)
            if not directory.is_dir():
                continue
            for path in directory.glob(f"{job_key}.p???.g*.*"):
                if not path.is_file():
                    continue
                try:
                    matches.append(Marker.from_path(state_root, path))
                except (WorkflowError, ValueError) as exc:
                    self.report_marker_fault(MarkerFault(path=path, reason=str(exc)))
        if len(matches) > 1:
            raise WorkspaceCorruptionError(f"job {job_key} has multiple markers at {placement}")
        if matches:
            self._index_note(matches[0])
        return matches[0] if matches else None

    def load_job(self, marker: Marker) -> JobDefinition:
        path = self.payload_path(marker.placement, marker.job_key) / "job.json"
        job = JobDefinition.from_path(path)
        if job.job_key != marker.job_key:
            raise FormatError("job.json identity disagrees with marker")
        if job.priority != marker.priority and marker.generation == 0:
            raise FormatError("submitted marker priority disagrees with job.json")
        return job

    def read_state(self, marker: Marker) -> dict[str, Any]:
        if marker.record_ref == "init":
            return {
                "format": "httk-workflow-state",
                "format_version": 1,
                "workspace_id": self.workspace_id,
                "job_id": marker.job_id,
                "job_key": marker.job_key,
                "placement": marker.placement.as_posix(),
                "state_generation": 0,
                "kind": "submitted",
                "previous_record_ref": None,
                "created_at": None,
                "priority": marker.priority,
            }
        frame = read_record(self.control, marker.record_ref, deadline_seconds=self.visibility_deadline)
        if (
            frame.get("format") != "httk-workflow-state"
            or frame.get("format_version") != 1
            or frame.get("workspace_id") != self.workspace_id
            or frame.get("job_key") != marker.job_key
            or frame.get("state_generation") != marker.generation
            or frame.get("kind") != marker.kind
        ):
            raise WorkspaceCorruptionError(f"state frame disagrees with marker {marker.path}")
        return frame

    def transition(
        self,
        writer: JournalWriter,
        marker: Marker,
        kind: str,
        updates: Mapping[str, object],
        *,
        priority: int | None = None,
    ) -> Marker:
        """Append a state frame and atomically move *marker* to it."""

        next_priority = marker.priority if priority is None else priority
        generation = marker.generation + 1
        if generation > (1 << 64) - 1:
            raise WorkspaceCorruptionError("state generation exhausted")
        frame: dict[str, object] = {
            "format": "httk-workflow-state",
            "format_version": 1,
            "workspace_id": self.workspace_id,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "state_generation": generation,
            "kind": kind,
            "previous_record_ref": None if marker.record_ref == "init" else marker.record_ref,
            "created_at": utc_now(),
            "priority": next_priority,
        }
        frame.update(updates)
        record_ref = writer.append(frame)
        destination = self.marker_path(kind, marker.placement, marker.job_key, next_priority, generation, record_ref)
        destination.parent.mkdir(parents=True, exist_ok=True)
        _LOGGER.debug(
            "moving marker %s from %s to %s at generation %d",
            marker.job_key,
            marker.kind,
            kind,
            generation,
        )
        return self._verified_marker_rename(marker, destination)

    def repoint_marker(self, writer: JournalWriter, marker: Marker, frame: Mapping[str, object]) -> Marker:
        """Publish a repair frame for *marker* and move the marker onto it.

        This is the repair counterpart of :meth:`transition`. The caller supplies
        the complete frame because what needs repairing is precisely the frame
        the marker references now, which cannot be read and therefore cannot be
        carried forward automatically. The frame must still name this marker's
        job and kind at the next generation, so a repair can never disguise a
        state change as a repair.
        """

        generation = marker.generation + 1
        expected: Mapping[str, object] = {
            "workspace_id": self.workspace_id,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "kind": marker.kind,
            "state_generation": generation,
        }
        for name, value in expected.items():
            if frame.get(name) != value:
                raise FormatError(f"a repair frame must keep {name} at {value!r}, not {frame.get(name)!r}")
        record_ref = writer.append(frame)
        destination = self.marker_path(
            marker.kind, marker.placement, marker.job_key, marker.priority, generation, record_ref
        )
        return self._verified_marker_rename(marker, destination)

    def _verified_marker_rename(self, marker: Marker, destination: Path) -> Marker:
        source = marker.path
        state_root = self.control / "state"
        last_error: OSError | None = None
        for attempt in visibility_attempts(self.visibility_deadline):
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.rename(source, destination)
            except OSError as exc:
                last_error = exc
                _LOGGER.warning(
                    "marker rename %s -> %s reported %s on attempt %d; verifying the destination",
                    source,
                    destination,
                    exc,
                    attempt + 1,
                )
            if destination.is_file():
                moved = Marker.from_path(state_root, destination)
                self._index_note(moved)
                return moved
            if source.is_file():
                _LOGGER.debug("marker %s is not yet visible at %s; retrying", source, destination)
                continue
            current = self.find_markers(marker.job_key)
            if len(current) == 1:
                raise TransitionLostError(f"another transition moved {source} to {current[0].path}")
            if len(current) > 1:
                raise WorkspaceCorruptionError(f"job {marker.job_key} has multiple markers")
        detail = f": {last_error}" if last_error is not None else ""
        raise WorkspaceUnavailableError(f"cannot resolve marker rename {source} -> {destination}{detail}")

    def _publish_path(self, source: Path, destination: Path) -> None:
        last_error: OSError | None = None
        for attempt in visibility_attempts(self.visibility_deadline):
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.rename(source, destination)
            except OSError as exc:
                last_error = exc
                _LOGGER.warning(
                    "publication %s -> %s reported %s on attempt %d; verifying the destination",
                    source,
                    destination,
                    exc,
                    attempt + 1,
                )
            if destination.exists():
                if not source.exists():
                    # Every marker publication that does not go through a
                    # transition — submission, child registration, transfer
                    # import — lands here, so this is where the index learns
                    # about it.
                    self._index_note_path(destination)
                    return
                try:
                    if source.samefile(destination):
                        continue
                except OSError:
                    pass
                raise FileExistsError(f"publication destination already exists: {destination}")
        detail = f": {last_error}" if last_error else ""
        raise WorkspaceUnavailableError(f"cannot resolve publication {source} -> {destination}{detail}")

    def submit(
        self,
        source: str | os.PathLike[str],
        placement: str | PurePosixPath,
        *,
        move: bool = False,
    ) -> Marker:
        """Copy or move a complete payload into the workspace and publish it."""

        source_path = Path(source).resolve()
        job = JobDefinition.from_path(source_path / "job.json")
        if job.data_mode == "transactional" and "transactional-data-v1" not in self.extensions:
            raise UnsupportedExtensionError("job requires transactional-data-v1")
        normalized_placement = normalize_placement(placement)
        target = self.payload_path(normalized_placement, job.job_key)
        if target.exists():
            raise FileExistsError(target)
        staging = self.control / "tmp" / f"submit.{uuid.uuid4()}"
        if move:
            try:
                os.rename(source_path, staging)
            except OSError as exc:
                if exc.errno == errno.EXDEV:
                    raise WorkflowWorkspaceError("move submission must remain on one filesystem") from exc
                raise
        else:
            shutil.copytree(source_path, staging, symlinks=False)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._publish_path(staging, target)
        temporary_marker = self.control / "tmp" / f"marker.{uuid.uuid4()}"
        temporary_marker.touch(exist_ok=False)
        if self.durable:
            fsync_directory(temporary_marker.parent)
        destination = self.marker_path("submitted", normalized_placement, job.job_key, job.priority, 0, "init")
        self._publish_path(temporary_marker, destination)
        return Marker.from_path(self.control / "state", destination)

    def validate_job_payload(self, marker: Marker) -> JobDefinition:
        """Perform manager-side immutable submission validation."""

        job = self.load_job(marker)
        if job.data_mode == "transactional" and "transactional-data-v1" not in self.extensions:
            raise UnsupportedExtensionError("job requires transactional-data-v1")
        return job

    def quarantine(self, path: Path, *, reason: str) -> Path:
        """Move a malformed protocol entry into the canonical quarantine."""

        identifier = f"{int(time.time())}-{uuid.uuid4()}"
        destination = self.control / "quarantine" / identifier
        destination.mkdir(parents=True)
        moved = destination / "entry"
        os.rename(path, moved)
        # A quarantined marker leaves the state tree, so a cached location for it
        # would be a hit that resolves to nothing on every later lookup.
        try:
            self._index_forget(Marker.from_path(self.control / "state", path).job_id)
        except (WorkflowError, ValueError):
            pass
        write_json_atomic(
            destination / "report.json",
            {"original_path": str(path), "reason": reason, "quarantined_at": utc_now()},
            durable=self.durable,
        )
        _LOGGER.warning(
            "quarantined %s as %s: %s",
            path,
            destination,
            reason,
            extra={"event": "quarantine", "entry": str(path), "quarantine": str(destination)},
        )
        return destination

    def payload_digest(self, marker: Marker) -> str:
        """Return the digest of one payload, ignoring runner-private entries."""

        return tree_digest(
            self.payload_path(marker.placement, marker.job_key),
            skip=is_payload_private,
        )

    def publish_request(self, request: Mapping[str, object]) -> Path:
        """Atomically publish an operator request."""

        temporary = self.control / "requests" / "tmp" / f"{uuid.uuid4()}.json"
        ready = self.control / "requests" / "ready" / temporary.name
        write_json_atomic(temporary, dict(request), durable=self.durable)
        self._publish_path(temporary, ready)
        return ready


class WorkflowWorkspaceError(WorkspaceUnavailableError):
    """A workspace operation could not be completed."""
