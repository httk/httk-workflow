"""Workflow workspace creation, submission, marker discovery, and transitions."""

import bisect
import errno
import logging
import os
import re
import shutil
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
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
    ACTIVE_STATE_KINDS,
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


def _validate_setting_key(key: str) -> str:
    """Return one application-setting key, refusing an ill-formed one.

    Application settings are a flat, dotted-name map (``vasp.command``,
    ``vasp.pseudo_library``) of small values a runner resolves at execution
    time — never nested objects, which belong in a job's ``inputs`` instead.
    """

    if not isinstance(key, str) or not key or key != key.strip() or "/" in key:
        raise ValueError(f"application setting name must be a nonempty dotted identifier: {key!r}")
    return key


def _validate_setting_value(key: str, value: object) -> object:
    """Return one application-setting value, refusing a non-scalar.

    ``bool`` is a subclass of ``int`` and is accepted as the JSON scalar it is.
    """

    if value is None or isinstance(value, (str, int, float)):
        return value
    raise ValueError(f"application setting {key!r} must be a JSON scalar, not {type(value).__name__}")


def _validate_settings(raw: object) -> dict[str, object]:
    """Return a validated flat map of application settings."""

    if not isinstance(raw, Mapping):
        raise FormatError("workspace settings must be a JSON object")
    return {_validate_setting_key(key): _validate_setting_value(str(key), value) for key, value in raw.items()}


# Anything below state/ that cannot possibly be a marker basename is ignored
# silently: NFS silly-renames, editor droppings, and other foreign files are
# not protocol entries and must never stop a scan or reach quarantine.
_MARKER_SHAPE_PATTERN = re.compile(r"\.p[0-9]{3}\.g[0-9a-z]+\.")

#: How many directory entries a streaming scheduling scan visits before it
#: takes a heartbeat opportunity. A walk of one enormous flat directory keeps a
#: manager's lease alive from inside the walk exactly as a walk across many
#: kinds does, rather than only between passes.
DISCOVERY_HEARTBEAT_STRIDE = 512

#: How many active-job locations one workspace instance caches before it evicts
#: the least recently touched. Terminal jobs are never indexed, so this only
#: bounds the working set: finished history never makes the index proportional
#: to the workspace's whole history.
DEFAULT_MARKER_INDEX_CAPACITY = 1 << 20


def _scandir_sorted(directory: Path) -> list[os.DirEntry[str]]:
    """Return one directory's entries by name, or nothing if it is not there.

    A directory that a concurrent transition renamed or removed while the walk
    was reaching it reads as empty rather than as an error: an incremental scan
    tolerates the churn of the very managers it runs beside, consistent with
    how a vanished marker becomes a silent miss rather than a fault.
    """

    try:
        with os.scandir(directory) as scan:
            entries = list(scan)
    except (FileNotFoundError, NotADirectoryError):
        return []
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR):
            return []
        raise
    entries.sort(key=lambda item: item.name)
    return entries


def _safe_is_dir(entry: os.DirEntry[str]) -> bool:
    """Report whether *entry* is a directory, tolerating its disappearance."""

    try:
        return entry.is_dir()
    except OSError:
        return False


def _cursor_relation(position: tuple[str, ...], after: tuple[str, ...] | None) -> str:
    """Classify a subtree at *position* against a resume cursor *after*.

    ``"descend"`` walks the whole subtree, ``"resume"`` walks it carrying the
    cursor because the resume point is inside it, and ``"skip"`` prunes it
    without a single ``scandir`` because every position it could contain was
    already consumed. This is what keeps resuming an interrupted walk cheap.
    """

    if after is None:
        return "descend"
    head = after[: len(position)]
    if position < head:
        return "skip"
    if position > head:
        return "descend"
    return "resume"


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


class Workspace:
    """One self-contained httk workflow filesystem workspace."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        mutable: bool = True,
        durable: bool = True,
        marker_index_capacity: int = DEFAULT_MARKER_INDEX_CAPACITY,
    ) -> None:
        if marker_index_capacity < 1:
            raise ValueError("marker_index_capacity must be positive")
        self._marker_index_capacity = marker_index_capacity
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
        # Job id -> where this instance last saw that job's marker, most recently
        # touched last. It is a pure cache of the state tree, built lazily from
        # one scan and maintained by every rename this instance performs; see
        # :meth:`find_marker_by_id`. Only active jobs are cached and the map is
        # capped, so it tracks the working set rather than the whole history: a
        # job that reaches a terminal kind is evicted, and interactive lookups of
        # a finished job pay one exhaustive scan instead.
        self._marker_index: "OrderedDict[str, _IndexEntry] | None" = None
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
    ) -> "Workspace":
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

    @property
    def settings(self) -> dict[str, object]:
        """Return this workspace's application settings, a flat dotted map.

        Application settings are distinct from :attr:`policy`, which tunes the
        engine. These are the values an application step resolves at run time —
        the VASP command, a pseudopotential library — one layer of the
        job-inputs → environment → workspace → default resolution a runner reads
        through :meth:`~httk.workflow.sdk.Attempt.setting`. A workspace written
        before the section existed reads as an empty map.
        """

        return _validate_settings(self.format.get("settings", {}))

    def set_setting(self, key: str, value: object) -> dict[str, object]:
        """Store one application setting and return the resulting map.

        The write is the same read-modify-write of ``format.json`` that
        :meth:`set_policy` uses: an exclusively created temporary and a rename,
        so a reader never sees a torn object, and last writer wins.
        """

        _validate_setting_key(key)
        _validate_setting_value(key, value)
        stored = read_json(self.control / "format.json")
        settings = _validate_settings(stored.get("settings", {}))
        settings[key] = value
        stored["settings"] = settings
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(settings)

    def unset_setting(self, key: str) -> dict[str, object]:
        """Remove one application setting, refusing one that is not set."""

        stored = read_json(self.control / "format.json")
        settings = _validate_settings(stored.get("settings", {}))
        if key not in settings:
            raise ValueError(f"application setting is not set: {key}")
        del settings[key]
        stored["settings"] = settings
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(settings)

    def seed_settings(self, seeds: Mapping[str, object]) -> dict[str, object]:
        """Merge *seeds* into the settings, keeping any value already set.

        Seeding happens once, when a workspace bound to a remote is created: the
        remote definition's whitelisted queue settings become the workspace's
        starting application settings. An explicit setting already present is
        never overwritten, so a value the operator chose outlives a reseed.
        """

        merged = _validate_settings(seeds)
        stored = read_json(self.control / "format.json")
        current = _validate_settings(stored.get("settings", {}))
        for key, value in merged.items():
            current.setdefault(key, value)
        stored["settings"] = current
        write_json_atomic(self.control / "format.json", stored, durable=self.durable)
        self.format = stored
        return dict(current)

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

    def _iter_state_subtree(
        self,
        directory: Path,
        rel: tuple[str, ...],
        after: tuple[str, ...] | None,
    ) -> Iterator[tuple[tuple[str, ...], "Marker | MarkerFault | None"]]:
        """Yield every entry below *directory* in stable pre-order, past *after*.

        ``directory`` sits at position ``rel`` relative to ``state/<kind>``.
        Each tuple is ``(position, entry)``: ``entry`` is the parsed marker or
        the fault of a marker-shaped file and ``None`` for every other visited
        entry — a subdirectory the walk descends, a foreign file, or a marker an
        earlier tick already consumed. The caller therefore counts one yield per
        directory entry examined, which is exactly the discovery budget.

        ``after`` prunes: an entry not strictly greater than it is skipped, and a
        subtree entirely at or below it is never opened, so resuming a walk costs
        the fan-out along the cursor path rather than a re-scan of all that came
        before it. The walk holds one directory's entries in memory at a time,
        never a materialized list of the tree, and never sorts globally.
        """

        state_root = self.control / "state"
        for entry in _scandir_sorted(directory):
            position = rel + (entry.name,)
            if _safe_is_dir(entry):
                relation = _cursor_relation(position, after)
                if relation == "skip":
                    continue
                yield position, None
                yield from self._iter_state_subtree(
                    directory / entry.name, position, after if relation == "resume" else None
                )
                continue
            if after is not None and position <= after:
                yield position, None
                continue
            if not _marker_shaped(entry.name):
                _LOGGER.debug("ignoring foreign file below state: %s", directory / entry.name)
                yield position, None
                continue
            path = directory / entry.name
            if not path.is_file():
                # A directory entry that does not resolve to a file is not a
                # marker the scan may report: it is a name the readdir surfaced
                # before the inode is visible, or one a concurrent transition
                # has already renamed away. Honouring the same ``is_file`` probe
                # the rename verification uses keeps discovery inside the one
                # visibility model, so a momentarily-invisible destination is
                # treated as absent rather than as a marker.
                yield position, None
                continue
            try:
                yield position, Marker.from_path(state_root, path)
            except (WorkflowError, ValueError) as exc:
                yield position, MarkerFault(path=path, reason=str(exc))

    def _subtree_roots(self, kind: str, roots: Sequence[PurePosixPath]) -> list[tuple[Path, tuple[str, ...]]]:
        """Return the ``(directory, position)`` roots a walk of *kind* starts at.

        With no placement assignment the walk starts at the whole kind; a
        deployment that restricts a manager to placement subtrees starts one walk
        per assigned prefix, so disjoint managers never scan each other's trees.
        """

        base = self.control / "state" / kind
        if not roots:
            return [(base, ())]
        return [(base.joinpath(*prefix.parts), prefix.parts) for prefix in roots]

    def walk_markers(
        self,
        kinds: Iterable[str] | None = None,
        *,
        roots: Sequence[PurePosixPath] = (),
        heartbeat: Callable[[], None] | None = None,
        heartbeat_every: int = DISCOVERY_HEARTBEAT_STRIDE,
    ) -> Iterator[Marker]:
        """Stream every schedulable marker of *kinds*, exhaustively.

        This is the streaming, cursorless counterpart of a bounded pass: it walks
        the same scandir tree with no discovery budget, reports every fault, and
        takes a heartbeat opportunity every *heartbeat_every* entries so a long
        exhaustive pass — polling running attempts, recovering claims — keeps its
        lease alive from inside the walk. A pass MAY restrict itself to placement
        *roots*; the debug workspace narrows what it surfaces through its private
        ``_scheduling_includes`` hook.
        """

        visited = 0
        for kind in tuple(kinds or CORE_STATE_KINDS):
            for start, rel in self._subtree_roots(kind, roots):
                for _position, entry in self._iter_state_subtree(start, rel, None):
                    visited += 1
                    if heartbeat is not None and visited % heartbeat_every == 0:
                        heartbeat()
                    if isinstance(entry, MarkerFault):
                        self.report_marker_fault(entry)
                    elif isinstance(entry, Marker) and self._scheduling_includes(entry):
                        yield entry

    def scan_marker_entries(self, kinds: Iterable[str] | None = None) -> Iterator[Marker | MarkerFault]:
        """Yield every marker below ``state/``, reporting damage per entry.

        One unusable entry must never hide the rest of the workspace, so a
        marker-shaped basename that fails validation is reported as a
        :class:`MarkerFault` instead of aborting the scan. This is the exhaustive
        walk the workspace tools (fsck, collection, status, harvest) use; the
        scheduling passes use the bounded :class:`~httk.workflow.workspace.MarkerStream` instead.
        """

        for kind in tuple(kinds or CORE_STATE_KINDS):
            for _position, entry in self._iter_state_subtree(self.control / "state" / kind, (), None):
                if entry is not None:
                    yield entry

    def scan_markers(self, kinds: Iterable[str] | None = None) -> Iterable[Marker]:
        for entry in self.scan_marker_entries(kinds):
            if isinstance(entry, Marker):
                yield entry
            else:
                self.report_marker_fault(entry)

    def _scheduling_includes(self, marker: Marker) -> bool:
        """Whether a scheduling scan of this workspace may surface *marker*.

        Production managers see the whole workspace; the private manager behind
        the foreground debug runner overrides this to the one job it is driving,
        so its streaming passes never schedule a marker outside that scope.
        """

        return True

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

    def _trim_index(self, index: "OrderedDict[str, _IndexEntry]") -> None:
        """Evict least-recently-touched entries until the cap is satisfied."""

        while len(index) > self._marker_index_capacity:
            index.popitem(last=False)

    def _index_note(self, marker: Marker) -> None:
        """Record where this instance just observed one job's marker.

        The index covers only the *active* kinds — a job that reaches a terminal
        kind, or moves into ``relocating`` or ``transferring``, is evicted rather
        than recorded, so the map tracks the working set and not the workspace's
        whole history. A recorded job is moved to the most-recently-touched end
        and the cap is enforced, so the index never grows without bound.
        """

        if self._marker_index is None:
            return
        if marker.kind not in ACTIVE_STATE_KINDS:
            self._marker_index.pop(marker.job_id, None)
            return
        self._marker_index[marker.job_id] = _IndexEntry(
            kind=marker.kind,
            placement=marker.placement,
            basename=marker.path.name,
        )
        self._marker_index.move_to_end(marker.job_id)
        self._trim_index(self._marker_index)

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

    def _rebuild_marker_index(self, wanted: str | None = None) -> Marker | None:
        """Rebuild the active index from one complete scan and locate *wanted*.

        The scan is deliberately the base-class one: a subclass may narrow what
        a manager *scans* for scheduling, but never what the workspace may look
        up, and an index built from a narrowed scan would answer "absent" for a
        job that is plainly there. Every core kind is walked so duplicates and a
        terminal *wanted* job are found, but only active jobs are cached — a
        finished job is located and returned without being kept, so the index
        never carries the history of a workspace that has run for years.
        """

        index: "OrderedDict[str, _IndexEntry]" = OrderedDict()
        duplicates: set[str] = set()
        seen: set[str] = set()
        found: Marker | None = None
        for entry in Workspace.scan_marker_entries(self, CORE_STATE_KINDS):
            if isinstance(entry, MarkerFault):
                self.report_marker_fault(entry)
                continue
            if entry.job_id in seen:
                duplicates.add(entry.job_id)
            seen.add(entry.job_id)
            if entry.kind in ACTIVE_STATE_KINDS:
                index[entry.job_id] = _IndexEntry(
                    kind=entry.kind,
                    placement=entry.placement,
                    basename=entry.path.name,
                )
            if wanted is not None and entry.job_id == wanted:
                found = entry
        self._trim_index(index)
        self._marker_index = index
        self._marker_duplicates = frozenset(duplicates)
        _LOGGER.debug(
            "rebuilt the marker index of workspace %s with %d active jobs of %d seen",
            self.workspace_id,
            len(index),
            len(seen),
        )
        return found

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
                    index.move_to_end(job_id)
                    return marker
                # The cached location is stale. The job most likely only changed
                # kind, and ordinary transitions never change placement, so the
                # finite state set at that placement is the cheap next probe.
                index.pop(job_id, None)
                probed = self._probe_placement(job_id, entry.placement)
                if probed is not None:
                    self._index_note(probed)
                    return probed
        found = self._rebuild_marker_index(job_id)
        if job_id in self._marker_duplicates:
            raise WorkspaceCorruptionError(f"job {job_id} has more than one state marker")
        return found

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
                    if self.durable:
                        # The rename installed a new name in the destination
                        # directory: a submitted or child marker, a published
                        # payload tree. A durable workspace synchronizes that
                        # directory entry so the publication survives a crash,
                        # not only a process interruption.
                        fsync_directory(destination.parent)
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
                    raise WorkspaceOperationError("move submission must remain on one filesystem") from exc
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


class MarkerStream:
    """A resumable, bounded, fair scandir walk of one active state kind.

    One stream serves one scheduling pass. It is workspace-level and reusable,
    but its cursor and rotation live only in the manager that holds it: nothing
    is written to disk, so two managers of one workspace never contend on a
    shared position, and a restarted manager simply begins a fresh cycle.

    Fairness is round-robin over the top-level placement roots the pass is
    assigned — or, with no assignment, the first-level components that exist
    under the kind. Each :meth:`advance` serves the roots in rotation, keeps a
    per-root resume position, and moves the rotation pointer on, so a subtree
    holding many markers can never starve a smaller sibling. Priority is therefore
    best-within-window rather than exact-global: bounded discovery
    is the whole point, and rotation is what guarantees the starved window is
    reached on a later tick.
    """

    def __init__(
        self,
        workspace: Workspace,
        kind: str,
        *,
        prefixes: Sequence[PurePosixPath] = (),
    ) -> None:
        self.workspace = workspace
        self.kind = kind
        self._prefixes = tuple(prefixes)
        # Root position -> the DFS position the last tick stopped after in that
        # root's subtree. A root absent from the map restarts from its
        # beginning, which is exactly what a completed cycle wants.
        self._cursors: dict[tuple[str, ...], tuple[str, ...]] = {}
        # The root the next advance begins its rotation at.
        self._rotation: tuple[str, ...] | None = None

    def _roots(self) -> list[tuple[str, ...]]:
        """Return the rotation roots, sorted, for one advance."""

        if self._prefixes:
            return sorted(prefix.parts for prefix in self._prefixes)
        base = self.workspace.control / "state" / self.kind
        return sorted((entry.name,) for entry in _scandir_sorted(base) if _safe_is_dir(entry))

    def advance(
        self,
        *,
        processing_budget: int,
        discovery_budget: int,
        heartbeat: Callable[[], None] | None = None,
        heartbeat_every: int = DISCOVERY_HEARTBEAT_STRIDE,
    ) -> list[Marker]:
        """Return up to *processing_budget* markers, visiting a bounded window.

        At most *processing_budget* markers are collected and at most
        *discovery_budget* *new* directory entries are visited; *heartbeat* is
        called every *heartbeat_every* entries examined, including the entries a
        resume re-lists on its way past the cursor. Roots are served in rotation
        with per-root resume, so the next tick continues where this one stopped
        and the rotation pointer advances so no root starves.

        A resume re-lists the entries at or before the cursor — ``scandir``
        cannot seek — so those are heartbeated but counted against neither the
        discovery budget nor the cursor. That keeps forward progress guaranteed:
        every advance either processes ``processing_budget`` markers past the
        cursor or exhausts the subtree, and never spends its whole budget
        re-skipping a directory it has already consumed.
        """

        roots = self._roots()
        if not roots:
            self._cursors.clear()
            self._rotation = None
            return []
        known = set(roots)
        self._cursors = {key: value for key, value in self._cursors.items() if key in known}
        start = bisect.bisect_left(roots, self._rotation) if self._rotation is not None else 0
        if start >= len(roots):
            start = 0
        order = roots[start:] + roots[:start]
        base = self.workspace.control / "state" / self.kind
        collected: list[Marker] = []
        visited = 0
        examined = 0
        last_root = order[0]
        for root in order:
            if len(collected) >= processing_budget or visited >= discovery_budget:
                break
            last_root = root
            after = self._cursors.get(root)
            reached_end = True
            for position, entry in self.workspace._iter_state_subtree(base.joinpath(*root), root, after):
                examined += 1
                if heartbeat is not None and examined % heartbeat_every == 0:
                    heartbeat()
                if after is not None and position <= after:
                    # An entry the walk re-lists while seeking past the cursor:
                    # already consumed on an earlier advance, so it neither
                    # advances the cursor nor spends the discovery budget.
                    continue
                visited += 1
                self._cursors[root] = position
                if isinstance(entry, MarkerFault):
                    self.workspace.report_marker_fault(entry)
                elif isinstance(entry, Marker) and self.workspace._scheduling_includes(entry):
                    collected.append(entry)
                if len(collected) >= processing_budget or visited >= discovery_budget:
                    reached_end = False
                    break
            if reached_end:
                # This root's subtree was exhausted within the budget, so the
                # next cycle restarts it rather than resuming a spent cursor.
                self._cursors.pop(root, None)
        self._rotation = roots[(roots.index(last_root) + 1) % len(roots)]
        return collected


class WorkspaceOperationError(WorkspaceUnavailableError):
    """A workspace operation could not be completed."""
