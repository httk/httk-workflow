"""Collect bounded, explicitly configured workspace garbage.

Except for a succeeded attempt's control directory, which its owning committing
manager removes after the commit is durable and its process is reaped, nothing in the engine frees disk
on its own. Managers do run the always-safe categories at startup and clean
exit; policy-gated artefacts orphaned by a crash are left in place. Over a long
campaign that costs
real space — one control directory per attempt, one journal writer directory
and one manager directory per process start, a full copy of every tree a
transaction replaced, an intact bundle for every transfer already acknowledged
— and on a quota'd HPC filesystem that is what fails first.

This module is the separate, explicit collector the specification asks for. It
is driven by ``policy.retention`` for aged categories: a limit that is not
configured means *keep*. A workspace whose operator has said nothing is still
tidied of things that cannot carry information at all — empty placement
mirrors, abandoned staging entries, and long-dead request receipts — plus the
one conditional case: a terminal marker whose payload the operator removed.

Everything here is conservative by construction. It never touches the
quarantine, a sealed transfer bundle, a persistent workdir, a payload beyond
its aged attempt-control directories, a marker of any job except a terminal
job whose payload directory is absent, a journal segment
that a current marker references, the directory of a manager that is still
heartbeating, ``runner-builds`` (machine-local rebuildable derived state), or
the runner store. Removal is bottom-up and mutates no
workspace state, so a collector that is killed halfway leaves a workspace that
is exactly as consistent as it was before.
"""

import logging
import os
import stat
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._manager_joins import children as join_children
from ._util import read_json, timestamp_seconds, utc_now, wait_for_paths
from .errors import FormatError, WorkflowError
from .journal import JournalWriter, parse_record_ref
from .models import (
    ATTEMPTS_DIRECTORY,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TERMINAL_KINDS,
    Marker,
)

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

#: The journal frame one collection appends. It is not a state frame, so every
#: reader that walks the journal for job history ignores it.
GC_FRAME_FORMAT = "httk-workflow-gc"
#: How long an entry may sit in a staging directory before it is certainly
#: abandoned. Every publication renames its staging entry out within one
#: operation, so a day is far beyond any honest in-flight window and this needs
#: no retention policy to be safe.
TMP_MAXIMUM_AGE_SECONDS = 24 * 60 * 60
#: How long a request claimed by a manager that has since died is kept. The
#: request either was applied — in which case the file is a receipt nobody
#: reads — or was interrupted, in which case a month is long past the point at
#: which replaying it against a moved job would be wanted.
RETIRED_REQUEST_MAXIMUM_AGE_SECONDS = 30 * 24 * 60 * 60
#: Every category a report carries, in the order a collection performs them.
GC_CATEGORIES = (
    "attempt_control",
    "removed_jobs",
    "transaction_trash",
    "retired_bundles",
    "transfer_records",
    "tmp_entries",
    "retired_requests",
    "journal_segments",
    "manager_directories",
    "placement_directories",
)
#: Categories whose entries cannot carry information and are safe to collect
#: without a retention policy. Managers run exactly these categories when they
#: attach and before a clean exit.
ALWAYS_SAFE_CATEGORIES = ("removed_jobs", "tmp_entries", "retired_requests", "placement_directories")
_SECONDS_PER_DAY = 86400.0


@dataclass(frozen=True)
class GcCategory:
    """Describe what one garbage category held and what became of it.

    :param name: Category name.
    :param candidates: Number of entries eligible for collection.
    :param removed: Number of entries removed.
    :param bytes_reclaimed: Estimated bytes held by the entries.
    :param entries: Paths of candidate entries.
    :param skipped: Whether this category was not selected or was gated.
    :param skip_reason: Why this category was skipped, when applicable.
    """

    name: str
    candidates: int = 0
    removed: int = 0
    bytes_reclaimed: int = 0
    entries: tuple[str, ...] = ()
    skipped: bool = False
    skip_reason: str | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this category.

        :return: JSON-compatible category members.
        """

        return {
            "name": self.name,
            "candidates": self.candidates,
            "removed": self.removed,
            "bytes_reclaimed": self.bytes_reclaimed,
            "entries": list(self.entries),
            "skipped": self.skipped,
            **({} if self.skip_reason is None else {"skip_reason": self.skip_reason}),
        }


@dataclass(frozen=True)
class GcReport:
    """Everything one collection considered, removed, and reclaimed.

    ``bytes_reclaimed`` is an estimate taken from the entries themselves before
    they were removed, so under a dry run it reports what a real run would free
    rather than what this one did.

    :param workspace_id: Identifier of the collected workspace.
    :param dry_run: Whether the collection made no filesystem changes.
    :param collected_at: Timestamp at which the report was produced.
    :param retention: Retention settings used for the collection.
    :param categories: Results for each collection category.
    :param removed_jobs: Job keys whose terminal markers were removed.
    :param record_ref: Journal reference for the collection record, if written.
    :param skipped: Categories or reasons skipped during collection.
    :param skipped_foreign: Foreign entries skipped by category.
    """

    workspace_id: str
    dry_run: bool
    collected_at: str
    retention: Mapping[str, object]
    categories: tuple[GcCategory, ...] = ()
    removed_jobs: tuple[str, ...] = ()
    record_ref: str | None = None
    skipped: tuple[str, ...] = ()
    skipped_foreign: Mapping[str, int] = field(default_factory=dict)

    @property
    def candidates(self) -> int:
        """Return how many entries the run found collectable.

        :return: Number of candidate entries.
        """

        return sum(category.candidates for category in self.categories)

    @property
    def removed(self) -> int:
        """Return how many entries the run actually removed.

        :return: Number of removed entries.
        """

        return sum(category.removed for category in self.categories)

    @property
    def bytes_reclaimed(self) -> int:
        """Return the estimated bytes the collected entries held.

        :return: Estimated reclaimed bytes.
        """

        return sum(category.bytes_reclaimed for category in self.categories)

    def category(self, name: str) -> GcCategory:
        """Return one named category, empty when the run collected nothing.

        :param name: Category name to find.
        :return: Matching category or an empty category with that name.
        """

        for category in self.categories:
            if category.name == name:
                return category
        return GcCategory(name)

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this report.

        :return: JSON-compatible report members.
        """

        return {
            "format": GC_FRAME_FORMAT,
            "format_version": 2,
            "workspace_id": self.workspace_id,
            "dry_run": self.dry_run,
            "collected_at": self.collected_at,
            "retention": dict(self.retention),
            "candidates": self.candidates,
            "removed": self.removed,
            "bytes_reclaimed": self.bytes_reclaimed,
            "skipped": list(self.skipped),
            "skipped_foreign": dict(self.skipped_foreign),
            "categories": [category.as_mapping() for category in self.categories],
            "removed_jobs": list(self.removed_jobs),
            **({} if self.record_ref is None else {"record_ref": self.record_ref}),
        }


@dataclass
class _Accumulator:
    """The running total of one category while a collection is in progress."""

    name: str
    candidates: int = 0
    removed: int = 0
    bytes_reclaimed: int = 0
    entries: list[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str | None = None

    def frozen(self) -> GcCategory:
        return GcCategory(
            name=self.name,
            candidates=self.candidates,
            removed=self.removed,
            bytes_reclaimed=self.bytes_reclaimed,
            entries=tuple(self.entries),
            skipped=self.skipped,
            skip_reason=self.skip_reason,
        )


def _entry_bytes(path: Path) -> int:
    """Return the size one directory entry occupies, or zero if it vanished."""

    try:
        return int(path.lstat().st_size)
    except OSError:
        return 0


def _tree_bytes(path: Path) -> int:
    """Estimate the bytes one entry holds, tolerating a concurrent removal.

    Directories are counted alongside files because the cost that bites on a
    quota'd filesystem is as often inodes and directory blocks as it is data.
    """

    total = _entry_bytes(path)
    if not path.is_dir() or path.is_symlink():
        return total
    for directory, directories, files in os.walk(path, topdown=True, onerror=lambda _: None):
        for name in (*directories, *files):
            total += _entry_bytes(Path(directory, name))
    return total


def _owned_by_current_user(path: Path) -> bool:
    """Return whether one entry may be removed by this process."""

    try:
        return path.lstat().st_uid == os.getuid()
    except FileNotFoundError:
        return True
    except OSError:
        return False


def _remove_tree(path: Path, *, dry_run: bool = False, on_foreign: Callable[[], None] | None = None) -> bool:
    """Remove one entry bottom-up, reporting whether it is gone afterwards.

    A killed collection must leave a workspace no less consistent than it found
    it, so removal proceeds from the leaves inwards and every step tolerates an
    entry that another process removed first. Nothing here is renamed and no
    state is rewritten, which is what makes an interrupted removal harmless:
    the partial remains are scratch that the next run collects again.
    """

    def foreign() -> None:
        if on_foreign is not None:
            on_foreign()

    if not _owned_by_current_user(path):
        foreign()
        return False
    if path.is_dir() and not path.is_symlink():
        failed = False

        def traversal_error(_error: OSError) -> None:
            nonlocal failed
            failed = True
            foreign()

        try:
            for directory, directories, files in os.walk(path, topdown=True, onerror=traversal_error):
                for name in (*directories, *files):
                    if not _owned_by_current_user(Path(directory, name)):
                        failed = True
                        foreign()
        except OSError:
            failed = True
            foreign()
        if failed or dry_run:
            return not failed

        def remove(entry: Path) -> bool:
            if not _owned_by_current_user(entry):
                foreign()
                return False
            if entry.is_dir() and not entry.is_symlink():
                try:
                    children = sorted(entry.iterdir())
                except OSError:
                    foreign()
                    return False
                if not all(remove(child) for child in children):
                    return False
                try:
                    entry.rmdir()
                except FileNotFoundError:
                    return True
                except OSError:
                    return False
                return True
            try:
                entry.unlink()
            except FileNotFoundError:
                return True
            except OSError:
                return False
            return True

        return remove(path)
    if dry_run:
        return True
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        _LOGGER.debug("cannot remove %s: %s", path, exc)
    return not path.exists()


def _mtime(path: Path) -> float | None:
    """Return the modification time of *path*, or ``None`` if it vanished."""

    try:
        return path.lstat().st_mtime
    except OSError:
        return None


def _iterdir(path: Path) -> list[Path]:
    """List one directory, treating an absent or unreadable one as empty."""

    try:
        return sorted(path.iterdir())
    except OSError:
        return []


def _is_real_directory(path: Path) -> bool:
    """Return whether *path* is an actual directory, without following links."""

    try:
        return stat.S_ISDIR(path.lstat().st_mode)
    except OSError:
        return False


def _marker_record_ref(name: str) -> str | None:
    """Return the record reference encoded in one marker basename."""

    reference = name.rsplit(".", 1)[-1]
    return None if reference == "init" else reference


class _Collection:
    """One pass over a workspace, driven by its retention policy."""

    def __init__(
        self,
        workspace: "Workspace",
        *,
        dry_run: bool,
        now: float,
        categories: Sequence[str] | None = None,
        journal_writer: JournalWriter | None = None,
        sizes: bool = True,
    ) -> None:
        self.workspace = workspace
        self.control = workspace.control
        self.dry_run = dry_run
        self.now = now
        self.retention = workspace.policy.retention
        self.journal_writer = journal_writer
        self.sizes = sizes
        requested = tuple(GC_CATEGORIES if categories is None else categories)
        unknown = set(requested) - set(GC_CATEGORIES)
        if unknown:
            raise ValueError(f"unknown garbage-collection category: {', '.join(sorted(unknown))}")
        selected = set(requested)
        # Manager-directory eligibility depends on the journal pass counting
        # the writer's surviving segments.
        if "manager_directories" in selected:
            selected.add("journal_segments")
        self._selected = frozenset(selected)
        self._accumulators = {name: _Accumulator(name) for name in GC_CATEGORIES}
        self._skipped: list[str] = []
        self._skipped_foreign: dict[str, int] = {}
        self._markers: list[Marker] | None = None
        self._removed_jobs: list[str] = []
        self._projected_removed: set[str] = set()
        self._live_managers: dict[str, str | None] | None = None
        self._opaque_live_manager = False
        # writer id -> how many of its segments survive this collection, which
        # is what decides whether the manager directory naming it is still
        # worth keeping.
        self._surviving_segments: dict[str, int] = {}

    # -- shared observations -------------------------------------------------

    def markers(self) -> list[Marker]:
        """Return every marker of the workspace, scanned exactly once."""

        if self._markers is None:
            self._markers = list(self.workspace.scan_markers(STATE_KINDS))
        return self._markers

    def live_managers(self) -> dict[str, str | None]:
        """Return the manager incarnations still heartbeating within a lease.

        The value is the journal writer each live manager owns, or ``None``
        when its ``manager.json`` cannot be read. A live manager whose writer
        cannot be determined makes every writer potentially live, which is
        recorded so that journal collection stands down entirely rather than
        guessing.
        """

        if self._live_managers is None:
            lease = self.workspace.policy.lease_seconds
            live: dict[str, str | None] = {}
            for manager_dir in _iterdir(self.control / "managers"):
                if not manager_dir.is_dir():
                    continue
                if not self._heartbeat_within(manager_dir, lease):
                    continue
                writer_id = self._writer_of(manager_dir)
                live[manager_dir.name] = writer_id
                if writer_id is None:
                    self._opaque_live_manager = True
            self._live_managers = live
        return self._live_managers

    def _heartbeat_within(self, manager_dir: Path, lease_seconds: float) -> bool:
        try:
            heartbeat = read_json(manager_dir / "heartbeat.json")
            updated = timestamp_seconds(str(heartbeat["updated_at"]))
        except (WorkflowError, KeyError, ValueError):
            return False
        return self.now - updated <= lease_seconds

    def _writer_of(self, manager_dir: Path) -> str | None:
        try:
            writer_id = read_json(manager_dir / "manager.json").get("writer_id")
        except WorkflowError:
            return None
        return writer_id if isinstance(writer_id, str) and writer_id else None

    def referenced_segments(self) -> set[tuple[str, int]]:
        """Return every journal segment a current marker's reference names.

        Only the segment a marker points *into* is protected. The frames behind
        it are deep history: collecting them is exactly what ``journal_days``
        buys, and the cost is that ``collect`` and ``job log`` report the job's
        timeline with ``gaps`` set rather than in full.

        A sealed transfer bundle keeps its marker inside the payload instead of
        the state tree, so the ledgers of bundles not yet retired are consulted
        as well and their segments are protected identically.
        """

        referenced: set[tuple[str, int]] = set()
        names = [marker.path.name for marker in self.markers()]
        for ledger_path in _iterdir(self.control / "transfers"):
            if not ledger_path.is_file() or ledger_path.suffix != ".json":
                continue
            try:
                ledger = read_json(ledger_path)
            except WorkflowError:
                continue
            if ledger.get("status") == "sealed" and isinstance(ledger.get("sealed_marker"), str):
                names.append(str(ledger["sealed_marker"]))
        for name in names:
            reference = _marker_record_ref(name)
            if reference is None:
                continue
            try:
                writer_id, segment, _offset, _length, _checksum = parse_record_ref(reference)
            except (FormatError, ValueError):
                continue
            referenced.add((writer_id, segment))
        return referenced

    # -- bookkeeping ---------------------------------------------------------

    def _cutoff(self, days: float | None) -> float | None:
        """Return the modification time at which *days* of retention expires."""

        return None if days is None else self.now - days * _SECONDS_PER_DAY

    def _aged(self, path: Path, cutoff: float) -> bool:
        modified = _mtime(path)
        return modified is not None and modified <= cutoff

    def _collect(self, category: str, path: Path, *, size: int | None = None) -> bool:
        """Account for one collectable entry and, unless dry, remove it."""

        self._account_candidate(category, path, size=size)
        foreign = 0

        def count_foreign() -> None:
            nonlocal foreign
            foreign += 1

        removed = _remove_tree(path, dry_run=self.dry_run, on_foreign=count_foreign)
        if foreign:
            self._skipped_foreign[category] = self._skipped_foreign.get(category, 0) + foreign
            _LOGGER.debug("skipping %s foreign entries below %s", foreign, path)
        if not self.dry_run and removed:
            accumulator = self._accumulators[category]
            accumulator.removed += 1
            _LOGGER.debug("collected %s entry %s", category, path)
        return removed

    def _account_candidate(self, category: str, path: Path, *, size: int | None = None) -> None:
        """Account for a candidate without attempting to remove it."""

        accumulator = self._accumulators[category]
        accumulator.candidates += 1
        accumulator.entries.append(str(path))
        accumulator.bytes_reclaimed += 0 if not self.sizes else (_tree_bytes(path) if size is None else size)

    def _entry_size(self, path: Path) -> int:
        """Return an entry's size when this pass is measuring sizes."""

        return _entry_bytes(path) if self.sizes else 0

    def _skip(self, category: str, reason: str) -> None:
        accumulator = self._accumulators[category]
        accumulator.skipped = True
        accumulator.skip_reason = reason
        self._skipped.append(f"{category}: {reason}")
        _LOGGER.debug("skipping %s collection: %s", category, reason)

    # -- categories ----------------------------------------------------------

    def collect_attempt_control(self) -> None:
        """Collect aged attempt-control directories of quiescent jobs.

        Failed and cancelled jobs retain their newest directory however old it
        is: it holds the outcome, the failure breadcrumb, and the runner's own
        logs for the attempt that decided the job. A succeeded job normally has
        no retained attempt evidence; a directory left after an owning manager
        dies before cleanup, or after an inherited commit, is collectable when
        it ages past this gate and the workspace lease grace. The grace also
        applies to every other non-failed/cancelled quiescent destination, so a
        runner lingering after publication is not collected immediately.
        """

        cutoff = self._cutoff(self.retention.attempt_control_days)
        if cutoff is None:
            self._skip("attempt_control", "retention.attempt_control_days is not configured")
            return
        lease_cutoff = self.now - self.workspace.policy.lease_seconds
        for marker in self.markers():
            if marker.kind not in QUIESCENT_KINDS:
                continue
            payload = self.workspace.payload_path(marker.placement, marker.job_key)
            controls: list[tuple[float, str, Path]] = []
            attempts = payload / ATTEMPTS_DIRECTORY
            if not _is_real_directory(attempts):
                continue
            for entry in _iterdir(attempts):
                if entry.is_symlink() or not _is_real_directory(entry):
                    continue
                modified = _mtime(entry)
                if modified is not None:
                    controls.append((modified, entry.name, entry))
            if marker.kind in {"failed", "cancelled"} and len(controls) < 2:
                continue
            controls.sort()
            retained = controls[-1:] if marker.kind in {"failed", "cancelled"} else ()
            for modified, _name, entry in controls:
                if entry in retained:
                    continue
                if self._aged(entry, cutoff) and (marker.kind in {"failed", "cancelled"} or modified <= lease_cutoff):
                    self._collect("attempt_control", entry)

    def _join_child_parents(self) -> dict[str, set[str]] | None:
        """Return non-terminal parents that reference each child job id.

        :return: Child job ids mapped to their non-terminal parent job keys, or
            ``None`` when a non-terminal marker's current state is unreadable.
        """

        parents: dict[str, set[str]] = {}
        for marker in self.markers():
            if marker.kind in TERMINAL_KINDS:
                continue
            try:
                state = self.workspace.read_state(marker)
                join = state.get("join")
                if join is None:
                    continue
                if not isinstance(join, Mapping):
                    raise WorkflowError("state.join is not an object")
                references = join_children(join)
                for reference in references:
                    child_id = reference.get("job_id") if isinstance(reference, Mapping) else None
                    if not isinstance(child_id, str):
                        raise WorkflowError("state.join.children contains an invalid child reference")
                    parents.setdefault(child_id, set()).add(marker.job_key)
            except (WorkflowError, OSError, TypeError, ValueError) as exc:
                self._skip("removed_jobs", f"cannot load non-terminal marker state {marker.job_key}: {exc}")
                return None
        return parents

    def collect_removed_jobs(self) -> None:
        """Collect terminal markers whose complete payload was removed.

        A non-terminal parent keeps a referenced child marker alive until the
        parent is terminal, because the parent may still need to observe that
        child's state. An unreadable non-terminal state makes the whole
        category stand down conservatively. After waiting for payload metadata,
        the parent map and current ``committing`` markers are checked again
        immediately before unlinking. This is a TOCTOU window of unbounded
        length in principle: GC may be descheduled between its rescan and the
        unlink. If a parent publishes a join referencing the removed child in
        that window, the join observes a missing child and may fail or stall;
        this is a scheduling-correctness consequence, not payload data loss.
        The operator rule is to remove children only when their parent is
        terminal; this guard is best-effort, not a lock.
        """

        parents = self._join_child_parents()
        if parents is None:
            return
        candidates: list[tuple[Marker, Path]] = []
        for marker in self.markers():
            if marker.kind not in TERMINAL_KINDS:
                continue
            payload = self.workspace.payload_path(marker.placement, marker.job_key)
            if not payload.exists():
                candidates.append((marker, payload))
        if not candidates:
            return
        absent = set(
            wait_for_paths(
                (payload for _marker, payload in candidates), deadline_seconds=self.workspace.visibility_deadline
            )
        )
        candidates = [(marker, payload) for marker, payload in candidates if payload in absent]
        if not candidates:
            return

        self._markers = list(self.workspace.scan_markers(STATE_KINDS))
        rescanned = {marker.path: marker for marker in self._markers if marker.kind in TERMINAL_KINDS}
        candidates = [
            (
                rescanned[marker.path],
                self.workspace.payload_path(rescanned[marker.path].placement, rescanned[marker.path].job_key),
            )
            for marker, _payload in candidates
            if marker.path in rescanned
        ]
        if not candidates:
            return
        parents = self._join_child_parents()
        if parents is None:
            return
        if any(marker.kind == "committing" for marker in self.markers()):
            self._skip("removed_jobs", "a marker is currently committing")
            return
        for marker, _payload in candidates:
            parent_keys = parents.get(marker.job_id)
            if parent_keys:
                self._account_candidate("removed_jobs", marker.path, size=self._entry_size(marker.path))
                parent_text = ", ".join(sorted(parent_keys))
                self._skip(
                    "removed_jobs",
                    f"kept child {marker.job_key}: referenced by non-terminal parent(s) {parent_text}",
                )
                continue
            removed = self._collect("removed_jobs", marker.path, size=self._entry_size(marker.path))
            if removed:
                self._markers = [item for item in self.markers() if item.path != marker.path]
                if self.dry_run:
                    self._projected_removed.add(str(marker.path))
                else:
                    self._removed_jobs.append(marker.job_key)

    def collect_transaction_trash(self) -> None:
        """Collect the trees a replayed transaction moved aside.

        The trash of a transaction is what makes its replay idempotent, so it
        is only collectable once the job has left ``committing`` for a
        quiescent state: at that point the destination transition has happened
        and no replay will consult it again.
        """

        cutoff = self._cutoff(self.retention.trash_days)
        if cutoff is None:
            self._skip("transaction_trash", "retention.trash_days is not configured")
            return
        for marker in self.markers():
            if marker.kind not in QUIESCENT_KINDS:
                continue
            payload = self.workspace.payload_path(marker.placement, marker.job_key)
            attempts = payload / ATTEMPTS_DIRECTORY
            if not _is_real_directory(attempts):
                continue
            for control in _iterdir(attempts):
                if control.is_symlink() or not _is_real_directory(control):
                    continue
                outcome_ready = control / "outcome.ready"
                transaction = outcome_ready / "transaction"
                trash = transaction / "trash"
                if (
                    not _is_real_directory(outcome_ready)
                    or not _is_real_directory(transaction)
                    or not _is_real_directory(trash)
                ):
                    continue
                for entry in _iterdir(trash):
                    if self._aged(entry, cutoff):
                        self._collect("transaction_trash", entry)
                self._rmdir(trash, "transaction_trash")

    def collect_retired_bundles(self) -> None:
        """Collect the transfer bundles a completed handover left behind.

        A retired bundle is a full second copy of a payload the destination has
        already acknowledged, which makes it the largest thing a busy transfer
        campaign accumulates. Its ledger is deliberately left untouched, so the
        audit record of the transfer survives the bytes it describes.
        """

        cutoff = self._cutoff(self.retention.trash_days)
        if cutoff is None:
            self._skip("retired_bundles", "retention.trash_days is not configured")
            return
        for entry in _iterdir(self.control / "transfers" / "retired"):
            if entry.is_dir() and self._aged(entry, cutoff):
                self._collect("retired_bundles", entry)

    def collect_transfer_records(self) -> None:
        """Collect the per-transfer receipts of imports already completed.

        An acknowledgement is the destination's idempotency receipt for one
        import. Collecting it after the configured trash retention means a
        bundle re-offered later than that is imported afresh, which the
        duplicate-job check still recognizes for as long as the imported job
        exists in this workspace.
        """

        cutoff = self._cutoff(self.retention.trash_days)
        if cutoff is None:
            self._skip("transfer_records", "retention.trash_days is not configured")
            return
        for name in ("acks", "imported"):
            for entry in _iterdir(self.control / "transfers" / name):
                if entry.is_file() and entry.suffix == ".json" and self._aged(entry, cutoff):
                    self._collect("transfer_records", entry, size=self._entry_size(entry))

    def collect_tmp_entries(self) -> None:
        """Collect abandoned staging entries, which need no retention policy.

        Every publication creates its staging entry and renames it away inside
        one operation, so an entry still sitting in a staging directory a day
        later belongs to a process that died and can never be resumed.
        """

        cutoff = self.now - TMP_MAXIMUM_AGE_SECONDS
        for staging in (self.control / "tmp", self.control / "requests" / "tmp"):
            for entry in _iterdir(staging):
                if self._aged(entry, cutoff):
                    self._collect("tmp_entries", entry)

    def collect_retired_requests(self) -> None:
        """Collect month-old request leftovers: claimed by the dead, and retired.

        Two directories hold requests that will never be acted on again, and
        both are always safe to prune after the same month: one claimed by a
        manager that is no longer heartbeating, and one a manager explicitly
        retired because it could never become actionable. The retirement record
        written beside a retired request ages out with it, since it only ever
        explained that one file.
        """

        cutoff = self.now - RETIRED_REQUEST_MAXIMUM_AGE_SECONDS
        live = self.live_managers()
        for manager_dir in _iterdir(self.control / "requests" / "claimed"):
            if not manager_dir.is_dir() or manager_dir.name in live:
                continue
            for entry in _iterdir(manager_dir):
                if entry.is_file() and self._aged(entry, cutoff):
                    self._collect("retired_requests", entry, size=self._entry_size(entry))
            self._rmdir(manager_dir, "retired_requests")
        for entry in _iterdir(self.control / "requests" / "retired"):
            if entry.is_file() and self._aged(entry, cutoff):
                self._collect("retired_requests", entry, size=self._entry_size(entry))

    def collect_journal_segments(self) -> None:
        """Collect aged journal segments no current marker references.

        Three conditions must hold together: the segment is older than
        ``journal_days``, no marker of this workspace — nor the sealed marker
        of a bundle awaiting handover — references it, and the writer that
        produced it belongs to no manager still heartbeating. The consequence
        is honest and worth stating: the deep history of an old job goes with
        the segments, and ``collect`` and ``job log`` then report that job's
        timeline with ``gaps`` set.
        """

        journal = self.control / "journal"
        cutoff = self._cutoff(self.retention.journal_days)
        if cutoff is None:
            self._skip("journal_segments", "retention.journal_days is not configured")
            self._count_all_segments(journal)
            return
        live = self.live_managers()
        if self._opaque_live_manager:
            self._skip("journal_segments", "a live manager does not name its journal writer")
            self._count_all_segments(journal)
            return
        live_writers = {writer_id for writer_id in live.values() if writer_id is not None}
        referenced = self.referenced_segments()
        for writer_dir in _iterdir(journal):
            writer_id = _writer_id_of(writer_dir)
            if writer_id is None:
                continue
            segments = [path for path in _iterdir(writer_dir) if path.suffix == ".hwj"]
            if writer_id in live_writers:
                self._surviving_segments[writer_id] = len(segments)
                continue
            surviving = 0
            for path in segments:
                number = _segment_number(path)
                if number is None or (writer_id, number) in referenced or not self._aged(path, cutoff):
                    surviving += 1
                    continue
                self._collect("journal_segments", path, size=self._entry_size(path))
            self._surviving_segments[writer_id] = surviving
            if surviving == 0:
                self._rmdir(writer_dir, "journal_segments")

    def _count_all_segments(self, journal: Path) -> None:
        """Record every segment as surviving, for a run that prunes none."""

        for writer_dir in _iterdir(journal):
            writer_id = _writer_id_of(writer_dir)
            if writer_id is not None:
                self._surviving_segments[writer_id] = len([p for p in _iterdir(writer_dir) if p.suffix == ".hwj"])

    def collect_manager_directories(self) -> None:
        """Collect the directories of dead manager incarnations.

        A manager directory is the only mapping from a journal writer to the
        host, process, and pools that produced it. A crashed directory may
        outlive its writer's segments until policy-gated collection removes it;
        clean managers remove their own directory. One whose
        ``manager.json`` cannot be read names no writer and is therefore kept:
        it is already an anomaly, and it is a few hundred bytes.
        """

        cutoff = self._cutoff(self.retention.journal_days)
        if cutoff is None:
            self._skip("manager_directories", "retention.journal_days is not configured")
            return
        live = self.live_managers()
        for manager_dir in _iterdir(self.control / "managers"):
            if not manager_dir.is_dir() or manager_dir.name in live:
                continue
            if not self._aged(manager_dir, cutoff):
                continue
            writer_id = self._writer_of(manager_dir)
            if writer_id is None or self._surviving_segments.get(writer_id, 0) > 0:
                continue
            self._collect("manager_directories", manager_dir)

    def collect_placement_directories(self) -> None:
        """Collect the empty placement mirrors left below every state kind.

        A job's placement is mirrored under each state kind it passes through,
        so a deep or job-unique placement leaves one empty hierarchy behind per
        kind. Pruning them needs no retention policy because an empty directory
        carries nothing, and it races a transition that is recreating exactly
        this path: the removal is therefore a bare ``rmdir`` whose failure is
        the expected outcome rather than an error.
        """

        for kind_dir in _iterdir(self.control / "state"):
            if not kind_dir.is_dir():
                continue
            emptied: set[str] = set()
            for directory, directories, files in os.walk(kind_dir, topdown=False, onerror=lambda _: None):
                actual_files = [name for name in files if os.path.join(directory, name) not in self._projected_removed]
                if directory == str(kind_dir) or actual_files:
                    continue
                if any(os.path.join(directory, name) not in emptied for name in directories):
                    continue
                path = Path(directory)
                accumulator = self._accumulators["placement_directories"]
                accumulator.candidates += 1
                accumulator.entries.append(directory)
                if self.sizes:
                    accumulator.bytes_reclaimed += _entry_bytes(path)
                if self.dry_run:
                    if _owned_by_current_user(path):
                        emptied.add(directory)
                    else:
                        self._skipped_foreign["placement_directories"] = (
                            self._skipped_foreign.get("placement_directories", 0) + 1
                        )
                elif self._rmdir(path, "placement_directories"):
                    accumulator.removed += 1
                    emptied.add(directory)

    def _rmdir(self, path: Path, category: str) -> bool:
        """Remove one directory if it is empty, tolerating every reason not to.

        A directory that is not empty, that another process removed first, or
        that a concurrent transition is repopulating are all ordinary outcomes
        here, never faults.
        """

        if not _owned_by_current_user(path):
            self._skipped_foreign[category] = self._skipped_foreign.get(category, 0) + 1
            _LOGGER.debug("skipping foreign %s directory %s", category, path)
            return False
        if self.dry_run:
            return False
        try:
            path.rmdir()
        except OSError:
            return False
        return True

    # -- driving -------------------------------------------------------------

    def execute(self) -> GcReport:
        """Perform every category in order and publish the resulting report."""

        for category in GC_CATEGORIES:
            if category not in self._selected:
                self._skip(category, "not selected")
                continue
            getattr(self, f"collect_{category}")()
        report = GcReport(
            workspace_id=self.workspace.workspace_id,
            dry_run=self.dry_run,
            collected_at=utc_now(),
            retention=self.retention.as_mapping(),
            categories=tuple(self._accumulators[name].frozen() for name in GC_CATEGORIES),
            removed_jobs=tuple(self._removed_jobs),
            skipped=tuple(self._skipped),
            skipped_foreign=dict(self._skipped_foreign),
        )
        return self._journal(report)

    def _journal(self, report: GcReport) -> GcReport:
        """Append one frame describing what was removed, if anything was.

        A run that removed nothing writes nothing: opening a journal writer
        creates a writer directory, and an empty collection that leaves one
        behind would be creating the garbage it came to collect.
        """

        if self.dry_run or not report.removed:
            return report
        frame: dict[str, Any] = {
            "format": GC_FRAME_FORMAT,
            "format_version": 2,
            "workspace_id": self.workspace.workspace_id,
            "collected_at": report.collected_at,
            "retention": dict(report.retention),
            "removed": report.removed,
            "bytes_reclaimed": report.bytes_reclaimed,
            "removed_jobs": list(report.removed_jobs),
            "skipped_foreign": dict(report.skipped_foreign),
            "categories": {
                category.name: {
                    "candidates": category.candidates,
                    "removed": category.removed,
                    "bytes_reclaimed": category.bytes_reclaimed,
                    "skipped": category.skipped,
                    **({} if category.skip_reason is None else {"skip_reason": category.skip_reason}),
                }
                for category in report.categories
            },
        }
        if self.journal_writer is not None:
            record_ref = self.journal_writer.append(frame)
        else:
            with self.workspace.open_journal_writer() as writer:
                record_ref = writer.append(frame)
        _LOGGER.info(
            "collected %d entries and about %d bytes from workspace %s",
            report.removed,
            report.bytes_reclaimed,
            self.workspace.workspace_id,
            extra={
                "event": "garbage_collected",
                "workspace_id": self.workspace.workspace_id,
                "removed": report.removed,
                "bytes_reclaimed": report.bytes_reclaimed,
            },
        )
        return GcReport(
            workspace_id=report.workspace_id,
            dry_run=report.dry_run,
            collected_at=report.collected_at,
            retention=report.retention,
            categories=report.categories,
            removed_jobs=report.removed_jobs,
            record_ref=record_ref,
            skipped=report.skipped,
            skipped_foreign=report.skipped_foreign,
        )


def _writer_id_of(writer_dir: Path) -> str | None:
    """Return the writer UUID one journal directory name denotes."""

    if not writer_dir.is_dir():
        return None
    try:
        writer_id = str(uuid.UUID(writer_dir.name))
    except ValueError:
        return None
    return writer_id if writer_id == writer_dir.name else None


def _segment_number(path: Path) -> int | None:
    """Return the segment number one journal file name encodes."""

    try:
        return int(path.stem, 36)
    except ValueError:
        return None


def collect_garbage(
    workspace: "Workspace",
    *,
    dry_run: bool = False,
    now: float | None = None,
    categories: Sequence[str] | None = None,
    journal_writer: JournalWriter | None = None,
    sizes: bool = True,
) -> GcReport:
    """Collect what the retention policy of *workspace* permits.

    Nothing is removed for a category whose limit is unset: an unconfigured
    retention means keep. The exceptions are the categories that cannot carry
    information — empty placement mirrors, staging entries abandoned for a day,
    and month-old request leftovers, whether claimed by a manager that is gone
    or explicitly retired — plus ``removed_jobs``, which is collected whatever
    the policy says.

    With *dry_run* the workspace is not touched at all and the report describes
    what a real run would have removed. *now* overrides the moment every age is
    measured against, which is how a test ages a workspace deterministically.

    :param workspace: Workspace whose retention policy is applied.
    :param dry_run: Whether to report candidates without removing them.
    :param now: Timestamp used to evaluate age limits.
    :param categories: Restrict collection to these categories, in the
        canonical :data:`GC_CATEGORIES` order. Selecting
        ``manager_directories`` also selects ``journal_segments`` because the
        manager-directory decision depends on that category's surviving
        segment count.
    :param journal_writer: Append a collection frame to this already-open
        writer instead of opening a new writer.
    :param sizes: Whether to calculate byte estimates. Set this to ``False``
        for count-only scans; no tree byte-sizing is performed and reported
        reclaimed bytes are zero.
    :return: Collection report.
    """

    return _Collection(
        workspace,
        dry_run=dry_run,
        now=time.time() if now is None else now,
        categories=categories,
        journal_writer=journal_writer,
        sizes=sizes,
    ).execute()


def iter_report_rows(report: GcReport) -> Iterator[tuple[str, int, int, int]]:
    """Yield the category rows a command-line collection prints.

    :param report: Collection report to render.
    :yield: Category row for each report category and the final total.
    """

    for category in report.categories:
        yield category.name, category.candidates, category.removed, category.bytes_reclaimed
    yield "total", report.candidates, report.removed, report.bytes_reclaimed
