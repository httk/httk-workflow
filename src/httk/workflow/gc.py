"""Bounded collection of the disk a running workspace accumulates.

Nothing in the engine frees disk on its own, and that is deliberate: neither a
runner nor a manager is ever required to run cleanup code, so every artefact a
crash could orphan is simply left in place. Over a long campaign that costs
real space — one control directory per attempt, one journal writer directory
and one manager directory per process start, a full copy of every tree a
transaction replaced, an intact bundle for every transfer already acknowledged
— and on a quota'd HPC filesystem that is what fails first.

This module is the separate, explicit collector the specification asks for. It
is driven entirely by ``policy.retention``: a limit that is not configured
means *keep*, so a workspace whose operator has said nothing is only ever
tidied of things that cannot carry information at all — empty placement
mirrors, abandoned staging entries, and long-dead request receipts.

Everything here is conservative by construction. It never touches the
quarantine, a sealed transfer bundle, a persistent workdir, a payload beyond
its aged attempt-control directories, the marker of any job, a journal segment
a current marker references, the directory of a manager that is still
heartbeating, or the runner store. Removal is bottom-up and mutates no
workspace state, so a collector that is killed halfway leaves a workspace that
is exactly as consistent as it was before.
"""

import logging
import os
import shutil
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._util import read_json, timestamp_seconds, utc_now
from .errors import FormatError, WorkflowError
from .journal import parse_record_ref
from .models import (
    ATTEMPT_CONTROL_PREFIX,
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
    "transaction_trash",
    "retired_bundles",
    "transfer_records",
    "tmp_entries",
    "retired_requests",
    "journal_segments",
    "manager_directories",
    "placement_directories",
)
_SECONDS_PER_DAY = 86400.0


@dataclass(frozen=True)
class GcCategory:
    """What one category of collectable garbage held and what became of it."""

    name: str
    candidates: int = 0
    removed: int = 0
    bytes_reclaimed: int = 0
    entries: tuple[str, ...] = ()

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this category."""

        return {
            "name": self.name,
            "candidates": self.candidates,
            "removed": self.removed,
            "bytes_reclaimed": self.bytes_reclaimed,
            "entries": list(self.entries),
        }


@dataclass(frozen=True)
class GcReport:
    """Everything one collection considered, removed, and reclaimed.

    ``bytes_reclaimed`` is an estimate taken from the entries themselves before
    they were removed, so under a dry run it reports what a real run would free
    rather than what this one did.
    """

    workspace_id: str
    dry_run: bool
    collected_at: str
    retention: Mapping[str, object]
    categories: tuple[GcCategory, ...] = ()
    record_ref: str | None = None
    skipped: tuple[str, ...] = ()

    @property
    def candidates(self) -> int:
        """Return how many entries the run found collectable."""

        return sum(category.candidates for category in self.categories)

    @property
    def removed(self) -> int:
        """Return how many entries the run actually removed."""

        return sum(category.removed for category in self.categories)

    @property
    def bytes_reclaimed(self) -> int:
        """Return the estimated bytes the collected entries held."""

        return sum(category.bytes_reclaimed for category in self.categories)

    def category(self, name: str) -> GcCategory:
        """Return one named category, empty when the run collected nothing."""

        for category in self.categories:
            if category.name == name:
                return category
        return GcCategory(name)

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this report."""

        return {
            "format": GC_FRAME_FORMAT,
            "format_version": 1,
            "workspace_id": self.workspace_id,
            "dry_run": self.dry_run,
            "collected_at": self.collected_at,
            "retention": dict(self.retention),
            "candidates": self.candidates,
            "removed": self.removed,
            "bytes_reclaimed": self.bytes_reclaimed,
            "skipped": list(self.skipped),
            "categories": [category.as_mapping() for category in self.categories],
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

    def frozen(self) -> GcCategory:
        return GcCategory(
            name=self.name,
            candidates=self.candidates,
            removed=self.removed,
            bytes_reclaimed=self.bytes_reclaimed,
            entries=tuple(self.entries),
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


def _remove_tree(path: Path) -> bool:
    """Remove one entry bottom-up, reporting whether it is gone afterwards.

    A killed collection must leave a workspace no less consistent than it found
    it, so removal proceeds from the leaves inwards and every step tolerates an
    entry that another process removed first. Nothing here is renamed and no
    state is rewritten, which is what makes an interrupted removal harmless:
    the partial remains are scratch that the next run collects again.
    """

    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path, onexc=lambda _function, _path, _error: None)
        else:
            path.unlink(missing_ok=True)
    except OSError as exc:  # pragma: no cover - defensive; rmtree swallows its own
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


def _marker_record_ref(name: str) -> str | None:
    """Return the record reference encoded in one marker basename."""

    reference = name.rsplit(".", 1)[-1]
    return None if reference == "init" else reference


class _Collection:
    """One pass over a workspace, driven by its retention policy."""

    def __init__(self, workspace: "Workspace", *, dry_run: bool, now: float) -> None:
        self.workspace = workspace
        self.control = workspace.control
        self.dry_run = dry_run
        self.now = now
        self.retention = workspace.policy.retention
        self._accumulators = {name: _Accumulator(name) for name in GC_CATEGORIES}
        self._skipped: list[str] = []
        self._markers: list[Marker] | None = None
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
        buys, and the cost is that ``harvest`` and ``job log`` report the job's
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

    def _collect(self, category: str, path: Path, *, size: int | None = None) -> None:
        """Account for one collectable entry and, unless dry, remove it."""

        accumulator = self._accumulators[category]
        accumulator.candidates += 1
        accumulator.entries.append(str(path))
        accumulator.bytes_reclaimed += _tree_bytes(path) if size is None else size
        if self.dry_run:
            return
        if _remove_tree(path):
            accumulator.removed += 1
            _LOGGER.debug("collected %s entry %s", category, path)

    def _skip(self, category: str, reason: str) -> None:
        self._skipped.append(f"{category}: {reason}")
        _LOGGER.debug("skipping %s collection: %s", category, reason)

    # -- categories ----------------------------------------------------------

    def collect_attempt_control(self) -> None:
        """Collect the aged attempt-control directories of terminal jobs.

        The newest directory of a job is never collected however old it is: it
        holds the outcome, the failure breadcrumb, and the runner's own logs
        for the attempt that decided the job, which is precisely what an
        operator looks at when a terminal job is questioned much later.
        """

        cutoff = self._cutoff(self.retention.attempt_control_days)
        if cutoff is None:
            self._skip("attempt_control", "retention.attempt_control_days is not configured")
            return
        for marker in self.markers():
            if marker.kind not in TERMINAL_KINDS:
                continue
            payload = self.workspace.payload_path(marker.placement, marker.job_key)
            controls: list[tuple[float, str, Path]] = []
            for entry in _iterdir(payload):
                if not entry.name.startswith(ATTEMPT_CONTROL_PREFIX) or not entry.is_dir():
                    continue
                modified = _mtime(entry)
                if modified is not None:
                    controls.append((modified, entry.name, entry))
            if len(controls) < 2:
                continue
            controls.sort()
            for _modified, _name, entry in controls[:-1]:
                if self._aged(entry, cutoff):
                    self._collect("attempt_control", entry)

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
            for control in _iterdir(payload):
                if not control.name.startswith(ATTEMPT_CONTROL_PREFIX):
                    continue
                trash = control / "outcome.ready" / "transaction" / "trash"
                if not trash.is_dir():
                    continue
                for entry in _iterdir(trash):
                    if self._aged(entry, cutoff):
                        self._collect("transaction_trash", entry)
                self._rmdir(trash)

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
                    self._collect("transfer_records", entry, size=_entry_bytes(entry))

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
                    self._collect("retired_requests", entry, size=_entry_bytes(entry))
            self._rmdir(manager_dir)
        for entry in _iterdir(self.control / "requests" / "retired"):
            if entry.is_file() and self._aged(entry, cutoff):
                self._collect("retired_requests", entry, size=_entry_bytes(entry))

    def collect_journal_segments(self) -> None:
        """Collect aged journal segments no current marker references.

        Three conditions must hold together: the segment is older than
        ``journal_days``, no marker of this workspace — nor the sealed marker
        of a bundle awaiting handover — references it, and the writer that
        produced it belongs to no manager still heartbeating. The consequence
        is honest and worth stating: the deep history of an old job goes with
        the segments, and ``harvest`` and ``job log`` then report that job's
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
                self._collect("journal_segments", path, size=_entry_bytes(path))
            self._surviving_segments[writer_id] = surviving
            if surviving == 0:
                self._rmdir(writer_dir)

    def _count_all_segments(self, journal: Path) -> None:
        """Record every segment as surviving, for a run that prunes none."""

        for writer_dir in _iterdir(journal):
            writer_id = _writer_id_of(writer_dir)
            if writer_id is not None:
                self._surviving_segments[writer_id] = len([p for p in _iterdir(writer_dir) if p.suffix == ".hwj"])

    def collect_manager_directories(self) -> None:
        """Collect the directories of dead manager incarnations.

        A manager directory is the only mapping from a journal writer to the
        host, process, and pools that produced it, so it outlives its manager
        for exactly as long as that writer's segments do. One whose
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
                if directory == str(kind_dir) or files:
                    continue
                if any(os.path.join(directory, name) not in emptied for name in directories):
                    continue
                path = Path(directory)
                accumulator = self._accumulators["placement_directories"]
                accumulator.candidates += 1
                accumulator.entries.append(directory)
                accumulator.bytes_reclaimed += _entry_bytes(path)
                if self.dry_run:
                    emptied.add(directory)
                elif self._rmdir(path):
                    accumulator.removed += 1
                    emptied.add(directory)

    def _rmdir(self, path: Path) -> bool:
        """Remove one directory if it is empty, tolerating every reason not to.

        A directory that is not empty, that another process removed first, or
        that a concurrent transition is repopulating are all ordinary outcomes
        here, never faults.
        """

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

        self.collect_attempt_control()
        self.collect_transaction_trash()
        self.collect_retired_bundles()
        self.collect_transfer_records()
        self.collect_tmp_entries()
        self.collect_retired_requests()
        self.collect_journal_segments()
        self.collect_manager_directories()
        self.collect_placement_directories()
        report = GcReport(
            workspace_id=self.workspace.workspace_id,
            dry_run=self.dry_run,
            collected_at=utc_now(),
            retention=self.retention.as_mapping(),
            categories=tuple(self._accumulators[name].frozen() for name in GC_CATEGORIES),
            skipped=tuple(self._skipped),
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
            "format_version": 1,
            "workspace_id": self.workspace.workspace_id,
            "collected_at": report.collected_at,
            "retention": dict(report.retention),
            "removed": report.removed,
            "bytes_reclaimed": report.bytes_reclaimed,
            "categories": {
                category.name: {
                    "candidates": category.candidates,
                    "removed": category.removed,
                    "bytes_reclaimed": category.bytes_reclaimed,
                }
                for category in report.categories
            },
        }
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
            record_ref=record_ref,
            skipped=report.skipped,
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
) -> GcReport:
    """Collect what the retention policy of *workspace* permits.

    Nothing is removed for a category whose limit is unset: an unconfigured
    retention means keep. The exceptions are the categories that cannot carry
    information — empty placement mirrors, staging entries abandoned for a day,
    and month-old request leftovers, whether claimed by a manager that is gone
    or explicitly retired — which are collected whatever the policy says.

    With *dry_run* the workspace is not touched at all and the report describes
    what a real run would have removed. *now* overrides the moment every age is
    measured against, which is how a test ages a workspace deterministically.
    """

    return _Collection(workspace, dry_run=dry_run, now=time.time() if now is None else now).execute()


def iter_report_rows(report: GcReport) -> Iterator[tuple[str, int, int, int]]:
    """Yield the category rows a command-line collection prints."""

    for category in report.categories:
        yield category.name, category.candidates, category.removed, category.bytes_reclaimed
    yield "total", report.candidates, report.removed, report.bytes_reclaimed
