"""Collect a workspace's garbage on the filesystem kernel.

One collection runs the categories of :data:`GC_CATEGORIES`, in order:

- ``dead_owners``: owners proven dead are recovered (their jobs return to the
  states they were claimed from); a same-host CLI owner killed by a signal is
  provable only on its host, so ``httk workspace gc`` there recovers it;
- ``attempt_control``: ``attempts/<A>/`` of unowned jobs older than
  ``retention.attempt_control_days`` (claim, remove, release back); a failed or
  cancelled job keeps its newest attempt;
- ``placement_directories``: empty placement directories of the unowned states;
- ``requests``: request files older than a day that are malformed or whose job
  cannot be found go to ``quarantine/``;
- ``manager_logs``: ``logs/managers/<owner-id>.log`` of owners that are gone,
  older than ``retention.trash_days``;
- ``owner_tombstones``: ``dead.json`` of recovered owners older than
  ``retention.owner_tombstone_days``;
- ``tmp_entries``: write temporaries older than a day in ``requests/`` and
  ``owners/*/``, and ``tmp/trash.<token>`` entries a crashed removal left.

A retention value of ``None`` (``null`` or ``"keep"``) skips its category.
Every removal goes through :mod:`httk.workflow._fs` or the kernel, so a
collection killed halfway leaves the workspace as consistent as before.
"""

import logging
import os
import stat
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from . import _death, _fs, _kernel, _requests
from ._kernel import Release
from ._state import UNOWNED_STATES
from ._util import json_bytes
from .errors import FormatError, WorkflowError
from .models import LOGS_DIRECTORY

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .workspace import Workspace

__all__ = [
    "GC_CATEGORIES",
    "GC_REPORT_FORMAT",
    "REQUEST_GRACE_SECONDS",
    "TMP_MAXIMUM_AGE_SECONDS",
    "GcCategory",
    "GcReport",
    "collect_garbage",
    "iter_report_rows",
    "quarantine",
    "quarantine_damaged",
]

_LOGGER = logging.getLogger(__name__)

GC_REPORT_FORMAT = "httk-workflow-gc"
#: How old a write temporary or a ``tmp/trash.<token>`` entry must be before it is garbage.
TMP_MAXIMUM_AGE_SECONDS = 24 * 60 * 60
#: How long a malformed request, or one whose job cannot be found, waits before it is quarantined.
REQUEST_GRACE_SECONDS = 24 * 60 * 60
#: Every category, in the order a collection runs them.
GC_CATEGORIES = (
    "dead_owners",
    "attempt_control",
    "placement_directories",
    "requests",
    "manager_logs",
    "owner_tombstones",
    "tmp_entries",
)
_DAY = 86400.0
_PRUNE_BUDGET = 100_000
_TRASH_NAME_LENGTH = len("trash.") + 16


@dataclass(frozen=True)
class GcCategory:
    """What one category found and removed.

    :param name: The category.
    :param candidates: Entries eligible for collection.
    :param removed: Entries removed (or quarantined, or recovered).
    :param bytes_reclaimed: Estimated bytes the candidates held.
    :param entries: The candidate paths.
    :param skipped: Whether the category did not run.
    :param skip_reason: Why it did not run.
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
    """Everything one collection considered and removed.

    :param workspace_id: The workspace.
    :param dry_run: Whether nothing was changed.
    :param collected_at: When the collection ran.
    :param retention: The retention policy it applied.
    :param categories: One result per category, in :data:`GC_CATEGORIES` order.
    """

    workspace_id: str
    dry_run: bool
    collected_at: str
    retention: Mapping[str, object]
    categories: tuple[GcCategory, ...] = ()

    @property
    def candidates(self) -> int:
        """The number of collectable entries found."""

        return sum(category.candidates for category in self.categories)

    @property
    def removed(self) -> int:
        """The number of entries removed."""

        return sum(category.removed for category in self.categories)

    @property
    def bytes_reclaimed(self) -> int:
        """The estimated bytes the candidates held."""

        return sum(category.bytes_reclaimed for category in self.categories)

    @property
    def skipped(self) -> tuple[str, ...]:
        """``"<category>: <reason>"`` for every category that did not run."""

        return tuple(f"{item.name}: {item.skip_reason}" for item in self.categories if item.skipped)

    def category(self, name: str) -> GcCategory:
        """Return one category's result, empty when it did not run.

        :param name: The category.
        :return: Its result.
        """

        return next((item for item in self.categories if item.name == name), GcCategory(name))

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this report.

        :return: JSON-compatible report members.
        """

        return {
            "format": GC_REPORT_FORMAT,
            "format_version": 3,
            "workspace_id": self.workspace_id,
            "dry_run": self.dry_run,
            "collected_at": self.collected_at,
            "retention": dict(self.retention),
            "candidates": self.candidates,
            "removed": self.removed,
            "bytes_reclaimed": self.bytes_reclaimed,
            "skipped": list(self.skipped),
            "categories": [item.as_mapping() for item in self.categories],
        }


@dataclass
class _Tally:
    name: str
    candidates: int = 0
    removed: int = 0
    bytes_reclaimed: int = 0
    entries: list[str] = field(default_factory=list)
    skip_reason: str | None = None

    def note(self, path: Path, size: int) -> None:
        self.candidates += 1
        self.bytes_reclaimed += size
        self.entries.append(str(path))

    def frozen(self) -> GcCategory:
        return GcCategory(
            self.name,
            self.candidates,
            self.removed,
            self.bytes_reclaimed,
            tuple(self.entries),
            self.skip_reason is not None,
            self.skip_reason,
        )


def _names(directory: Path) -> list[str]:
    try:
        return sorted(os.listdir(directory))
    except (FileNotFoundError, NotADirectoryError):
        return []


def _mtime(path: Path) -> float | None:
    try:
        return os.lstat(path).st_mtime
    except FileNotFoundError:
        return None


def _bytes(path: Path, sizes: bool) -> int:
    """Sum the sizes below *path* without following links (``0`` unless *sizes*)."""

    if not sizes:
        return 0
    try:
        information = os.lstat(path)
    except FileNotFoundError:
        return 0
    if not stat.S_ISDIR(information.st_mode):
        return information.st_size
    total = 0
    for root, directories, files in os.walk(path):
        for name in directories + files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except FileNotFoundError:
                continue
    return total


def _into_quarantine(workspace: _kernel.KernelWorkspace, entry: Path, reason: str) -> Path:
    target = workspace.control / "quarantine" / f"{int(time.time())}-{_fs.fresh_token()}"
    _fs.move_owned(_fs.loc(entry), _fs.loc(target / "entry"), durable=workspace.durable)
    record = {"source": entry.name, "reason": reason, "at": datetime.now(UTC).isoformat()}
    _fs.write_file(_fs.loc(target / "reason.json"), json_bytes(record) + b"\n", durable=workspace.durable)
    _LOGGER.warning("quarantined %s: %s", entry.name, reason, extra={"event": "quarantined", "path": str(target)})
    return target / "entry"


def _finish_quarantine(owner: _kernel.Owner, scratch: Path) -> bool:
    """The scratch reconciler of an interrupted quarantine: what was taken goes on to ``quarantine/``."""

    for name in _names(scratch):
        _into_quarantine(owner.workspace, scratch / name, "an interrupted quarantine")
    return True


_kernel.register_reconciler("quarantine", _finish_quarantine)


def quarantine(workspace: "Workspace", owner: _kernel.Owner, path: Path, reason: str) -> Path | None:
    """Move one contested entry to ``quarantine/<epoch>-<token>/entry`` beside a ``reason.json``.

    The entry is won into the owner's ``quarantine`` scratch first, whose
    reconciler finishes the move if the owner dies in between.

    :param workspace: The workspace.
    :param owner: The owner that takes the entry.
    :param path: The entry.
    :param reason: Why it is quarantined.
    :return: The quarantined entry, or ``None`` when another actor moved it first.
    """

    scratch = _kernel.take(workspace, owner, _fs.loc(path), "quarantine")
    if scratch is None:
        return None
    moved = _into_quarantine(workspace, scratch / path.name, reason)
    _fs.remove_empty_dir(_fs.loc(scratch))
    return moved


def quarantine_damaged(workspace: "Workspace", owner: _kernel.Owner, job_id: str, reason: str) -> Path | None:
    """Quarantine a job this owner won but holds no handle of (its ``job.json`` was unreadable at the claim).

    :param workspace: The workspace.
    :param owner: The owner whose ``owned/<owner-id>/`` holds the job.
    :param job_id: The job UUID.
    :param reason: Why.
    :return: The quarantined entry, or ``None`` when the job is not (or no longer) there.
    """

    for ref in owner.owned():
        if ref.job_id == job_id and ref.path not in owner._held:
            _LOGGER.error("quarantining damaged job %s: %s", ref.job_key, reason, extra={"event": "job_damaged"})
            return quarantine(workspace, owner, ref.path, reason)
    return None


class _Collection:
    """One pass over a workspace."""

    def __init__(
        self, workspace: "Workspace", owner: _kernel.Owner | None, *, dry_run: bool, now: float, sizes: bool
    ) -> None:
        self.workspace, self.owner, self.dry_run, self.now, self.sizes = workspace, owner, dry_run, now, sizes
        self.retention = workspace.policy.retention
        self.control = workspace.control

    def _cutoff(self, days: float | None) -> float | None:
        return None if days is None else self.now - days * _DAY

    def _aged(self, path: Path, cutoff: float) -> bool:
        mtime = _mtime(path)
        return mtime is not None and mtime < cutoff

    def _remove(self, tally: _Tally, path: Path) -> None:
        """Remove one file or tree (a tree through a trash name in ``tmp/``), unless dry run."""

        tally.note(path, _bytes(path, self.sizes))
        if self.dry_run:
            return
        try:
            if stat.S_ISDIR(os.lstat(path).st_mode):
                tmp = self.control / "tmp"
                _fs.discard(_fs.loc(path), trash_dir=tmp, durable=self.workspace.durable)
            else:
                _fs.remove_file(_fs.loc(path), durable=self.workspace.durable)
        except FileNotFoundError:
            return
        except (OSError, _fs.UnsafePath) as exc:
            _LOGGER.warning("could not remove %s: %s", path, exc)
            return
        tally.removed += 1

    # -- categories -------------------------------------------------------------------------------------------

    def dead_owners(self, tally: _Tally) -> None:
        scheduler, here = _death.SchedulerQueries(), _death.process_identity()
        for record in _kernel.list_owners(self.workspace):
            # A closed owner leaves no owner.json; a recovered one keeps only dead.json.
            if record.record is None or (self.owner is not None and record.owner_id == self.owner.owner_id):
                continue
            if self.dry_run or self.owner is None:
                verdict, _evidence = _death.probe(
                    record.path, visibility_deadline=self.workspace.visibility_deadline, scheduler=scheduler, here=here
                )
                if verdict is _death.Liveness.DEAD:
                    tally.note(record.path, 0)
                continue
            try:
                if (
                    _kernel.probe_owner(self.workspace, record.owner_id, scheduler=scheduler)
                    is not _death.Liveness.DEAD
                ):
                    continue
                tally.note(record.path, 0)
                report = _kernel.recover(self.workspace, self.owner, record.owner_id)
            except ValueError:
                continue  # an owner of this very process is never probed
            except WorkflowError as exc:
                _LOGGER.info("recovery of owner %s did not finish: %s", record.owner_id, exc)
                continue
            tally.removed += 1
            _LOGGER.warning(
                "recovered dead owner %s: %d job(s) returned, %d quarantined",
                record.owner_id,
                len(report.returned),
                len(report.quarantined),
                extra={"event": "owner_recovered", "dead_owner": record.owner_id},
            )

    def attempt_control(self, tally: _Tally) -> None:
        cutoff = self._cutoff(self.retention.attempt_control_days)
        assert cutoff is not None
        for state in UNOWNED_STATES:
            for ref in _kernel.list_jobs(self.workspace, state):
                attempts = ref.path / "attempts"
                names = [name for name in _names(attempts) if not name.startswith(".")]
                if state in ("failed", "cancelled") and names:
                    # The newest attempt explains the outcome: it stays.
                    newest = max(names, key=lambda name: _mtime(attempts / name) or 0.0)
                    names.remove(newest)
                aged = [name for name in names if self._aged(attempts / name, cutoff)]
                if aged:
                    self._collect_attempts(tally, ref, aged)

    def _collect_attempts(self, tally: _Tally, ref: _kernel.JobRef, names: list[str]) -> None:
        for name in names:
            tally.note(ref.path / "attempts" / name, _bytes(ref.path / "attempts" / name, self.sizes))
        if self.dry_run or self.owner is None:
            return
        from .removal import release

        try:
            owned = _kernel.claim(self.workspace, self.owner, ref)
        except (FormatError, _fs.UnsafePath) as exc:
            quarantine_damaged(self.workspace, self.owner, ref.job_id, f"job.json is damaged: {exc}")
            return
        if owned is None:
            return  # another actor moved it first
        try:
            doc = owned.read_state()
        except (FormatError, _fs.UnsafePath):
            doc = None
        if doc is None or owned.pending_release() is not None or doc.commit is not None or doc.phase["kind"] != "idle":
            # Unfinished owner work is a manager's reconcile; the job goes back unchanged.
            owned._return()
            return
        removed = [name for name in names if owned.discard_subtree(f"attempts/{name}")]
        tally.removed += len(removed)
        owned.append_log("collected", detail={"attempts": removed})
        release(owned, doc, Release(owned.from_state, owned.from_priority))

    def placement_directories(self, tally: _Tally) -> None:
        if self.dry_run:
            return
        removed = _kernel.prune_empty_placements(self.workspace, budget=_PRUNE_BUDGET)
        tally.candidates += removed
        tally.removed += removed

    def requests(self, tally: _Tally) -> None:
        directory = self.control / "requests"
        cutoff = self.now - REQUEST_GRACE_SECONDS
        problems: dict[Path, str] = {}
        wanted: dict[str, list[tuple[Path, PurePosixPath]]] = {}
        for name in _names(directory):
            path = directory / name
            if name.startswith(".") or not self._aged(path, cutoff):
                continue
            try:
                request = _requests.parse(path)
            except FileNotFoundError:
                continue
            except FormatError as exc:
                problems[path] = f"malformed request: {exc}"
                continue
            wanted.setdefault(request.job_id, []).append((path, request.placement))
        if wanted:
            placements = sorted({placement for items in wanted.values() for _path, placement in items})
            found = _kernel.locate_many(self.workspace, sorted(wanted), placements=placements, settle=True)
            for job_id, items in wanted.items():
                if job_id not in found:
                    problems.update((path, f"no job {job_id} at its placement or owned") for path, _ in items)
        for path, reason in sorted(problems.items()):
            tally.note(path, _bytes(path, self.sizes))
            if self.owner is not None and not self.dry_run and quarantine(self.workspace, self.owner, path, reason):
                tally.removed += 1

    def manager_logs(self, tally: _Tally) -> None:
        cutoff = self._cutoff(self.retention.trash_days)
        assert cutoff is not None
        logs = self.workspace.root / LOGS_DIRECTORY / "managers"
        for name in _names(logs):
            owner_id = name.removesuffix(".1").removesuffix(".log")
            if not name.endswith((".log", ".log.1")) or (self.control / "owners" / owner_id / "owner.json").exists():
                continue
            if self._aged(logs / name, cutoff):
                self._remove(tally, logs / name)

    def owner_tombstones(self, tally: _Tally) -> None:
        cutoff = self._cutoff(self.retention.owner_tombstone_days)
        assert cutoff is not None
        for record in _kernel.list_owners(self.workspace):
            tombstone = record.path / "dead.json"
            # Only a recovered owner: nothing left but its tombstone (and write temporaries).
            if record.record is not None or record.tombstone is None or not self._aged(tombstone, cutoff):
                continue
            if any(not name.startswith(".") for name in _names(record.path) if name != "dead.json"):
                continue
            if (self.workspace.jobs / _kernel.OWNED / record.owner_id).exists():
                continue
            tally.note(record.path, _bytes(record.path, self.sizes))
            if self.dry_run:
                continue
            for name in _names(record.path):
                _fs.remove_file(_fs.loc(record.path / name), durable=self.workspace.durable)
            if _fs.remove_empty_dir(_fs.loc(record.path)):
                tally.removed += 1

    def tmp_entries(self, tally: _Tally) -> None:
        cutoff = self.now - TMP_MAXIMUM_AGE_SECONDS
        owners = self.control / "owners"
        for directory in [self.control / "requests", *(owners / name for name in _names(owners))]:
            for name in _names(directory):
                if name.startswith(".") and name.endswith(".tmp") and self._aged(directory / name, cutoff):
                    self._remove(tally, directory / name)
        tmp = self.control / "tmp"
        for name in _names(tmp):
            # Only entries already moved for deletion carry this name; a crashed removal left them.
            if name.startswith("trash.") and len(name) == _TRASH_NAME_LENGTH and self._aged(tmp / name, cutoff):
                self._remove(tally, tmp / name)

    # -- the pass ---------------------------------------------------------------------------------------------

    def execute(self, categories: Sequence[str]) -> GcReport:
        gates = {
            "attempt_control": ("attempt_control_days", self.retention.attempt_control_days),
            "manager_logs": ("trash_days", self.retention.trash_days),
            "owner_tombstones": ("owner_tombstone_days", self.retention.owner_tombstone_days),
        }
        results: list[GcCategory] = []
        for name in GC_CATEGORIES:
            tally = _Tally(name)
            if name not in categories:
                tally.skip_reason = "not selected"
            elif name in gates and gates[name][1] is None:
                tally.skip_reason = f"retention.{gates[name][0]} keeps everything"
            else:
                getattr(self, name)(tally)
            results.append(tally.frozen())
        return GcReport(
            self.workspace.workspace_id,
            self.dry_run,
            datetime.now(UTC).isoformat(),
            self.retention.as_mapping(),
            tuple(results),
        )


def collect_garbage(
    workspace: "Workspace",
    *,
    owner: _kernel.Owner | None = None,
    dry_run: bool = False,
    now: float | None = None,
    categories: Sequence[str] | None = None,
    sizes: bool = True,
    journal_writer: object = None,
) -> GcReport:
    """Collect what the workspace's retention policy permits.

    :param workspace: The workspace.
    :param owner: The owner that claims and recovers (a manager passes its own); a CLI owner is registered for
        the collection when omitted (not for a dry run).
    :param dry_run: Report candidates without changing anything (``placement_directories`` reports nothing).
    :param now: The moment ages are measured against (tests age a workspace with it).
    :param categories: Run only these categories of :data:`GC_CATEGORIES`.
    :param sizes: Estimate the bytes of each candidate.
    :param journal_writer: Ignored: workspaces have no journal any more.
    :return: The report.
    :raises ValueError: For an unknown category.
    """

    del journal_writer
    selected = GC_CATEGORIES if categories is None else tuple(categories)
    unknown = sorted(set(selected) - set(GC_CATEGORIES))
    if unknown:
        raise ValueError(f"unknown gc categories: {', '.join(unknown)}")
    clock = time.time() if now is None else now
    if owner is not None or dry_run:
        return _Collection(workspace, owner, dry_run=dry_run, now=clock, sizes=sizes).execute(selected)
    with _kernel.register_owner(workspace, kind="cli", label="workspace gc", allocation=None, advertised={}) as cli:
        return _Collection(workspace, cli, dry_run=False, now=clock, sizes=sizes).execute(selected)


def iter_report_rows(report: GcReport) -> Iterator[tuple[str, int, int, int]]:
    """Yield the category rows a command-line collection prints, then the total.

    :param report: The report.
    :yield: ``(category, candidates, removed, bytes)``.
    """

    for category in report.categories:
        yield category.name, category.candidates, category.removed, category.bytes_reclaimed
    yield "total", report.candidates, report.removed, report.bytes_reclaimed
