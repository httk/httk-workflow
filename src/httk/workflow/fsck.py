"""Check a workspace's job tree for what the kernel cannot read or did not expect.

The check walks ``jobs/`` and reports:

- ``unparsable_name``: an entry that is neither a placement directory nor a job
  directory name of its state (a file, a symlink, a malformed name, an unknown
  ``jobs/`` or ``owned/`` entry);
- ``duplicate_job``: one job UUID in two places;
- ``orphan_owned``: ``jobs/owned/<owner-id>/`` without ``owners/<owner-id>/``;
- ``tombstoned_owner_with_jobs``: a recovered owner (only ``dead.json`` left) that holds jobs, launches or
  scratch again; the ``dead_owners`` category of ``httk workspace gc`` recovers it;
- ``unreadable_state``: a ``state.json`` that exists but cannot be decoded;
- ``foreign_owner``: a job directory another user owns.

Nothing is repaired: with ``repair`` the unparsable entries are moved to
``quarantine/``, and every other finding is left to the operator.
"""

import os
import stat
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from . import _kernel
from ._kernel import JobRef
from ._state import UNOWNED_STATES, read_state_unowned
from .errors import FormatError
from .gc import quarantine

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .workspace import Workspace

__all__ = ["FSCK_REPORT_FORMAT", "FsckFinding", "FsckReport", "check_workspace"]

FSCK_REPORT_FORMAT = "httk-workflow-fsck"
_OWNER_ID_LENGTH = 32


@dataclass(frozen=True)
class FsckFinding:
    """One thing the check found.

    :param entry: The path.
    :param problem: ``unparsable_name``, ``duplicate_job``, ``orphan_owned``, ``tombstoned_owner_with_jobs``,
        ``unreadable_state`` or ``foreign_owner``.
    :param detail: A human-readable explanation.
    :param action: ``reported`` or ``quarantined``.
    :param job_key: The job key, when the name parses.
    :param job_id: The job UUID, when the name parses.
    """

    entry: Path
    problem: str
    detail: str
    action: str = "reported"
    job_key: str | None = None
    job_id: str | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this finding.

        :return: JSON-compatible finding members.
        """

        result: dict[str, object] = {
            "entry": str(self.entry),
            "problem": self.problem,
            "detail": self.detail,
            "action": self.action,
        }
        if self.job_key is not None:
            result["job_key"] = self.job_key
        if self.job_id is not None:
            result["job_id"] = self.job_id
        return result


@dataclass(frozen=True)
class FsckReport:
    """Everything one check found and did.

    :param workspace_id: The workspace.
    :param jobs_checked: The number of job directories inspected.
    :param findings: The findings.
    :param counts: Findings by problem.
    """

    workspace_id: str
    jobs_checked: int
    findings: tuple[FsckFinding, ...] = ()
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """Whether the check found nothing."""

        return not self.findings

    @property
    def unresolved(self) -> int:
        """The number of findings left for the operator (everything not quarantined)."""

        return sum(1 for finding in self.findings if finding.action == "reported")

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this report.

        :return: JSON-compatible report members.
        """

        return {
            "format": FSCK_REPORT_FORMAT,
            "format_version": 3,
            "workspace_id": self.workspace_id,
            "jobs_checked": self.jobs_checked,
            "counts": dict(self.counts),
            "findings": [finding.as_mapping() for finding in self.findings],
        }


def _entries(directory: Path) -> list[os.DirEntry[str]]:
    try:
        with os.scandir(directory) as listing:
            return sorted(listing, key=lambda entry: entry.name)
    except (FileNotFoundError, NotADirectoryError):
        return []


def _walk(directory: Path, state: str, placement: PurePosixPath) -> Iterator[JobRef | Path]:
    """Yield the jobs below one placement directory of an unowned state, and every entry that is not one."""

    for entry in _entries(directory):
        path = Path(entry.path)
        if not entry.is_dir(follow_symlinks=False):
            yield path
        elif "~" not in entry.name:
            yield from _walk(path, state, placement / entry.name)
        else:
            try:
                yield JobRef.from_path(path, state=state, placement=placement)
            except FormatError:
                yield path


def _jobs(workspace: "Workspace") -> Iterator[JobRef | Path]:
    """Yield every job of the workspace, and every entry of ``jobs/`` that is not a job."""

    owned = workspace.jobs / _kernel.OWNED
    for entry in _entries(workspace.jobs):
        if entry.name not in (*UNOWNED_STATES, _kernel.OWNED) or not entry.is_dir(follow_symlinks=False):
            yield Path(entry.path)
    for state in UNOWNED_STATES:
        yield from _walk(workspace.jobs / state, state, PurePosixPath())
    for owner in _entries(owned):
        if not owner.is_dir(follow_symlinks=False) or len(owner.name) != _OWNER_ID_LENGTH:
            yield Path(owner.path)
            continue
        for entry in _entries(Path(owner.path)):
            try:
                if not entry.is_dir(follow_symlinks=False):
                    raise FormatError(f"{entry.path} is not a directory")
                yield JobRef.from_path(Path(entry.path), state=_kernel.OWNED, owner_id=owner.name)
            except FormatError:
                yield Path(entry.path)


def check_workspace(
    workspace: "Workspace", *, repair: bool = False, quarantine_unrepairable: bool = False
) -> FsckReport:
    """Check the workspace's job tree.

    :param workspace: The workspace.
    :param repair: Quarantine the unparsable entries (the only repair there is).
    :param quarantine_unrepairable: The same as *repair*.
    :return: The report.
    """

    findings: list[FsckFinding] = []
    unparsable: list[Path] = []
    seen: dict[str, list[JobRef]] = {}
    uid = os.geteuid()
    checked = 0
    for item in _jobs(workspace):
        if isinstance(item, Path):
            unparsable.append(item)
            continue
        checked += 1
        seen.setdefault(item.job_id, []).append(item)
        ids = {"job_key": item.job_key, "job_id": item.job_id}
        try:
            owner_uid = os.lstat(item.path).st_uid
        except FileNotFoundError:
            continue  # it moved after the listing
        if owner_uid != uid:
            findings.append(FsckFinding(item.path, "foreign_owner", f"owned by uid {owner_uid}, not {uid}", **ids))
        if read_state_unowned(item.path / "state.json")[1]:
            findings.append(FsckFinding(item.path / "state.json", "unreadable_state", "cannot be decoded", **ids))
    findings += _unparsable(workspace, unparsable, quarantining=repair or quarantine_unrepairable)
    for job_id, refs in sorted(seen.items()):
        if len(refs) > 1:
            places = ", ".join(str(ref.path) for ref in refs)
            findings.extend(
                FsckFinding(
                    ref.path,
                    "duplicate_job",
                    f"job {job_id} is in {len(refs)} places: {places}",
                    job_key=ref.job_key,
                    job_id=job_id,
                )
                for ref in refs
            )
    owners = workspace.control / "owners"
    for entry in _entries(workspace.jobs / _kernel.OWNED):
        if entry.is_dir(follow_symlinks=False) and not stat.S_ISDIR(_mode(owners / entry.name)):
            findings.append(FsckFinding(Path(entry.path), "orphan_owned", f"owners/{entry.name}/ does not exist"))
    recoverable = set(_kernel.recoverable_owners(workspace))
    findings.extend(
        FsckFinding(item.path, "tombstoned_owner_with_jobs", "recovered, but holds jobs, launches or scratch again")
        for item in _kernel.list_owners(workspace)
        if item.record is None and item.owner_id in recoverable
    )
    counts: dict[str, int] = {}
    for finding in findings:
        counts[finding.problem] = counts.get(finding.problem, 0) + 1
    return FsckReport(workspace.workspace_id, checked, tuple(findings), counts)


def _unparsable(workspace: "Workspace", entries: list[Path], *, quarantining: bool) -> list[FsckFinding]:
    detail = "neither a placement nor a job directory of its state"
    if not quarantining or not entries:
        return [FsckFinding(entry, "unparsable_name", detail) for entry in entries]
    findings: list[FsckFinding] = []
    with _kernel.register_owner(workspace, kind="cli", label="workspace fsck", allocation=None, advertised={}) as owner:
        for entry in entries:
            moved = quarantine(workspace, owner, entry, f"fsck: unparsable_name: {entry}")
            findings.append(
                FsckFinding(entry, "unparsable_name", detail)
                if moved is None
                else FsckFinding(entry, "unparsable_name", f"{detail}; moved to {moved}", "quarantined")
            )
    return findings


def _mode(path: Path) -> int:
    try:
        return os.lstat(path).st_mode
    except FileNotFoundError:
        return 0
