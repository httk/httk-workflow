"""Workspace consistency check and conservative marker repair.

A marker is the authoritative state of a job, and everything it says beyond its
kind, priority, and generation lives in the journal frame it references. A lost
segment, a torn write on a node that crashed, or a marker that was renamed onto
a frame that never reached storage therefore leaves a job whose state cannot be
read at all. This module finds those jobs and, when asked, re-points each
damaged marker at the newest frame of that job which is still readable.

The check is deliberately conservative. It never touches a claimed, running, or
committing marker whose manager is still heartbeating, it never walks forward
onto a frame no marker ever committed, and it repairs nothing at all unless
repair is requested.
"""

import logging
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._util import read_json, timestamp_seconds, utc_now
from .errors import WorkflowError
from .journal import JournalFrame, JournalWriter, iter_journal_frames, verify_record
from .models import STATE_KINDS, Marker
from .workspace import MarkerFault

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from .workspace import Workspace

_LOGGER = logging.getLogger(__name__)

FSCK_REPORT_FORMAT = "httk-workflow-fsck"
#: State kinds whose marker may belong to a manager that is still working on
#: it. A damaged one of these is reported, never repaired, while its manager
#: keeps heartbeating: the manager owns the transition that follows.
#:
#: ``cancelling`` is one of them. Its marker is a fence a live manager put in
#: place and is still acting on — terminating the attempt and verifying its
#: exit — so re-pointing it at an older frame would drop the attempt identity
#: that manager needs and could reanimate a job whose process is being stopped.
LIVE_KINDS = frozenset({"claimed", "running", "committing", "cancelling"})
#: What one damaged marker was actually done about.
ACTIONS = ("reported", "repaired", "quarantined", "skipped_live")


@dataclass(frozen=True)
class FsckFinding:
    """One marker that does not resolve to a readable, matching frame."""

    entry: Path
    problem: str
    detail: str
    action: str = "reported"
    job_key: str | None = None
    job_id: str | None = None
    kind: str | None = None
    generation: int | None = None
    record_ref: str | None = None
    repaired_record_ref: str | None = None

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this finding."""

        result: dict[str, object] = {
            "entry": str(self.entry),
            "problem": self.problem,
            "detail": self.detail,
            "action": self.action,
        }
        for name in ("job_key", "job_id", "kind", "generation", "record_ref", "repaired_record_ref"):
            value = getattr(self, name)
            if value is not None:
                result[name] = value
        return result


@dataclass(frozen=True)
class FsckReport:
    """Everything one workspace check found and did."""

    workspace_id: str
    markers_checked: int
    findings: tuple[FsckFinding, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """Report whether every marker resolved to its own journal frame."""

        return not self.findings

    @property
    def unresolved(self) -> int:
        """Return how many findings the run left for an operator to handle."""

        return sum(1 for finding in self.findings if finding.action in ("reported", "skipped_live"))

    def as_mapping(self) -> dict[str, object]:
        """Return the JSON representation of this report."""

        return {
            "format": FSCK_REPORT_FORMAT,
            "format_version": 1,
            "workspace_id": self.workspace_id,
            "markers_checked": self.markers_checked,
            "counts": dict(self.counts),
            "findings": [finding.as_mapping() for finding in self.findings],
        }


class _JournalIndex:
    """Every readable state frame of the journal, grouped by job.

    A damaged marker cannot be followed backwards along its own chain, because
    the frame holding ``previous_record_ref`` is the unreadable one. The only
    remaining evidence is the journal itself, so a repair walks the segments
    once and keeps the frames that name each job.
    """

    def __init__(self, workspace: "Workspace") -> None:
        self._workspace = workspace
        self._by_job: dict[str, list[JournalFrame]] | None = None

    def frames_for(self, job_id: str) -> list[JournalFrame]:
        """Return every readable state frame naming *job_id*, oldest first."""

        if self._by_job is None:
            self._by_job = self._build()
        return self._by_job.get(job_id, [])

    def _build(self) -> dict[str, list[JournalFrame]]:
        workspace_id = self._workspace.workspace_id
        grouped: dict[str, list[JournalFrame]] = {}
        for entry in iter_journal_frames(self._workspace.control):
            frame = entry.frame
            if frame.get("format") != "httk-workflow-state" or frame.get("format_version") != 1:
                continue
            if frame.get("workspace_id") != workspace_id:
                continue
            job_id = frame.get("job_id")
            if not isinstance(job_id, str):
                continue
            grouped.setdefault(job_id, []).append(entry)
        _LOGGER.debug("indexed journal frames for %d jobs", len(grouped))
        return grouped


def _frame_generation(entry: JournalFrame) -> int | None:
    value = entry.frame.get("state_generation")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _identity_problem(workspace: "Workspace", marker: Marker, frame: Mapping[str, Any]) -> str | None:
    """Return why *frame* is not the frame *marker* names, if it is not."""

    expected: list[tuple[str, object]] = [
        ("format", "httk-workflow-state"),
        ("format_version", 1),
        ("workspace_id", workspace.workspace_id),
        ("job_id", marker.job_id),
        ("job_key", marker.job_key),
        ("kind", marker.kind),
        ("state_generation", marker.generation),
    ]
    for name, value in expected:
        if frame.get(name) != value:
            return f"frame {name} is {frame.get(name)!r}, but the marker names {value!r}"
    return None


def _manager_is_live(workspace: "Workspace", manager_id: str, lease_seconds: float) -> bool:
    """Report whether *manager_id* heartbeated within its own lease."""

    try:
        heartbeat = read_json(workspace.control / "managers" / manager_id / "heartbeat.json")
        updated = timestamp_seconds(str(heartbeat["updated_at"]))
    except (WorkflowError, KeyError, ValueError):
        return False
    return time.time() - updated <= lease_seconds


def _live_owner(workspace: "Workspace", marker: Marker, index: _JournalIndex) -> str | None:
    """Return the manager still working on *marker*, if the evidence shows one.

    The damaged frame cannot say who owns the job, so the owner is taken from
    the readable frames of the same job. A heartbeat inside that manager's own
    lease is treated as ownership, and ownership means hands off.
    """

    if marker.kind not in LIVE_KINDS:
        return None
    default_lease = workspace.policy.lease_seconds
    for entry in reversed(index.frames_for(marker.job_id)):
        manager_id = entry.frame.get("manager_id")
        if not isinstance(manager_id, str) or not manager_id:
            continue
        lease = entry.frame.get("lease_seconds")
        lease_seconds = (
            float(lease) if isinstance(lease, (int, float)) and not isinstance(lease, bool) else default_lease
        )
        if _manager_is_live(workspace, manager_id, lease_seconds):
            return manager_id
    return None


def _last_good_frame(marker: Marker, index: _JournalIndex) -> JournalFrame | None:
    """Return the newest readable frame of this job older than *marker*.

    Only strictly older generations are candidates. A frame at or beyond the
    marker's own generation is either the damaged one or a transition that was
    appended and never committed by a rename, and adopting either would invent
    state no marker ever published.
    """

    best: JournalFrame | None = None
    best_generation = -1
    for entry in index.frames_for(marker.job_id):
        if entry.frame.get("job_key") != marker.job_key:
            continue
        if entry.frame.get("kind") not in STATE_KINDS:
            continue
        generation = _frame_generation(entry)
        if generation is None or generation >= marker.generation:
            continue
        if generation >= best_generation:
            best = entry
            best_generation = generation
    return best


def _repair_frame(workspace: "Workspace", marker: Marker, recovered: JournalFrame, problem: str) -> dict[str, object]:
    """Build the ``fsck_repair`` frame that replaces an unreadable one.

    The members of the recovered frame are carried forward so that a repaired
    job keeps its activation, attempt, and data counters, and the marker keeps
    its own kind, placement, and priority: the state tree is what an operator
    and every manager already believe about this job.
    """

    frame: dict[str, object] = dict(recovered.frame)
    frame.update(
        {
            "format": "httk-workflow-state",
            "format_version": 1,
            "workspace_id": workspace.workspace_id,
            "job_id": marker.job_id,
            "job_key": marker.job_key,
            "placement": marker.placement.as_posix(),
            "state_generation": marker.generation + 1,
            "kind": marker.kind,
            "previous_record_ref": recovered.record_ref,
            "created_at": utc_now(),
            "priority": marker.priority,
            "reason": "fsck_repair",
            "fsck_repair": {
                "replaced_record_ref": marker.record_ref,
                "problem": problem,
                "recovered_record_ref": recovered.record_ref,
                "recovered_generation": _frame_generation(recovered),
                "repaired_at": utc_now(),
            },
        }
    )
    return frame


def _marker_entries(workspace: "Workspace") -> Iterator[Marker | MarkerFault]:
    """Yield every marker-shaped entry of every state kind, core or not."""

    return workspace.scan_marker_entries(STATE_KINDS)


def check_workspace(
    workspace: "Workspace",
    *,
    repair: bool = False,
    quarantine_unrepairable: bool = False,
) -> FsckReport:
    """Verify that every marker resolves to its own readable journal frame.

    Without *repair* nothing is written: every damaged marker is reported.
    With *repair* a damaged marker whose job still has a readable older frame
    is re-pointed at a fresh ``fsck_repair`` frame written by this check's own
    journal writer, which leaves the job loadable and schedulable again. A
    marker with no readable history is left alone unless
    *quarantine_unrepairable* also asks for it to be moved out of the way.
    """

    if quarantine_unrepairable and not repair:
        raise ValueError("quarantining unrepairable markers requires repair")
    index = _JournalIndex(workspace)
    findings: list[FsckFinding] = []
    checked = 0
    writer: JournalWriter | None = None
    try:
        for entry in _marker_entries(workspace):
            if isinstance(entry, MarkerFault):
                findings.append(
                    _handle_unrepairable(
                        workspace,
                        FsckFinding(entry=entry.path, problem="unparseable_name", detail=entry.reason),
                        quarantine=quarantine_unrepairable,
                    )
                )
                continue
            checked += 1
            problem = _inspect(workspace, entry)
            if problem is None:
                continue
            code, detail = problem
            finding = FsckFinding(
                entry=entry.path,
                problem=code,
                detail=detail,
                job_key=entry.job_key,
                job_id=entry.job_id,
                kind=entry.kind,
                generation=entry.generation,
                record_ref=entry.record_ref,
            )
            _LOGGER.warning(
                "marker %s does not resolve: %s (%s)",
                entry.path,
                code,
                detail,
                extra={"event": "fsck_finding", "entry": str(entry.path), "problem": code},
            )
            if not repair:
                findings.append(finding)
                continue
            owner = _live_owner(workspace, entry, index)
            if owner is not None:
                _LOGGER.info("leaving %s alone: manager %s is still heartbeating", entry.path, owner)
                findings.append(replace(finding, action="skipped_live", detail=f"{detail}; manager {owner} is live"))
                continue
            recovered = _last_good_frame(entry, index)
            if recovered is None:
                findings.append(_handle_unrepairable(workspace, finding, quarantine=quarantine_unrepairable))
                continue
            if writer is None:
                writer = workspace.open_journal_writer()
            repaired = workspace.repoint_marker(writer, entry, _repair_frame(workspace, entry, recovered, code))
            _LOGGER.warning(
                "repaired %s: generation %d now references %s recovered from %s",
                entry.job_key,
                repaired.generation,
                repaired.record_ref,
                recovered.record_ref,
                extra={"event": "fsck_repaired", "job_key": entry.job_key, "job_id": entry.job_id},
            )
            findings.append(
                replace(
                    finding,
                    action="repaired",
                    entry=repaired.path,
                    repaired_record_ref=repaired.record_ref,
                )
            )
    finally:
        if writer is not None:
            writer.close()
    counts = {action: sum(1 for finding in findings if finding.action == action) for action in ACTIONS}
    return FsckReport(
        workspace_id=workspace.workspace_id,
        markers_checked=checked,
        findings=tuple(findings),
        counts=counts,
    )


def _inspect(workspace: "Workspace", marker: Marker) -> tuple[str, str] | None:
    """Return the problem of one marker, or ``None`` when it resolves."""

    if marker.record_ref == "init":
        # The only marker without a frame is the one submission creates.
        if marker.kind == "submitted" and marker.generation == 0:
            return None
        return (
            "identity_mismatch",
            f"a marker at generation {marker.generation} in {marker.kind} may not reference the initial state",
        )
    verification = verify_record(
        workspace.control,
        marker.record_ref,
        deadline_seconds=workspace.visibility_deadline,
    )
    if verification.frame is None:
        return str(verification.problem), verification.detail
    mismatch = _identity_problem(workspace, marker, verification.frame)
    if mismatch is not None:
        return "identity_mismatch", mismatch
    return None


def _handle_unrepairable(
    workspace: "Workspace",
    finding: FsckFinding,
    *,
    quarantine: bool,
) -> FsckFinding:
    """Report, and optionally quarantine, a marker nothing can restore."""

    if not quarantine:
        return finding
    destination = workspace.quarantine(finding.entry, reason=f"fsck: {finding.problem}: {finding.detail}")
    return replace(finding, action="quarantined", detail=f"{finding.detail}; moved to {destination}")
