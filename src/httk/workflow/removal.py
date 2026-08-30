"""Explicit removal of jobs and their workspace payloads.

Containment checks are static: a concurrent replacement of a placement
ancestor by the workspace's own owner is outside the threat model, as it is
for ``attempts/`` and ``logs/``.
"""

import os
import shutil
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ._manager_joins import children as join_children
from .errors import WorkflowError
from .models import STATE_KINDS, TERMINAL_KINDS, Marker

if TYPE_CHECKING:  # pragma: no cover
    from .workspace import Workspace

# Keep this definition here so the removal operation and garbage collection
# share one source of truth without making this module depend on ``gc``.
REMOVABLE_KINDS = TERMINAL_KINDS | {"submitted", "ready"}


def _is_real_directory(path: Path, *, missing_ok: bool = False) -> bool:
    """Return whether *path* is a directory without following a symlink."""

    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return missing_ok
    return stat.S_ISDIR(mode)


@dataclass(frozen=True)
class RemovalOutcome:
    """Describe what happened to one selected job.

    :param job_key: Stable key of the selected job.
    :param kind: Marker kind observed during preflight.
    :param removed: Whether the payload and marker were removed.
    :param reason: Refusal or filesystem failure reason, if any.
    """

    job_key: str
    kind: str
    removed: bool
    reason: str | None = None

    @property
    def refused(self) -> bool:
        """Return whether this job was not removed."""

        return not self.removed


@dataclass(frozen=True)
class RemovalReport:
    """Report the per-job results of one all-or-nothing preflight.

    :param outcomes: One outcome for every marker supplied to the operation.
    """

    outcomes: tuple[RemovalOutcome, ...]

    @property
    def removed(self) -> tuple[RemovalOutcome, ...]:
        """Return jobs whose payload and marker were removed."""

        return tuple(outcome for outcome in self.outcomes if outcome.removed)

    @property
    def refused(self) -> tuple[RemovalOutcome, ...]:
        """Return jobs refused or left incomplete by the operation."""

        return tuple(outcome for outcome in self.outcomes if outcome.refused)

    @property
    def removed_count(self) -> int:
        """Return the number of jobs removed."""

        return len(self.removed)


def join_child_parents(
    workspace: "Workspace", markers: Sequence[Marker] | None = None
) -> tuple[dict[str, set[str]] | None, str | None]:
    """Scan non-terminal states for parents that reference join children.

    :param workspace: Workspace whose non-terminal marker states are scanned.
    :param markers: Already scanned markers to inspect, or ``None`` to scan
        the non-terminal state trees.
    :return: Child id to parent-key map, or ``None`` and a read failure reason.
    """

    parents: dict[str, set[str]] = {}
    non_terminal = tuple(kind for kind in STATE_KINDS if kind not in TERMINAL_KINDS)
    observed = markers if markers is not None else workspace.scan_markers(non_terminal)
    for marker in observed:
        try:
            state = workspace.read_state(marker)
            join = state.get("join")
            if join is None:
                continue
            if not isinstance(join, Mapping):
                raise WorkflowError("state.join is not an object")
            for reference in join_children(join):
                child_id = reference.get("job_id") if isinstance(reference, Mapping) else None
                if not isinstance(child_id, str):
                    raise WorkflowError("state.join.children contains an invalid child reference")
                parents.setdefault(child_id, set()).add(marker.job_key)
        except (WorkflowError, OSError, TypeError, ValueError) as exc:
            return None, f"cannot load non-terminal marker state {marker.job_key}: {exc}"
    return parents, None


def _remove_payload(path: Path) -> None:
    """Remove a validated payload tree without following a symlink at its root."""

    try:
        path.lstat()
    except FileNotFoundError:
        return
    if not _is_real_directory(path):
        return
    shutil.rmtree(path)


def _payload_safety(workspace: "Workspace", marker: Marker) -> str | None:
    """Validate payload containment and its runner-owned directories."""

    current = workspace.root
    if not _is_real_directory(current):
        return f"workspace root is not a real directory: {current}"
    for component in marker.placement.parts:
        current /= component
        if not _is_real_directory(current, missing_ok=True):
            if not current.exists() and not current.is_symlink():
                return None
            return f"payload placement is not a real directory: {current}"
    payload = current / marker.job_key
    if not _is_real_directory(payload, missing_ok=True):
        if not payload.exists() and not payload.is_symlink():
            return None
        return f"payload is not a real directory: {payload}"
    for name in ("attempts", "logs"):
        child = payload / name
        if (child.exists() or child.is_symlink()) and not _is_real_directory(child):
            return f"payload {name} directory is not a real directory: {child}"
    return None


def _remove_one(workspace: "Workspace", marker: Marker) -> RemovalOutcome:
    """Remove one marker first, then its validated payload.

    The marker unlink is the per-job commit point. If a manager moved it after
    preflight, the missing marker means the job changed state, so its payload
    must be left untouched rather than deleting a new attempt's workspace.
    """

    try:
        safety = _payload_safety(workspace, marker)
        if safety is not None:
            return RemovalOutcome(marker.job_key, marker.kind, False, safety)
        try:
            os.unlink(marker.path)
        except FileNotFoundError:
            return RemovalOutcome(marker.job_key, marker.kind, False, "state changed; not removed")
        safety = _payload_safety(workspace, marker)
        if safety is not None:
            return RemovalOutcome(marker.job_key, marker.kind, False, safety)
        _remove_payload(workspace.payload_path(marker.placement, marker.job_key))
    except OSError as exc:
        return RemovalOutcome(marker.job_key, marker.kind, False, str(exc))
    return RemovalOutcome(marker.job_key, marker.kind, True)


def _remove_jobs(workspace: "Workspace", markers: "Sequence[Marker]", *, force: bool) -> RemovalReport:
    """Implement :func:`remove_jobs` with the explicit parent-guard switch."""

    from .projects import discover_project
    from .seals import is_job_sealed, is_project_sealed, is_workspace_sealed

    selected = tuple(markers)
    invalid = tuple(marker for marker in selected if marker.kind not in REMOVABLE_KINDS)
    if invalid:
        first = invalid[0]
        reason = f"job {first.job_key} is {first.kind}; cancel it first: httk job request cancel …"
        return RemovalReport(
            tuple(
                RemovalOutcome(
                    marker.job_key,
                    marker.kind,
                    False,
                    reason if marker in invalid else f"batch preflight refused: {reason}",
                )
                for marker in selected
            )
        )

    workspace_reason: str | None = None
    if is_workspace_sealed(workspace):
        workspace_reason = "workspace is sealed; unseal it first"
    else:
        project = discover_project(workspace.root)
        if project is not None and is_project_sealed(project):
            workspace_reason = "project is sealed; unseal it first"
    if workspace_reason is not None:
        return RemovalReport(
            tuple(RemovalOutcome(marker.job_key, marker.kind, False, workspace_reason) for marker in selected)
        )

    sealed = tuple(marker for marker in selected if is_job_sealed(workspace, marker.job_key))
    if sealed:
        first_sealed = sealed[0]
        sealed_reason = f"job {first_sealed.job_key} is sealed; unseal it first"
        return RemovalReport(
            tuple(
                RemovalOutcome(
                    marker.job_key,
                    marker.kind,
                    False,
                    f"job {marker.job_key} is sealed; unseal it first"
                    if marker in sealed
                    else f"batch preflight refused: {sealed_reason}",
                )
                for marker in selected
            )
        )

    if not force:
        parents, error = join_child_parents(workspace)
        if parents is None:
            reason = error or "cannot inspect join parents safely"
            return RemovalReport(
                tuple(RemovalOutcome(marker.job_key, marker.kind, False, reason) for marker in selected)
            )
        guarded = [(marker, parents.get(marker.job_id, set())) for marker in selected]
        if any(parent_keys for _marker, parent_keys in guarded):
            first_guarded = next((item for item in guarded if item[1]), None)
            assert first_guarded is not None
            guarded_reason = (
                f"job {first_guarded[0].job_key} is referenced as a join child by non-terminal parent(s): "
                + ", ".join(sorted(first_guarded[1]))
            )
            return RemovalReport(
                tuple(
                    RemovalOutcome(
                        marker.job_key,
                        marker.kind,
                        False,
                        (
                            f"job {marker.job_key} is referenced as a join child by non-terminal parent(s): "
                            + ", ".join(sorted(parent_keys))
                        )
                        if parent_keys
                        else f"batch preflight refused: {guarded_reason}",
                    )
                    for marker, parent_keys in guarded
                )
            )

    safety = tuple((marker, _payload_safety(workspace, marker)) for marker in selected)
    unsafe = tuple((marker, reason) for marker, reason in safety if reason is not None)
    if unsafe:
        first_reason = unsafe[0][1]
        return RemovalReport(
            tuple(
                RemovalOutcome(
                    marker.job_key,
                    marker.kind,
                    False,
                    reason if reason is not None else f"batch preflight refused: {first_reason}",
                )
                for marker, reason in safety
            )
        )

    return RemovalReport(tuple(_remove_one(workspace, marker) for marker in selected))


def remove_jobs(workspace: "Workspace", markers: "Sequence[Marker]", *, force: bool = False) -> RemovalReport:
    """Remove selected removable jobs after a complete safety preflight.

    :param workspace: Workspace containing the selected jobs.
    :param markers: Current markers selected by the caller.
    :param force: Skip the non-terminal join-parent guard.
    :return: Per-job removal or refusal outcomes.
    """

    return _remove_jobs(workspace, markers, force=force)


__all__ = ["REMOVABLE_KINDS", "RemovalOutcome", "RemovalReport", "join_child_parents", "remove_jobs"]
