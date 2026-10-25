"""Define exceptions raised by :mod:`httk.workflow`."""

# Seal errors are owned by httk-core so isinstance checks agree across the
# boundary: workflow code that seals through core, and core code that raises
# on a workflow member, share one SealError / SealedError type.
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from httk.core.project.sealing import SealedError, SealError

if TYPE_CHECKING:
    from ._kernel import JobRef

__all__ = [
    "ConfinementUnavailableError",
    "DuplicateTagError",
    "FormatError",
    "ResolutionMiss",
    "RunnerResolutionError",
    "SealError",
    "SealedError",
    "TransactionError",
    "TransitionLostError",
    "UnsupportedExtensionError",
    "WorkflowError",
    "WorkspaceCorruptionError",
    "WorkspaceUnavailableError",
]


class WorkflowError(RuntimeError):
    """Base class for workflow protocol failures."""


class ConfinementUnavailableError(WorkflowError):
    """Bubblewrap is missing or cannot build the attempt sandbox (``manager.confine=bwrap``)."""


class DuplicateTagError(WorkflowError):
    """A new job's tag is already used by a job of the same placement, in any state.

    :param tag: The tag.
    :param placement: The placement.
    :param existing: The job already carrying the tag there.
    """

    def __init__(self, tag: str, placement: PurePosixPath, existing: "JobRef") -> None:
        super().__init__(
            f"a job with tag {tag!r} already exists in placement {placement.as_posix()!r}: "
            f"{existing.job_key} ({existing.state}); pass duplicate_tag='allow' to create another or 'skip' to skip it"
        )
        self.tag = tag
        self.placement = placement
        self.existing = existing


class ResolutionMiss(ValueError):
    """A name did not resolve, without indicating malformed configuration."""


class FormatError(WorkflowError, ValueError):
    """A workspace, job, journal frame, outcome, or request is malformed."""


class WorkspaceUnavailableError(WorkflowError):
    """The workspace cannot currently provide a coherent protocol view."""


class WorkspaceCorruptionError(WorkflowError):
    """The authoritative filesystem state is internally inconsistent."""


class TransitionLostError(WorkflowError):
    """Another actor committed a transition from the expected marker."""


class UnsupportedExtensionError(WorkflowError):
    """A workspace requires an extension this implementation does not support."""


class TransactionError(WorkflowError):
    """A transactional-data manifest cannot be safely replayed."""


class RunnerResolutionError(WorkflowError):
    """A shared runner cannot be resolved or verified.

    The failure carries the exact protocol failure ``code`` the manager records,
    so an unresolvable runner (``runner_unavailable``), a missing build for
    this platform (``runner_not_built``), a failed platform probe or build
    (``runner_build_failed``), an embedded ``workflow/`` that is missing,
    refused or changed (``protocol_error``), and an installed code's
    unavailable Bash API (``code_support_unavailable``) stay distinguishable to
    an operator.

    :param code: Protocol failure code recorded by the manager.
    :param message: Human-readable failure description.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
