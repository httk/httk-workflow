"""Define exceptions raised by :mod:`httk.workflow`."""

__all__ = [
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


class SealError(WorkflowError):
    """A seal cannot be written or verified.

    This is the *cannot proceed* failure: no signing key is available, or a
    child level a seal must cover is itself unsealed. It never means that a seal
    refused an action because something was already sealed; that is
    :class:`SealedError`.
    """


class SealedError(WorkflowError):
    """An action was refused because the subject, or its enclosure, is sealed.

    Re-sealing a job whose recorded contents differ, or unsealing a level while
    the level above it still binds it, is refused rather than silently allowed:
    a seal that a wider seal already commits to must not change beneath it.
    """


class RunnerResolutionError(WorkflowError):
    """A shared runner cannot be resolved or verified.

    The failure carries the exact protocol failure ``code`` the manager records,
    so an unresolvable runner (``runner_unavailable``), a runner whose bytes
    disagree with the digest the job pinned (``runner_mismatch``), a
    missing registration (``runner_not_built``), and a failed foreground build
    (``runner_build_failed``) stay distinguishable to an operator.

    :param code: Protocol failure code recorded by the manager.
    :param message: Human-readable failure description.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
