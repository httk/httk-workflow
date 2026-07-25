"""Exceptions raised by :mod:`httk.workflow`."""


class WorkflowError(Exception):
    """Base class for workflow protocol failures."""


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
