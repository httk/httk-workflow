"""Exceptions raised by :mod:`httk.workflow`."""


class WorkflowError(Exception):
    """Base class for workflow protocol failures."""


class FormatError(WorkflowError, ValueError):
    """A store, job, journal frame, outcome, or request is malformed."""


class StoreUnavailableError(WorkflowError):
    """The store cannot currently provide a coherent protocol view."""


class StoreCorruptionError(WorkflowError):
    """The authoritative filesystem state is internally inconsistent."""


class TransitionLostError(WorkflowError):
    """Another actor committed a transition from the expected marker."""


class UnsupportedExtensionError(WorkflowError):
    """A store requires an extension this implementation does not support."""


class TransactionError(WorkflowError):
    """A transactional-data manifest cannot be safely replayed."""
