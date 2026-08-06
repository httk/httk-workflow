"""Filesystem-native workflow execution for httk₂.

The package presents three layers, each with its own import home:

* **Filesystem protocol** — the language-neutral on-disk contract lives in
  :mod:`httk.workflow.protocol`. Independent tools read and verify a workspace
  through it and the specification alone.
* **Execution / authoring** — the surface a runner author uses. :class:`Runner`,
  :class:`Attempt`, and the small set of job and result types below are exported
  here; the lower-level runtime helpers live in :mod:`httk.workflow.runtime` and
  :mod:`httk.workflow.runtime_utils`, and job scaffolding in
  :mod:`httk.workflow.scaffold`.
* **Orchestration and management** — :class:`Workspace`, :class:`TaskManager`,
  and :func:`job_records` drive and inspect a running workspace. The management
  operations that surround them (transfers, manifests, hygiene, configuration,
  adapters, supervision, and the VASP and v1 compatibility surfaces) live in
  their own named submodules rather than in this root.

The normal lifecycle is instantiate a job, run it, then collect its outputs.
Only the deliberate top-level surface is re-exported here; everything else is
reached through its submodule.
"""

from .collecting import CollectedJob, JobRecord, collect, job_records
from .errors import (
    FormatError,
    RunnerResolutionError,
    TransactionError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
    WorkspaceCorruptionError,
    WorkspaceUnavailableError,
)
from .manager import TaskManager
from .runtime_builders import JobState
from .scaffold import ScaffoldedJob, new_job, new_jobs
from .sdk import (
    Attempt,
    ChildrenView,
    ChildResult,
    ChildSpec,
    InstantiateHandler,
    Runner,
    RunnerRef,
)
from .workspace import Workspace

__all__ = [
    "Attempt",
    "ChildResult",
    "ChildSpec",
    "ChildrenView",
    "CollectedJob",
    "FormatError",
    "InstantiateHandler",
    "JobRecord",
    "JobState",
    # Execution / authoring surface.
    "Runner",
    "RunnerRef",
    "RunnerResolutionError",
    "ScaffoldedJob",
    "TaskManager",
    "TransactionError",
    "TransitionLostError",
    "UnsupportedExtensionError",
    # The public exception family.
    "WorkflowError",
    # Orchestration and management entry points.
    "Workspace",
    "WorkspaceCorruptionError",
    "WorkspaceUnavailableError",
    "collect",
    "job_records",
    "new_job",
    "new_jobs",
]
