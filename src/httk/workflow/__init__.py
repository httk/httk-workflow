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
  and :func:`harvest` drive and inspect a running workspace. The management
  operations that surround them (transfers, manifests, hygiene, configuration,
  adapters, supervision, and the VASP and v1 compatibility surfaces) live in
  their own named submodules rather than in this root.

Only the deliberate top-level surface is re-exported here; everything else is
reached through its submodule.
"""

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
from .harvesting import HarvestRecord, harvest
from .manager import TaskManager
from .runtime_builders import JobState
from .scaffold import ScaffoldedJob, new_job, new_jobs
from .sdk import (
    Attempt,
    ChildrenView,
    ChildResult,
    ChildSpec,
    Runner,
    RunnerRef,
)
from .workspace import Workspace

__all__ = [
    # Orchestration and management entry points.
    "Workspace",
    "TaskManager",
    "harvest",
    "HarvestRecord",
    # Execution / authoring surface.
    "Runner",
    "Attempt",
    "ChildSpec",
    "RunnerRef",
    "ChildResult",
    "ChildrenView",
    "JobState",
    "new_job",
    "new_jobs",
    "ScaffoldedJob",
    # The public exception family.
    "WorkflowError",
    "FormatError",
    "WorkspaceUnavailableError",
    "WorkspaceCorruptionError",
    "TransitionLostError",
    "UnsupportedExtensionError",
    "TransactionError",
    "RunnerResolutionError",
]
