"""Expose filesystem-native workflow execution for httk₂.

The package presents three layers, each with its own import home:

* **Filesystem protocol** — the language-neutral on-disk contract lives in
  :mod:`httk.workflow.protocol`. Independent tools read and verify a workspace
  through it and the specification alone.
* **Execution / authoring** — the surface a runner author uses. :class:`Runner`,
  :class:`Attempt`, and the small set of job and result types below are exported
  here; the lower-level runtime helpers live in :mod:`httk.workflow.runtime` and
  :mod:`httk.workflow.runtime_utils`, and job scaffolding in
  :mod:`httk.workflow.scaffold`. Python, the host language, has its SDK in
  :mod:`httk.workflow.sdk`; the runner SDKs for the other programming languages
  (Ada, Bash, C, C++, Fortran, Java, Perl, Rust) ship as data files in the
  installed ``httk/workflow/languages`` directory, which the manager exports as
  ``HTTK_WORKFLOW_LANGUAGES_DIR``.
* **Orchestration and management** — :class:`Workspace`, :class:`TaskManager`,
  and :func:`job_records` drive and inspect a running workspace. The management
  operations that surround them (transfers, manifests, hygiene, configuration,
  adapters, supervision, the :mod:`httk.workflow.codes` toolkit for
  simulation-code support packages, and the :mod:`httk.workflow.compat`
  consumers of other workflow systems) live in their own named submodules
  rather than in this root.

The normal lifecycle is instantiate a job, run it, then collect its outputs.
Only the deliberate top-level surface is re-exported here; everything else is
reached through its submodule.
"""

from .calculations import AmbiguousClaimError, DirectoryClaim, IdentityCollisionError, claims, collect_tree
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
from .hookapi import Claim, Unclaimed
from .id_keys import UnstableIdentityError, ledger_key
from .manager import NotIdleError, TaskManager, WorkCensus
from .runtime_builders import JobState
from .scaffold import ScaffoldedJob, new_job, new_jobs, scaffold_job
from .sdk import (
    Attempt,
    ChildrenView,
    ChildResult,
    ChildSpec,
    InstantiateHandler,
    ParentJob,
    Runner,
    RunnerRef,
)
from .storing import store_collected
from .workspace import Workspace

__all__ = [
    "AmbiguousClaimError",
    "Attempt",
    "ChildResult",
    "ChildSpec",
    "ChildrenView",
    "Claim",
    "CollectedJob",
    "DirectoryClaim",
    "FormatError",
    "IdentityCollisionError",
    "InstantiateHandler",
    "JobRecord",
    "JobState",
    "NotIdleError",
    "ParentJob",
    # Execution / authoring surface.
    "Runner",
    "RunnerRef",
    "RunnerResolutionError",
    "ScaffoldedJob",
    "TaskManager",
    "TransactionError",
    "TransitionLostError",
    "Unclaimed",
    "UnstableIdentityError",
    "UnsupportedExtensionError",
    # Orchestration and management entry points.
    "WorkCensus",
    # The public exception family.
    "WorkflowError",
    "Workspace",
    "WorkspaceCorruptionError",
    "WorkspaceUnavailableError",
    "claims",
    "collect",
    "collect_tree",
    "job_records",
    "ledger_key",
    "new_job",
    "new_jobs",
    "scaffold_job",
    "store_collected",
]
