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
  operations that surround them (manifests, hygiene, configuration,
  adapters, supervision, the :mod:`httk.workflow.codes` toolkit for
  simulation-code support packages, and the :mod:`httk.workflow.compat`
  consumers of other workflow systems) live in their own named submodules
  rather than in this root.

The normal lifecycle is instantiate a job, run it, then collect its outputs.
Only the deliberate top-level surface is re-exported here; everything else is
reached through its submodule.
"""

import importlib
from typing import Any

# The root re-exports resolve lazily (PEP 562 ``__getattr__``, the SPEC 1 pattern) so that
# ``import httk.workflow``, and therefore ``python3 -m httk.workflow.adapter_runtime`` (the packaged remote
# adapter, spawned once per remote operation), stays free of httk.core and numpy. Their static view, for type
# checkers and the API reference, is the ``__init__.pyi`` stub beside this file.
# pyright: reportUnsupportedDunderAll=false

__all__ = [
    "AmbiguousClaimError",
    "Attempt",
    "ChildResult",
    "ChildSpec",
    "ChildrenView",
    "Claim",
    "CollectedJob",
    "CollectedPostprocess",
    "DirectoryClaim",
    "DuplicateTagError",
    "FormatError",
    "IdentityCollisionError",
    "InstantiateHandler",
    "JobRecord",
    "JobState",
    "NotIdleError",
    "ParentJob",
    # Execution / authoring surface.
    "Runner",
    "RunnerResolutionError",
    "ScaffoldedJob",
    "StructureInput",
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
    "collect_postprocess",
    "collect_tree",
    "job_records",
    "ledger_key",
    "new_job",
    "new_jobs",
    "scaffold_job",
    "store_collected",
    "store_postprocess",
]

_LAZY_EXPORTS = {
    "AmbiguousClaimError": "calculations",
    "DirectoryClaim": "calculations",
    "IdentityCollisionError": "calculations",
    "claims": "calculations",
    "collect_tree": "calculations",
    "CollectedJob": "collecting",
    "JobRecord": "collecting",
    "StructureInput": "collecting",
    "collect": "collecting",
    "job_records": "collecting",
    "DuplicateTagError": "errors",
    "FormatError": "errors",
    "RunnerResolutionError": "errors",
    "TransactionError": "errors",
    "TransitionLostError": "errors",
    "UnsupportedExtensionError": "errors",
    "WorkflowError": "errors",
    "WorkspaceCorruptionError": "errors",
    "WorkspaceUnavailableError": "errors",
    "Claim": "hookapi",
    "Unclaimed": "hookapi",
    "UnstableIdentityError": "id_keys",
    "ledger_key": "id_keys",
    "NotIdleError": "manager",
    "TaskManager": "manager",
    "WorkCensus": "manager",
    "CollectedPostprocess": "postprocessing",
    "collect_postprocess": "postprocessing",
    "JobState": "runtime_builders",
    "ScaffoldedJob": "scaffold",
    "new_job": "scaffold",
    "new_jobs": "scaffold",
    "scaffold_job": "scaffold",
    "Attempt": "sdk",
    "ChildrenView": "sdk",
    "ChildResult": "sdk",
    "ChildSpec": "sdk",
    "InstantiateHandler": "sdk",
    "ParentJob": "sdk",
    "Runner": "sdk",
    "store_collected": "storing",
    "store_postprocess": "storing",
    "Workspace": "workspace",
}


def __getattr__(name: str) -> Any:
    """Import a root re-export from its submodule on first access.

    :param name: The module attribute to resolve.
    :return: The requested re-export.
    :raises AttributeError: If ``name`` is not a root re-export.
    """
    submodule = _LAZY_EXPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(f".{submodule}", __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """List the module attributes, including the not yet resolved re-exports.

    :return: The sorted attribute names.
    """
    return sorted(set(globals()) | set(__all__))
