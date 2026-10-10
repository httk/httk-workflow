"""The language-neutral filesystem protocol surface of *httk₂* workflows.

This module is the one deliberate public home of the on-disk protocol: the
shapes, validators, and primitives an implementation in any language reads and
writes to interoperate through a workspace. The normative specification is
the filesystem protocol reference in the httk-workflow documentation; everything
named here is what that document describes, and an independent inspection or
verification tool should be able to work from this namespace and that document
alone.

Nothing here is manager bookkeeping, a subprocess wrapper, a CLI handler, or a
scheduling pass — those live in their own modules and are not part of the
protocol. The implementations are owned by the modules re-exported below (the
job model, the state document, the job-name format of the filesystem kernel,
:mod:`~httk.workflow.models` and the runtime builders), which are internal
detail from the protocol's point of view; import the names from here.
"""

from ._job import JobDefinition
from ._kernel import JobName, JobRef, format_job_name, parse_job_name
from ._state import StateDoc
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
from .models import (
    ATTEMPTS_DIRECTORY,
    CORE_PROFILE,
    EXCHANGE_DIRECTORY,
    JOB_STATE_DIRECTORY,
    JOBS_DIRECTORY,
    LOGS_DIRECTORY,
    POSTPROCESS_DIRECTORY,
    SUPPORTED_EXTENSIONS,
    Failure,
    RetentionPolicy,
    RetryPolicy,
    WorkspacePolicy,
    canonical_uuid,
    check_job_placement,
    is_payload_private,
    job_digest,
    make_job_key,
    normalize_placement,
    parse_job_key,
    parse_placement_text,
    placement_text,
    validate_declaration_name,
    validate_declarations,
    validate_failure,
    validate_label,
    validate_parameters,
    validate_resources,
    validate_step,
)
from .runtime import AttemptContext
from .runtime_builders import (
    ChildReference,
    JobSpec,
    JoinCondition,
    OutcomeAction,
    OutcomeDraft,
    ReplayableWorkdirBatch,
    RunLog,
    TransactionBuilder,
    join_mapping,
    prepare_job_payload,
    replay_transaction,
)

__all__ = [
    # -- workspace format and profile ------------------------------------
    "ATTEMPTS_DIRECTORY",
    "CORE_PROFILE",
    "EXCHANGE_DIRECTORY",
    "JOBS_DIRECTORY",
    "JOB_STATE_DIRECTORY",
    "LOGS_DIRECTORY",
    "POSTPROCESS_DIRECTORY",
    "SUPPORTED_EXTENSIONS",
    # -- attempt context and outcome documents ---------------------------
    "AttemptContext",
    "ChildReference",
    "Failure",
    # -- protocol error family -------------------------------------------
    "FormatError",
    # -- immutable job definitions and state documents -------------------
    "JobDefinition",
    # -- job names and references ----------------------------------------
    "JobName",
    "JobRef",
    "JobSpec",
    "JoinCondition",
    "OutcomeAction",
    "OutcomeDraft",
    # -- replayable data transactions ------------------------------------
    "ReplayableWorkdirBatch",
    "RetentionPolicy",
    "RetryPolicy",
    "RunLog",
    "RunnerResolutionError",
    "StateDoc",
    "TransactionBuilder",
    "TransactionError",
    "TransitionLostError",
    "UnsupportedExtensionError",
    "WorkflowError",
    "WorkspaceCorruptionError",
    "WorkspacePolicy",
    "WorkspaceUnavailableError",
    # -- validators ------------------------------------------------------
    "canonical_uuid",
    "check_job_placement",
    "format_job_name",
    "is_payload_private",
    "job_digest",
    "join_mapping",
    "make_job_key",
    "normalize_placement",
    "parse_job_key",
    "parse_job_name",
    "parse_placement_text",
    "placement_text",
    "prepare_job_payload",
    "replay_transaction",
    "validate_declaration_name",
    "validate_declarations",
    "validate_failure",
    "validate_label",
    "validate_parameters",
    "validate_resources",
    "validate_step",
]
