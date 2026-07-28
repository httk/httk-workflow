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
protocol. The implementations are owned by the modules re-exported below
(:mod:`~httk.workflow.models`, :mod:`~httk.workflow.journal`,
:mod:`~httk.workflow.transactions`, and the runtime builders), which are
internal detail from the protocol's point of view; import the names from here.
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
from .journal import (
    JournalFrame,
    RecordVerification,
    encode_record_ref,
    iter_journal_frames,
    iter_segment_frames,
    parse_record_ref,
    read_record,
    segment_path,
    verify_record,
)
from .models import (
    CARRIED_STATE_MEMBERS,
    CORE_PROFILE,
    CORE_STATE_KINDS,
    QUIESCENT_KINDS,
    READABLE_CORE_PROFILES,
    RUNNER_SOURCES,
    STATE_KINDS,
    SUPPORTED_EXTENSIONS,
    TERMINAL_KINDS,
    WITHDRAWN_EXTENSIONS,
    Failure,
    JobDefinition,
    Marker,
    RetentionPolicy,
    RetryPolicy,
    StateFrame,
    WorkspacePolicy,
    canonical_uuid,
    is_payload_private,
    job_digest,
    make_job_key,
    marker_basename,
    normalize_placement,
    parse_job_key,
    parse_package_runner,
    to_base36,
    validate_attempt_control,
    validate_declaration_name,
    validate_declarations,
    validate_failure,
    validate_inputs,
    validate_label,
    validate_runner_path,
    validate_sha256,
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
)
from .transactions import replay_transaction
from .workspace import MarkerFault

__all__ = [
    "CARRIED_STATE_MEMBERS",
    # -- workspace format and profile ------------------------------------
    "CORE_PROFILE",
    "CORE_STATE_KINDS",
    "QUIESCENT_KINDS",
    "READABLE_CORE_PROFILES",
    "RUNNER_SOURCES",
    "STATE_KINDS",
    "SUPPORTED_EXTENSIONS",
    "TERMINAL_KINDS",
    "WITHDRAWN_EXTENSIONS",
    # -- attempt context and outcome documents ---------------------------
    "AttemptContext",
    "ChildReference",
    "Failure",
    "FormatError",
    # -- immutable job definitions ---------------------------------------
    "JobDefinition",
    "JobSpec",
    "JoinCondition",
    "JournalFrame",
    # -- markers and transitions -----------------------------------------
    "Marker",
    "MarkerFault",
    "OutcomeAction",
    "OutcomeDraft",
    "RecordVerification",
    "ReplayableWorkdirBatch",
    "RetentionPolicy",
    "RetryPolicy",
    "RunLog",
    "RunnerResolutionError",
    "StateFrame",
    # -- replayable data transactions ------------------------------------
    "TransactionBuilder",
    "TransactionError",
    "TransitionLostError",
    "UnsupportedExtensionError",
    # -- protocol error family -------------------------------------------
    "WorkflowError",
    "WorkspaceCorruptionError",
    "WorkspacePolicy",
    "WorkspaceUnavailableError",
    "canonical_uuid",
    # -- journal records and references ----------------------------------
    "encode_record_ref",
    "is_payload_private",
    "iter_journal_frames",
    "iter_segment_frames",
    "job_digest",
    "join_mapping",
    "make_job_key",
    "marker_basename",
    "normalize_placement",
    "parse_job_key",
    "parse_package_runner",
    "parse_record_ref",
    "prepare_job_payload",
    "read_record",
    "replay_transaction",
    "segment_path",
    "to_base36",
    "validate_attempt_control",
    "validate_declaration_name",
    "validate_declarations",
    "validate_failure",
    "validate_inputs",
    "validate_label",
    "validate_runner_path",
    "validate_sha256",
    "validate_step",
    "verify_record",
]
