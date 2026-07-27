"""Compatibility surface for job introspection and foreground debugging."""

# The imports below deliberately preserve the complete historical module surface.
# ruff cannot infer names exported by the whitespace-based __all__ assembly.
# ruff: noqa: F401

import json
import shutil
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from .._util import read_json, timestamp_seconds
from ..errors import WorkflowError, WorkspaceCorruptionError
from ..journal import read_record
from ..manager import TaskManager
from ..manifests import read_maintenance_lock
from ..models import (
    CORE_PROFILE,
    CORE_STATE_KINDS,
    DEFAULT_LEASE_SECONDS,
    QUIESCENT_KINDS,
    STATE_KINDS,
    TERMINAL_KINDS,
    JobDefinition,
    Marker,
    normalize_placement,
    validate_step,
)
from ..workspace import MarkerFault, Workspace
from ._debug import (
    DEBUG_EXIT_FAILED,
    DEBUG_EXIT_SUCCEEDED,
    DEBUG_EXIT_UNFINISHED,
    DebugOutcome,
    ScopedWorkspace,
    _drive,
    _drive_children,
    _exit_code,
    _frame_summary,
    _print_line,
    _pump,
    _stage_payload_with_step,
    _Tail,
    debug_job,
)
from ._diagnosis import (
    JOB_DIAGNOSIS_FORMAT,
    BudgetStatus,
    Check,
    ClaimRequirements,
    Diagnosis,
    ManagerRecord,
    _breadcrumb_check,
    _budget_checks,
    _continue_checks,
    _describe_child,
    _Diagnosing,
    _label_sequence,
    _label_set,
    _maintenance_check,
    _manager_checks,
    _owner_checks,
    _placement_covered,
    _profile_check,
    _requirement_checks,
    budget_status,
    claim_requirements,
    explain_job,
    manager_refusals,
    observe_join,
    read_managers,
)
from ._reading import (
    _HISTORY_READ_DEADLINE_SECONDS,
    JOB_HISTORY_FORMAT,
    JOB_LIST_FORMAT,
    _attempt_control,
    _job_of,
    _optional_float,
    _optional_int,
    _optional_string,
    _state_of,
    _workdir_relative,
    job_frames,
    list_jobs,
    read_error_breadcrumb,
    resolve_job,
)
from ._rendering import (
    JOB_REPORT_FORMAT,
    _pair,
    describe_job,
    render_frames,
    render_job,
    render_rows,
)

__all__ = """
JOB_REPORT_FORMAT JOB_HISTORY_FORMAT JOB_DIAGNOSIS_FORMAT JOB_LIST_FORMAT
DEBUG_EXIT_SUCCEEDED DEBUG_EXIT_FAILED DEBUG_EXIT_UNFINISHED resolve_job read_managers
ManagerRecord ClaimRequirements claim_requirements manager_refusals BudgetStatus budget_status observe_join
read_error_breadcrumb describe_job render_job job_frames render_frames list_jobs render_rows Check Diagnosis explain_job
ScopedWorkspace DebugOutcome debug_job _Tail _drive _drive_children _exit_code _frame_summary _print_line _pump
_stage_payload_with_step _Diagnosing _describe_child _label_sequence _label_set _maintenance_check _manager_checks
_owner_checks _placement_covered _profile_check _requirement_checks _budget_checks _breadcrumb_check _continue_checks
_HISTORY_READ_DEADLINE_SECONDS _attempt_control _job_of _optional_float _optional_int _optional_string _state_of
_workdir_relative _pair Any Callable Iterable Iterator Mapping Sequence Path PurePosixPath dataclass field json shutil
tempfile time read_json timestamp_seconds WorkflowError WorkspaceCorruptionError read_record TaskManager read_maintenance_lock
CORE_PROFILE CORE_STATE_KINDS DEFAULT_LEASE_SECONDS QUIESCENT_KINDS STATE_KINDS TERMINAL_KINDS JobDefinition Marker
normalize_placement validate_step MarkerFault Workspace
""".split()  # pyright: ignore[reportUnsupportedDunderAll]
