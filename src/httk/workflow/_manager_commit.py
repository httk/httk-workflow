"""Private outcome and commit decisions used by the task manager."""

import errno
import logging
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from . import gc as _gc
from ._util import read_json, require_int, require_string
from .errors import FormatError, TransactionError, UnsupportedExtensionError
from .models import (
    Failure,
    JobDefinition,
    Marker,
    StateFrame,
    is_payload_private,
    normalize_placement,
    validate_failure,
    validate_label,
    validate_resources,
    validate_step,
)

_LOGGER = logging.getLogger("httk.workflow.manager")


def _remove_attempt_control_tree(control: Path, job_key: str) -> None:
    """Best-effort remove one validated attempt-control tree and its container."""

    attempts = control.parent
    removed = _gc._remove_tree(control)
    if not removed and control.exists():
        raise OSError(f"attempt control tree remains: {control}")
    try:
        attempts.rmdir()
    except FileNotFoundError:
        pass
    except OSError as exc:
        if exc.errno != errno.ENOTEMPTY:
            _LOGGER.warning("cannot remove empty attempt container for %s: %s", job_key, exc)


def _remove_committed_attempt_control(manager: Any, marker: Marker, state: StateFrame, destination: Marker) -> None:
    """Remove local committed control only after this manager has reaped it.

    A live local attempt is marked for cleanup and removed when its process is
    reaped. A committing marker recovered by a manager that never owned the
    process is deliberately left for ``attempt_control`` garbage collection.
    """

    if destination.kind in {"failed", "cancelled"}:
        return
    attempt_id = state.attempt_id or ""
    local = manager._running.get(attempt_id)
    if local is not None:
        local.cleanup_pending = True
        local.cleanup_request = (marker, state, destination)
        if not local.reaped:
            return
    elif attempt_id not in manager._reaped_attempts:
        # This is an inherited commit: no process exit was proven by this
        # manager, so the evidence remains for the collector.
        return
    try:
        control = manager._attempt_control_path(marker, state)
        _remove_attempt_control_tree(control, marker.job_key)
    except Exception as exc:
        _LOGGER.warning("cannot remove committed attempt control for %s: %s", marker.job_key, exc)
    finally:
        manager._reaped_attempts.discard(attempt_id)


def failure(code: str, message: str, *, exit_status: int | None = None) -> dict[str, object]:
    details: dict[str, object] = {"log_paths": ["logs/stdio.out"]}
    if exit_status is not None:
        details["exit_status"] = exit_status
    return Failure(code, message, details=details).as_mapping()


def nested_reason(outcome: Mapping[str, Any], key: str) -> str:
    value = outcome.get(key)
    if not isinstance(value, Mapping) or not isinstance(value.get("reason"), str):
        raise FormatError(f"{key} outcome requires a reason")
    return str(value["reason"])


def attempt_budget_failure(job: JobDefinition, attempt_ordinal: int, total_attempts: int) -> str | None:
    per_activation = job.retry_policy.maximum_attempts_per_activation
    if per_activation is not None and attempt_ordinal > per_activation:
        return "maximum_attempts_per_activation exceeded"
    total = job.retry_policy.maximum_total_attempts
    if total is not None and total_attempts > total:
        return "maximum_total_attempts exceeded"
    return None


def retry_budget_available(job: JobDefinition, state: StateFrame) -> bool:
    attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
    total = state.total_attempts if state.total_attempts is not None else attempts
    maximum = job.retry_policy.maximum_attempts_per_activation
    if maximum is not None and attempts >= maximum:
        return False
    maximum_total = job.retry_policy.maximum_total_attempts
    return maximum_total is None or total < maximum_total


def declared_runner_steps(marker: Marker, outcome: Mapping[str, Any], log: Any) -> list[str] | None:
    declared = outcome.get("runner_steps")
    if declared is None:
        return None
    if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes)):
        log.warning("ignoring runner_steps of %s: not an array", marker.job_key)
        return None
    try:
        return [validate_step(item, "runner_steps item") for item in declared]
    except FormatError as exc:
        log.warning("ignoring runner_steps of %s: %s", marker.job_key, exc)
        return None


# A malformed or partial outcome is the signature of an outcome assembled in
# place instead of published by an atomic rename, so the remedy is attached to
# the parse and shape errors — not to the identity comparisons, where the
# outcome is well-formed but stale or foreign and the remedy would mislead.
_ASSEMBLY_REMEDY = (
    "; an outcome must be published by renaming a staged directory onto outcome.ready, never assembled in place"
)


def read_outcome(path: Path, marker: Marker, state: StateFrame) -> dict[str, Any]:
    try:
        outcome = read_json(path)
    except FormatError as exc:
        raise FormatError(f"{exc}{_ASSEMBLY_REMEDY}") from exc
    if outcome.get("format") != "httk-workflow-outcome" or outcome.get("format_version") != 2:
        raise FormatError(f"outcome must use httk-workflow-outcome version 2{_ASSEMBLY_REMEDY}")
    for key, expected in (
        ("job_id", marker.job_id),
        ("activation_id", state.activation_id),
        ("attempt_id", state.attempt_id),
    ):
        if outcome.get(key) != expected:
            raise FormatError(f"outcome {key} is {outcome.get(key)!r} but this attempt is {expected!r}")
    if not isinstance(outcome.get("action"), str):
        raise FormatError(f"outcome action must be a string{_ASSEMBLY_REMEDY}")
    return outcome


def child_digests(outcome_path: Path, digest: Callable[..., str]) -> dict[str, str]:
    jobs_dir = outcome_path / "children" / "jobs"
    if not jobs_dir.is_dir():
        return {}
    return {path.name: digest(path, skip=is_payload_private) for path in sorted(jobs_dir.iterdir()) if path.is_dir()}


def spawn_labels(outcome_path: Path) -> dict[str, str]:
    spawn_path = outcome_path / "children" / "spawn.json"
    if not spawn_path.is_file():
        return {}
    entries = read_json(spawn_path).get("children")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise FormatError("spawn children must be an array")
    labels: dict[str, str] = {}
    used: set[str] = set()
    for raw in entries:
        if not isinstance(raw, Mapping):
            raise FormatError("spawn child must be an object")
        job_key = require_string(raw.get("job_key"), "spawn child job_key")
        label = validate_label(raw.get("label"), "spawn child label")
        if label in used:
            raise FormatError(f"spawn child label is not unique within the spawn set: {label}")
        used.add(label)
        labels[job_key] = label
    return labels


def labeled_join(join: Mapping[str, Any], outcome_path: Path) -> dict[str, object]:
    labels = spawn_labels(outcome_path)
    children = join.get("children")
    if not isinstance(children, Sequence) or isinstance(children, (str, bytes)):
        return dict(join)
    referenced: list[object] = []
    for raw in children:
        if not isinstance(raw, Mapping):
            referenced.append(raw)
            continue
        reference = dict(raw)
        if reference.get("label") is None:
            label = labels.get(str(reference.get("job_key", "")))
            if label is not None:
                reference["label"] = label
        referenced.append(reference)
    return {**join, "children": referenced}


def context_children(join_summary: object) -> list[dict[str, object]]:
    if not isinstance(join_summary, Sequence) or isinstance(join_summary, (str, bytes)):
        return []
    return [dict(item) for item in join_summary if isinstance(item, Mapping)]


def _retain_deferred_pause_process(state: StateFrame, progress: StateFrame) -> StateFrame:
    """Retain the committing attempt identity when a pause is deferred."""

    if state.pause_requested is None:
        return progress
    if "process" not in state.members:
        raise FormatError("a deferred pause requires the committing process identity")
    return StateFrame.replace(progress, process=state.members["process"])


def process_committing(manager: Any, marker: Marker) -> None:
    """Apply a validated committing outcome through the manager's effects."""

    state = manager._read_frame(marker)
    job = manager.workspace.load_job(marker)
    outcome_path = manager._outcome_path(marker, state)
    outcome = manager._read_outcome(outcome_path / "outcome.json", marker, state)
    data_generation = state.data_generation
    transaction_path = outcome_path / "transaction"
    if transaction_path.is_dir():
        if job.data_mode != "transactional" or data_generation is None:
            raise TransactionError("transaction published by a nontransactional job")
        if outcome.get("expected_data_generation") != data_generation:
            raise TransactionError("outcome expected_data_generation is stale")
        from .transactions import replay_transaction

        changed_data = replay_transaction(
            transaction_path,
            manager.workspace.payload_path(marker.placement, marker.job_key) / "data",
            expected_generation=data_generation,
            durable=manager.workspace.durable,
        )
        if changed_data:
            data_generation += 1
    manager._register_children(marker, state, outcome_path)
    executor = manager._executor_for(job)
    if executor is None:
        return
    from .executors import OutcomeCommit

    executor.commit_outcome(
        OutcomeCommit(
            job=job,
            marker=marker,
            payload=manager.workspace.payload_path(marker.placement, marker.job_key),
            outcome_path=outcome_path,
            outcome=outcome,
        )
    )
    action = outcome["action"]
    next_resources: dict[str, int] | None = None
    if "resources" in outcome:
        if action not in {"advance", "wait"}:
            raise FormatError("outcome resources are only valid for advance or wait")
        next_resources = validate_resources(outcome["resources"], "outcome.resources")
    progress = StateFrame.replace(state.carried(), data_generation=data_generation)
    declared_steps = manager._declared_runner_steps(marker, outcome)
    if declared_steps is not None:
        progress = StateFrame.replace(progress, runner_steps=declared_steps)
    progress = _retain_deferred_pause_process(state, progress)
    priority_raw = outcome.get("priority")
    next_priority = (
        marker.priority if priority_raw is None else require_int(priority_raw, "outcome.priority", maximum=999)
    )
    if action == "advance":
        destination = manager._advance(
            marker,
            job,
            state,
            validate_step(outcome.get("next_step"), "next_step"),
            progress,
            resources=next_resources,
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "retry":
        destination = manager._retry(
            marker, job, state, progress, nested_reason(outcome, "retry"), unclean=False, priority=next_priority
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "wait":
        next_step = validate_step(outcome.get("next_step"), "next_step")
        join = outcome.get("join")
        if not isinstance(join, Mapping):
            raise FormatError("wait outcome requires a join object")
        destination = manager._transition(
            marker,
            "waiting",
            StateFrame.replace(
                progress,
                next_step=next_step,
                resources=next_resources,
                join=manager._labeled_join(join, outcome_path),
                reason="waiting_for_children",
            ),
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "succeed":
        destination = manager._transition(
            marker, "succeeded", StateFrame.replace(progress, reason="succeeded"), priority=next_priority
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "fail":
        try:
            failure_value = validate_failure(outcome.get("failure"))
        except FormatError as exc:
            failure_value = Failure(
                "protocol_error",
                f"runner published a malformed failure object: {exc}",
                details={"log_paths": ["logs/stdio.out"]},
            )
            reason = "protocol_error"
        else:
            reason = "declared_failure"
            if failure_value.retryable and manager._retry_budget_available(job, state):
                _LOGGER.info(
                    "retrying %s: the runner declared %s retryable",
                    marker.job_key,
                    failure_value.code,
                    extra=manager._event("declared_retry", marker, failure_code=failure_value.code),
                )
                destination = manager._retry(
                    marker, job, state, progress, failure_value.code, unclean=False, priority=next_priority
                )
                _remove_committed_attempt_control(manager, marker, state, destination)
                return
        destination = manager._transition(
            marker,
            "failed",
            StateFrame.replace(progress, failure=failure_value.as_mapping(), reason=reason),
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    elif action == "pause":
        paused = progress
        if "process" in state.members:
            paused = StateFrame.replace(paused, process=state.members["process"])
        destination = manager._transition(
            marker,
            "paused",
            StateFrame.replace(paused, pause=outcome.get("pause"), reason="step_paused"),
            priority=next_priority,
        )
        _remove_committed_attempt_control(manager, marker, state, destination)
    else:
        raise FormatError(f"unsupported outcome action: {action!r}")


def advance(
    manager: Any,
    marker: Marker,
    job: JobDefinition,
    state: StateFrame,
    next_step: str,
    progress: StateFrame,
    *,
    reason: str = "advance",
    join_summary: Sequence[object] | None = None,
    resources: Mapping[str, int] | None = None,
    priority: int | None = None,
) -> Marker:
    progress = _retain_deferred_pause_process(state, progress)
    activation_ordinal = (state.activation_ordinal if state.activation_ordinal is not None else 1) + 1
    maximum = job.retry_policy.maximum_activations
    if maximum is not None and activation_ordinal > maximum:
        return manager._transition(
            marker,
            "failed",
            StateFrame.replace(
                progress, failure=failure("budget_exhausted", "maximum_activations exceeded"), reason="budget_exhausted"
            ),
        )
    return manager._transition(
        marker,
        "ready",
        StateFrame.replace(
            progress,
            step=next_step,
            activation_id=str(uuid.uuid4()),
            activation_ordinal=activation_ordinal,
            attempt_ordinal=0,
            reason=reason,
            join_summary=join_summary,
            resources=resources,
            previous_attempt_id=state.attempt_id,
        ),
        priority=priority,
    )


def retry(
    manager: Any,
    marker: Marker,
    job: JobDefinition,
    state: StateFrame,
    progress: StateFrame,
    reason: str,
    *,
    unclean: bool,
    takeover_evidence: Mapping[str, object] | None = None,
    priority: int | None = None,
) -> Marker:
    progress = _retain_deferred_pause_process(state, progress)
    current_attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
    maximum = job.retry_policy.maximum_attempts_per_activation
    if maximum is not None and current_attempts >= maximum:
        return manager._transition(
            marker,
            "failed",
            StateFrame.replace(progress, failure=failure("retry_exhausted", reason), reason="retry_exhausted"),
        )
    evidence_value = {} if takeover_evidence is None else dict(takeover_evidence)
    retried = StateFrame.replace(
        progress,
        reason=reason,
        unclean_restart=unclean,
        unsafe_persistent_takeover=evidence_value.get("evidence") == "unsafe_persistent_takeover",
        previous_attempt_id=state.attempt_id,
    )
    if takeover_evidence is not None:
        retried = StateFrame.replace(retried, takeover_evidence=evidence_value)
    return manager._transition(marker, "ready", retried, priority=priority)


def register_children(
    manager: Any, marker: Marker, state: StateFrame, outcome_path: Path, digest: Callable[..., str]
) -> None:
    children_dir = outcome_path / "children"
    if not children_dir.is_dir():
        return
    spawn = read_json(children_dir / "spawn.json")
    entries = spawn.get("children")
    if not isinstance(entries, Sequence) or isinstance(entries, (str, bytes)):
        raise FormatError("spawn children must be an array")
    manager._spawn_labels(outcome_path)
    expected_digests = state.child_digests
    if expected_digests is None and state.has("child_digests"):
        raise FormatError("committing child_digests must be an object")
    expected_digests = {} if expected_digests is None else expected_digests
    for raw in entries:
        if not isinstance(raw, Mapping):
            raise FormatError("spawn child must be an object")
        job_key = str(raw.get("job_key", ""))
        placement = normalize_placement(str(raw.get("placement", "")))
        if raw.get("workspace_id", manager.workspace.workspace_id) != manager.workspace.workspace_id:
            raise UnsupportedExtensionError("cross-workspace spawn children are not supported")
        source = children_dir / "jobs" / job_key
        expected_digest = str(expected_digests.get(job_key, ""))
        target = manager.workspace.payload_path(placement, job_key)
        published_here = False
        if source.is_dir():
            child = JobDefinition.from_mapping(read_json(source / "job.json"))
            if child.job_key != job_key:
                raise FormatError("spawn job_key disagrees with child job.json")
            if digest(source, skip=is_payload_private) != expected_digest:
                raise FormatError("spawn child changed after outcome publication")
            target.parent.mkdir(parents=True, exist_ok=True)
            manager.workspace._publish_path(source, target)
            published_here = True
        if not target.is_dir():
            raise FormatError(f"registered child bundle does not match: {job_key}")
        if manager.workspace.find_marker_at(job_key, placement) is not None:
            continue
        if not published_here and digest(target, skip=is_payload_private) != expected_digest:
            raise FormatError(f"registered child bundle does not match: {job_key}")
        child = JobDefinition.from_mapping(read_json(target / "job.json"))
        temporary = manager.workspace.control / "tmp" / f"child-marker.{uuid.uuid4()}"
        temporary.touch(exist_ok=False)
        destination = manager.workspace.marker_path("submitted", placement, job_key, child.priority, 0, "init")
        manager.workspace._publish_path(temporary, destination)


def handle_attempt_failure(
    manager: Any,
    marker: Marker,
    job: JobDefinition,
    code: str,
    message: str,
    *,
    exit_status: int | None = None,
    unclean: bool = True,
    takeover_evidence: Mapping[str, object] | None = None,
    logger: Any,
) -> None:
    logger.warning(
        "attempt of %s failed with %s: %s",
        marker.job_key,
        code,
        message,
        extra=manager._event("attempt_failure", marker, failure_code=code, exit_status=exit_status),
    )
    try:
        state = manager._read_frame(marker)
        progress = state.carried()
        if code in job.retry_policy.retry_on:
            manager._retry(marker, job, state, progress, code, unclean=unclean, takeover_evidence=takeover_evidence)
            return
        failed = StateFrame.replace(progress, failure=failure(code, message, exit_status=exit_status), reason=code)
        if "process" in state.members:
            failed = StateFrame.replace(failed, process=state.members["process"])
        if takeover_evidence is not None:
            failed = StateFrame.replace(failed, takeover_evidence=dict(takeover_evidence))
        manager._transition(marker, "failed", failed)
    except Exception as exc:
        from .errors import TransitionLostError, WorkflowError

        if isinstance(exc, TransitionLostError):
            logger.debug("failure record for %s was lost to another actor", marker.job_key)
        elif isinstance(exc, (WorkflowError, OSError)):
            manager._report_anomaly(
                f"failure:{marker.job_key}",
                f"cannot record the {code} failure of {marker.job_key}: {exc}",
                manager._event("failure_error", marker, failure_code=code),
            )
        else:
            raise


def resume(manager: Any, logger: Any) -> bool:
    from .errors import (
        FormatError,
        TransactionError,
        TransitionLostError,
        WorkflowError,
    )

    changed = False
    for marker in manager._window("resume_committing", "committing"):
        manager._pace()
        loaded = manager._load_job_and_state(marker, "resume_committing")
        if loaded is None:
            continue
        job, state = loaded
        if manager._executor_for(job) is None:
            logger.debug(
                "skipping committing job %s: runner executor %s is not served here", marker.job_key, job.runner_executor
            )
            continue
        try:
            manager._process_committing(marker)
        except TransitionLostError:
            pass
        except (FormatError, TransactionError) as exc:
            # A replay that fails midway is transaction corruption; a manifest or
            # outcome the manager cannot parse is a protocol violation of the
            # runner. Both used to be reported as corruption, which lied about a
            # malformed outcome.
            code = "transaction_corruption" if isinstance(exc, TransactionError) else "protocol_error"
            logger.error("commit of %s failed: %s", marker.job_key, exc, extra=manager._event("commit_failed", marker))
            try:
                manager._transition(
                    marker,
                    "failed",
                    StateFrame.replace(
                        StateFrame.replace(state.carried(), process=state.members["process"])
                        if "process" in state.members
                        else state.carried(),
                        failure=manager._failure(code, str(exc)),
                        reason="commit_failed",
                    ),
                )
            except TransitionLostError:
                pass
        except (WorkflowError, OSError) as exc:
            manager._report_anomaly(
                f"resume_committing:{marker.job_key}",
                f"cannot resume the commit of {marker.job_key}: {exc}",
                manager._event("commit_error", marker),
            )
        changed = True
    return changed
