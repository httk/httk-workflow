"""Private scheduling decisions used by :mod:`httk.workflow.manager`."""

import logging
import uuid
from collections import Counter
from typing import TYPE_CHECKING, Any

from .errors import (
    FormatError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .models import JobDefinition, Marker, StateFrame

if TYPE_CHECKING:
    from .manager import WorkCensus

_LOGGER = logging.getLogger("httk.workflow.manager")


def eligible_ready(manager: Any) -> list[Marker]:
    """Filter and order the ready window for one manager's capabilities."""

    eligible: list[Marker] = []
    for marker in manager._window("eligible_ready", "ready"):
        manager._pace()
        try:
            job: JobDefinition = manager.workspace.load_job(marker)
        except (WorkflowError, OSError) as exc:
            manager._report_anomaly(
                f"ready:{marker.job_key}",
                f"skipping ready job {marker.job_key}: {exc}",
                manager._event("job_unusable", marker, pass_name="ready"),
            )
            continue
        manager._reported.pop(f"ready:{marker.job_key}", None)
        if manager._executor_for(job) is None:
            _LOGGER.debug(
                "skipping ready job %s: runner executor %s is not served here",
                marker.job_key,
                job.runner_executor,
            )
            continue
        if not manager.accept_any_pool and job.claim_pool not in manager.pools:
            _LOGGER.debug(
                "skipping ready job %s: pool %s is not served here",
                marker.job_key,
                job.claim_pool,
            )
            continue
        if not job.required_capabilities <= manager.capabilities:
            _LOGGER.debug(
                "skipping ready job %s: missing capabilities %s",
                marker.job_key,
                ",".join(sorted(job.required_capabilities - manager.capabilities)),
            )
            continue
        eligible.append(marker)
    eligible.sort(key=lambda item: (item.priority, item.path.as_posix()))
    return eligible


def attempt_budget_failure(job: JobDefinition, attempt_ordinal: int, total_attempts: int) -> str | None:
    per_activation = job.retry_policy.maximum_attempts_per_activation
    if per_activation is not None and attempt_ordinal > per_activation:
        return "maximum_attempts_per_activation exceeded"
    total = job.retry_policy.maximum_total_attempts
    if total is not None and total_attempts > total:
        return "maximum_total_attempts exceeded"
    return None


def retry_budget_available(job: JobDefinition, state: Any) -> bool:
    """Report whether another attempt of this activation is permitted."""

    attempts = state.attempt_ordinal if state.attempt_ordinal is not None else 1
    total = state.total_attempts if state.total_attempts is not None else attempts
    per_activation = job.retry_policy.maximum_attempts_per_activation
    if per_activation is not None and attempts >= per_activation:
        return False
    maximum_total = job.retry_policy.maximum_total_attempts
    return maximum_total is None or total < maximum_total


def window_policy(maximum_pass_markers: int, discovery_budget: int) -> tuple[int, int]:
    """Return the bounded-pass limits as one explicit scheduling decision."""

    return maximum_pass_markers, discovery_budget


def register_submissions(manager: Any) -> bool:
    changed = False
    for marker in manager._window("register_submissions", "submitted"):
        manager._pace()
        try:
            job = manager.workspace.validate_job_payload(marker)
            executor = manager._executor_for(job)
            if executor is None:
                _LOGGER.debug(
                    "skipping submitted job %s: runner executor %s is not served here",
                    marker.job_key,
                    job.runner_executor,
                )
                continue
            executor.validate(job, manager.workspace.payload_path(marker.placement, marker.job_key))
            manager._transition(
                marker,
                "ready",
                StateFrame.of(
                    step=job.initial_step,
                    activation_id=str(uuid.uuid4()),
                    activation_ordinal=1,
                    attempt_ordinal=0,
                    total_attempts=0,
                    data_generation=0 if job.data_mode == "transactional" else None,
                    reason="submitted",
                    job_digest=job.digest,
                ),
            )
        except TransitionLostError:
            pass
        except (FormatError, UnsupportedExtensionError) as exc:
            try:
                manager._transition(
                    marker,
                    "failed",
                    StateFrame.of(
                        failure=manager._failure("protocol_error", str(exc)),
                        data_generation=None,
                        reason="submission_invalid",
                    ),
                )
            except TransitionLostError:
                pass
        changed = True
    return changed


def recover_abandoned_claims(manager: Any, logger: Any) -> bool:
    changed = False
    for marker in list(manager._walk(("claimed",))):
        manager._pace()
        loaded = manager._load_job_and_state(marker, "recover_claims")
        if loaded is None:
            continue
        job, state = loaded
        if manager._executor_for(job) is None:
            logger.debug(
                "skipping claimed job %s: runner executor %s is not served here", marker.job_key, job.runner_executor
            )
            continue
        try:
            owner = state.manager_id
        except FormatError as exc:
            manager._report_anomaly(
                f"claim_owner:{marker.job_key}",
                f"claimed job {marker.job_key} does not name a usable manager: {exc}",
                manager._event("protocol_error", marker),
            )
            owner = None
        if manager._manager_alive(
            owner, lease_seconds=manager.lease_seconds if state.lease_seconds is None else state.lease_seconds
        ):
            continue
        logger.warning(
            "recovering %s: the claim of manager %s was abandoned",
            marker.job_key,
            owner or "-",
            extra=manager._event("claim_recovered", marker, previous_manager=owner),
        )
        manager._release_claim(marker, "claim_abandoned", state)
        changed = True
    return changed


def claim_pass(manager: Any, changed: bool, logger: Any) -> bool:
    if manager._draining:
        logger.debug("draining: not claiming new work")
        return changed
    if manager._maintenance_paused():
        return changed
    for marker in manager._eligible_ready():
        if len(manager._running) >= manager.maximum_workers:
            break
        try:
            changed |= manager._claim_and_launch(marker)
        except TransitionLostError as exc:
            logger.debug("claim of %s was lost to another actor: %s", marker.job_key, exc)
            continue
        except (WorkflowError, OSError) as exc:
            manager._report_anomaly(
                f"claim:{marker.job_key}",
                f"cannot claim or launch {marker.job_key}: {exc}",
                manager._event("claim_error", marker),
            )
            changed = True
    return changed


def _classify_pending(manager: Any, marker: Marker, blocked: dict[str, Counter[str]]) -> bool:
    """Classify one submitted or ready marker, returning whether it is actionable.

    A submitted job only needs its executor served to register; a ready job must
    also match the pool and capabilities. A job this manager cannot progress is
    attributed to exactly one missing requirement, in the same order
    :func:`eligible_ready` checks them.
    """

    try:
        job = manager.workspace.load_job(marker)
    except (WorkflowError, OSError):
        # An unreadable submitted job may still register once repaired; an
        # unreadable ready job is a local defect the scheduler already reports.
        return marker.kind == "submitted"
    if manager._executor_for(job) is None:
        blocked["executor"][job.runner_executor] += 1
        return False
    if marker.kind == "submitted":
        return True
    if not manager.accept_any_pool and job.claim_pool not in manager.pools:
        blocked["pool"][job.claim_pool] += 1
        return False
    missing = job.required_capabilities - manager.capabilities
    if missing:
        blocked["capability"][min(missing)] += 1
        return False
    return True


def work_census(manager: Any) -> "WorkCensus":
    """Scan the workspace once and tag every job by why it is this manager's work.

    Actionability applies exactly the claim predicates of :func:`eligible_ready`:
    a ready job counts as actionable only if this manager could claim it. A
    wrong-pool, missing-capability, or unserved-executor job is not actionable
    — the manager can do nothing about it — so it is reported for the operator
    instead of silently keeping the manager awake or silently letting it exit.
    """

    from .manager import WorkCensus

    # ponytail: succeeded/failed are counted by an exhaustive marker walk. It is
    # cheap per entry and runs only on a settled tick or at idle exit, never in
    # the hot claim path, so no cursor or cache is warranted.
    succeeded = failed = waiting = paused = 0
    for marker in manager._walk(("succeeded", "failed", "waiting", "paused")):
        if marker.kind == "succeeded":
            succeeded += 1
        elif marker.kind == "failed":
            failed += 1
        elif marker.kind == "waiting":
            waiting += 1
        else:
            paused += 1
    blocked: dict[str, Counter[str]] = {"executor": Counter(), "pool": Counter(), "capability": Counter()}
    ready_claimable = 0
    actionable = 0
    for marker in manager._walk(("submitted", "ready")):
        if _classify_pending(manager, marker, blocked):
            actionable += 1
            if marker.kind == "ready":
                ready_claimable += 1
    unreadable = 0
    for marker in manager._walk(("committing", "cancelling")):
        try:
            job = manager.workspace.load_job(marker)
        except (WorkflowError, OSError):
            # A committing/cancelling job whose definition cannot be read is not
            # this manager's work: no pass can advance it, so counting it
            # actionable used to spin the manager to its idle timeout. The
            # scheduler already reports the damage as an anomaly; here it is a
            # named census bucket so the operator sees it and idle exit is prompt.
            unreadable += 1
            continue
        if manager._executor_for(job) is not None:
            actionable += 1
    return WorkCensus(
        succeeded=succeeded,
        failed=failed,
        ready_claimable=ready_claimable,
        ready_blocked={kind: dict(counter) for kind, counter in blocked.items() if counter},
        waiting=waiting,
        paused=paused,
        actionable_count=actionable,
        unreadable=unreadable,
    )
