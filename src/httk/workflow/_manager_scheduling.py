"""Private scheduling decisions used by :mod:`httk.workflow.manager`."""

import logging
import uuid
from typing import Any

from .errors import (
    FormatError,
    TransitionLostError,
    UnsupportedExtensionError,
    WorkflowError,
)
from .models import JobDefinition, Marker, StateFrame

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
        if manager._backend_for(job) is None:
            _LOGGER.debug(
                "skipping ready job %s: runner backend %s is not served here",
                marker.job_key,
                job.runner_backend,
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
            backend = manager._backend_for(job)
            if backend is None:
                _LOGGER.debug(
                    "skipping submitted job %s: runner backend %s is not served here",
                    marker.job_key,
                    job.runner_backend,
                )
                continue
            backend.validate(job, manager.workspace.payload_path(marker.placement, marker.job_key))
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
        if manager._backend_for(job) is None:
            logger.debug(
                "skipping claimed job %s: runner backend %s is not served here", marker.job_key, job.runner_backend
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


def has_actionable_work(manager: Any) -> bool:
    for marker in manager._walk(("submitted", "ready", "committing", "cancelling")):
        try:
            job = manager.workspace.load_job(marker)
        except (WorkflowError, OSError):
            if marker.kind == "submitted":
                return True
            continue
        if manager._backend_for(job) is not None:
            return True
    return False
