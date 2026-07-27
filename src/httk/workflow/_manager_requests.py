"""Private operator-request validation and classification helpers."""

import logging
import os
from collections.abc import Mapping
from typing import Any

from ._util import read_json
from .configuration import verify_document
from .errors import FormatError, TransitionLostError, WorkflowError
from .models import TERMINAL_KINDS, StateFrame, normalize_placement, validate_step

_LOGGER = logging.getLogger("httk.workflow.manager")


def operator_key(request: Mapping[str, Any], log: Any, event: Any) -> str | None:
    signature = verify_document(request)
    if not signature.present:
        log.debug(
            "request %s carries no operator signature",
            request.get("request_id"),
            extra=event("request_unsigned", request_id=str(request.get("request_id"))),
        )
        return None
    if not signature.valid:
        raise FormatError(f"operator signature is invalid: {signature.reason}")
    log.info(
        "request %s is signed by %s",
        request.get("request_id"),
        signature.operator_key,
        extra=event(
            "request_signature_verified", request_id=str(request.get("request_id")), operator_key=signature.operator_key
        ),
    )
    return signature.operator_key


def resolve_marker(manager: Any, request: Mapping[str, Any]) -> Any:
    job_key = request.get("job_key")
    placement = request.get("placement")
    if not isinstance(job_key, str) or not isinstance(placement, str):
        raise FormatError("request must carry a job_key and placement")
    return manager.workspace.find_marker_at(job_key, normalize_placement(placement))


def validate_envelope(request: Mapping[str, Any]) -> None:
    if request.get("format") != "httk-workflow-request" or request.get("format_version") != 1:
        raise FormatError("request must use httk-workflow-request version 1")


def action_class(action: object, kind: str) -> str:
    if action == "cancel" and kind not in {"succeeded", "failed", "cancelled"}:
        return "cancel"
    if action == "set_priority" and kind in {"submitted", "ready", "waiting", "paused", "failed"}:
        return "set_priority"
    if action == "pause" and kind in {"submitted", "ready", "waiting"}:
        return "pause"
    if action == "continue" and kind in {"failed", "paused"}:
        return "continue"
    if action == "override_step" and kind in {"failed", "paused"}:
        return "override_step"
    raise FormatError(f"request action {action!r} is invalid from state {kind}")


def apply(manager: Any, request: Mapping[str, Any]) -> str | None:
    validate_envelope(request)
    operator_key_value = manager._request_operator_key(request)
    marker = manager._resolve_request_marker(request)
    if marker is None:
        raise FormatError("request job does not exist")
    if marker.generation != int(request.get("expected_generation", -1)):
        return f"the job is at generation {marker.generation}, not the expected one"
    if marker.record_ref != request.get("expected_record_ref"):
        return "the job state changed after the request was published"
    action = request.get("action")
    state = manager._read_frame(marker)
    job = manager.workspace.load_job(marker)
    audit = StateFrame.of(
        state.carried(),
        operator=request.get("operator"),
        operator_reason=request.get("reason"),
        request_id=request.get("request_id"),
    )
    if operator_key_value is not None:
        audit = StateFrame.of(audit, operator_key=operator_key_value)
    if action == "cancel" and marker.kind not in TERMINAL_KINDS:
        return manager._request_cancel(marker, state, request, operator_key_value)
    if action == "set_priority" and marker.kind in {"submitted", "ready", "waiting", "paused", "failed"}:
        priority = int(request.get("priority", -1))
        preserved = state.select(("join", "join_summary", "next_step", "pause", "failure"))
        manager._transition(
            marker,
            marker.kind,
            StateFrame.of(StateFrame({**audit.members, **preserved.members}), reason="operator_priority"),
            priority=priority,
        )
        return None
    if action == "pause" and marker.kind in {"submitted", "ready", "waiting"}:
        manager._transition(marker, "paused", StateFrame.of(audit, reason="operator_pause"))
        return None
    if action in {"continue", "override_step"} and marker.kind in {"failed", "paused"}:
        hazard = manager._decided_join_hazard(marker, job)
        if hazard is not None:
            if not bool(request.get("force")):
                return (
                    f"this job was already consumed by the decided join of parent {hazard['parent_job_key']}, "
                    f"which observed it as {hazard['observed_kind']}; reviving it now races the parent reading its outputs. "
                    "Republish the request with force to accept that hazard, or continue the parent instead"
                )
            audit = StateFrame.of(audit, revival_hazard=hazard)
            _LOGGER.warning(
                "forced revival of %s: its parent %s already decided a join on it",
                marker.job_key,
                hazard["parent_job_key"],
                extra=manager._event("revival_hazard", marker, **hazard),
            )
    if action == "continue" and marker.kind in {"failed", "paused"}:
        if state.activation_id is None:
            raise FormatError("job has no runnable activation to continue")
        manager._retry(marker, job, state, audit, "manual_continue", unclean=False)
        return None
    if action == "override_step" and marker.kind in {"failed", "paused"}:
        manager._advance(
            marker,
            job,
            state,
            validate_step(request.get("step"), "request.step"),
            audit,
            reason="operator_override_step",
        )
        return None
    raise FormatError(f"request action {action!r} is invalid from state {marker.kind}")


def handle(manager: Any) -> bool:
    ready_dir = manager.workspace.control / "requests" / "ready"
    changed = False
    for request_path in sorted(ready_dir.iterdir()) if ready_dir.exists() else ():
        if not request_path.is_file() or request_path.name in manager._deferred_requests:
            continue
        manager._pace()
        try:
            request = read_json(request_path)
            marker = manager._resolve_request_marker(request)
            if marker is not None:
                job = manager.workspace.load_job(marker)
                if manager._backend_for(job) is None:
                    _LOGGER.info(
                        "leaving request %s to another manager: runner backend %s is not served here",
                        request_path.name,
                        job.runner_backend,
                        extra=manager._event(
                            "request_deferred", request=request_path.name, runner_backend=job.runner_backend
                        ),
                    )
                    manager._deferred_requests.add(request_path.name)
                    continue
        except (WorkflowError, OSError):
            pass
        claimed_dir = manager.workspace.control / "requests" / "claimed" / manager.manager_id
        claimed_dir.mkdir(parents=True, exist_ok=True)
        claimed_path = claimed_dir / request_path.name
        try:
            os.rename(request_path, claimed_path)
        except OSError as exc:
            _LOGGER.debug("request %s was claimed elsewhere: %s", request_path.name, exc)
            continue
        try:
            unactionable = manager._apply_request(read_json(claimed_path))
        except TransitionLostError:
            manager._retire_request(claimed_path, "the job moved to another state first")
        except (WorkflowError, OSError, ValueError) as exc:
            try:
                manager.workspace.quarantine(claimed_path, reason=f"invalid request: {exc}")
            except OSError as failure:
                manager._report_anomaly(
                    f"request:{claimed_path.name}",
                    f"cannot quarantine the invalid request {claimed_path.name}: {failure}",
                    manager._event("request_error", request=claimed_path.name),
                )
        else:
            if unactionable is not None:
                manager._retire_request(claimed_path, unactionable)
            else:
                _LOGGER.info(
                    "handled request %s",
                    claimed_path.name,
                    extra=manager._event("request_handled", request=claimed_path.name),
                )
                claimed_path.unlink(missing_ok=True)
        changed = True
    return changed
