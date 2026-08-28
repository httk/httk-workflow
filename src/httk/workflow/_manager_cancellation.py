"""Private cancellation evidence and fencing decisions."""

import signal
import time
from collections.abc import Mapping
from typing import Any

from .errors import FormatError, WorkflowError
from .models import StateFrame


def evidence(
    manager: Any,
    marker: Any,
    state: Any,
    *,
    read_json: Any,
    utc_now: Any,
) -> dict[str, object] | None:
    """Assess whether the fenced attempt is proven to have stopped."""

    attempt_id = state.attempt_id or ""
    local = manager._running.get(attempt_id)
    if local is not None:
        exit_status = local.process.poll()
        if exit_status is None:
            return None
        return {
            "verified": "process_exited",
            "hostname": manager.hostname,
            "pid": local.process.pid,
            "exit_status": exit_status,
            "verified_at": utc_now(),
        }
    try:
        process = read_json(manager._attempt_control_path(marker, state) / "process.json")
    except (FormatError, WorkflowError):
        return {"verified": "no_launched_process", "verified_at": utc_now()}
    if process.get("hostname") != manager.hostname:
        return None
    try:
        group = int(process["process_group"])
    except (KeyError, TypeError, ValueError):
        return {"verified": "no_launched_process", "verified_at": utc_now()}
    if manager._process_group_alive(group):
        return None
    return {
        "verified": "process_group_absent",
        "hostname": manager.hostname,
        "process_group": group,
        "verified_at": utc_now(),
    }


def should_kill(now: float, kill_at: float | None) -> bool:
    return kill_at is not None and now >= kill_at


def fence_members(state: Any, members: tuple[str, ...], request: Mapping[str, Any], operator_key: str | None) -> Any:
    audited = state.select(members)
    updates = {
        "operator": request.get("operator"),
        "operator_reason": request.get("reason"),
        "request_id": request.get("request_id"),
    }
    result = type(state).replace(audited, **updates)
    if operator_key is not None:
        result = type(state).replace(result, operator_key=operator_key)
    return result


def finish(manager: Any, marker: Any, state: Any, members: tuple[str, ...], *, utc_now: Any, logger: Any) -> bool:
    attempt_id = state.attempt_id or ""
    now = time.monotonic()
    kill_at = manager._cancel_kill_at.get(attempt_id)
    if kill_at is None:
        manager._cancel_kill_at[attempt_id] = now + manager.cancel_grace_seconds
        manager._terminate_attempt(marker, state)
    elif should_kill(now, kill_at):
        logger.warning(
            "cancelled attempt %s of %s did not exit within %.0fs; killing its process group",
            attempt_id or "-",
            marker.job_key,
            manager.cancel_grace_seconds,
            extra=manager._event("cancel_kill", marker, attempt_id=attempt_id),
        )
        manager._terminate_attempt(marker, state, signal.SIGKILL)
    proof = manager._cancellation_evidence(marker, state)
    if proof is None:
        manager._report_unverifiable_cancellation(marker, state)
        return False
    local = manager._running.pop(attempt_id, None)
    if local is not None:
        return_code = local.process.poll()
        if return_code is None:
            return_code = -signal.SIGKILL
        manager._write_attempt_end(local, return_code)
    manager._cancel_kill_at.pop(attempt_id, None)
    manager._cancel_unverified.discard(attempt_id)
    manager._transition(
        marker, "cancelled", StateFrame.replace(state.select(members), cancellation=proof, reason="operator_cancel")
    )
    logger.warning(
        "cancelled %s: attempt %s is %s",
        marker.job_key,
        attempt_id or "-",
        proof.get("verified"),
        extra=manager._event("cancel_completed", marker, attempt_id=attempt_id, **proof),
    )
    return True


def request_cancel(
    manager: Any,
    marker: Any,
    state: Any,
    request: Mapping[str, Any],
    operator_key: str | None,
    members: tuple[str, ...],
    *,
    utc_now: Any,
    logger: Any,
) -> str | None:
    audited = StateFrame.replace(
        state.select(members),
        operator=request.get("operator"),
        operator_reason=request.get("reason"),
        request_id=request.get("request_id"),
    )
    if operator_key is not None:
        audited = StateFrame.replace(audited, operator_key=operator_key)
    if marker.kind == "cancelling":
        return "the job is already being cancelled"
    if marker.kind == "running":
        fenced = manager._transition(marker, "cancelling", StateFrame.replace(audited, reason="operator_cancel"))
        logger.warning(
            "cancelling %s: attempt %s is fenced and will be stopped",
            fenced.job_key,
            state.attempt_id or "-",
            extra=manager._event("cancel_fenced", fenced, attempt_id=state.attempt_id),
        )
        local = manager._running.get(state.attempt_id or "")
        if local is not None:
            local.cancelling = True
            local.fenced = True
        return None
    manager._terminate_attempt(marker, state)
    manager._transition(
        marker,
        "cancelled",
        StateFrame.replace(
            audited,
            cancellation={"verified": "no_live_attempt", "from_kind": marker.kind, "verified_at": utc_now()},
            reason="operator_cancel",
        ),
    )
    return None


def report_unverifiable(
    manager: Any, marker: Any, state: Any, members: tuple[str, ...], *, utc_now: Any, logger: Any
) -> None:
    attempt_id = state.attempt_id or ""
    if attempt_id in manager._cancel_unverified:
        logger.debug("cancellation of %s is still unverifiable here", marker.job_key)
        return
    manager._cancel_unverified.add(attempt_id)
    detail = "its process is recorded on another host, so this manager cannot prove that it stopped"
    logger.warning(
        "cancellation of %s is not finished: %s",
        marker.job_key,
        detail,
        extra=manager._event("cancel_unverified", marker, attempt_id=attempt_id),
    )
    try:
        manager._transition(
            marker,
            "cancelling",
            StateFrame.replace(
                state.select(members),
                cancellation={
                    "verified": None,
                    "problem": "unverifiable_here",
                    "detail": detail,
                    "observed_by": manager.manager_id,
                    "observed_at": utc_now(),
                },
                reason="cancel_unverified",
            ),
        )
    except Exception as exc:
        from .errors import TransitionLostError

        if isinstance(exc, TransitionLostError):
            logger.debug("cancellation note for %s was lost to another actor", marker.job_key)
        else:
            raise


def process(manager: Any, logger: Any) -> bool:
    from .errors import TransitionLostError, WorkflowError

    changed = False
    for marker in manager._window("process_cancelling", "cancelling"):
        manager._pace()
        loaded = manager._load_job_and_state(marker, "process_cancelling")
        if loaded is None:
            continue
        job, state = loaded
        if manager._executor_for(job) is None:
            logger.debug(
                "skipping cancelling job %s: runner executor %s is not served here", marker.job_key, job.runner_executor
            )
            continue
        try:
            changed |= manager._finish_cancellation(marker, state)
        except TransitionLostError:
            logger.debug("cancellation of %s was completed by another actor", marker.job_key)
        except (WorkflowError, OSError) as exc:
            manager._report_anomaly(
                f"cancelling:{marker.job_key}",
                f"cannot complete the cancellation of {marker.job_key}: {exc}",
                manager._event("cancel_error", marker),
            )
    return changed
