"""Private join observation and condition decisions."""

import logging
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any

from ._util import require_mapping
from .errors import FormatError, WorkflowError
from .models import (
    TERMINAL_KINDS,
    StateFrame,
    canonical_uuid,
    normalize_placement,
    validate_failure,
    validate_label,
    validate_step,
)

_LOGGER = logging.getLogger("httk.workflow.manager")


def children(join: Mapping[str, Any]) -> Sequence[object]:
    values = join.get("children")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or not values:
        raise FormatError("state.join.children must be a nonempty array")
    return values


def satisfied(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> bool:
    if condition == "all_succeeded":
        return all(kind == "succeeded" for kind in kinds)
    if condition == "all_terminal":
        return all(kind in TERMINAL_KINDS for kind in kinds)
    if condition == "any_succeeded":
        return any(kind == "succeeded" for kind in kinds)
    if condition == "at_least":
        return sum(kind == "succeeded" for kind in kinds) >= int(join.get("count", 0))
    raise FormatError(f"unknown join condition: {condition!r}")


def impossible(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> bool:
    if condition == "all_succeeded":
        return any(kind in {"failed", "cancelled"} for kind in kinds)
    if condition == "all_terminal":
        return False
    if condition == "any_succeeded":
        return all(kind in TERMINAL_KINDS for kind in kinds) and not any(kind == "succeeded" for kind in kinds)
    if condition == "at_least":
        count = int(join.get("count", 0))
        successes = sum(kind == "succeeded" for kind in kinds)
        nonterminal = sum(kind not in TERMINAL_KINDS for kind in kinds)
        return successes + nonterminal < count
    raise FormatError(f"unknown join condition: {condition!r}")


def classify(condition: str, join: Mapping[str, Any], kinds: Sequence[str]) -> str:
    if satisfied(condition, join, kinds):
        return "satisfied"
    if impossible(condition, join, kinds):
        return "impossible"
    return "blocked"


def observe_children(manager: Any, children: Sequence[object]) -> tuple[list[dict[str, object]], str | None]:
    observations: list[dict[str, object]] = []
    for child_ref in children:
        reference = require_mapping(child_ref, "join child")
        child_id = canonical_uuid(reference.get("job_id"), "join child job_id")
        label_raw = reference.get("label")
        label = None if label_raw is None else validate_label(label_raw, "join child label")
        placement_hint = reference.get("placement_hint")
        job_key = reference.get("job_key")
        if not isinstance(placement_hint, str) or not isinstance(job_key, str):
            raise FormatError("join child reference must carry a job_key and placement_hint")
        child_marker = manager.workspace.find_marker_at(job_key, normalize_placement(placement_hint))
        if child_marker is not None and child_marker.job_id != child_id:
            raise FormatError("join child identity disagrees with placement hint")
        if child_marker is None:
            return observations, child_id
        observations.append(
            {
                "workspace_id": manager.workspace.workspace_id,
                "job_id": child_id,
                "job_key": child_marker.job_key,
                "label": label,
                "placement": child_marker.placement.as_posix(),
                "kind": child_marker.kind,
                "state_generation": child_marker.generation,
                "record_ref": child_marker.record_ref,
                "payload_path": (child_marker.placement / child_marker.job_key).as_posix(),
                **child_evidence(manager, child_marker),
            }
        )
    return observations, None


def child_evidence(manager: Any, marker: Any) -> dict[str, object]:
    payload = marker.placement / marker.job_key
    try:
        state = manager._read_frame(marker)
    except (WorkflowError, OSError) as exc:
        _LOGGER.debug("cannot read the state frame of join child %s: %s", marker.job_key, exc)
        state = None
    if state is None:
        state = StateFrame()
    failure: object = None
    if marker.kind in {"failed", "cancelled"}:
        raw = state.failure
        if raw is not None:
            try:
                failure = validate_failure(raw).as_mapping()
            except FormatError:
                failure = dict(raw)
    return {
        "failure": failure,
        "workdir_path": child_workdir_path(manager, marker, state, payload),
        "data_generation": state.data_generation,
    }


def child_workdir_path(manager: Any, marker: Any, state: Any, payload: Any) -> str | None:
    recorded = state.workdir
    try:
        job = manager.workspace.load_job(marker)
    except (WorkflowError, OSError):
        return (payload / PurePosixPath(recorded)).as_posix() if recorded else None
    if job.workdir_mode == "persistent":
        return (payload / job.workdir_path).as_posix()
    attempt_id = state.attempt_id
    if attempt_id is not None:
        base = job.workdir_path
        return (payload / base.parent / f"{base.name}.{attempt_id}").as_posix()
    return (payload / PurePosixPath(recorded)).as_posix() if recorded else None


def evaluate(manager: Any, marker: Any, parent_job: Any, state: Any) -> bool:
    join = require_mapping(state.join, "state.join")
    observations, unresolved = manager._observe_join_children(children(join))
    if unresolved is not None:
        if not manager._join_child_grace_expired(marker, unresolved):
            _LOGGER.debug("join child %s of %s is not yet visible", unresolved, marker.job_key)
            return False
        manager._fail_waiting(
            marker,
            state,
            "dependency_failure",
            f"join child {unresolved} cannot be resolved in this workspace",
            "join_unresolvable",
        )
        return True
    manager._join_unresolved.pop(marker.job_key, None)
    condition = str(join.get("condition", ""))
    kinds = [str(item["kind"]) for item in observations]
    result = classify(condition, join, kinds)
    if result == "blocked":
        _LOGGER.debug("join %s of %s is still pending: children are %s", condition, marker.job_key, kinds)
        return False
    if result == "satisfied":
        manager._advance(
            marker,
            parent_job,
            state,
            validate_step(state.next_step, "next_step"),
            state.carried(),
            reason="join_satisfied",
            join_summary=observations,
        )
        return True
    on_impossible = join.get("on_impossible")
    if isinstance(on_impossible, Mapping) and on_impossible.get("action") == "advance":
        manager._advance(
            marker,
            parent_job,
            state,
            validate_step(on_impossible.get("next_step"), "on_impossible.next_step"),
            state.carried(),
            reason="join_impossible",
            join_summary=observations,
        )
        return True
    manager._transition(
        marker,
        "failed",
        StateFrame.of(
            state.carried(),
            failure=manager._failure("dependency_failure", "join condition became impossible"),
            join_summary=observations,
            reason="join_impossible",
        ),
    )
    return True
