"""Machine-readable reports and terminal renderers for job introspection."""

import json
from collections.abc import Mapping, Sequence
from typing import Any

from ..models import Marker
from ..workspace import Workspace
from ._diagnosis import (
    BudgetStatus,
    _describe_child,
    budget_status,
    claim_requirements,
    observe_join,
)
from ._reading import (
    _attempt_control,
    _job_of,
    _optional_int,
    _optional_string,
    _state_of,
    _workdir_relative,
    read_error_breadcrumb,
)

JOB_REPORT_FORMAT = "httk-workflow-job-report"


def describe_job(workspace: Workspace, marker: Marker) -> dict[str, Any]:
    """Return the complete machine-readable report of one job."""

    state, state_error = _state_of(workspace, marker)
    job, job_error = _job_of(workspace, marker)
    payload = workspace.payload_path(marker.placement, marker.job_key)
    workdir = _workdir_relative(job, state)
    control = _attempt_control(workspace, marker, state)
    report: dict[str, Any] = {
        "format": JOB_REPORT_FORMAT,
        "format_version": 1,
        "workspace": str(workspace.root),
        "workspace_id": workspace.workspace_id,
        "core_profile": workspace.core_profile,
        "job_id": marker.job_id,
        "job_key": marker.job_key,
        "state": marker.kind,
        "placement": marker.placement.as_posix(),
        "priority": marker.priority,
        "generation": marker.generation,
        "record_ref": marker.record_ref,
        "marker": str(marker.path),
        "reason": state.get("reason"),
        "created_at": state.get("created_at"),
        "step": state.get("step"),
        "runner_steps": state.get("runner_steps"),
        "activation": {
            "id": state.get("activation_id"),
            "ordinal": state.get("activation_ordinal"),
        },
        "attempt": {
            "id": state.get("attempt_id"),
            "ordinal": state.get("attempt_ordinal"),
            "total": state.get("total_attempts"),
            "control": None if control is None else str(control),
        },
        "failure": state.get("failure"),
        "pause": state.get("pause"),
        "data_generation": state.get("data_generation"),
        "paths": {
            "payload": str(payload),
            "workdir": None if workdir is None else str(payload.joinpath(*workdir.parts)),
            "data": str(payload / "data") if job is not None and job.data_mode == "transactional" else None,
        },
        "state_error": state_error,
        "job_error": job_error,
    }
    if job is not None:
        report.update(
            {
                "name": job.name,
                "workflow": job.workflow,
                "tag": job.tag,
                "job_digest": job.digest,
                "initial_step": job.initial_step,
                "runner": {
                    "executor": job.runner_executor,
                    "source": job.runner_source,
                    "path": job.runner_path.as_posix(),
                    "sha256": job.runner_sha256,
                    "arguments": list(job.runner_arguments),
                },
                "claim": claim_requirements(job).as_mapping(),
                "budgets": budget_status(job, state).as_mapping(),
                "retry_on": sorted(job.retry_policy.retry_on),
                "workdir_mode": job.workdir_mode,
                "data_mode": job.data_mode,
            }
        )
    report["registered_job_digest"] = _optional_string(state.get("job_digest"))
    if marker.kind in {"claimed", "running", "committing"}:
        report["owner"] = {
            "manager_id": state.get("manager_id"),
            "writer_id": state.get("writer_id"),
            "lease_seconds": state.get("lease_seconds"),
            "started_at": state.get("started_at"),
        }
    if marker.kind == "waiting":
        join = state.get("join")
        report["join"] = {
            "condition": join.get("condition") if isinstance(join, Mapping) else None,
            "count": join.get("count") if isinstance(join, Mapping) else None,
            "next_step": state.get("next_step"),
            "children": observe_join(workspace, join) if isinstance(join, Mapping) else [],
        }
    if marker.kind in {"failed", "cancelled", "paused"}:
        report["error_breadcrumb"] = read_error_breadcrumb(control)
    return report


def _pair(name: str, value: object) -> str:
    return f"{name:<22s} {'-' if value is None or value == '' else value}"


def render_job(report: Mapping[str, Any]) -> str:
    """Render the report of :func:`describe_job` for a terminal."""

    lines = [
        f"job {report['job_key']} ({report['state']})",
        _pair("job id", report["job_id"]),
        _pair("name", report.get("name")),
        _pair("workflow", report.get("workflow")),
        _pair("placement", report["placement"]),
        _pair("priority", report["priority"]),
        _pair("generation", report["generation"]),
        _pair("reason", report.get("reason")),
        _pair("updated at", report.get("created_at")),
        _pair("job digest", report.get("job_digest")),
    ]
    runner = report.get("runner")
    if isinstance(runner, Mapping):
        lines.append(
            _pair(
                "runner",
                f"{runner['source']}:{runner['path']} (executor {runner['executor']})",
            )
        )
        lines.append(_pair("runner sha256", runner.get("sha256") or "pinned by the job digest"))
        arguments = runner.get("arguments")
        if isinstance(arguments, Sequence) and arguments:
            lines.append(_pair("runner arguments", " ".join(str(item) for item in arguments)))
    claim = report.get("claim")
    if isinstance(claim, Mapping):
        capabilities = claim.get("required_capabilities")
        joined = ",".join(str(item) for item in capabilities) if isinstance(capabilities, Sequence) else ""
        lines.append(_pair("claim pool", claim.get("claim_pool")))
        lines.append(_pair("capabilities", joined or "-"))
    lines.append(
        _pair(
            "step",
            f"{report.get('step') or '-'} (initial {report.get('initial_step') or '-'})",
        )
    )
    steps = report.get("runner_steps")
    if isinstance(steps, Sequence) and not isinstance(steps, (str, bytes)):
        lines.append(_pair("runner steps", ",".join(str(item) for item in steps)))
    activation = report.get("activation")
    attempt = report.get("attempt")
    if isinstance(activation, Mapping):
        lines.append(
            _pair(
                "activation",
                f"{activation.get('ordinal') or '-'} ({activation.get('id') or '-'})",
            )
        )
    if isinstance(attempt, Mapping):
        lines.append(
            _pair(
                "attempt",
                f"{attempt.get('ordinal') or '-'} of activation, {attempt.get('total') or '-'} total ({attempt.get('id') or '-'})",
            )
        )
    budgets = report.get("budgets")
    if isinstance(budgets, Mapping):
        status = BudgetStatus(
            attempts_this_activation=int(budgets["attempts_this_activation"]),
            maximum_attempts_per_activation=_optional_int(budgets["maximum_attempts_per_activation"]),
            total_attempts=int(budgets["total_attempts"]),
            maximum_total_attempts=_optional_int(budgets["maximum_total_attempts"]),
            activations=int(budgets["activations"]),
            maximum_activations=_optional_int(budgets["maximum_activations"]),
        )
        for index, text in enumerate(status.describe()):
            lines.append(_pair("budgets" if index == 0 else "", text))
    owner = report.get("owner")
    if isinstance(owner, Mapping):
        lines.append(_pair("owning manager", owner.get("manager_id")))
        lines.append(_pair("lease seconds", owner.get("lease_seconds")))
        lines.append(_pair("started at", owner.get("started_at")))
    paths = report.get("paths")
    if isinstance(paths, Mapping):
        lines.append(_pair("payload", paths.get("payload")))
        lines.append(_pair("workdir", paths.get("workdir")))
        if paths.get("data"):
            lines.append(_pair("data", paths.get("data")))
    if isinstance(attempt, Mapping) and attempt.get("control"):
        lines.append(_pair("attempt control", attempt.get("control")))
    failure = report.get("failure")
    if isinstance(failure, Mapping):
        lines.append(_pair("failure", f"{failure.get('code')}: {failure.get('message')}"))
        details = failure.get("details")
        if isinstance(details, Mapping) and details:
            lines.append(_pair("failure details", json.dumps(details, sort_keys=True)))
        if failure.get("retryable"):
            lines.append(_pair("", "the runner declared this failure retryable"))
    pause = report.get("pause")
    if pause:
        lines.append(_pair("pause", json.dumps(pause, sort_keys=True)))
    join = report.get("join")
    if isinstance(join, Mapping):
        lines.append(
            _pair(
                "join",
                f"{join.get('condition') or '-'} then step {join.get('next_step') or '-'}",
            )
        )
        children = join.get("children")
        if isinstance(children, Sequence):
            for child in children:
                if isinstance(child, Mapping):
                    lines.append(_pair("", _describe_child(child)))
    breadcrumb = report.get("error_breadcrumb")
    if isinstance(breadcrumb, Mapping):
        lines.append(
            _pair(
                "error breadcrumb",
                f"{breadcrumb.get('exception')}: {breadcrumb.get('message')} in step {breadcrumb.get('step')}",
            )
        )
    for name in ("state_error", "job_error"):
        if report.get(name):
            lines.append(_pair(name.replace("_", " "), report[name]))
    return "\n".join(lines)


def render_frames(frames: Sequence[Mapping[str, Any]]) -> str:
    """Render the frames of :func:`job_frames` one line each, oldest first."""

    lines: list[str] = []
    previous_kind = "submitted"
    for frame in frames:
        error = frame.get("error")
        if error is not None:
            lines.append(f"{'?':32s} {'-':<6s} {frame.get('record_ref')}: {error}")
            continue
        kind = str(frame.get("kind", "?"))
        created = str(frame.get("created_at") or "-")
        generation = frame.get("state_generation")
        transition = f"{previous_kind}->{kind}"
        previous_kind = kind
        parts = [f"{created:32s}", f"g{generation:<5}", f"{transition:<24s}"]
        parts.append(f"step={frame.get('step') or '-'}")
        parts.append(f"attempt={frame.get('attempt_ordinal') if frame.get('attempt_ordinal') is not None else '-'}")
        parts.append(f"reason={frame.get('reason') or '-'}")
        if isinstance(frame.get("outcome_action"), str):
            parts.append(f"action={frame['outcome_action']}")
        failure = frame.get("failure")
        if isinstance(failure, Mapping):
            parts.append(f"failure={failure.get('code')}")
        lines.append(" ".join(parts))
    if not lines:
        lines.append("this job has no recorded transition: it is still exactly as submitted")
    return "\n".join(lines)


def render_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    """Render the rows of :func:`list_jobs` as a plain table."""

    if not rows:
        return "no job matches this selection"
    width = max(len(str(row["job_key"])) for row in rows)
    lines = [f"{'JOB':{width}s} {'STATE':<11s} {'STEP':<16s} {'PRI':>3s} PLACEMENT"]
    for row in rows:
        lines.append(
            f"{row['job_key']!s:{width}s} {row['state']!s:<11s} "
            f"{row['step'] or '-'!s:<16s} {int(row['priority']):>3d} {row['placement']}"
        )
    return "\n".join(lines)
