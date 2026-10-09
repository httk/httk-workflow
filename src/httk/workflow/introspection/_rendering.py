"""Machine-readable reports and terminal renderers for job introspection."""

import json
from collections.abc import Mapping, Sequence
from typing import Any

from .._kernel import OWNED, JobRef
from .._state import _thaw
from ..models import placement_text
from ..workspace import Workspace
from ._diagnosis import BudgetStatus, _describe_child, budget_status, claim_requirements, observe_join
from ._reading import attempt_control, job_placement, read_error_breadcrumb, read_job, read_last_headline, read_state

JOB_REPORT_FORMAT = "httk-workflow-job-report"


def describe_job(workspace: Workspace, ref: JobRef, *, include_children: bool = True) -> dict[str, Any]:
    """Return the machine-readable report of one job.

    :param workspace: Workspace containing the job.
    :param ref: The job.
    :param include_children: Include per-child observations for waiting joins.
    :return: The job report mapping.
    """

    doc, state_error = read_state(ref)
    job, job_error = read_job(ref)
    placement = job_placement(ref)
    control = attempt_control(ref, doc)
    data = ref.path / "data"
    report: dict[str, Any] = {
        "format": JOB_REPORT_FORMAT,
        "format_version": 3,
        "workspace": str(workspace.root),
        "workspace_id": workspace.workspace_id,
        "job_id": ref.job_id,
        "job_key": ref.job_key,
        "state": ref.state,
        "placement": None if placement is None else placement_text(placement),
        "priority": ref.priority,
        "token": ref.token,
        "owner_id": ref.owner_id,
        "claimed_from": ref.from_state,
        "phase": None if doc is None else _thaw(doc.phase),
        "updated_at": None if doc is None else doc.updated_at,
        "step": None if doc is None or doc.activation is None else doc.activation.get("step"),
        "runner_steps": None if doc is None else _thaw(doc.runner_steps),
        "activation": None if doc is None else _thaw(doc.activation),
        "attempt": None if doc is None else _thaw(doc.attempt),
        "counters": None if doc is None else _thaw(doc.counters),
        "attempt_control": None if control is None else str(control),
        "failure": None if doc is None else _thaw(doc.failure),
        "seal": None if doc is None else _thaw(doc.seal),
        "workflow_pin": None if doc is None else _thaw(doc.workflow_pin),
        "paths": {
            "payload": str(ref.path),
            "workdir": str(ref.path / "run"),
            "data": str(data) if data.is_dir() else None,
        },
        "last_headline": read_last_headline(ref.path),
        "state_error": state_error,
        "job_error": job_error,
    }
    if job is not None:
        report.update(
            {
                "name": job.name,
                "workflow": {"id": job.workflow_id, "name": job.workflow_name},
                "tag": job.tag,
                "job_digest": job.digest,
                "initial_step": job.initial_step,
                "claim": claim_requirements(job).as_mapping(),
                "budgets": budget_status(job, doc).as_mapping(),
                "retry_on": sorted(job.retry_policy.retry_on),
            }
        )
    if ref.state == "waiting":
        join = None if doc is None else doc.join
        report["join"] = {
            "condition": None if join is None else join.get("condition"),
            "count": None if join is None else join.get("count"),
            "next_step": None if join is None else join.get("next_step"),
            "children": observe_join(workspace, join) if include_children and join is not None else [],
        }
    if ref.state in {"failed", "cancelled", "paused"}:
        report["error_breadcrumb"] = read_error_breadcrumb(control)
    return report


def _pair(name: str, value: object) -> str:
    return f"{name:<22s} {'-' if value is None or value == '' else value}"


def render_job(report: Mapping[str, Any]) -> str:
    """Render the report of :func:`describe_job` for a terminal.

    :param report: The report.
    :return: The text.
    """

    workflow = report.get("workflow")
    lines = [
        f"job {report['job_key']} ({report['state']})",
        _pair("job id", report["job_id"]),
        _pair("name", report.get("name")),
        _pair("workflow", f"{workflow['name']} ({workflow['id']})" if isinstance(workflow, Mapping) else None),
        _pair("placement", report["placement"]),
        _pair("priority", report["priority"]),
        _pair("token", report.get("token")),
        _pair("updated at", report.get("updated_at")),
        _pair("job digest", report.get("job_digest")),
    ]
    if report.get("state") == OWNED:
        lines.append(_pair("owner", f"{report.get('owner_id')} (claimed from {report.get('claimed_from')})"))
    phase = report.get("phase")
    if isinstance(phase, Mapping):
        lines.append(_pair("phase", phase.get("kind")))
    claim = report.get("claim")
    if isinstance(claim, Mapping):
        capabilities = claim.get("required_capabilities")
        joined = ",".join(str(item) for item in capabilities) if isinstance(capabilities, Sequence) else ""
        lines.append(_pair("claim pool", claim.get("claim_pool")))
        lines.append(_pair("capabilities", joined or "-"))
    lines.append(_pair("step", f"{report.get('step') or '-'} (initial {report.get('initial_step') or '-'})"))
    steps = report.get("runner_steps")
    if isinstance(steps, Sequence) and not isinstance(steps, (str, bytes)):
        lines.append(_pair("runner steps", ",".join(str(item) for item in steps)))
    activation, attempt, counters = report.get("activation"), report.get("attempt"), report.get("counters")
    if isinstance(activation, Mapping):
        lines.append(
            _pair("activation", f"{activation.get('ordinal')} ({activation.get('reason')}, {activation.get('id')})")
        )
    if isinstance(attempt, Mapping):
        total = counters.get("attempts_total") if isinstance(counters, Mapping) else None
        lines.append(
            _pair("attempt", f"{attempt.get('ordinal')} of activation, {total or '-'} total ({attempt.get('id')})")
        )
    budgets = report.get("budgets")
    if isinstance(budgets, Mapping):
        status = BudgetStatus(
            attempts_this_activation=int(budgets["attempts_this_activation"]),
            maximum_attempts_per_activation=budgets["maximum_attempts_per_activation"],
            total_attempts=int(budgets["total_attempts"]),
            maximum_total_attempts=budgets["maximum_total_attempts"],
            activations=int(budgets["activations"]),
            maximum_activations=budgets["maximum_activations"],
        )
        for index, text in enumerate(status.describe()):
            lines.append(_pair("budgets" if index == 0 else "", text))
    paths = report.get("paths")
    if isinstance(paths, Mapping):
        lines.append(_pair("payload", paths.get("payload")))
        lines.append(_pair("workdir", paths.get("workdir")))
        if paths.get("data"):
            lines.append(_pair("data", paths.get("data")))
    if report.get("attempt_control"):
        lines.append(_pair("attempt control", report["attempt_control"]))
    failure = report.get("failure")
    if isinstance(failure, Mapping):
        lines.append(_pair("failure", f"{failure.get('code')}: {failure.get('message')}"))
        details = failure.get("details")
        if isinstance(details, Mapping) and details:
            lines.append(_pair("failure details", json.dumps(details, sort_keys=True)))
        if failure.get("retryable"):
            lines.append(_pair("", "the runner declared this failure retryable"))
    join = report.get("join")
    if isinstance(join, Mapping):
        lines.append(_pair("join", f"{join.get('condition') or '-'} then step {join.get('next_step') or '-'}"))
        for child in join.get("children") or ():
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
    elif isinstance(failure, Mapping) and failure.get("code") == "process_failure":
        # A process that died without publishing an outcome usually left no error.json; say so.
        lines.append(_pair("error breadcrumb", "the last attempt left no error.json breadcrumb"))
    headline = report.get("last_headline")
    if isinstance(headline, str) and headline:
        lines.append(_pair("last headline", headline))
    for name in ("state_error", "job_error"):
        if report.get(name):
            lines.append(_pair(name.replace("_", " "), report[name]))
    return "\n".join(lines)


def render_events(events: Sequence[Mapping[str, Any]]) -> str:
    """Render the owner run-log events of :func:`~httk.workflow.introspection.job_events`, one line each.

    :param events: The events, oldest first.
    :return: The text.
    """

    lines: list[str] = []
    for event in events:
        if event.get("error") is not None:
            lines.append(f"{'?':32s} {event['error']}")
            continue
        parts = [f"{event.get('at') or '-'!s:32s}", f"{event.get('event') or '?'!s:<16s}"]
        if event.get("from") is not None or event.get("to") is not None:
            parts.append(f"{event.get('from') or '-'}->{event.get('to') or '-'}")
        for name in ("step", "attempt_id"):
            if event.get(name) is not None:
                parts.append(f"{name.removesuffix('_id')}={event[name]}")
        if event.get("detail") is not None:
            detail = event["detail"]
            parts.append(f"detail={detail if isinstance(detail, str) else json.dumps(detail, sort_keys=True)}")
        lines.append(" ".join(parts))
    if not lines:
        lines.append("this job has no owner history: no owner has claimed it yet")
    return "\n".join(lines)


def render_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    """Render the rows of :func:`~httk.workflow.introspection.list_jobs` as a plain table.

    :param rows: The rows.
    :return: The text.
    """

    if not rows:
        return "no job matches this selection"
    width = max(len(str(row["job_key"])) for row in rows)
    lines = [f"{'JOB':{width}s} {'STATE':<10s} {'STEP':<16s} {'PRI':>3s} PLACEMENT"]
    for row in rows:
        lines.append(
            f"{row['job_key']!s:{width}s} {row['state']!s:<10s} "
            f"{row.get('step') or '-'!s:<16s} {int(row['priority']):>3d} {row.get('placement') or '-'}"
        )
    return "\n".join(lines)
