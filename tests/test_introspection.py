"""Job introspection commands and the foreground debug runner."""

import dataclasses
import json
import os
import socket
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow._logging import reset_logging
from httk.workflow._util import utc_now
from httk.workflow.introspection import (
    DEBUG_EXIT_FAILED,
    DEBUG_EXIT_SUCCEEDED,
    DEBUG_EXIT_UNFINISHED,
    debug_job,
    describe_job,
    explain_job,
    job_frames,
    list_jobs,
    render_frames,
    resolve_job,
)
from httk.workflow.manifests import MAINTENANCE_LOCK_FILE
from httk.workflow.workflow_cli import command

_THREE_STEP_RUNNER = """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
step = context["step"]
print("runner is working on " + step)
sys.stderr.write("diagnostic from " + step + "\\n")
(run / "steps.txt").open("a").write(step + "\\n")
following = {"prepare": "relax", "relax": "collect"}
if step in following:
    body = {"action": "advance", "next_step": following[step]}
else:
    body = {"action": "succeed"}
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "runner_steps": ["prepare", "relax", "collect"],
    **body,
}
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""

_BREADCRUMB_RUNNER = """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
(control / "error.json").write_text(json.dumps({
    "format": "httk-workflow-runner-error",
    "format_version": 1,
    "step": os.environ["HTTK_WORKFLOW_STEP"],
    "exception": "RuntimeError",
    "message": "the inputs are missing",
    "traceback": "Traceback (most recent call last): RuntimeError",
}))
sys.exit(7)
"""

_PAUSING_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "pause",
    "pause": {"reason": "operator inspection"},
}))
os.rename(temporary, control / "outcome.ready")
"""

_CHILD_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
print("the child is working on " + context["step"])
temporary = control / "outcome.tmp.child"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""

_PARENT_RUNNER = '''#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

CHILD_SOURCE = """@CHILD@"""
CHILD_EXECUTOR = "@EXECUTOR@"
context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {
    "format": "httk-workflow-outcome",
    "format_version": 1,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}
if context["step"] == "gather":
    print("the parent gathered " + str(len(context["children"])) + " child(ren)")
    outcome = {**base, "action": "succeed"}
else:
    child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), "child"))
    child_key = "child--" + child_id
    child_dir = temporary / "children" / "jobs" / child_key
    (child_dir / "files").mkdir(parents=True)
    runner = child_dir / "files" / "runner"
    runner.write_text(CHILD_SOURCE)
    runner.chmod(0o755)
    (child_dir / "job.json").write_text(json.dumps({
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": child_id,
        "tag": "child",
        "name": "Child job",
        "workflow": "tests.child",
        "runner": {"executor": CHILD_EXECUTOR, "path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "run",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": {
            "workspace_id": context["workspace_id"],
            "job_id": context["job_id"],
            "activation_id": context["activation_id"],
        },
    }))
    (temporary / "children" / "spawn.json").write_text(json.dumps({
        "children": [{
            "workspace_id": context["workspace_id"],
            "job_id": child_id,
            "job_key": child_key,
            "label": "only",
            "placement": "project/children",
        }],
    }))
    outcome = {
        **base,
        "action": "wait",
        "next_step": "gather",
        "join": {
            "children": [{
                "workspace_id": context["workspace_id"],
                "job_id": child_id,
                "job_key": child_key,
                "placement_hint": "project/children",
            }],
            "condition": "all_succeeded",
        },
    }
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
'''


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep the console handler ``job debug`` installs out of other tests."""

    reset_logging()
    yield
    reset_logging()


def _parent_runner(*, child_executor: str = "path") -> str:
    return _PARENT_RUNNER.replace("@CHILD@", _CHILD_RUNNER).replace("@EXECUTOR@", child_executor)


def _payload(
    root: Path,
    runner_source: str,
    *,
    initial_step: str = "prepare",
    pool: str = "default",
    capabilities: list[str] | None = None,
    attempts_per_activation: int = 3,
    tag: str = "example",
) -> tuple[Path, str]:
    """Write one complete payload directory and return it with its job id."""

    job_id = str(uuid.uuid4())
    payload = root / "payload"
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 1,
        "id": job_id,
        "tag": tag,
        "name": "Introspection example",
        "workflow": "tests.introspection",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": pool, "required_capabilities": capabilities or []},
        "retry_policy": {
            "maximum_attempts_per_activation": attempts_per_activation,
            "maximum_total_attempts": 10,
            "maximum_activations": 5,
            "retry_on": [],
        },
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _workspace(tmp_path: Path) -> Workspace:
    return Workspace.initialize(tmp_path / "workspace")


def _run(workspace: Workspace, *, pools: tuple[str, ...] = ("default",)) -> None:
    with TaskManager(workspace, pools=pools, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)


def _register(workspace: Workspace, *, pools: tuple[str, ...] = ("default",)) -> None:
    """Register every submission without waiting for anything to be claimable.

    A ready job whose pool no manager serves is never idle work for
    :meth:`TaskManager.run_until_idle`, which deliberately ignores routing, so
    these cases drive the scheduling pass explicitly instead.
    """

    with TaskManager(workspace, pools=pools, heartbeat_interval=0.01) as manager:
        manager.tick()
        manager.tick()


def _hold_maintenance_lock(workspace: Workspace) -> None:
    """Write a live maintenance lock held by this very process."""

    (workspace.control / MAINTENANCE_LOCK_FILE).write_text(
        json.dumps({"created": utc_now(), "hostname": socket.gethostname(), "pid": os.getpid()}),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# show, log, and list
# ---------------------------------------------------------------------------


def test_show_reports_the_authoritative_state_of_a_finished_job(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/show")
    _run(workspace)

    marker = resolve_job(workspace, job_id[:8])
    report = describe_job(workspace, marker)
    assert report["state"] == "succeeded"
    assert report["job_key"] == f"example--{job_id}"
    assert report["step"] == "collect"
    assert report["initial_step"] == "prepare"
    assert report["runner_steps"] == ["prepare", "relax", "collect"]
    assert report["runner"] == {
        "executor": "path",
        "source": "payload",
        "path": "files/runner",
        "sha256": None,
        "arguments": [],
    }
    assert report["claim"] == {"claim_pool": "default", "required_capabilities": [], "runner_executor": "path"}
    # Only the registration frame carries the digest the manager validated.
    assert report["registered_job_digest"] is None
    assert len(str(report["job_digest"])) == 64
    assert report["budgets"]["activations"] == 3
    assert report["budgets"]["total_attempts"] == 3
    assert report["budgets"]["maximum_attempts_per_activation"] == 3
    assert Path(str(report["paths"]["workdir"])).is_dir()
    assert report["paths"]["data"] is None
    assert report["state_error"] is None and report["job_error"] is None


def test_show_prints_a_human_report_and_a_single_json_object(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/human")
    _run(workspace)
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)

    assert command(["job", "show", ws, job_id], context) == 0
    text = capsys.readouterr().out
    assert f"job example--{job_id} (succeeded)" in text
    assert "runner                 payload:files/runner (executor path)" in text
    assert "attempts this activation 1/3" in text

    assert command(["job", "show", ws, job_id, "--json"], context) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["format"] == "httk-workflow-job-report"
    assert report["job_id"] == job_id


def test_show_refuses_an_ambiguous_or_unknown_selector(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    for index in range(2):
        payload, _ = _payload(tmp_path / f"source{index}", _THREE_STEP_RUNNER)
        workspace.submit(payload, "project/many")
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)

    assert command(["job", "show", ws, "example"], context) == 2
    assert "matches 2 jobs" in capsys.readouterr().err
    assert command(["job", "show", ws, "0" * 36], context) == 2
    assert "no job in" in capsys.readouterr().err


def test_log_renders_every_transition_oldest_first(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/log")
    _run(workspace)
    ws = register_ws(CLIContext("httk", tmp_path), workspace.root)

    marker = resolve_job(workspace, job_id)
    frames = job_frames(workspace, marker)
    kinds = [frame["kind"] for frame in frames]
    assert kinds[:5] == ["ready", "claimed", "running", "committing", "ready"]
    assert kinds[-1] == "succeeded"
    assert [frame["state_generation"] for frame in frames] == list(range(1, len(frames) + 1))

    rendered = render_frames(frames)
    assert "submitted->ready" in rendered
    assert "step=prepare" in rendered and "step=collect" in rendered
    assert "reason=advance" in rendered

    assert command(["job", "log", ws, job_id, "--limit", "2", "--json"], CLIContext("httk", tmp_path)) == 0
    payload_json = json.loads(capsys.readouterr().out)
    assert payload_json["format"] == "httk-workflow-job-history"
    assert [frame["kind"] for frame in payload_json["frames"]] == ["committing", "succeeded"]


def test_log_reports_an_unreadable_frame_and_keeps_what_exists(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/damaged")
    _run(workspace)

    marker = resolve_job(workspace, job_id)
    lost = dataclasses.replace(marker, record_ref="w" + "0" * 32 + marker.record_ref[33:])
    frames = job_frames(workspace, lost)
    assert len(frames) == 1
    assert "not coherently visible" in str(frames[0]["error"])
    assert "not coherently visible" in render_frames(frames)


def test_log_of_a_submitted_job_reports_that_it_never_moved(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/fresh")
    frames = job_frames(workspace, resolve_job(workspace, job_id))
    assert frames == []
    assert "no recorded transition" in render_frames(frames)


def test_list_reports_one_row_per_job_and_filters_by_kind(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    finished, finished_id = _payload(tmp_path / "finished", _THREE_STEP_RUNNER)
    workspace.submit(finished, "project/list/done")
    _run(workspace)
    fresh, fresh_id = _payload(tmp_path / "fresh", _THREE_STEP_RUNNER)
    workspace.submit(fresh, "project/list/new")

    rows = list_jobs(workspace)
    assert {str(row["state"]) for row in rows} == {"succeeded", "submitted"}
    by_id = {str(row["job_id"]): row for row in rows}
    assert by_id[finished_id]["step"] == "collect"
    assert by_id[fresh_id]["step"] is None

    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)
    assert command(["job", "list", ws, "--kind", "submitted"], context) == 0
    table = capsys.readouterr().out
    assert "JOB" in table and fresh_id in table and finished_id not in table

    assert command(["job", "list", ws, "--placement", "project/list/done", "--json"], context) == 0
    listed = json.loads(capsys.readouterr().out)
    assert [row["job_id"] for row in listed["jobs"]] == [finished_id]


# ---------------------------------------------------------------------------
# why
# ---------------------------------------------------------------------------


def test_why_reports_a_submitted_job_that_no_manager_registers(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/unregistered")

    diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    assert diagnosis.state == "submitted" and diagnosis.blocked
    assert "no manager has validated it" in diagnosis.summary
    detail = {check.name: check.detail for check in diagnosis.checks}
    assert detail["registered manager"] == "no manager has ever registered in this workspace"
    assert any("httk workflow manager run" in hint for hint in diagnosis.hints)


def test_why_names_the_claim_pool_no_live_manager_serves(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER, pool="gpu", capabilities=["cuda"])
    workspace.submit(payload, "project/unmatched")
    _register(workspace)

    marker = resolve_job(workspace, job_id)
    assert marker.kind == "ready"
    diagnosis = explain_job(workspace, marker)
    assert diagnosis.blocked
    checks = {check.name: check for check in diagnosis.checks}
    assert checks["claim pool"].detail == "this job asks for pool gpu"
    assert checks["required capabilities"].detail == "cuda"
    assert checks["eligible manager"].satisfied is False
    assert any(
        "does not serve claim pool gpu" in check.detail and "lacks capabilities cuda" in check.detail
        for check in diagnosis.checks
        if check.name == "live manager"
    )
    assert any("--pool gpu --capability cuda" in hint for hint in diagnosis.hints)

    ws = register_ws(CLIContext("httk", tmp_path), workspace.root)
    assert command(["job", "why", ws, job_id], CLIContext("httk", tmp_path)) == 0
    rendered = capsys.readouterr().out
    assert f"job example--{job_id} is ready" in rendered
    assert "no  eligible manager" in rendered


def test_why_reports_a_live_maintenance_lock(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER, pool="gpu")
    workspace.submit(payload, "project/locked")
    _register(workspace)
    _hold_maintenance_lock(workspace)

    diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    assert "maintenance lock stops every manager" in diagnosis.summary
    lock = next(check for check in diagnosis.checks if check.name == "maintenance lock")
    assert lock.satisfied is False and "launching is paused" in lock.detail

    ws = register_ws(CLIContext("httk", tmp_path), workspace.root)
    assert command(["job", "why", ws, job_id, "--json"], CLIContext("httk", tmp_path)) == 0
    reported = json.loads(capsys.readouterr().out)
    assert reported["format"] == "httk-workflow-job-diagnosis"
    assert reported["blocked"] is True


def test_why_reports_a_failed_job_with_its_breadcrumb_and_continue(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _BREADCRUMB_RUNNER)
    workspace.submit(payload, "project/failed")
    _run(workspace)

    marker = resolve_job(workspace, job_id)
    assert marker.kind == "failed"
    diagnosis = explain_job(workspace, marker)
    assert "failed with process_failure" in diagnosis.summary
    checks = {check.name: check for check in diagnosis.checks}
    assert "runner exited with status 7" in checks["failure"].detail
    assert "step prepare raised RuntimeError: the inputs are missing" == checks["error breadcrumb"].detail
    assert checks["operator continue"].satisfied is True
    assert any("continue --operator" in hint for hint in diagnosis.hints)

    report = describe_job(workspace, marker)
    assert report["error_breadcrumb"]["exception"] == "RuntimeError"
    assert report["failure"]["details"] == {"exit_status": 7}


def test_why_reports_an_exhausted_continue_budget(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _BREADCRUMB_RUNNER, attempts_per_activation=1)
    workspace.submit(payload, "project/exhausted")
    _run(workspace)

    diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    checks = {check.name: check for check in diagnosis.checks}
    assert checks["operator continue"].satisfied is False
    assert "retry_exhausted" in checks["operator continue"].detail
    assert any("override_step" in hint for hint in diagnosis.hints)


def test_why_lists_the_pending_child_of_a_waiting_parent(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(
        tmp_path / "source",
        _parent_runner(child_executor="unserved"),
        initial_step="branch",
    )
    workspace.submit(payload, "project/waiting")
    _run(workspace)

    parent = resolve_job(workspace, job_id)
    assert parent.kind == "waiting"
    diagnosis = explain_job(workspace, parent)
    assert diagnosis.blocked
    assert "waits for 1 of 1 child(ren)" in diagnosis.summary
    children = [check for check in diagnosis.checks if check.name == "join child"]
    assert len(children) == 1
    assert children[0].satisfied is False
    assert children[0].detail.startswith("only: child--")
    assert "is submitted" in children[0].detail
    condition = next(check for check in diagnosis.checks if check.name == "join condition")
    assert condition.detail == "all_succeeded then step gather"

    report = describe_job(workspace, parent)
    assert report["join"]["condition"] == "all_succeeded"
    assert report["join"]["children"][0]["label"] == "only"
    assert report["join"]["children"][0]["kind"] == "submitted"

    # The blocked child itself explains that nothing here serves its executor.
    child = next(marker for marker in workspace.scan_markers() if marker.job_key.startswith("child--"))
    child_diagnosis = explain_job(workspace, child)
    assert child_diagnosis.state == "submitted"
    assert any(
        "does not serve runner executor unserved" in check.detail
        for check in child_diagnosis.checks
        if check.name == "live manager"
    )


def test_why_reports_a_running_job_owned_by_a_live_manager(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/running")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        marker = resolve_job(workspace, job_id)
        assert marker.kind in {"claimed", "running"}
        diagnosis = explain_job(workspace, marker)
        assert not diagnosis.blocked
        assert "by a live manager" in diagnosis.summary
        owner = next(check for check in diagnosis.checks if check.name == "owning manager")
        assert owner.satisfied is True and manager.manager_id in owner.detail
        manager.run_until_idle(timeout=60.0)
    assert (
        explain_job(workspace, resolve_job(workspace, job_id)).summary == "this job succeeded; nothing is left to run"
    )


def test_why_reports_an_expired_lease_as_recoverable(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/expired")
    manager = TaskManager(workspace, heartbeat_interval=0.01, lease_seconds=0.0)
    try:
        manager.tick()
    finally:
        manager.close()
    marker = resolve_job(workspace, job_id)
    diagnosis = explain_job(workspace, marker)
    assert diagnosis.blocked
    assert "lease expired" in diagnosis.summary
    lease = next(check for check in diagnosis.checks if check.name == "lease")
    assert lease.satisfied is False and "recovers it" in lease.detail


def test_why_reports_a_paused_job(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _PAUSING_RUNNER)
    workspace.submit(payload, "project/paused")
    _run(workspace)

    diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    assert diagnosis.state == "paused" and diagnosis.blocked
    assert "only an operator request moves it" in diagnosis.summary
    pause = next(check for check in diagnosis.checks if check.name == "pause")
    assert "operator inspection" in pause.detail
    assert any("continue --operator" in hint for hint in diagnosis.hints)


# ---------------------------------------------------------------------------
# debug
# ---------------------------------------------------------------------------


def _collector() -> tuple[list[str], Callable[[str], None]]:
    """Return a console sink and the lines the debug runner writes into it."""

    lines: list[str] = []
    return lines, lines.append


def test_debug_drives_a_three_step_runner_in_the_foreground(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/debug")

    lines, write = _collector()
    outcome = debug_job(workspace, job_id, emit=write)
    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED
    assert outcome.state == "succeeded"
    text = "\n".join(lines)
    for step in ("prepare", "relax", "collect"):
        assert f"[{step}] runner is working on {step}" in text
        assert f"[{step}] (stderr) diagnostic from {step}" in text
        assert f"step={step}" in text
    assert "[debug] example--" in text and "finished as succeeded" in text
    assert text.count("committing") == 3
    marker = resolve_job(workspace, job_id)
    assert marker.kind == "succeeded"


def test_debug_submits_a_fresh_payload_at_the_step_override(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)

    lines, write = _collector()
    outcome = debug_job(workspace, str(payload), placement="scratch/debug", step="collect", emit=write)
    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED
    text = "\n".join(lines)
    assert "initial step overridden to collect" in text
    assert "[collect] runner is working on collect" in text
    assert "runner is working on prepare" not in text
    marker = resolve_job(workspace, job_id)
    assert marker.placement.as_posix() == "scratch/debug"
    steps = workspace.payload_path(marker.placement, marker.job_key) / "run" / "steps.txt"
    assert steps.read_text(encoding="utf-8").splitlines() == ["collect"]
    # The submitted payload is the only copy: the staging directory is gone.
    assert (payload / "job.json").is_file()
    assert json.loads((payload / "job.json").read_text(encoding="utf-8"))["initial_step"] == "prepare"


def test_debug_refuses_to_override_the_step_of_an_existing_job(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/history")
    with pytest.raises(ValueError, match="already has a history"):
        debug_job(workspace, job_id, step="collect", emit=lambda line: None)


def test_debug_refuses_to_run_behind_a_live_maintenance_lock(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    workspace.submit(payload, "project/fenced")
    _hold_maintenance_lock(workspace)
    with pytest.raises(ValueError, match="maintenance lock"):
        debug_job(workspace, job_id, emit=lambda line: None)


def test_debug_exit_codes_report_the_terminal_state(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    failing, failing_id = _payload(tmp_path / "failing", _BREADCRUMB_RUNNER, attempts_per_activation=1)
    workspace.submit(failing, "project/exit/failed")
    pausing, pausing_id = _payload(tmp_path / "pausing", _PAUSING_RUNNER)
    workspace.submit(pausing, "project/exit/paused")

    lines, write = _collector()
    assert debug_job(workspace, failing_id, emit=write).exit_code == DEBUG_EXIT_FAILED
    assert "failure=process_failure" in "\n".join(lines)
    assert debug_job(workspace, pausing_id, emit=lambda line: None).exit_code == DEBUG_EXIT_UNFINISHED


def test_debug_only_drives_the_selected_job(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    target, target_id = _payload(tmp_path / "target", _THREE_STEP_RUNNER)
    workspace.submit(target, "project/scoped/target")
    other, other_id = _payload(tmp_path / "other", _THREE_STEP_RUNNER)
    workspace.submit(other, "project/scoped/other")

    assert debug_job(workspace, target_id, emit=lambda line: None).exit_code == DEBUG_EXIT_SUCCEEDED
    assert resolve_job(workspace, target_id).kind == "succeeded"
    assert resolve_job(workspace, other_id).kind == "submitted"


def test_debug_follows_children_only_when_asked(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _parent_runner(), initial_step="branch")
    workspace.submit(payload, "project/family")

    lines, write = _collector()
    stopped = debug_job(workspace, job_id, emit=write)
    assert stopped.exit_code == DEBUG_EXIT_UNFINISHED and stopped.state == "waiting"
    assert "rerun with --follow-children" in "\n".join(lines)

    lines, write = _collector()
    finished = debug_job(workspace, job_id, follow_children=True, emit=write)
    assert finished.exit_code == DEBUG_EXIT_SUCCEEDED
    text = "\n".join(lines)
    assert "[debug child:only] child--" in text
    assert "[child:only run] the child is working on run" in text
    assert "[gather] the parent gathered 1 child(ren)" in text
    assert resolve_job(workspace, job_id).kind == "succeeded"


def test_debug_runs_from_the_command_line(tmp_path: Path, capsys) -> None:
    workspace = _workspace(tmp_path)
    payload, job_id = _payload(tmp_path / "source", _THREE_STEP_RUNNER)
    context = CLIContext("httk", tmp_path)
    ws = register_ws(context, workspace.root)

    assert command(["job", "debug", ws, str(payload), "--placement", "cli/debug"], context) == 0
    text = capsys.readouterr().out
    assert "[prepare] runner is working on prepare" in text
    assert resolve_job(workspace, job_id).kind == "succeeded"
