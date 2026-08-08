"""Tier B UX guarantees: stalls surfaced, misdiagnosis corrected.

Every test here pins one operator-visible guarantee: the manager says what it is
doing, a job it cannot progress is reported rather than silently spun or silently
skipped, and a stall (a foreign persistent writer, a wedged commit, an
unresolvable join) is diagnosed truthfully and survives a restart.
"""

import json
import logging
import signal
import time
import uuid
from pathlib import Path

from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.introspection import explain_job, resolve_job
from httk.workflow.models import StateFrame
from httk.workflow.workflow_cli import command

_SUCCEED_RUNNER = """#!/usr/bin/env python3
import json, os
from pathlib import Path
context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
tmp = control / "outcome.tmp.test"
tmp.mkdir()
(tmp / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome", "format_version": 1,
    "job_id": context["job_id"], "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"], "action": "succeed",
}))
os.rename(tmp, control / "outcome.ready")
"""

_FAIL_RUNNER = """#!/usr/bin/env python3
import json, os
from pathlib import Path
context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
tmp = control / "outcome.tmp.test"
tmp.mkdir()
(tmp / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome", "format_version": 1,
    "job_id": context["job_id"], "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"], "action": "fail",
    "failure": {"code": "process_failure", "message": "boom"},
}))
os.rename(tmp, control / "outcome.ready")
"""

_SLEEP_RUNNER = """#!/usr/bin/env python3
import time
time.sleep(600)
"""

_CRASH_RUNNER = """#!/usr/bin/env python3
import sys
sys.exit(1)
"""

_GHOST_JOIN_RUNNER = """#!/usr/bin/env python3
import json, os, uuid
from pathlib import Path
context = json.loads(Path(os.environ["HTTK_WORKFLOW_CONTEXT"]).read_text())
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
child_id = str(uuid.uuid5(uuid.UUID(context["job_id"]), "ghost"))
tmp = control / "outcome.tmp.test"
tmp.mkdir()
(tmp / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome", "format_version": 1,
    "job_id": context["job_id"], "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"], "action": "wait", "next_step": "aggregate",
    "join": {"children": [{
        "workspace_id": context["workspace_id"], "job_id": child_id,
        "job_key": "child--" + child_id, "placement_hint": "project/ghosts",
    }], "condition": "all_succeeded"},
}))
os.rename(tmp, control / "outcome.ready")
"""


def _payload(
    root: Path,
    runner_source: str,
    *,
    tag: str,
    pool: str = "default",
    executor: str = "path",
    capabilities: tuple[str, ...] = (),
    workdir_mode: str = "persistent",
    initial_step: str = "only",
) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    (payload / "job.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-job",
                "format_version": 1,
                "id": job_id,
                "tag": tag,
                "name": f"Tier B {tag}",
                "workflow": "tests.ux",
                "runner": {"path": "files/runner", "arguments": [], "executor": executor},
                "workdir": {"mode": workdir_mode, "path": "run"},
                "data": {"mode": "none"},
                "initial_step": initial_step,
                "priority": 500,
                "claim": {"pool": pool, "required_capabilities": list(capabilities)},
                "retry_policy": {"maximum_attempts_per_activation": 1, "retry_on": []},
                "resources": {},
                "parent": None,
            }
        ),
        encoding="utf-8",
    )
    return payload, job_id


# ---------------------------------------------------------------------------
# Fix 1 + 2: startup banner, and the top-level run leaf serving a gated job.
# ---------------------------------------------------------------------------


def test_run_leaf_capability_claims_a_gated_job_and_prints_a_banner(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="gated", capabilities=("docker",))
    workspace.submit(payload, "project/gated")
    name = register_ws(context, workspace.root, "gated-ws")

    assert command(["run", name, "--capability", "docker", "--idle-timeout", "20"], context) == 0
    error = capsys.readouterr().err
    # One unconditional banner line: the workspace, the log path, and what is served.
    assert "serving" in error and str(workspace.root) in error
    assert "capabilities=docker" in error and "log " in error
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "succeeded"


def test_run_leaf_without_the_capability_reports_the_gate_and_stays_idle(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "source", _SUCCEED_RUNNER, tag="gated", capabilities=("docker",))
    workspace.submit(payload, "project/gated")
    name = register_ws(context, workspace.root, "gated-ws")

    assert command(["run", name, "--idle-timeout", "20"], context) == 0
    error = capsys.readouterr().err
    assert "1 not claimable here (capability=docker: 1)" in error
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "ready"


# ---------------------------------------------------------------------------
# Fix 3: reason-tagged idle census and the mismatch-naming timeout advice.
# ---------------------------------------------------------------------------


def test_work_census_counts_each_class(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    ok_payload, _ = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="ok")
    bad_payload, _ = _payload(tmp_path / "src", _FAIL_RUNNER, tag="bad")
    pool_payload, _ = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="poolmiss", pool="vasp")
    exec_payload, _ = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="execmiss", executor="docker")
    join_payload, _ = _payload(tmp_path / "src", _GHOST_JOIN_RUNNER, tag="ghost", initial_step="branch")
    for payload, placement in (
        (ok_payload, "project/ok"),
        (bad_payload, "project/bad"),
        (pool_payload, "project/poolmiss"),
        (exec_payload, "project/execmiss"),
        (join_payload, "project/ghost"),
    ):
        workspace.submit(payload, placement)

    with TaskManager(workspace, heartbeat_interval=0.01, join_grace_seconds=3600.0) as manager:
        # Drive the runnable jobs to their terminal or waiting states, leaving the
        # unclaimable ones exactly where they are.
        manager.run_until_idle(timeout=30.0)
        census = manager._work_census()

    assert census.succeeded >= 1
    assert census.failed >= 1
    assert census.ready_blocked.get("pool", {}).get("vasp") == 1
    assert census.ready_blocked.get("executor", {}).get("docker") == 1
    assert census.waiting == 1
    assert not census.actionable
    line = census.summary_line()
    assert "not claimable here" in line
    assert "pool=vasp: 1" in line and "executor=docker: 1" in line
    assert "1 waiting on children" in line


def test_paused_job_is_counted_and_not_actionable(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="pause")
    workspace.submit(payload, "project/pause")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = resolve_job(workspace, job_id)
        state = StateFrame.from_mapping(workspace.read_state(ready))
        manager._transition(ready, "paused", StateFrame.of(state.carried(), reason="operator_paused"))
        census = manager._work_census()
    assert census.paused == 1
    assert not census.actionable
    assert "1 paused" in census.summary_line()


def test_timeout_advice_names_the_mismatch() -> None:
    from httk.workflow.manager import WorkCensus

    census = WorkCensus(
        succeeded=0,
        failed=0,
        ready_claimable=0,
        ready_blocked={"pool": {"vasp": 2}, "capability": {"docker": 1}, "executor": {"container": 1}},
        waiting=0,
        paused=0,
        actionable_count=1,
    )
    message = census.timeout_message(3600.0)
    assert "does not serve pool(s) vasp" in message
    assert "capability(ies) docker" in message
    # Every mismatch class present is named, including the executor remedy that
    # used to be dropped whenever a pool or capability also mismatched.
    assert "executor(s) container" in message
    assert "--pool vasp" in message and "--capability docker" in message
    assert "has executor(s) container installed" in message
    # The generic "rerun or raise --idle-timeout" advice is gone when there is a
    # concrete mismatch to name.
    assert "raise --idle-timeout" not in message


# ---------------------------------------------------------------------------
# Fix 4: a foreign persistent writer cannot be recovered from this host.
# ---------------------------------------------------------------------------


def test_why_reports_a_foreign_host_persistent_writer_as_blocked(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SLEEP_RUNNER, tag="persistent")
    workspace.submit(payload, "project/persistent")
    with TaskManager(workspace, heartbeat_interval=0.01, lease_seconds=0.0) as manager:
        manager.tick()
        marker = resolve_job(workspace, job_id)
        assert marker.kind == "running"
        state = StateFrame.from_mapping(workspace.read_state(marker))
        control = workspace.payload_path(marker.placement, marker.job_key) / str(state.attempt_control)
        process = json.loads((control / "process.json").read_text())
        process["hostname"] = "faraway-host"
        (control / "process.json").write_text(json.dumps(process))
        diagnosis = explain_job(workspace, marker)
        manager._signal_running_attempts(signal.SIGKILL)
    assert diagnosis.blocked
    assert "faraway-host" in diagnosis.summary
    hint = " ".join(diagnosis.hints)
    assert "run a manager on faraway-host" in hint
    assert "--unsafe-persistent-takeover" in hint


# ---------------------------------------------------------------------------
# Fix 5: join grace is persisted and expires across two manager instances.
# ---------------------------------------------------------------------------


def test_join_grace_expires_across_two_manager_instances(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _GHOST_JOIN_RUNNER, tag="ghost", initial_step="branch")
    workspace.submit(payload, "project/ghost")

    # First manager: the child is unresolvable but the grace has not elapsed, so
    # the job stays waiting and the first-unresolved instant is persisted.
    with TaskManager(workspace, heartbeat_interval=0.01, join_grace_seconds=1.5) as first:
        first.run_until_idle(timeout=30.0)
    waiting = workspace.find_marker_by_id(job_id)
    assert waiting is not None and waiting.kind == "waiting"
    recorded = workspace.read_state(waiting).get("join_unresolved")
    assert isinstance(recorded, dict) and "first_unresolved_at" in recorded
    # Stored as an ISO timestamp like every other frame time, not a raw epoch float.
    stamp = recorded["first_unresolved_at"]
    assert isinstance(stamp, str) and "T" in stamp and stamp.endswith("Z")

    # The grace elapses while no manager runs at all; a fresh instance — with an
    # empty in-memory clock — still fails it, because the deadline is in the frame.
    time.sleep(1.7)
    with TaskManager(workspace, heartbeat_interval=0.01, join_grace_seconds=1.5) as second:
        second.run_until_idle(timeout=30.0)
    failed = workspace.find_marker_by_id(job_id)
    assert failed is not None and failed.kind == "failed"
    state = workspace.read_state(failed)
    assert state["reason"] == "join_unresolvable"
    assert state["failure"]["code"] == "dependency_failure"


# ---------------------------------------------------------------------------
# Fix 6: a repeating commit anomaly surfaces in job why.
# ---------------------------------------------------------------------------


def test_committing_wedge_surfaces_in_job_why(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SLEEP_RUNNER, tag="wedge")
    workspace.submit(payload, "project/wedge")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        running = resolve_job(workspace, job_id)
        assert running.kind == "running"
        state = StateFrame.from_mapping(workspace.read_state(running))
        committing = manager._transition(
            running,
            "committing",
            StateFrame.of(
                state.carried(),
                manager_id=manager.manager_id,
                writer_id=manager.writer.writer_id,
                attempt_id=str(state.attempt_id),
                attempt_control=str(state.attempt_control),
                outcome_action="succeed",
                reason="outcome_published",
            ),
        )
        key = f"resume_committing:{committing.job_key}"
        fields = manager._event("commit_error", committing)
        # First report is loud but transient; the repeat is the wedge that persists.
        manager._report_anomaly(key, "cannot resume the commit: disk is full", fields)
        control = workspace.payload_path(committing.placement, committing.job_key) / str(state.attempt_control)
        assert not (control / "commit-wedge.json").is_file()
        manager._report_anomaly(key, "cannot resume the commit: disk is full", fields)
        assert (control / "commit-wedge.json").is_file()
        manager._signal_running_attempts(signal.SIGKILL)
    diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    assert diagnosis.state == "committing" and diagnosis.blocked
    assert "wedged" in diagnosis.summary and "disk is full" in diagnosis.summary


def test_corrupt_committing_job_idles_promptly_and_is_reported(tmp_path: Path) -> None:
    # Regression: a committing job with an unreadable definition is not this
    # manager's work. It must not count as actionable — otherwise the run burns
    # the whole idle timeout — and it must be named in the census, not hidden.
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SLEEP_RUNNER, tag="corrupt")
    workspace.submit(payload, "project/corrupt")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.tick()
        running = resolve_job(workspace, job_id)
        assert running.kind == "running"
        state = StateFrame.from_mapping(workspace.read_state(running))
        committing = manager._transition(
            running,
            "committing",
            StateFrame.of(
                state.carried(),
                manager_id=manager.manager_id,
                writer_id=manager.writer.writer_id,
                attempt_id=str(state.attempt_id),
                attempt_control=str(state.attempt_control),
                outcome_action="succeed",
                reason="outcome_published",
            ),
        )
        manager._signal_running_attempts(signal.SIGKILL)
        # Corrupt the definition so no pass can ever advance the commit.
        job_path = workspace.payload_path(committing.placement, committing.job_key) / "job.json"
        job_path.write_text("{ not valid json", encoding="utf-8")
        started = time.monotonic()
        census = manager.run_until_idle(timeout=30.0)
        elapsed = time.monotonic() - started
    assert elapsed < 10.0  # prompt idle, not a 30s spin
    assert census.unreadable == 1
    assert not census.actionable
    assert "1 with an unreadable definition" in census.summary_line()
    assert "workspace fsck" in census.timeout_message(30.0)


# ---------------------------------------------------------------------------
# Fix 7: a placement prefix that matches nothing warns once.
# ---------------------------------------------------------------------------


def test_empty_placement_prefix_warns_once(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, _ = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="here")
    workspace.submit(payload, "project/here")
    # Attach a handler directly to the manager logger rather than relying on
    # caplog: a preceding CLI test may have reconfigured httk logging so the
    # record does not propagate to caplog's root handler.
    records: list[logging.LogRecord] = []

    class _Collector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("httk.workflow.manager")
    handler = _Collector()
    previous_level = logger.level
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    try:
        with TaskManager(
            workspace,
            heartbeat_interval=0.01,
            placement_prefixes=("project/nowhere", "project/here"),
        ):
            pass
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    warnings = [record for record in records if "placement prefix" in record.getMessage()]
    assert len(warnings) == 1
    assert "project/nowhere" in warnings[0].getMessage()


# ---------------------------------------------------------------------------
# Fix 8: publishing a request warns when no live manager serves its executor.
# ---------------------------------------------------------------------------


def test_job_request_warns_when_no_live_manager_serves_the_executor(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="orphan")
    workspace.submit(payload, "project/orphan")
    name = register_ws(context, workspace.root, "orphan-ws")

    assert command(["job", "request", name, job_id, "cancel", "--operator", "me", "--reason", "x"], context) == 0
    error = capsys.readouterr().err
    assert "no live manager currently serves executor 'path'" in error


def test_job_request_is_quiet_when_a_live_manager_serves_the_executor(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="served")
    workspace.submit(payload, "project/served")
    name = register_ws(context, workspace.root, "served-ws")

    with TaskManager(workspace, heartbeat_interval=0.01):
        capsys.readouterr()
        assert command(["job", "request", name, job_id, "cancel", "--operator", "me", "--reason", "x"], context) == 0
        error = capsys.readouterr().err
    assert "no live manager currently serves" not in error


# ---------------------------------------------------------------------------
# Tier C: refusal, attempt history, flapping, requests, and the managers command.
# ---------------------------------------------------------------------------


def _rewrite_job(payload: Path, **members: object) -> None:
    """Overwrite named members of a prepared job.json."""

    definition = json.loads((payload / "job.json").read_text(encoding="utf-8"))
    definition.update(members)
    (payload / "job.json").write_text(json.dumps(definition), encoding="utf-8")


def test_why_reports_a_runner_module_the_live_manager_refuses(tmp_path: Path) -> None:
    """A ready job whose runner module no live manager allows is refused, not welcomed (item 5)."""

    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="pkg")
    _rewrite_job(
        payload,
        runner={
            "executor": "path",
            "source": "installed",
            "path": "pkg:acme.runners/run",
            "sha256": "0" * 64,
            "arguments": [],
        },
    )
    workspace.submit(payload, "project/pkg")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        submitted = resolve_job(workspace, job_id)
        state = StateFrame.from_mapping(workspace.read_state(submitted))
        manager._transition(
            submitted,
            "ready",
            StateFrame.of(
                state.carried(),
                step="only",
                activation_id="v1",
                activation_ordinal=1,
                attempt_ordinal=0,
                total_attempts=0,
                reason="submitted",
            ),
        )
        diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    details = " ".join(check.detail for check in diagnosis.checks)
    assert "does not allow runner module acme.runners" in details
    assert "no live manager matches" in details


def test_why_reports_the_attempt_history_across_activations(tmp_path: Path) -> None:
    """A retried, crashing job's history is one folded check line (item 6)."""

    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _CRASH_RUNNER, tag="crash")
    _rewrite_job(payload, retry_policy={"maximum_attempts_per_activation": 3, "retry_on": ["process_failure"]})
    workspace.submit(payload, "project/crash")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=30.0)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None and marker.kind == "failed"
    diagnosis = explain_job(workspace, marker)
    history = [check.detail for check in diagnosis.checks if check.name == "attempt history"]
    assert history and "3 attempts across 1 activations" in history[0]
    assert "after unclean exits" in history[0]


def test_why_flags_an_unlimited_budget_job_as_flapping(tmp_path: Path) -> None:
    """An unlimited-budget job past the flap threshold is named as flapping (item 7)."""

    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="flap")
    _rewrite_job(payload, retry_policy={"retry_on": []})
    workspace.submit(payload, "project/flap")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = resolve_job(workspace, job_id)
        state = StateFrame.from_mapping(workspace.read_state(ready))
        manager._transition(
            ready,
            "failed",
            StateFrame.of(
                state.carried(),
                total_attempts=11,
                attempt_ordinal=11,
                failure={"code": "process_failure", "message": "boom"},
                reason="process_failure",
            ),
        )
        diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    flapping = [check.detail for check in diagnosis.checks if check.name == "flapping"]
    assert flapping and "attempted 11 times under an unlimited budget" in flapping[0]


def test_why_surfaces_a_pending_operator_request(tmp_path: Path) -> None:
    """A request sitting unapplied in requests/ready is surfaced with its executor (item 8)."""

    workspace = Workspace.initialize(tmp_path / "ws")
    payload, job_id = _payload(tmp_path / "src", _SUCCEED_RUNNER, tag="req")
    workspace.submit(payload, "project/req")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager._register_submissions()
        ready = resolve_job(workspace, job_id)
        state = StateFrame.from_mapping(workspace.read_state(ready))
        failed = manager._transition(
            ready,
            "failed",
            StateFrame.of(
                state.carried(), failure={"code": "process_failure", "message": "boom"}, reason="process_failure"
            ),
        )
        workspace.publish_request(
            {
                "format": "httk-workflow-request",
                "format_version": 1,
                "request_id": str(uuid.uuid4()),
                "job_id": failed.job_id,
                "job_key": failed.job_key,
                "placement": failed.placement.as_posix(),
                "expected_generation": failed.generation,
                "expected_record_ref": failed.record_ref,
                "action": "continue",
                "operator": "alice",
                "reason": "resume",
            }
        )
        diagnosis = explain_job(workspace, resolve_job(workspace, job_id))
    pending = [check.detail for check in diagnosis.checks if check.name == "pending request"]
    assert pending and "a continue request from alice is pending" in pending[0]
    assert "executor 'path'" in pending[0]


def test_workspace_managers_lists_the_serving_managers(tmp_path: Path, capsys) -> None:
    """The managers command answers 'what serves this workspace?' directly (item 10)."""

    context = CLIContext("httk", tmp_path)
    workspace = Workspace.initialize(tmp_path / "ws")
    name = register_ws(context, workspace.root, "mgr-ws")
    with TaskManager(workspace, capabilities=["docker"], heartbeat_interval=0.01) as manager:
        assert command(["workspace", "managers", name], context) == 0
        human = capsys.readouterr().out
        assert manager.manager_id in human
        assert "live" in human and "capabilities=docker" in human and "runner-modules=httk.workflow" in human
        assert command(["workspace", "managers", name, "--json"], context) == 0
        rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 1
    assert rows[0]["manager_id"] == manager.manager_id and rows[0]["alive"] is True
    assert "httk.workflow" in rows[0]["runner_modules"]
