import sys
from pathlib import Path

from httk.workflow import TaskManager, Workspace
from httk.workflow._v1_runner import replay_v1_atomic
from httk.workflow.v1 import V1RunnerBackend, V1TaskManager, submit_v1_task


def _legacy_source(root: Path, source: str, *, program: str = "ht_steps") -> Path:
    payload = root / "legacy-source"
    payload.mkdir()
    runner = payload / program
    runner.write_text(source, encoding="utf-8")
    runner.chmod(0o755)
    return payload


def test_v1_multistep_atomic_job_and_priority(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
if [ "$STEP" = "start" ]; then
    HT_TASK_ATOMIC_SECTION_START
    printf 'committed\\n' > result.txt
    HT_TASK_ATOMIC_SECTION_END_NEXT finish
fi
if [ "$STEP" = "finish" ]; then
    HT_TASK_SET_PRIORITY 5
    printf '%s:%s\\n' "$HTTK_WORKFLOW_IS_RESTART" "$HTTK_WORKFLOW_UNCLEAN_RESTART" > restart.txt
    HT_TASK_FINISHED
fi
exit 1
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/00")

    with V1TaskManager(workspace, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None
    assert marker.kind == "succeeded"
    assert marker.priority == 900
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workdir / "result.txt").read_text(encoding="utf-8") == "committed\n"
    assert (workdir / "restart.txt").read_text(encoding="utf-8") == "0:0\n"


def test_native_manager_leaves_v1_submission_for_v1_manager(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
HT_TASK_FINISHED
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "combined/project")

    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=2)
    untouched = workspace.find_marker_by_id(submitted.job_id)
    assert untouched is not None and untouched.kind == "submitted"

    with V1TaskManager(workspace, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)
    finished = workspace.find_marker_by_id(submitted.job_id)
    assert finished is not None and finished.kind == "succeeded"


def test_v1_unclean_adapter_restart_is_visible_to_shell_step(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
if [ ! -e first-attempt ]; then
    touch first-attempt
    kill -KILL "$PPID"
    exit 9
fi
printf '%s:%s\\n' "$HTTK_WORKFLOW_IS_RESTART" "$HTTK_WORKFLOW_UNCLEAN_RESTART" > observed-restart
HT_TASK_FINISHED
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/restart", attempts=2)

    with V1TaskManager(workspace, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workdir / "observed-restart").read_text(encoding="utf-8") == "1:1\n"


def test_v1_published_atomic_next_step_is_not_rerun(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
printf '%s\\n' "$STEP" >> executed-steps
if [ "$STEP" = "finish" ]; then
    HT_TASK_FINISHED
fi
exit 1
""",
    )
    atomic = source / "ht.run.resume" / "ht.atomic.0"
    atomic.mkdir(parents=True)
    (atomic / "ht.nextstep").write_text("finish\n", encoding="utf-8")
    (atomic / "committed-result").write_text("complete\n", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/atomic-recovery")

    with V1TaskManager(workspace, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workdir / "executed-steps").read_text(encoding="utf-8").splitlines() == ["finish"]
    assert (workdir / "committed-result").read_text(encoding="utf-8") == "complete\n"


def test_v1_dynamic_subtask_is_registered_joined_and_linked(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
MAKE_CHILD() {
    cp "$HT_TASK_TOP_DIR/child_ht_steps" ht_steps
    chmod +x ht_steps
}
if [ "$STEP" = "start" ]; then
    mkdir -p children
    printf 'one\\n' | HT_TASK_CREATE MAKE_CHILD children child any 4
    HT_TASK_SUBTASKS after
fi
if [ "$STEP" = "after" ]; then
    printf 'parent-finished\\n' > parent-result
    HT_TASK_FINISHED
fi
exit 1
""",
    )
    child = source / "child_ht_steps"
    child.write_text(
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
printf 'child-finished\\n' > child-result
HT_TASK_FINISHED
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent = submit_v1_task(workspace, source, "project/tree")

    with V1TaskManager(workspace, maximum_workers=2, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=15)

    parent_marker = workspace.find_marker_by_id(parent.job_id)
    assert parent_marker is not None and parent_marker.kind == "succeeded"
    markers = list(workspace.scan_markers())
    assert len(markers) == 2
    child_marker = next(marker for marker in markers if marker.job_id != parent.job_id)
    assert child_marker.kind == "succeeded"
    parent_payload = workspace.payload_path(parent_marker.placement, parent_marker.job_key)
    workdir = parent_payload / "ht.run.current"
    assert (workdir / "parent-result").read_text(encoding="utf-8") == "parent-finished\n"
    links = list((workdir / "children").glob("ht.task.any.one.child.0.unclaimed.4.*"))
    assert len(links) == 1
    assert links[0].is_symlink()
    assert links[0].name.endswith(".finished")
    assert links[0].resolve() == workspace.payload_path(child_marker.placement, child_marker.job_key)
    links[0].unlink()
    with V1TaskManager(workspace, log_compression="none"):
        pass
    rebuilt = list((workdir / "children").glob("ht.task.any.one.child.0.unclaimed.4.finished"))
    assert len(rebuilt) == 1
    assert rebuilt[0].resolve() == workspace.payload_path(child_marker.placement, child_marker.job_key)


def test_v1_ht_run_exit_zero_succeeds(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
printf 'complete\\n'
exit 0
""",
        program="ht_run",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/ht-run")

    with V1TaskManager(workspace, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    payload = workspace.payload_path(marker.placement, marker.job_key)
    assert "complete" in (payload / "ht.taskmgr.stdout").read_text(encoding="utf-8")


def test_v1_declared_break_runs_freeze_and_fails(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
if [ "$STEP" = "freeze" ]; then
    printf 'frozen\\n' > freeze-result
    exit 0
fi
HT_TASK_BROKEN
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/broken")

    with V1TaskManager(workspace, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "failed"
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workdir / "freeze-result").read_text(encoding="utf-8") == "frozen\n"
    assert not (workdir / "ht.nextstep").exists()
    state = workspace.read_state(marker)
    assert state["failure"]["code"] == "declared_failure"


def test_v1_adapter_runs_as_a_module_of_this_package(tmp_path: Path) -> None:
    """No file path and no sys.path hack: the adapter is imported as a module."""

    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
HT_TASK_FINISHED
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/module")
    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None
    backend = V1RunnerBackend(log_compression="none")
    from httk.workflow.backends import AttemptLaunch

    payload = workspace.payload_path(marker.placement, marker.job_key)
    command = list(
        backend.command(
            AttemptLaunch(
                job=workspace.load_job(marker),
                marker=marker,
                payload=payload,
                workdir=payload,
                control=payload,
                context_path=payload / "context.json",
                context={},
                runner=payload / "ht_steps",
            )
        )
    )
    assert command[:3] == [sys.executable, "-m", "httk.workflow._v1_runner"]
    assert not any(item.endswith("_v1_runner.py") for item in command)
    assert "sys.path.insert" not in (Path(__file__).parents[1] / "src/httk/workflow/_v1_runner.py").read_text()


def test_v1_terminal_outcome_does_not_publish_leftover_subtasks(tmp_path: Path) -> None:
    """A finished parent must not spawn the task directories it abandoned."""

    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
MAKE_CHILD() {
    cp "$HT_TASK_TOP_DIR/child_ht_steps" ht_steps
    chmod +x ht_steps
}
mkdir -p abandoned
printf 'one\\n' | HT_TASK_CREATE MAKE_CHILD abandoned child any 4
HT_TASK_FINISHED
""",
    )
    child = source / "child_ht_steps"
    child.write_text(
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
HT_TASK_FINISHED
""",
        encoding="utf-8",
    )
    child.chmod(0o755)
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent = submit_v1_task(workspace, source, "project/abandoned")

    with V1TaskManager(workspace, maximum_workers=2, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=15)

    marker = workspace.find_marker_by_id(parent.job_id)
    assert marker is not None and marker.kind == "succeeded"
    # The abandoned task directory stays exactly what it was: a directory in the
    # payload, not a schedulable job nothing will ever join.
    assert [item.job_id for item in workspace.scan_markers()] == [parent.job_id]
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    leftover = list((workdir / "abandoned").glob("ht.task.any.one.child.*"))
    assert len(leftover) == 1 and leftover[0].is_dir() and not leftover[0].is_symlink()


def test_v1_failing_freeze_leaves_a_note_in_the_run_log(tmp_path: Path) -> None:
    """Best effort must not mean invisible: a broken freeze says so."""

    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
if [ "$STEP" = "freeze" ]; then
    exit 7
fi
HT_TASK_BROKEN
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = submit_v1_task(workspace, source, "project/freeze-failure")

    with V1TaskManager(workspace, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "failed"
    payload = workspace.payload_path(marker.placement, marker.job_key)
    log = (payload / "ht.taskmgr.stdout").read_text(encoding="utf-8")
    assert "httk-workflow: the legacy freeze step returned exit status 7" in log
    # The freeze failure never replaces the real outcome.
    assert workspace.read_state(marker)["failure"]["code"] == "declared_failure"


def test_v1_atomic_replay_is_idempotent(tmp_path: Path) -> None:
    workdir = tmp_path / "run"
    atomic = workdir / "ht.atomic.test"
    atomic.mkdir(parents=True)
    (workdir / "old-name").write_text("moved", encoding="utf-8")
    (atomic / "ht.atommv.old-name").write_text("new-name\n", encoding="utf-8")
    (atomic / "all-or-nothing").write_text("committed", encoding="utf-8")

    replay_v1_atomic(workdir)
    replay_v1_atomic(workdir)

    assert not atomic.exists()
    assert not (workdir / "old-name").exists()
    assert (workdir / "new-name").read_text(encoding="utf-8") == "moved"
    assert (workdir / "all-or-nothing").read_text(encoding="utf-8") == "committed"
