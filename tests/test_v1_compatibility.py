from pathlib import Path

from httk.workflow import TaskManager, V1TaskManager, WorkflowStore, submit_v1_task
from httk.workflow._v1_runner import replay_v1_atomic


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
    store = WorkflowStore.initialize(tmp_path / "store")
    submitted = submit_v1_task(store, source, "project/00")

    with V1TaskManager(store, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = store.find_marker_by_id(submitted.job_id)
    assert marker is not None
    assert marker.kind == "succeeded"
    assert marker.priority == 900
    workspace = store.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workspace / "result.txt").read_text(encoding="utf-8") == "committed\n"
    assert (workspace / "restart.txt").read_text(encoding="utf-8") == "0:0\n"


def test_native_manager_leaves_v1_submission_for_v1_manager(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
HT_TASK_FINISHED
""",
    )
    store = WorkflowStore.initialize(tmp_path / "store")
    submitted = submit_v1_task(store, source, "combined/project")

    with TaskManager(store) as manager:
        manager.run_until_idle(timeout=2)
    untouched = store.find_marker_by_id(submitted.job_id)
    assert untouched is not None and untouched.kind == "submitted"

    with V1TaskManager(store, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)
    finished = store.find_marker_by_id(submitted.job_id)
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
    store = WorkflowStore.initialize(tmp_path / "store")
    submitted = submit_v1_task(store, source, "project/restart", attempts=2)

    with V1TaskManager(store, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = store.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    workspace = store.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workspace / "observed-restart").read_text(encoding="utf-8") == "1:1\n"


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
    store = WorkflowStore.initialize(tmp_path / "store")
    submitted = submit_v1_task(store, source, "project/atomic-recovery")

    with V1TaskManager(store, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = store.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    workspace = store.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workspace / "executed-steps").read_text(encoding="utf-8").splitlines() == ["finish"]
    assert (workspace / "committed-result").read_text(encoding="utf-8") == "complete\n"


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
    store = WorkflowStore.initialize(tmp_path / "store")
    parent = submit_v1_task(store, source, "project/tree")

    with V1TaskManager(store, maximum_workers=2, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=15)

    parent_marker = store.find_marker_by_id(parent.job_id)
    assert parent_marker is not None and parent_marker.kind == "succeeded"
    markers = list(store.scan_markers())
    assert len(markers) == 2
    child_marker = next(marker for marker in markers if marker.job_id != parent.job_id)
    assert child_marker.kind == "succeeded"
    parent_payload = store.payload_path(parent_marker.placement, parent_marker.job_key)
    workspace = parent_payload / "ht.run.current"
    assert (workspace / "parent-result").read_text(encoding="utf-8") == "parent-finished\n"
    links = list((workspace / "children").glob("ht.task.any.one.child.0.unclaimed.4.*"))
    assert len(links) == 1
    assert links[0].is_symlink()
    assert links[0].name.endswith(".finished")
    assert links[0].resolve() == store.payload_path(child_marker.placement, child_marker.job_key)
    links[0].unlink()
    with V1TaskManager(store, log_compression="none"):
        pass
    rebuilt = list((workspace / "children").glob("ht.task.any.one.child.0.unclaimed.4.finished"))
    assert len(rebuilt) == 1
    assert rebuilt[0].resolve() == store.payload_path(child_marker.placement, child_marker.job_key)


def test_v1_ht_run_exit_zero_succeeds(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
printf 'complete\\n'
exit 0
""",
        program="ht_run",
    )
    store = WorkflowStore.initialize(tmp_path / "store")
    submitted = submit_v1_task(store, source, "project/ht-run")

    with V1TaskManager(store, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = store.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    payload = store.payload_path(marker.placement, marker.job_key)
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
    store = WorkflowStore.initialize(tmp_path / "store")
    submitted = submit_v1_task(store, source, "project/broken")

    with V1TaskManager(store, heartbeat_interval=0.01, log_compression="none") as manager:
        manager.run_until_idle(timeout=10)

    marker = store.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "failed"
    workspace = store.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workspace / "freeze-result").read_text(encoding="utf-8") == "frozen\n"
    assert not (workspace / "ht.nextstep").exists()
    state = store.read_state(marker)
    assert state["failure"]["class"] == "declared_failure"


def test_v1_atomic_replay_is_idempotent(tmp_path: Path) -> None:
    workspace = tmp_path / "run"
    atomic = workspace / "ht.atomic.test"
    atomic.mkdir(parents=True)
    (workspace / "old-name").write_text("moved", encoding="utf-8")
    (atomic / "ht.atommv.old-name").write_text("new-name\n", encoding="utf-8")
    (atomic / "all-or-nothing").write_text("committed", encoding="utf-8")

    replay_v1_atomic(workspace)
    replay_v1_atomic(workspace)

    assert not atomic.exists()
    assert not (workspace / "old-name").exists()
    assert (workspace / "new-name").read_text(encoding="utf-8") == "moved"
    assert (workspace / "all-or-nothing").read_text(encoding="utf-8") == "committed"
