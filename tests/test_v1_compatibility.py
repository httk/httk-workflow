from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from httk.workflow import Attempt, TaskManager, Workspace, collect, new_job, new_jobs
from httk.workflow.languages.httk_v1 import v1_runner
from httk.workflow.languages.httk_v1.v1_runner import _continue_with_children, _task_directories, replay_v1_atomic
from httk.workflow.packages import load_workflow_package
from httk.workflow.protocol import JobDefinition
from httk.workflow.runtime_builders import JobState
from httk.workflow.scaffold import resolve_workflow


def _legacy_source(root: Path, source: str, *, program: str = "ht_steps") -> Path:
    payload = root / "legacy-source"
    payload.mkdir()
    (payload / "httk_workflow.toml").write_text(
        """[workflow]
id = "tests.v1.legacy"
[workflow.runner]
language = "httk-v1"
attempts = 10
[workflow.environment."httk_v1.log_compression"]
default = "none"
""",
        encoding="utf-8",
    )
    runner = payload / program
    runner.write_text(source, encoding="utf-8")
    runner.chmod(0o755)
    return payload


def _new_v1_job(workspace: Workspace, source: Path, placement: str, *, attempts: int = 10):
    """Submit the legacy fixture through its packaged language realization."""

    manifest = source / "httk_workflow.toml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8").replace("attempts = 10", f"attempts = {attempts}"), encoding="utf-8"
    )
    return new_job(workspace, source, placement=placement)


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
    submitted = _new_v1_job(workspace, source, "project/00")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None
    assert marker.kind == "succeeded"
    assert marker.priority == 900
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workdir / "result.txt").read_text(encoding="utf-8") == "committed\n"
    assert (workdir / "restart.txt").read_text(encoding="utf-8") == "0:0\n"


def test_native_manager_runs_v1_package(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
HT_TASK_FINISHED
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = _new_v1_job(workspace, source, "combined/project")

    with TaskManager(workspace) as manager:
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
    submitted = _new_v1_job(workspace, source, "project/restart", attempts=2)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
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
    submitted = _new_v1_job(workspace, source, "project/atomic-recovery")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
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
    parent = _new_v1_job(workspace, source, "project/tree")

    with TaskManager(workspace, maximum_workers=2, heartbeat_interval=0.01) as manager:
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
    pending = list((workdir / "children").glob("ht.task.any.one.child.0.none.4.waitstart"))
    assert len(pending) == 1 and pending[0].is_dir()
    spawned = JobState(parent_payload).read()["v1_spawned"]
    assert isinstance(spawned, list)
    assert _task_directories(parent_payload, set(cast(list[str], spawned))) == []


def test_v1_walker_keeps_nested_logs_directory_tasks(tmp_path: Path) -> None:
    root = tmp_path / "legacy-payload"
    pending = root / "results" / "logs" / "ht.task.any.one.child.0.none.4.waitstart"
    pending.mkdir(parents=True)

    assert _task_directories(root, set()) == [pending]


def test_v1_next_spawns_payload_children(tmp_path: Path) -> None:
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
MAKE_CHILD() { cp "$HT_TASK_TOP_DIR/child_ht_steps" ht_steps; chmod +x ht_steps; }
if [ "$STEP" = start ]; then
    mkdir -p "$HT_TASK_TOP_DIR/children"
    printf 'Structure_A\\n' | HT_TASK_CREATE MAKE_CHILD "$HT_TASK_TOP_DIR/children" child any 4
    HT_TASK_NEXT finish
fi
touch parent-finished
HT_TASK_FINISHED
""",
    )
    child = source / "child_ht_steps"
    child.write_text(
        "#!/usr/bin/env bash\n. \"$HTTK_DIR/Execution/tasks/ht_tasks_api.sh\"\nHT_TASK_INIT \"$@\"\ntouch child-finished\nHT_TASK_FINISHED\n",
        encoding="utf-8",
    )
    child.chmod(0o755)
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent = _new_v1_job(workspace, source, "project/next")
    with TaskManager(workspace, maximum_workers=2, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=15)
    child_marker = next(marker for marker in workspace.scan_markers() if marker.job_id != parent.job_id)
    assert child_marker.kind == "succeeded"
    assert workspace.load_job(child_marker).tag == "any-structure_a"


def test_v1_payload_children_sanitize_long_tags(tmp_path: Path) -> None:
    task_id = "A" * 60
    source = _legacy_source(
        tmp_path,
        f"""#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
MAKE_CHILD() {{ cp "$HT_TASK_TOP_DIR/child_ht_steps" ht_steps; chmod +x ht_steps; }}
if [ "$STEP" = start ]; then
    mkdir -p "$HT_TASK_TOP_DIR/children"
    printf '{task_id}\\n' | HT_TASK_CREATE MAKE_CHILD "$HT_TASK_TOP_DIR/children" child any 4
    HT_TASK_SUBTASKS finish
fi
HT_TASK_FINISHED
""",
    )
    child = source / "child_ht_steps"
    child.write_text(
        "#!/usr/bin/env bash\n. \"$HTTK_DIR/Execution/tasks/ht_tasks_api.sh\"\nHT_TASK_INIT \"$@\"\nHT_TASK_FINISHED\n",
        encoding="utf-8",
    )
    child.chmod(0o755)
    workspace = Workspace.initialize(tmp_path / "workspace")
    parent = _new_v1_job(workspace, source, "project/payload-child")
    with TaskManager(workspace, maximum_workers=2, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=15)
    child_marker = next(marker for marker in workspace.scan_markers() if marker.job_id != parent.job_id)
    assert workspace.load_job(child_marker).tag == f"any-{task_id.lower()}"[:48]


def test_v1_pending_join_replays_after_state_checkpoint_crash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = tmp_path / "payload"
    source = payload / "children" / "ht.task.any.one.child.0.none.4.waitstart"
    source.mkdir(parents=True)
    state: dict[str, object] = {}
    gathered: list[str] = []
    attempt = SimpleNamespace(
        payload=payload,
        context=SimpleNamespace(activation_id="activation"),
        state=SimpleNamespace(
            get=lambda name, default=None: state.get(name, default), merge=lambda values: state.update(values)
        ),
        gather=lambda step, **_kwargs: gathered.append(step),
        advance=lambda _step, **_kwargs: None,
    )

    def crash_after_checkpoint(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected crash")

    publish_pending = v1_runner._publish_pending
    monkeypatch.setattr(v1_runner, "_publish_pending", crash_after_checkpoint)
    with pytest.raises(RuntimeError, match="injected crash"):
        _continue_with_children(
            cast(Attempt, attempt), mode="gather", step="after", priority=None, sources=[source], attempts=1
        )
    pending = state["v1_pending"]
    assert isinstance(pending, dict)
    monkeypatch.setattr(v1_runner, "_publish_pending", publish_pending)
    monkeypatch.setattr(v1_runner, "_spawn", lambda *_args: ["children/ht.task.any.one.child.0.none.4.waitstart"])
    v1_runner._publish_pending(cast(Attempt, attempt), pending, 1)
    assert gathered == ["start"]


def test_v1_priority_join_replays_after_second_checkpoint_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = tmp_path / "payload"
    source = payload / "children" / "ht.task.any.one.child.0.none.4.waitstart"
    source.mkdir(parents=True)
    state: dict[str, object] = {}
    gathered: list[dict[str, object]] = []
    checkpoints = 0

    def merge(values: dict[str, object]) -> None:
        nonlocal checkpoints
        state.update(values)
        checkpoints += 1
        if checkpoints == 2:
            raise RuntimeError("injected crash")

    attempt = SimpleNamespace(
        payload=payload,
        context=SimpleNamespace(activation_id="activation"),
        state=SimpleNamespace(get=lambda name, default=None: state.get(name, default), merge=merge),
        gather=lambda _step, **kwargs: gathered.append(kwargs),
        advance=lambda _step, **_kwargs: None,
    )
    monkeypatch.setattr(v1_runner, "_spawn", lambda *_args: ["children/ht.task.any.one.child.0.none.4.waitstart"])
    with pytest.raises(RuntimeError, match="injected crash"):
        _continue_with_children(
            cast(Attempt, attempt), mode="gather", step="after", priority=900, sources=[source], attempts=1
        )
    pending = state["v1_pending"]
    assert isinstance(pending, dict)
    v1_runner._publish_pending(cast(Attempt, attempt), pending, 1)
    assert gathered == [{"when": "all_terminal", "priority": 900}]


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
    submitted = _new_v1_job(workspace, source, "project/ht-run")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "succeeded"
    payload = workspace.payload_path(marker.placement, marker.job_key)
    assert "complete" in (payload / "ht.taskmgr.stdout").read_text(encoding="utf-8")


def test_v1_configuration_error_fails_once_without_retry_amplifying(tmp_path: Path) -> None:
    # Exit 2 without writing ht.nextstep is a deterministic protocol defect: no
    # retry can fix it, so it must be a single-attempt structured failure whose
    # message survives, not a retried process_failure ending in retry_exhausted.
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
exit 2
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = _new_v1_job(workspace, source, "project/badconfig", attempts=5)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "failed"
    state = workspace.read_state(marker)
    assert state["failure"]["code"] == "v1.configuration_invalid"
    assert "legacy exit 2 requires ht.nextstep" in state["failure"]["message"]
    assert not state["failure"].get("retryable", False)
    assert state["total_attempts"] == 1


def test_v1_missing_runner_is_a_retryable_runner_unavailable(tmp_path: Path) -> None:
    # A legacy runner not yet visible (NFS lag) is transient, not a deterministic
    # config defect: the failure is a retryable v1.runner_unavailable, message
    # intact, so a manager may try again rather than needing an operator resume.
    source = _legacy_source(
        tmp_path,
        """#!/usr/bin/env bash
exit 0
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = _new_v1_job(workspace, source, "project/lag", attempts=1)
    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None
    (workspace.payload_path(marker.placement, marker.job_key) / "ht_steps").chmod(0o600)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "failed"
    failure = workspace.read_state(marker)["failure"]
    assert failure["code"] == "v1.runner_unavailable"
    assert failure.get("retryable") is True
    assert "missing or not executable" in failure["message"]


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
HT_TASK_SET_PRIORITY 5
HT_TASK_BROKEN
""",
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    submitted = _new_v1_job(workspace, source, "project/broken")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=10)

    marker = workspace.find_marker_by_id(submitted.job_id)
    assert marker is not None and marker.kind == "failed"
    assert marker.priority == 900
    workdir = workspace.payload_path(marker.placement, marker.job_key) / "ht.run.current"
    assert (workdir / "freeze-result").read_text(encoding="utf-8") == "frozen\n"
    assert not (workdir / "ht.nextstep").exists()
    state = workspace.read_state(marker)
    assert state["failure"]["code"] == "declared_failure"


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
    parent = _new_v1_job(workspace, source, "project/abandoned")

    with TaskManager(workspace, maximum_workers=2, heartbeat_interval=0.01) as manager:
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
    submitted = _new_v1_job(workspace, source, "project/freeze-failure")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
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


def _v1_package(root: Path, manifest: str, program: str) -> Path:
    root.mkdir()
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    runner = root / "ht_steps"
    runner.write_text(program, encoding="utf-8")
    runner.chmod(0o755)
    script = root / "report.sh"
    script.write_text("#!/bin/sh\n", encoding="utf-8")
    script.chmod(0o755)
    return root


_V1_MANIFEST = '''
[workflow]
id = "tests.v1.package"

[workflow.runner]
language = "httk-v1"
taskset = "default"
attempts = 2

[workflow.inputs.value]

[workflow.outputs.result]
entry_type = "records"

[workflow.collect]
file = "collect.py"

[workflow.postprocess.report]
file = "report.sh"
'''

_V1_PROGRAM = '''#!/usr/bin/env bash
. "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
HT_TASK_INIT "$@"
cat ../result.txt > result.txt
HT_TASK_FINISHED
'''


def test_v1_language_manifest_package_runs_and_collects(tmp_path: Path) -> None:
    package = _v1_package(tmp_path / "package", _V1_MANIFEST, _V1_PROGRAM)
    (package / "result.txt.template").write_text("$value\n", encoding="utf-8")
    (package / "collect.py").write_text(
        "from httk.core import DataRecord\n"
        "def collect(record):\n"
        "    value = (record.workdir / 'result.txt').read_text().strip()\n"
        "    return {'result': DataRecord.from_value('https://example.test/result', 'result', value)}\n",
        encoding="utf-8",
    )
    load_workflow_package(package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    jobs = list(
        new_jobs(
            workspace,
            package,
            ({"inputs": {"value": value}} for value in ("one", "two", "three")),
        )
    )
    assert len(jobs) == 3
    definition = JobDefinition.from_path(jobs[0].payload / "job.json")
    assert definition.runner_executor == "path"
    assert definition.runner_source == "installed"
    assert definition.claim_pool == "default"
    assert definition.workdir_path.as_posix() == "ht.run.current"
    assert definition.workflow == "tests.v1.package"
    declared = definition.environment["declared"]
    assert isinstance(declared, dict)
    runtime_root = declared["httk_v1.root"]
    assert isinstance(runtime_root, dict) and runtime_root["default"] == ""
    assert definition.raw["compatibility"] == {
        "profile": "httk-v1-task-v1",
        "program": "ht_steps",
        "legacy_priority": 3,
        "attempts": 2,
    }
    assert not (jobs[0].payload / "httk_workflow.toml").exists()
    assert not (jobs[0].payload / "collect.py").exists()
    assert not (jobs[0].payload / "report.sh").exists()

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=15)
    assert all(workspace.find_marker_by_id(job.job_id).kind == "succeeded" for job in jobs)  # type: ignore[union-attr]
    collected = list(collect(workspace))
    assert {cast(Any, item.outputs["result"]).value for item in collected} == {"one", "two", "three"}
    assert all(item.run is not None and item.missing_collector is None for item in collected)


def test_v1_language_campaign_snapshots_template_members(tmp_path: Path) -> None:
    package = _v1_package(tmp_path / "package", _V1_MANIFEST, _V1_PROGRAM)
    (package / "result.txt.template").write_text("$value\n", encoding="utf-8")
    (package / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    campaign = new_jobs(
        workspace,
        package,
        [{"inputs": {"value": "one"}}, {"inputs": {"value": "two"}}],
    )
    jobs = [next(campaign)]
    (package / "result.txt.template").write_text("changed\n", encoding="utf-8")
    jobs.extend(campaign)
    assert [job.payload.joinpath("result.txt").read_text(encoding="utf-8") for job in jobs] == ["one\n", "two\n"]


def test_v1_snapshot_preserves_empty_directories_and_modes(tmp_path: Path) -> None:
    package = _v1_package(tmp_path / "package", _V1_MANIFEST, _V1_PROGRAM)
    (package / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    empty = package / "empty"
    empty.mkdir()
    empty.chmod(0o700)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"value": "value"})
    payload_empty = job.payload / "empty"
    assert payload_empty.is_dir()
    assert list(payload_empty.iterdir()) == []
    assert payload_empty.stat().st_mode & 0o777 == 0o700


def test_v1_language_rejects_symlinked_package_members(tmp_path: Path) -> None:
    package = tmp_path / "package"
    package.mkdir()
    runner = package / "ht_steps"
    runner.write_text(_V1_PROGRAM, encoding="utf-8")
    runner.chmod(0o755)
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    (package / "linked.txt").symlink_to(outside)
    workspace = Workspace.initialize(tmp_path / "workspace")
    with pytest.raises(ValueError, match=r"symlink.*linked\.txt"):
        new_job(workspace, package, format="httk-v1")


def test_v1_ht_instantiate_contract_runs_and_unlinks_script(tmp_path: Path) -> None:
    manifest = _V1_MANIFEST.replace("tests.v1.package", "tests.v1.instantiate")
    package = _v1_package(
        tmp_path / "package",
        manifest,
        _V1_PROGRAM.replace("cat ../result.txt > result.txt", "cat ../generated.txt > result.txt"),
    )
    (package / "ht.instantiate.py").write_text(
        "from pathlib import Path\nPath('generated.txt').write_text(value)\n", encoding="utf-8"
    )
    (package / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    load_workflow_package(package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"value": "instantiated"})
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=15)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert not (job.payload / "ht.instantiate.py").exists()
    assert (job.payload / "generated.txt").read_text(encoding="utf-8") == "instantiated"


def test_v1_template_runner_is_rendered_executable_and_runs(tmp_path: Path) -> None:
    manifest = _V1_MANIFEST.replace("tests.v1.package", "tests.v1.template")
    package = tmp_path / "package"
    package.mkdir()
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    runner = package / "ht_steps.template"
    runner.write_text(
        _V1_PROGRAM.replace("cat ../result.txt > result.txt", "printf '%s\\n' \"value-$value\" > result.txt"),
        encoding="utf-8",
    )
    runner.chmod(0o751)
    (package / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    (package / "report.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    load_workflow_package(package)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"value": "rendered"})
    assert (job.payload / "ht_steps").is_file()
    assert (job.payload / "ht_steps").stat().st_mode & 0o777 == 0o751
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=15)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    assert (job.payload / "ht.run.current" / "result.txt").read_text(encoding="utf-8") == "value-rendered\n"


def test_v1_bare_directory_is_realized_and_degraded_on_collect(tmp_path: Path) -> None:
    root = tmp_path / "bare"
    root.mkdir()
    runner = root / "ht_steps.template"
    runner.write_text(
        _V1_PROGRAM.replace("cat ../result.txt > result.txt", "printf '%s\\n' \"$value\" > result.txt"),
        encoding="utf-8",
    )
    runner.chmod(0o755)
    assert resolve_workflow(root, format="httk-v1").language == "httk-v1"
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, root, parameters={"value": "bare"}, format="httk-v1")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=2)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "succeeded"
    collected = next(collect(workspace))
    assert collected.outputs == {}
    assert collected.missing_collector is not None
    assert "declares [workflow.collect]" in collected.missing_collector


def test_v1_bare_directory_is_not_auto_matched(tmp_path: Path) -> None:
    root = tmp_path / "bare"
    root.mkdir()
    (root / "ht_steps").write_text(_V1_PROGRAM, encoding="utf-8")
    (root / "ht_steps").chmod(0o755)

    with pytest.raises(ValueError, match="unknown workflow path"):
        resolve_workflow(root)


def test_v1_environment_wrapper_and_log_compression(tmp_path: Path) -> None:
    package = _v1_package(tmp_path / "package", _V1_MANIFEST, "#!/bin/sh\nprintf run\\n\n")
    (package / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    wrapper = tmp_path / "wrapper"
    wrapper.write_text("#!/bin/sh\ntouch wrapped\nexec \"$@\"\n", encoding="utf-8")
    wrapper.chmod(0o755)
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(
        workspace,
        package,
        environment={"httk_v1.wrapper": str(wrapper), "httk_v1.log_compression": "none"},
    )
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=10)
    assert (job.payload / "ht.run.current" / "wrapped").is_file()
    assert (job.payload / "ht.taskmgr.stdout").is_file()


def test_v1_default_bzip2_log_and_timeout(tmp_path: Path) -> None:
    package = _v1_package(
        tmp_path / "package",
        _V1_MANIFEST,
        "#!/usr/bin/env bash\n. \"$HTTK_DIR/Execution/tasks/ht_tasks_api.sh\"\nHT_TASK_INIT \"$@\"\nif [ \"$STEP\" = freeze ]; then touch frozen; exit 0; fi\nsleep 2\n",
    )
    (package / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, environment={"httk_v1.timeout": 1})
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=10)
    marker = workspace.find_marker_by_id(job.job_id)
    assert marker is not None and marker.kind == "failed"
    assert (job.payload / "ht.run.current" / "frozen").is_file()
    package2 = _v1_package(
        tmp_path / "package2",
        _V1_MANIFEST.replace("tests.v1.package", "tests.v1.bzip2"),
        "#!/usr/bin/env bash\n. \"$HTTK_DIR/Execution/tasks/ht_tasks_api.sh\"\nHT_TASK_INIT \"$@\"\nprintf run\\n\nHT_TASK_FINISHED\n",
    )
    (package2 / "collect.py").write_text("def collect(record): return {}\n", encoding="utf-8")
    job2 = new_job(workspace, package2)
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=10)
    assert (job2.payload / "ht.taskmgr.stdout").is_file()
    assert (job2.payload / "ht.run.current" / "ht.taskmgr.stdout.bz2").is_file()
