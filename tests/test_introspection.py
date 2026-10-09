"""Job introspection (show, log, list, why) and the foreground debug runner on the filesystem kernel."""

import dataclasses
import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as v3
from httk.workflow import TaskManager, Workspace, _kernel
from httk.workflow._job import JobDefinition
from httk.workflow._logging import reset_logging
from httk.workflow.introspection import (
    DEBUG_EXIT_FAILED,
    DEBUG_EXIT_SUCCEEDED,
    DEBUG_EXIT_UNFINISHED,
    _diagnosis,
    debug_job,
    describe_job,
    explain_job,
    job_events,
    list_jobs,
    render_events,
    render_job,
    resolve_job,
    resolve_job_selector,
    resolve_job_selectors,
)

#: Three activations, each advancing to the next step, then success.
_THREE_STEPS = {"start": "advance:finish", "finish": "advance:gather", "gather": "succeed"}
_STEPS = ["start", "finish", "gather"]
_RETRY = {"maximum_attempts_per_activation": 3, "maximum_total_attempts": 10, "maximum_activations": 5}
_BREADCRUMB = {
    "start": {
        "format": "httk-workflow-runner-error",
        "format_version": 2,
        "step": "start",
        "exception": "RuntimeError",
        "message": "the inputs are missing",
        "traceback": "Traceback (most recent call last): RuntimeError",
    }
}


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    """Keep the console handler ``job debug`` installs out of other tests."""

    reset_logging()
    yield
    reset_logging()


@pytest.fixture
def ws(tmp_path: Path) -> Workspace:
    return v3.workspace(tmp_path / "workspace")


@pytest.fixture
def installed(ws: Workspace, tmp_path: Path) -> v3._store.Installed:
    return v3.install(ws, tmp_path / "package")


def _submit(ws: Workspace, installed: Any, script: dict[str, str], **options: Any) -> _kernel.JobRef:
    options.setdefault("tag", "example")
    options.setdefault("retry_policy", _RETRY)
    options.setdefault("parameters", {})
    options["parameters"] = {"runner_steps": _STEPS, **options["parameters"]}
    return v3.submit(ws, installed, script, **options)


def _tick_until(manager: TaskManager, predicate: Callable[[], bool], seconds: float = 60.0) -> None:
    deadline = time.monotonic() + seconds
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        manager.tick()
        time.sleep(0.02)


# ---------------------------------------------------------------------------
# show, log, and list
# ---------------------------------------------------------------------------


def test_show_reports_the_authoritative_state_of_a_finished_job(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS, placement="project/show")
    v3.run(ws)

    ref = resolve_job(ws, submitted.job_id[:8])
    report = describe_job(ws, ref)
    assert report["state"] == "succeeded" and report["token"] == ref.token
    assert report["job_key"] == f"example--{submitted.job_id}"
    assert report["placement"] == "project/show"
    assert report["step"] == "gather" and report["initial_step"] == "start"
    assert report["phase"] == {"kind": "idle", "attempt_id": None}
    assert report["runner_steps"] == _STEPS
    assert report["workflow"] == {"id": installed.id, "name": "demo"}
    assert report["workflow_pin"] == {"id": installed.id, "tree_sha256": installed.record["tree_sha256"]}
    assert report["claim"] == {"claim_pool": "default", "required_capabilities": []}
    assert len(str(report["job_digest"])) == 64
    assert report["budgets"]["activations"] == 3
    assert report["budgets"]["total_attempts"] == 3
    assert report["budgets"]["maximum_attempts_per_activation"] == 3
    assert Path(str(report["paths"]["workdir"])).is_dir()
    assert report["paths"]["data"] is None
    assert report["state_error"] is None and report["job_error"] is None
    text = render_job(report)
    assert f"job example--{submitted.job_id} (succeeded)" in text
    assert "attempts this activation 1/3" in text


def test_a_selector_must_name_exactly_one_job(ws: Workspace, installed: Any) -> None:
    for _ in range(2):
        _submit(ws, installed, _THREE_STEPS, placement="project/many")
    with pytest.raises(ValueError, match="matches 2 jobs"):
        resolve_job(ws, "example")
    with pytest.raises(ValueError, match="no job in"):
        resolve_job(ws, "0" * 36)
    with pytest.raises(ValueError, match="cannot be empty"):
        resolve_job(ws, "")


def test_log_renders_the_owner_run_log_oldest_first(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS, placement="project/log")
    v3.run(ws)

    events = job_events(resolve_job(ws, submitted.job_id))
    names = [event["event"] for event in events]
    assert names[:7] == ["claimed", "attempt_started", "launched", "attempt_ended", "outcome", "committed", "released"]
    assert names[-1] == "released" and names.count("committed") == 3
    assert [event["step"] for event in events if event["event"] == "attempt_started"] == _STEPS
    assert [event["event"] for event in job_events(resolve_job(ws, submitted.job_id), limit=2)] == names[-2:]

    rendered = render_events(events)
    assert "ready->owned" in rendered and "ready->succeeded" in rendered
    assert "step=start" in rendered and "step=gather" in rendered
    assert "detail=advance" in rendered


def test_log_reports_an_unreadable_line_and_keeps_what_exists(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, {"start": "succeed"})
    v3.run(ws)
    ref = resolve_job(ws, submitted.job_id)
    log = ref.path / "logs" / "runlog.jsonl"
    lines = log.read_text(encoding="utf-8").splitlines()
    log.write_text("\n".join([lines[0], "not json", *lines[1:]]) + "\n" + '{"torn', encoding="utf-8")

    events = job_events(ref)
    # The damaged line is reported in place; the torn last line is a write in progress, not damage.
    assert len(events) == len(lines) + 1
    assert "line 2 is not a JSON object" in str(events[1]["error"])
    assert "line 2 is not a JSON object" in render_events(events)


def test_log_of_an_unclaimed_job_reports_that_no_owner_moved_it(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS)
    events = job_events(resolve_job(ws, submitted.job_id))
    assert events == []
    assert "no owner has claimed it yet" in render_events(events)


def test_job_selector_paths_expand_directories_and_deduplicate(ws: Workspace, installed: Any) -> None:
    first = _submit(ws, installed, _THREE_STEPS, placement="work/silicon").job_id
    second = _submit(ws, installed, _THREE_STEPS, placement="work/nested/germanium").job_id
    third = _submit(ws, installed, _THREE_STEPS, placement="other").job_id
    cwd = ws.root

    def ids(selectors: list[str]) -> list[str]:
        return [ref.job_id for ref in resolve_job_selectors(ws, cwd, selectors)]

    assert ids(["jobs/ready/work/silicon"]) == [first]
    # A directory names every job below it, in path order.
    assert ids(["jobs/ready/work"]) == [second, first]
    assert ids(["jobs/ready/work/silicon*"]) == [first]
    assert [ref.job_id for ref in resolve_job_selector(ws, cwd, "jobs/ready/work/**")] == [second, first]
    assert ids(["jobs/ready/work", "jobs/ready/work/silicon"]) == [second, first]
    assert ids(["jobs/ready"]) == [third, second, first]
    # A job directory itself is a selector, and so is an id next to a path.
    job_dir = next((ws.jobs / "ready" / "other").iterdir())
    assert ids([str(job_dir), first]) == [third, first]


def test_job_selector_paths_report_outside_or_missing_jobs(ws: Workspace, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(ValueError, match="is not inside workspace"):
        resolve_job_selectors(ws, tmp_path, [str(outside)])
    with pytest.raises(ValueError, match="not below the jobs directory"):
        resolve_job_selectors(ws, ws.root, [str(ws.control)])
    with pytest.raises(ValueError, match="no path matches 'missing\\*'"):
        resolve_job_selectors(ws, ws.root, ["missing*"])
    empty = ws.jobs / "ready" / "empty"
    empty.mkdir(parents=True)
    with pytest.raises(ValueError, match="no jobs below"):
        resolve_job_selectors(ws, tmp_path, [str(empty)])
    (empty / "job.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="is a file, not a job directory"):
        resolve_job_selectors(ws, ws.root, [str(empty / "job.json")])


def test_an_existing_path_wins_over_a_tag_prefix(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS, tag="silicon", placement="silicon")
    _submit(ws, installed, _THREE_STEPS, tag="silicon", placement="elsewhere")

    (ref,) = resolve_job_selectors(ws, ws.jobs / "ready", ["silicon"])
    assert ref.job_id == submitted.job_id


def test_list_reports_one_row_per_job_and_filters_by_state(ws: Workspace, installed: Any) -> None:
    finished = _submit(ws, installed, _THREE_STEPS, placement="project/list/done").job_id
    v3.run(ws)
    fresh = _submit(ws, installed, _THREE_STEPS, placement="project/list/new").job_id

    rows = list_jobs(ws)
    assert {str(row["state"]) for row in rows} == {"succeeded", "ready"}
    by_id = {str(row["job_id"]): row for row in rows}
    assert by_id[finished]["step"] == "gather" and by_id[finished]["phase"] == "idle"
    assert by_id[fresh]["step"] is None and by_id[fresh]["phase"] is None
    assert [row["job_id"] for row in list_jobs(ws, kinds=("ready",))] == [fresh]
    assert [row["job_id"] for row in list_jobs(ws, placement_prefix="project/list/done")] == [finished]


# ---------------------------------------------------------------------------
# why
# ---------------------------------------------------------------------------


def test_why_reports_a_ready_job_no_manager_serves(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS)

    diagnosis = explain_job(ws, resolve_job(ws, submitted.job_id))
    assert diagnosis.state == "ready" and diagnosis.blocked
    detail = {check.name: check.detail for check in diagnosis.checks}
    assert detail["registered manager"] == "no manager is registered in this workspace"
    assert detail["installed workflow"] == f"{installed.id} and its calls are installed and built here"
    assert any("httk manager run" in hint for hint in diagnosis.hints)
    assert any("httk job debug" in hint for hint in diagnosis.hints)


def test_why_reports_an_uninstalled_workflow(ws: Workspace) -> None:
    submitted = _submit(ws, ("local:absent", "absent"), _THREE_STEPS)

    checks = {check.name: check for check in explain_job(ws, resolve_job(ws, submitted.job_id)).checks}
    assert checks["installed workflow"].satisfied is False
    assert "local:absent" in checks["installed workflow"].detail


def test_why_reports_a_job_owned_by_another_user(
    ws: Workspace, installed: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS)
    real = _diagnosis.read_managers
    monkeypatch.setattr(
        _diagnosis,
        "read_managers",
        lambda workspace: [dataclasses.replace(item, uid=(item.uid or 0) + 1) for item in real(workspace)],
    )
    with TaskManager(ws):
        diagnosis = explain_job(ws, resolve_job(ws, submitted.job_id))

    assert any(
        "is owned by another user (uid" in check.detail
        and "managers run only jobs owned by their account" in check.detail
        for check in diagnosis.checks
    )


def test_why_names_the_claim_pool_no_live_manager_serves(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS, pool="gpu", capabilities=("cuda",))
    with TaskManager(ws) as manager:
        manager.tick()
        ref = resolve_job(ws, submitted.job_id)
        assert ref.state == "ready"
        diagnosis = explain_job(ws, ref)
        assert diagnosis.blocked
        checks = {check.name: check for check in diagnosis.checks}
        assert checks["claim pool"].detail == "this job asks for pool gpu"
        assert checks["required capabilities"].detail == "cuda"
        assert checks["eligible manager"].satisfied is False
        assert any(
            "does not serve claim pool gpu" in check.detail
            and "lacks capabilities cuda" in check.detail
            and manager.manager_id in check.detail
            for check in diagnosis.checks
            if check.name == "live manager"
        )
        assert any("manager run --pool gpu --workspace WORKSPACE --capability cuda" in hint for hint in diagnosis.hints)
        rendered = diagnosis.render()
        assert f"job example--{submitted.job_id} is ready" in rendered and "no  eligible manager" in rendered
        assert diagnosis.as_mapping()["format"] == "httk-workflow-job-diagnosis"

    # A cleanly closed manager removed its owner record.
    exited = explain_job(ws, resolve_job(ws, submitted.job_id))
    registered = next(check for check in exited.checks if check.name == "registered manager")
    assert registered.satisfied is False and "no manager is registered" in registered.detail


def test_why_reports_a_failed_job_with_its_breadcrumb_and_continue(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, {"start": "exit"}, parameters={"breadcrumb": _BREADCRUMB})
    v3.run(ws)

    ref = resolve_job(ws, submitted.job_id)
    assert ref.state == "failed"
    diagnosis = explain_job(ws, ref)
    assert "failed with process_failure" in diagnosis.summary
    checks = {check.name: check for check in diagnosis.checks}
    assert checks["failure"].detail.startswith("process_failure: ")
    assert checks["error breadcrumb"].detail == "step start raised RuntimeError: the inputs are missing"
    assert checks["operator continue"].satisfied is True
    assert any("job request continue --workspace WORKSPACE" in hint for hint in diagnosis.hints)
    assert checks["attempt history"].detail.startswith("1 attempts across 1 activations")

    report = describe_job(ws, ref)
    assert report["error_breadcrumb"]["exception"] == "RuntimeError"
    assert report["failure"]["details"] == {"exit_status": 3, "log_paths": ["logs/stdio.out"]}


def test_why_prefers_a_retained_log_path_from_the_failure(
    ws: Workspace, installed: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitted = _submit(ws, installed, {"start": "exit"})
    v3.run(ws)
    ref = resolve_job(ws, submitted.job_id)
    doc = v3.state_of(ref)
    assert doc.failure is not None
    failure = {**doc.failure, "details": {"log_paths": ["diagnostics/retained-stdio.out"]}}
    monkeypatch.setattr(_diagnosis, "read_state", lambda _ref: (doc.updated(failure=failure), None))

    logs = next(check for check in explain_job(ws, ref).checks if check.name == "attempt logs")
    assert "diagnostics/retained-stdio.out" in logs.detail


def test_render_job_surfaces_a_process_failure_breadcrumb_headline_and_control(ws: Workspace, installed: Any) -> None:
    headline = {"start": "halfway through relax"}
    submitted = _submit(ws, installed, {"start": "exit"}, parameters={"headline": headline})
    v3.run(ws)

    ref = resolve_job(ws, submitted.job_id)
    report = describe_job(ws, ref)
    assert report["failure"]["code"] == "process_failure"
    assert report["last_headline"] == "halfway through relax"
    assert report["error_breadcrumb"] is None
    assert report["attempt_control"] is not None and Path(report["attempt_control"]).is_dir()

    rendered = render_job(report)
    assert "the last attempt left no error.json breadcrumb" in rendered
    assert "last headline" in rendered and "halfway through relax" in rendered
    assert "attempt control" in rendered


def test_why_reports_an_exhausted_continue_budget(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, {"start": "exit"}, retry_policy={"maximum_attempts_per_activation": 1})
    v3.run(ws)

    diagnosis = explain_job(ws, resolve_job(ws, submitted.job_id))
    checks = {check.name: check for check in diagnosis.checks}
    assert checks["operator continue"].satisfied is False
    assert "retry_exhausted" in checks["operator continue"].detail
    assert any("override_step" in hint for hint in diagnosis.hints)


def test_why_lists_the_pending_child_of_a_waiting_parent(ws: Workspace, installed: Any) -> None:
    # The child's workflow is not installed, so it stays ready and the parent keeps waiting.
    child = {"label": "only", "script": {"start": "succeed"}, "workflow": {"id": "local:absent", "name": "absent"}}
    spawn = {"children": [child], "next_step": "gather"}
    submitted = _submit(ws, installed, {"start": "spawn", "gather": "succeed"}, parameters={"spawn": spawn})
    with TaskManager(ws) as manager:
        _tick_until(manager, lambda: bool(list(_kernel.list_jobs(ws, "waiting"))))

        parent = resolve_job(ws, submitted.job_id)
        diagnosis = explain_job(ws, parent)
        assert diagnosis.blocked
        assert "waits for 1 of 1 child(ren)" in diagnosis.summary
        (child_check,) = [check for check in diagnosis.checks if check.name == "join child"]
        assert child_check.satisfied is False
        assert child_check.detail.startswith("only: only--") and child_check.detail.endswith(" is ready")
        condition = next(check for check in diagnosis.checks if check.name == "join condition")
        assert condition.detail == "all_succeeded then step gather"

        report = describe_job(ws, parent)
        assert report["join"]["condition"] == "all_succeeded"
        assert report["join"]["children"][0]["label"] == "only"
        assert report["join"]["children"][0]["kind"] == "ready"

        # The blocked child itself explains that its workflow is not installed here.
        child_ref = next(ref for ref in _kernel.list_jobs(ws, "ready") if ref.job_key.startswith("only--"))
        child_checks = {check.name: check for check in explain_job(ws, child_ref).checks}
        assert child_checks["installed workflow"].satisfied is False


def test_why_reports_a_job_owned_by_a_live_manager(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, {"start": "sleep"})
    with TaskManager(ws, cancel_grace_seconds=0.5) as manager:
        manager.tick()
        ref = resolve_job(ws, submitted.job_id)
        assert ref.state == "owned" and ref.owner_id == manager.manager_id
        diagnosis = explain_job(ws, ref)
        assert not diagnosis.blocked
        assert "owned by a live owner (phase running)" in diagnosis.summary
        liveness = next(check for check in diagnosis.checks if check.name == "owner liveness")
        assert liveness.satisfied is True
        owner = next(check for check in diagnosis.checks if check.name == "owner")
        assert f"manager {manager.manager_id}" in owner.detail
        report = describe_job(ws, ref)
        assert report["owner_id"] == manager.manager_id and report["phase"]["kind"] == "running"
        assert "owner" in render_job(report)


def test_why_reports_a_dead_owner_as_recoverable(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, {"start": "sleep"})
    manager = TaskManager(ws, cancel_grace_seconds=0.5)
    try:
        manager.tick()
        evidence = [{"subject": f"owner {manager.manager_id}", "rule": "operator", "detail": "test"}]
        _kernel.attest_dead(ws, manager.manager_id, by="operator", evidence=evidence)
        diagnosis = explain_job(ws, resolve_job(ws, submitted.job_id))
    finally:
        manager.close()
    assert diagnosis.blocked
    assert "owner is dead" in diagnosis.summary and "recovers it" in diagnosis.summary
    liveness = next(check for check in diagnosis.checks if check.name == "owner liveness")
    assert liveness.satisfied is False and "next manager tick recovers this job" in liveness.detail


def test_why_reports_a_paused_job(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, {"start": "pause"})
    v3.run(ws)

    diagnosis = explain_job(ws, resolve_job(ws, submitted.job_id))
    assert diagnosis.state == "paused" and diagnosis.blocked
    assert "only an operator request moves it" in diagnosis.summary
    pause = next(check for check in diagnosis.checks if check.name == "pause")
    assert "operator inspection" in pause.detail
    assert any("job request continue --workspace WORKSPACE" in hint for hint in diagnosis.hints)


def test_why_lists_a_pending_request(ws: Workspace, installed: Any) -> None:
    from httk.workflow import _requests

    submitted = _submit(ws, installed, {"start": "exit"})
    v3.run(ws)
    ref = resolve_job(ws, submitted.job_id)
    assert ref.placement is not None
    _requests.post(ws, action="continue", job_id=ref.job_id, placement=ref.placement, operator="alice", reason="again")

    pending = [check for check in explain_job(ws, ref).checks if check.name == "pending request"]
    assert len(pending) == 1 and "a continue request from alice waits" in pending[0].detail


# ---------------------------------------------------------------------------
# debug
# ---------------------------------------------------------------------------


def _collector() -> tuple[list[str], Callable[[str], None]]:
    """Return a console sink and the lines the debug runner writes into it."""

    lines: list[str] = []
    return lines, lines.append


def test_debug_drives_a_three_step_runner_in_the_foreground(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS, placement="project/debug")

    lines, write = _collector()
    outcome = debug_job(ws, submitted.job_id, emit=write)
    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED and outcome.state == "succeeded"
    text = "\n".join(lines)
    for step in _STEPS:
        assert f"runner is working on {step}" in text
        assert f"diagnostic from {step}" in text
        assert f"step={step}" in text
    assert "[debug] example--" in text and "finished as succeeded" in text
    ref = resolve_job(ws, submitted.job_id)
    assert ref.state == "succeeded"
    assert not (ref.path / "attempts").exists()
    events = job_events(ref)
    assert [event["detail"] for event in events if event["event"] == "outcome"] == ["advance", "advance", "succeed"]
    chronicle = (ref.path / "logs" / "stdio.out").read_text(encoding="utf-8").splitlines()
    for step in _STEPS:
        start = next(
            index
            for index, line in enumerate(chronicle)
            if line.startswith("=== httk attempt") and f" step {step} " in line and " started " in line
        )
        end = next(
            index
            for index in range(start + 1, len(chronicle))
            if chronicle[index].startswith("=== httk attempt") and " ended " in chronicle[index]
        )
        block = chronicle[start : end + 1]
        assert f"runner is working on {step}" in block
        assert f"diagnostic from {step}" in block


def test_debug_treats_an_in_workspace_job_directory_as_the_existing_job(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS, placement="project/debug-path")

    outcome = debug_job(ws, str(submitted.path), emit=lambda line: None)

    assert outcome.job_id == submitted.job_id and outcome.state == "succeeded"


def _payload(root: Path, installed: Any, script: dict[str, str], **options: Any) -> Path:
    """Write a prepared payload directory (outside the workspace) holding one ``job.json``."""

    root.mkdir(parents=True)
    options.setdefault("tag", "example")
    options.setdefault("retry_policy", _RETRY)
    mapping = v3.job_mapping(installed, script, **options)
    (root / "job.json").write_bytes(JobDefinition.from_mapping(mapping).encode())
    return root


def test_debug_admits_declared_resources_for_every_job_step(ws: Workspace, installed: Any, tmp_path: Path) -> None:
    payload = _payload(
        tmp_path / "source",
        installed,
        {"start": "succeed"},
        resources={"procs": 2},
        step_resources={"start": {"gpus": 1}},
    )

    outcome = debug_job(ws, str(payload), emit=lambda line: None)

    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED and outcome.state == "succeeded"


def test_debug_runs_a_job_declaring_time_resources(ws: Workspace, installed: Any, tmp_path: Path) -> None:
    payload = _payload(
        tmp_path / "source",
        installed,
        {"start": "succeed"},
        resources={"maxtime": 3600, "mintime": 60, "procs": 1},
        step_resources={"start": {"maxtime": 600}},
    )

    outcome = debug_job(ws, str(payload), emit=lambda line: None)

    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED


def test_debug_refreshes_capacity_for_dynamic_resources(ws: Workspace, installed: Any, tmp_path: Path) -> None:
    payload = _payload(
        tmp_path / "source",
        installed,
        {"start": "advance:finish", "finish": "succeed"},
        resources={"procs": 1},
        parameters={"resources": {"start": {"procs": 3}}},
    )

    outcome = debug_job(ws, str(payload), timeout=30.0, emit=lambda line: None)

    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED and outcome.state == "succeeded"


def test_debug_submits_a_fresh_payload_at_the_step_override(ws: Workspace, installed: Any, tmp_path: Path) -> None:
    files = {step: {f"{step}.txt": step} for step in _STEPS}
    payload = _payload(tmp_path / "source", installed, _THREE_STEPS, parameters={"files": files})
    job_id = json.loads((payload / "job.json").read_text(encoding="utf-8"))["id"]

    lines, write = _collector()
    outcome = debug_job(ws, str(payload), placement="scratch/debug", step="gather", emit=write)
    assert outcome.exit_code == DEBUG_EXIT_SUCCEEDED
    text = "\n".join(lines)
    assert "initial step overridden to gather" in text
    assert "runner is working on gather" in text and "runner is working on start" not in text
    ref = resolve_job(ws, job_id)
    assert ref.placement is not None and ref.placement.as_posix() == "scratch/debug"
    assert sorted(path.name for path in (ref.path / "run").glob("*.txt")) == ["gather.txt"]
    # The prepared payload is left as it was: the submitted job is a copy.
    assert json.loads((payload / "job.json").read_text(encoding="utf-8"))["initial_step"] == "start"


def test_debug_refuses_to_override_the_step_of_an_existing_job(ws: Workspace, installed: Any) -> None:
    submitted = _submit(ws, installed, _THREE_STEPS)
    with pytest.raises(ValueError, match="already exists"):
        debug_job(ws, submitted.job_id, step="gather", emit=lambda line: None)


def test_debug_exit_codes_report_the_terminal_state(ws: Workspace, installed: Any) -> None:
    failing = _submit(ws, installed, {"start": "exit"}, retry_policy={"maximum_attempts_per_activation": 1})
    pausing = _submit(ws, installed, {"start": "pause"})

    lines, write = _collector()
    assert debug_job(ws, failing.job_id, emit=write).exit_code == DEBUG_EXIT_FAILED
    assert "failure=process_failure" in "\n".join(lines)
    assert debug_job(ws, pausing.job_id, emit=lambda line: None).exit_code == DEBUG_EXIT_UNFINISHED


def test_debug_only_drives_the_selected_job(ws: Workspace, installed: Any) -> None:
    target = _submit(ws, installed, _THREE_STEPS, placement="project/scoped/target")
    other = _submit(ws, installed, _THREE_STEPS, placement="project/scoped/other")

    assert debug_job(ws, target.job_id, emit=lambda line: None).exit_code == DEBUG_EXIT_SUCCEEDED
    assert resolve_job(ws, target.job_id).state == "succeeded"
    assert resolve_job(ws, other.job_id).state == "ready"


def test_debug_follows_children_only_when_asked(ws: Workspace, installed: Any) -> None:
    spawn = {"children": [{"label": "only", "script": {"start": "succeed"}}], "next_step": "gather"}
    submitted = _submit(ws, installed, {"start": "spawn", "gather": "succeed"}, parameters={"spawn": spawn})

    lines, write = _collector()
    stopped = debug_job(ws, submitted.job_id, emit=write)
    assert stopped.exit_code == DEBUG_EXIT_UNFINISHED and stopped.state == "waiting"
    assert "rerun with --follow-children" in "\n".join(lines)

    lines, write = _collector()
    finished = debug_job(ws, submitted.job_id, follow_children=True, emit=write)
    assert finished.exit_code == DEBUG_EXIT_SUCCEEDED
    text = "\n".join(lines)
    assert "[debug child:only] only--" in text
    assert "runner is working on gather" in text
    assert resolve_job(ws, submitted.job_id).state == "succeeded"
