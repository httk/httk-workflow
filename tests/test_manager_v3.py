"""End-to-end tests of the task manager on the filesystem kernel, with real runner processes."""

import hashlib
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace, _kernel, _requests, _store
from httk.workflow._job import JobDefinition
from httk.workflow._state import StateDoc, read_state_unowned

pytestmark = pytest.mark.slow

SOURCE = Path(__file__).resolve().parents[1] / "src"

#: The runner of the test workflow: each step does what ``parameters.script[<step>]`` says, records the attempt
#: in ``run/attempts.jsonl`` and publishes its outcome by renaming a staged directory onto ``outcome.ready``.
RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, time, uuid

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job = json.loads((pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
step = context["step"]
behavior = job["parameters"]["script"][step]
record = {key: context[key] for key in ("step", "attempt_ordinal", "is_restart", "attempt_reason")}
workspace = pathlib.Path(os.environ["HTTK_WORKFLOW_WORKSPACE_DIR"])
record["children"] = sorted(
    (child["label"], child["kind"], (workspace / child["payload_path"] / "job.json").is_file())
    for child in context["children"]
)
with open("attempts.jsonl", "a") as stream:
    stream.write(json.dumps(record) + "\\n")
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2)
draft = control / "outcome.tmp.x"
draft.mkdir()
if behavior.startswith("spawn:"):
    # spawn:<child behavior>:<on_impossible step or ->: two children, then wait for all of them to succeed.
    _, child_behavior, rescue = behavior.split(":")
    entries, references = [], []
    for index in range(2):
        child_id, spawn_id, tag = str(uuid.uuid4()), str(uuid.uuid4()), f"child{index}"
        key, placement = f"{tag}--{child_id}", f"{job['placement']}/c{index}"
        child = dict(job, id=child_id, tag=tag, name=tag, placement=placement)
        child["parameters"] = {"script": {"start": child_behavior}}
        child["parent"] = {
            "workspace_id": context["workspace_id"], "job_id": job["id"], "job_key": context["job_key"],
            "placement": job["placement"], "activation_id": context["activation_id"], "spawn_id": spawn_id,
        }
        (draft / "children" / "jobs" / key).mkdir(parents=True)
        (draft / "children" / "jobs" / key / "job.json").write_text(json.dumps(child))
        entries.append({"job_key": key, "label": tag, "placement": placement, "spawn_id": spawn_id})
        references.append(
            {"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": key, "placement_hint": placement}
        )
    spawn = {"format": "httk-workflow-spawn", "format_version": 2, "children": entries}
    (draft / "children" / "spawn.json").write_text(json.dumps(spawn))
    join = {"children": references, "condition": "all_succeeded"}
    if rescue != "-":
        join["on_impossible"] = {"action": "advance", "next_step": rescue}
    outcome.update(action="wait", next_step="gather", join=join)
elif behavior.startswith("advance:"):
    outcome.update(action="advance", next_step=behavior.split(":", 1)[1])
elif behavior.startswith("wait_then:"):
    # Advance once the test drops run/go.
    pathlib.Path("waiting").touch()
    while not pathlib.Path("go").exists():
        time.sleep(0.02)
    outcome.update(action="advance", next_step=behavior.split(":", 1)[1])
elif behavior in ("fifo", "symlinked_state"):
    # Content a seal cannot cover, planted by the job before it succeeds.
    if behavior == "fifo":
        os.mkfifo("pipe")
    else:
        (pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / ".httk-job").symlink_to(control)
    outcome.update(action="succeed")
elif behavior in ("txn_conflict", "txn_reserved"):
    # A committed transaction staging a file over the run/ directory, or into the trusted logs/.
    staged = control / "txn" / "000001" / ("run" if behavior == "txn_conflict" else "logs/planted")
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_text("staged")
    outcome.update(action="succeed")
elif behavior == "retry_once" and context["attempt_ordinal"] == 1:
    outcome.update(action="retry", retry={"reason": "try_again"})
elif behavior == "fail":
    outcome.update(action="fail", failure={"code": "converge", "message": "did not converge"})
elif behavior == "sleep" or (behavior == "crash_once" and not context["is_restart"]):
    pathlib.Path("sleeping").write_text(str(os.getpid()))
    time.sleep(120)
else:
    outcome.update(action="succeed")
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return Workspace.initialize(tmp_path / "ws", durable=False, policy={"visibility_deadline_seconds": 0.05})


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    package = tmp_path / "demo"
    package.mkdir()
    manifest = '[workflow]\nname = "demo"\n\n[workflow.runner]\nsteps = ["start", "finish", "gather", "rescue"]\ninitial_step = "start"\n'
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (package / "run").write_text(RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    with cli_owner(ws) as owner:
        return _store.install(ws, owner, package)


def cli_owner(ws: Workspace) -> _kernel.Owner:
    return _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})


def submit(
    ws: Workspace,
    workflow_id: str,
    script: dict[str, str],
    *,
    retry_on: tuple[str, ...] = (),
    seal_succeeded: bool | None = None,
) -> _kernel.JobRef:
    """Submit one v3 job built in a CLI owner's scratch."""

    job = JobDefinition.from_mapping(
        {
            "format": "httk-workflow-job",
            "format_version": 3,
            "id": str(uuid.uuid4()),
            "tag": "demo",
            "name": "demo job",
            "placement": "project/0",
            "workflow": {"id": workflow_id, "name": "demo"},
            "initial_step": "start",
            "priority": 500,
            "claim": {"pool": "default", "required_capabilities": []},
            "retry_policy": {"maximum_attempts_per_activation": 3, "retry_on": list(retry_on)},
            "resources": {},
            "step_resources": {},
            "parameters": {"script": script},
            "declarations": {},
            "declared": {},
            "environment": {},
            "parent": None,
            "seal_succeeded": seal_succeeded,
        }
    )
    with cli_owner(ws) as owner:
        staging = owner.scratch("submit") / "job"
        staging.mkdir()
        (staging / "job.json").write_bytes(job.encode())
        return _kernel.submit(ws, owner, staging)


def only(ws: Workspace, state: str) -> _kernel.JobRef:
    refs = list(_kernel.list_jobs(ws, state))
    assert len(refs) == 1, f"{state}: {refs}"
    return refs[0]


def state_of(ref: _kernel.JobRef) -> StateDoc:
    doc, damaged = read_state_unowned(ref.path / "state.json")
    assert doc is not None and not damaged
    return doc


def events(ref: _kernel.JobRef) -> list[str]:
    lines = (ref.path / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line)["event"] for line in lines]


def attempts(ref: _kernel.JobRef) -> list[dict[str, object]]:
    lines = (ref.path / "run" / "attempts.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def run(ws: Workspace) -> None:
    with TaskManager(ws) as manager:
        manager.run_until_idle(timeout=60)
    # A clean close removes the owner record entirely.
    assert not [item for item in _kernel.list_owners(ws) if item.record is not None]


ONE_ATTEMPT = ["claimed", "attempt_started", "launched", "attempt_ended", "outcome", "committed", "released"]
#: A succeeding attempt seals the job before its commit is recorded.
SUCCEED = [*ONE_ATTEMPT[:5], "sealed", *ONE_ATTEMPT[5:]]


def test_one_step_succeeds(ws: Workspace, installed: _store.Installed) -> None:
    submitted = submit(ws, installed.id, {"start": "succeed"})
    run(ws)
    done = only(ws, "succeeded")
    assert done.job_id == submitted.job_id
    assert done.path.parent == ws.jobs / "succeeded" / "project" / "0"
    doc = state_of(done)
    assert doc.phase == {"kind": "idle", "attempt_id": None}
    assert doc.commit is None and doc.failure is None
    assert doc.release_to is not None and doc.release_to.state == "succeeded"
    assert doc.workflow_pin == {"id": installed.id, "tree_sha256": installed.record["tree_sha256"]}
    assert events(done) == SUCCEED
    # The job is sealed (unsigned: the test has no signing key), and the seal is recorded in state.json.
    seal = done.path / ".httk-job" / "seal.json"
    assert doc.seal == {"sha256": hashlib.sha256(seal.read_bytes()).hexdigest(), "signed": False}
    assert json.loads(seal.read_bytes())["signatures"] == []
    # A succeeded job keeps no attempt directory; the chronicle has its start and end markers.
    assert not (done.path / "attempts").exists()
    chronicle = (done.path / "logs" / "stdio.out").read_text(encoding="utf-8")
    assert "step start ordinal 1 started" in chronicle and "exit 0 outcome succeed" in chronicle


def test_advance_across_two_steps(ws: Workspace, installed: _store.Installed) -> None:
    submit(ws, installed.id, {"start": "advance:finish", "finish": "succeed"})
    run(ws)
    done = only(ws, "succeeded")
    assert [item["step"] for item in attempts(done)] == ["start", "finish"]
    doc = state_of(done)
    assert doc.activation is not None and doc.activation["step"] == "finish" and doc.activation["ordinal"] == 2
    assert events(done) == ONE_ATTEMPT + SUCCEED


def test_retry_then_succeed(ws: Workspace, installed: _store.Installed) -> None:
    submit(ws, installed.id, {"start": "retry_once"})
    run(ws)
    done = only(ws, "succeeded")
    first, second = attempts(done)
    assert (first["attempt_ordinal"], first["is_restart"]) == (1, False)
    assert (second["attempt_ordinal"], second["is_restart"], second["attempt_reason"]) == (2, True, "try_again")
    assert state_of(done).counters == {"activations": 1, "attempts_total": 2}


def test_declared_failure_fails_the_job(ws: Workspace, installed: _store.Installed) -> None:
    submit(ws, installed.id, {"start": "fail"})
    run(ws)
    failed = only(ws, "failed")
    doc = state_of(failed)
    assert doc.failure is not None and doc.failure["code"] == "converge"
    assert events(failed) == [*ONE_ATTEMPT[:-1], "failed", "released"]
    # A failed job keeps its attempt as evidence.
    assert (failed.path / "attempts").is_dir()


def test_a_job_whose_workflow_is_not_installed_is_never_claimed(ws: Workspace) -> None:
    ref = submit(ws, "local:missing", {"start": "succeed"})
    with TaskManager(ws) as manager:
        census = manager.run_until_idle(timeout=30)
    assert census.ready_claimable == 0
    assert list(census.ready_blocked["calls"].values()) == [1]
    assert only(ws, "ready").path == ref.path
    assert not (ref.path / "state.json").exists() and not (ref.path / "logs").exists()


def _wait_for(predicate: Callable[[], bool], seconds: float = 60.0) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition not reached")


def _group_gone(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    return False


def test_a_crashed_manager_is_recovered_and_its_attempt_rerun(ws: Workspace, installed: _store.Installed) -> None:
    submit(ws, installed.id, {"start": "crash_once"}, retry_on=("owner_lost",))
    code = (
        "from httk.workflow import TaskManager, Workspace\n"
        f"TaskManager(Workspace({str(ws.root)!r}, durable=False)).run_until_idle(timeout=120)\n"
    )
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    manager = subprocess.Popen([sys.executable, "-c", code], env=environment)
    try:
        _wait_for(lambda: any((ws.jobs / "owned").glob("*/*/run/sleeping")))
    finally:
        manager.send_signal(signal.SIGKILL)
        manager.wait()
    # The node "crashed": the attempt dies with its manager.
    (record,) = (ws.control / "owners").glob("*/launches/*/process.json")
    pgid = json.loads(record.read_text(encoding="utf-8"))["pgid"]
    os.killpg(pgid, signal.SIGKILL)
    _wait_for(lambda: _group_gone(pgid))

    run(ws)
    done = only(ws, "succeeded")
    first, second = attempts(done)
    assert first["is_restart"] is False
    assert (second["attempt_ordinal"], second["is_restart"], second["attempt_reason"]) == (2, True, "owner_lost")
    log = events(done)
    assert log[: log.index("recovered")] == ["claimed", "attempt_started", "launched", "claimed"]
    assert log[-len(SUCCEED) :] == SUCCEED
    doc = state_of(done)
    assert [item["code"] for item in doc.failure_history] == ["owner_lost"]
    # The dead manager keeps only its tombstone.
    (dead,) = [item for item in _kernel.list_owners(ws) if item.tombstone is not None]
    assert sorted(path.name for path in dead.path.iterdir()) == ["dead.json"]


def test_sealing_opt_out(ws: Workspace, installed: _store.Installed) -> None:
    submit(ws, installed.id, {"start": "succeed"}, seal_succeeded=False)
    run(ws)
    done = only(ws, "succeeded")
    assert state_of(done).seal == {"disabled": True}
    assert not (done.path / ".httk-job" / "seal.json").exists()
    assert "sealed" not in events(done)


def test_children_join_and_resume_with_observations(ws: Workspace, installed: _store.Installed) -> None:
    parent = submit(ws, installed.id, {"start": "spawn:succeed:-", "gather": "succeed"})
    run(ws)
    done = [ref for ref in _kernel.list_jobs(ws, "succeeded")]
    assert len(done) == 3
    (resumed,) = [ref for ref in done if ref.job_id == parent.job_id]
    start, gather = attempts(resumed)
    assert start["children"] == []
    assert gather["step"] == "gather"
    # The gathering step saw both children, succeeded, with their payloads located at launch.
    assert gather["children"] == [["child0", "succeeded", True], ["child1", "succeeded", True]]
    doc = state_of(resumed)
    assert doc.join is None and [item["state"] for item in doc.observations] == ["succeeded", "succeeded"]
    assert sorted(str(child["label"]) for child in doc.children) == ["child0", "child1"]
    assert doc.activation is not None and doc.activation["reason"] == "join"
    for child in (ref for ref in done if ref.job_id != parent.job_id):
        assert child.path.parent.parent == ws.jobs / "succeeded" / "project" / "0"
        definition = JobDefinition.from_path(child.path / "job.json")
        assert definition.parent is not None and definition.parent["job_id"] == parent.job_id


def _post(ws: Workspace, ref: _kernel.JobRef, action: str, *, priority: int | None = None, force: bool = False) -> None:
    # The placement is a hint; every test job but the children sits at project/0.
    placement = ref.placement.as_posix() if ref.placement is not None else "project/0"
    _requests.post(
        ws,
        action=action,
        job_id=ref.job_id,
        placement=placement,
        operator="tester",
        reason="test",
        priority=priority,
        force=force,
    )


def test_an_impossible_join_takes_on_impossible_and_a_consumed_child_is_not_revived(
    ws: Workspace, installed: _store.Installed
) -> None:
    parent = submit(ws, installed.id, {"start": "spawn:fail:rescue", "rescue": "succeed"})
    run(ws)
    resumed = only(ws, "succeeded")
    assert resumed.job_id == parent.job_id
    assert [item["step"] for item in attempts(resumed)] == ["start", "rescue"]
    children = list(_kernel.list_jobs(ws, "failed"))
    assert len(children) == 2
    # The decided join consumed the failed children: continuing one is refused without force...
    child = children[0]
    _post(ws, child, "continue")
    run(ws)
    refused = only_by_id(ws, child.job_id)
    assert refused.state == "failed"
    assert [entry["event"] for entry in state_of(refused).history_tail][-2:] == ["request_dropped", "released"]
    # ...and applied with it: the child runs (and fails) again.
    _post(ws, refused, "continue", force=True)
    run(ws)
    revived = only_by_id(ws, child.job_id)
    assert revived.state == "failed" and len(attempts(revived)) == 2


def test_an_impossible_join_without_on_impossible_fails_the_parent(ws: Workspace, installed: _store.Installed) -> None:
    parent = submit(ws, installed.id, {"start": "spawn:fail:-"})
    run(ws)
    failed = only_by_id(ws, parent.job_id)
    assert failed.state == "failed"
    doc = state_of(failed)
    assert doc.failure is not None and doc.failure["code"] == "dependency_failure"


def only_by_id(ws: Workspace, job_id: str) -> _kernel.JobRef:
    found = [ref for state in _kernel.UNOWNED_STATES for ref in _kernel.list_jobs(ws, state) if ref.job_id == job_id]
    assert len(found) == 1, found
    return found[0]


def test_cancel_a_running_job(ws: Workspace, installed: _store.Installed) -> None:
    ref = submit(ws, installed.id, {"start": "sleep"})
    with TaskManager(ws, cancel_grace_seconds=1.0) as manager:
        _wait_for(lambda: (manager.tick() or True) and any((ws.jobs / "owned").glob("*/*/run/sleeping")))
        _post(ws, ref, "cancel")
        _wait_for(lambda: (manager.tick() or True) and not manager.running_attempts)
        manager.run_until_idle(timeout=30)
    cancelled = only(ws, "cancelled")
    log = events(cancelled)
    assert log[-4:] == ["attempt_ended", "committed", "request_applied", "released"]
    assert not list((ws.control / "requests").iterdir())
    # A cancelled job keeps its attempt as evidence.
    assert (cancelled.path / "attempts").is_dir()


def test_pause_then_continue_a_ready_job(ws: Workspace, installed: _store.Installed) -> None:
    ref = submit(ws, installed.id, {"start": "succeed"})
    _post(ws, ref, "pause")
    run(ws)
    paused = only(ws, "paused")
    assert not (paused.path / "run").exists()
    _post(ws, paused, "continue")
    run(ws)
    assert only(ws, "succeeded").job_id == ref.job_id


def test_set_priority_of_a_job_no_manager_can_run(ws: Workspace) -> None:
    ref = submit(ws, "local:missing", {"start": "succeed"})
    _post(ws, ref, "set_priority", priority=100)
    run(ws)
    assert only(ws, "ready").priority == 100


def test_delete_a_terminal_job(ws: Workspace, installed: _store.Installed) -> None:
    ref = submit(ws, installed.id, {"start": "fail"})
    run(ws)
    _post(ws, only(ws, "failed"), "delete")
    run(ws)
    assert not [found for state in _kernel.UNOWNED_STATES for found in _kernel.list_jobs(ws, state)]
    assert not list((ws.control / "requests").iterdir())
    assert not (ref.path.exists() or only_or_none(ws, ref.job_id))


def only_or_none(ws: Workspace, job_id: str) -> _kernel.JobRef | None:
    return _kernel.locate(ws, job_id, placement_hint=None, exhaustive=True)


@pytest.mark.parametrize("planted", ["fifo", "symlinked_state"])
def test_content_a_seal_cannot_cover_fails_the_job_and_the_manager_keeps_serving(
    ws: Workspace, installed: _store.Installed, planted: str
) -> None:
    bad = submit(ws, installed.id, {"start": planted})
    run(ws)
    failed = only(ws, "failed")
    assert failed.job_id == bad.job_id
    doc = state_of(failed)
    assert doc.failure is not None and doc.failure["code"] == "protocol_error"
    assert "cannot seal" in str(doc.failure["message"]) and doc.seal is None
    good = submit(ws, installed.id, {"start": "succeed"})
    run(ws)
    assert only(ws, "succeeded").job_id == good.job_id


@pytest.mark.parametrize(("behavior", "code"), [("txn_conflict", "data_conflict"), ("txn_reserved", "protocol_error")])
def test_a_committed_transaction_that_cannot_apply_fails_the_job(
    ws: Workspace, installed: _store.Installed, behavior: str, code: str
) -> None:
    submit(ws, installed.id, {"start": behavior})
    run(ws)
    doc = state_of(only(ws, "failed"))
    assert doc.failure is not None and doc.failure["code"] == code
    assert doc.seal is None


def test_the_workspace_seal_setting_turns_sealing_off(ws: Workspace, installed: _store.Installed) -> None:
    ws.set_setting("seal.succeeded", "off")
    submit(ws, installed.id, {"start": "succeed"})
    run(ws)
    assert state_of(only(ws, "succeeded")).seal == {"disabled": True}


def test_a_pause_posted_during_an_attempt_takes_effect_at_its_commit(
    ws: Workspace, installed: _store.Installed
) -> None:
    ref = submit(ws, installed.id, {"start": "wait_then:finish", "finish": "succeed"})
    with TaskManager(ws) as manager:
        _wait_for(lambda: (manager.tick() or True) and any((ws.jobs / "owned").glob("*/*/run/waiting")))
        _post(ws, ref, "pause")
        (go,) = (ws.jobs / "owned").glob("*/*/run")
        (go / "go").touch()
        _wait_for(lambda: (manager.tick() or True) and not manager.running_attempts)
        manager.run_until_idle(timeout=30)
    paused = only(ws, "paused")
    assert [item["step"] for item in attempts(paused)] == ["start"]
    doc = state_of(paused)
    assert doc.activation is not None and doc.activation["step"] == "finish"
    assert events(paused)[-2:] == ["request_applied", "released"]


def _fail_stop_at(monkeypatch: pytest.MonkeyPatch, target: object, name: str) -> list[str]:
    """Make *target.name* raise OwnerLost once, as if the owner died right there; return the dead owner ids."""

    real = getattr(target, name)
    dead: list[str] = []

    def crash(*arguments: object, **options: object) -> object:
        if not dead:
            dead.append("crashed")
            raise _kernel.OwnerLost("simulated crash")
        return real(*arguments, **options)

    monkeypatch.setattr(target, name, crash)
    return dead


def _crash_then_recover(ws: Workspace, monkeypatch: pytest.MonkeyPatch, target: object, name: str) -> None:
    """Run a manager until it fail-stops at *target.name*, then attest it dead and recover its jobs."""

    _fail_stop_at(monkeypatch, target, name)
    manager = TaskManager(ws)
    with pytest.raises(_kernel.OwnerLost):
        manager.run_until_idle(timeout=30)
    manager.close()
    monkeypatch.undo()
    _kernel.attest_dead(ws, manager.manager_id, by="operator", evidence=[], reason="test crash")
    with cli_owner(ws) as owner:
        _kernel.recover(ws, owner, manager.manager_id)


def test_a_commit_intent_is_finished_by_a_second_owner(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    submit(ws, installed.id, {"start": "succeed"})
    # The first owner dies after writing the commit intent, before the release.
    _crash_then_recover(ws, monkeypatch, TaskManager, "_release")
    ready = only(ws, "ready")
    assert state_of(ready).commit is not None
    run(ws)
    done = only(ws, "succeeded")
    assert len(attempts(done)) == 1, "a decided outcome is never relaunched"
    log = events(done)
    assert log.count("launched") == 1 and "recovered" in log and log[-1] == "released"
    # The replay writes no run-log line of the commit a second time.
    assert log.count("committed") == 1


def test_a_pending_release_is_finished_by_a_second_owner(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    submit(ws, installed.id, {"start": "succeed"})
    # The first owner dies after recording release_to, before the move.
    _crash_then_recover(ws, monkeypatch, _kernel.OwnedJob, "_move_out")
    ready = only(ws, "ready")
    doc = state_of(ready)
    assert doc.release_to is not None and doc.release_to.state == "succeeded" and doc.commit is None
    run(ws)
    done = only(ws, "succeeded")
    assert len(attempts(done)) == 1
    assert events(done)[-2:] == ["claimed", "released"]


def test_an_attempt_left_launching_is_rerun_without_counting(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import manager as manager_module

    submit(ws, installed.id, {"start": "succeed"})
    _crash_then_recover(ws, monkeypatch, manager_module, "start_gated")
    left = state_of(only(ws, "ready"))
    assert left.phase["kind"] == "launching" and left.attempt is not None
    run(ws)
    done = only(ws, "succeeded")
    (record,) = attempts(done)
    assert record["attempt_ordinal"] == 1 and record["is_restart"] is False
    doc = state_of(done)
    assert doc.counters["attempts_total"] == 1 and doc.attempt is not None and doc.attempt["id"] == left.attempt["id"]


def test_the_drain_point_stops_the_attempt_and_releases_the_job(ws: Workspace, installed: _store.Installed) -> None:
    submit(ws, installed.id, {"start": "sleep"}, retry_on=("owner_lost",))
    with TaskManager(ws, end_time=time.time() + 3.0, deadline_margin=1.0, cancel_grace_seconds=1.0) as manager:
        manager.run_until_idle(timeout=30, drain_timeout=10.0, drain_grace_seconds=1.0)
        assert manager.drained == "deadline"
    # The stopped attempt is lost to its owner, retried by policy, and released to wait for another manager.
    ready = only(ws, "ready")
    doc = state_of(ready)
    assert doc.failure is not None and doc.failure["code"] == "owner_lost"
    assert doc.attempt is not None and doc.attempt["unclean"] is True and doc.attempt["started_at"] is None


def test_owner_json_records_the_end_time_and_the_drain_point(tmp_path: Path) -> None:
    ws = Workspace.initialize(tmp_path / "ws", durable=False)
    end = time.time() + 3600.0
    with TaskManager(ws, heartbeat_interval=0.01, end_time=end, deadline_margin=600.0) as manager:
        record = json.loads((ws.control / "owners" / manager.manager_id / "owner.json").read_text(encoding="utf-8"))
        assert (record["end_time"], record["drain_start"]) == (end, end - 600.0)
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        record = json.loads((ws.control / "owners" / manager.manager_id / "owner.json").read_text(encoding="utf-8"))
        assert (record["end_time"], record["drain_start"]) == (None, None)
