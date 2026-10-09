"""End-to-end jobs of installed workflows with SDK-free runners, and the runner-side builders they publish with."""

import json
import os
import uuid
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace, _kernel
from httk.workflow.errors import TransactionError
from httk.workflow.runtime_builders import JobSpec, ReplayableWorkdirBatch, prepare_job_payload, replay_transaction
from httk.workflow.scaffold import new_job
from test_job_creation import install, workspace_at

_OUTCOME = """
def publish(context, control, **members):
    draft = control / "outcome.tmp.test"
    draft.mkdir()
    outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
    outcome.update(format="httk-workflow-outcome", format_version=2, **members)
    (draft / "outcome.json").write_text(json.dumps(outcome))
    os.rename(draft, control / "outcome.ready")
"""

_PREAMBLE = (
    "#!/usr/bin/env python3\nimport json, os, sys\nfrom pathlib import Path\n"
    + _OUTCOME
    + 'context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])\n'
    + 'control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])\n'
    + 'run = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])\n'
)

_TWO_STEP_RUNNER = (
    _PREAMBLE
    + """(run / "steps.txt").open("a").write(context["step"] + "\\n")
if context["step"] == "prepare":
    publish(context, control, action="advance", next_step="collect")
else:
    publish(context, control, action="succeed")
"""
)


def _package(root: Path, runner: str, steps: tuple[str, ...] = ("prepare", "collect")) -> Path:
    root.mkdir(parents=True)
    (root / "httk_workflow.toml").write_text(
        f'[workflow]\nname = "tests.example"\n\n[workflow.runner]\nsteps = {json.dumps(list(steps))}\n'
        f'initial_step = "{steps[0]}"\n',
        encoding="utf-8",
    )
    (root / "run").write_text(runner, encoding="utf-8")
    (root / "run").chmod(0o755)
    return root


def _run(ws: Workspace) -> None:
    with TaskManager(ws) as manager:
        manager.run_until_idle(timeout=60)


def _only(ws: Workspace, state: str) -> _kernel.JobRef:
    (ref,) = _kernel.list_jobs(ws, state)
    return ref


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return workspace_at(tmp_path / "workspace")


def test_submit_and_run_multistep_persistent_job(ws: Workspace, tmp_path: Path) -> None:
    job = new_job(ws, _package(tmp_path / "package", _TWO_STEP_RUNNER), placement="project/a", install=True)
    _run(ws)
    done = _only(ws, "succeeded")
    assert done.job_id == job.job_id and done.path.parent == ws.jobs / "succeeded" / "project" / "a"
    assert (done.path / "run" / "steps.txt").read_text(encoding="utf-8").splitlines() == ["prepare", "collect"]


def test_a_job_spec_job_with_retry_restarts_after_a_failed_exit(ws: Workspace, tmp_path: Path) -> None:
    runner = (
        _PREAMBLE
        + """count_path = run / "count"
count = int(count_path.read_text()) + 1 if count_path.exists() else 1
count_path.write_text(str(count))
if count == 1:
    sys.exit(9)
assert context["is_restart"] is True and context["attempt_reason"] == "process_failure"
publish(context, control, action="succeed")
"""
    )
    installed = install(ws, _package(tmp_path / "package", runner, ("start",)))
    spec = JobSpec(
        name="restart",
        workflow_id=installed.id,
        workflow_name=installed.name,
        placement="project/restart",
        maximum_attempts_per_activation=3,
        retry_on=("process_failure",),
    )
    owner = _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})
    try:
        staging = owner.scratch("submit") / "job"
        definition = prepare_job_payload(staging, spec)
        assert definition.retry_policy.retry_on == frozenset({"process_failure"})
        _kernel.submit(ws, owner, staging)
    finally:
        owner.close()
    _run(ws)
    done = _only(ws, "succeeded")
    assert done.job_id == definition.id
    assert (done.path / "run" / "count").read_text() == "2"


def test_committed_transactions_land_in_data(ws: Workspace, tmp_path: Path) -> None:
    runner = (
        _PREAMBLE
        + """staged = control / "txn" / "000000.tmp" / "data"
staged.mkdir(parents=True)
(staged / "result.txt").write_text("complete\\n")
os.rename(control / "txn" / "000000.tmp", control / "txn" / "000000")
publish(context, control, action="succeed")
"""
    )
    new_job(ws, _package(tmp_path / "package", runner, ("start",)), install=True)
    _run(ws)
    assert (_only(ws, "succeeded").path / "data" / "result.txt").read_text(encoding="utf-8") == "complete\n"


def test_a_child_spawned_with_the_runtime_builders_is_validated_published_and_joined(
    ws: Workspace, tmp_path: Path
) -> None:
    # The parent publishes through OutcomeDraft and JobSpec exactly as the SDK does, so the child job.json
    # and spawn.json it writes must be what the manager's child validation accepts.
    runner = (
        _PREAMBLE
        + """from httk.workflow.runtime import AttemptContext
from httk.workflow.runtime_builders import JobSpec, OutcomeDraft
job = json.loads((Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
if context["step"] == "start":
    draft = OutcomeDraft(AttemptContext.from_mapping(context), control)
    spec = JobSpec(name="child", workflow_id=job["workflow"]["id"], workflow_name=job["workflow"]["name"],
                   initial_step="child", tag="kid")
    draft.add_child_job(spec.as_mapping(), "project/children", label="kid")
    draft.publish("wait", next_step="aggregate")
elif context["step"] == "aggregate":
    (run / "children.json").write_text(json.dumps(context["children"]))
    publish(context, control, action="succeed")
else:
    publish(context, control, action="succeed")
"""
    )
    parent = new_job(
        ws, _package(tmp_path / "package", runner, ("start", "child", "aggregate")), placement="project", install=True
    )
    _run(ws)
    done = {ref.job_id: ref for ref in _kernel.list_jobs(ws, "succeeded")}
    assert len(done) == 2 and parent.job_id in done
    (child,) = [ref for job_id, ref in done.items() if job_id != parent.job_id]
    assert child.path.parent == ws.jobs / "succeeded" / "project" / "children"
    document = json.loads((child.path / "job.json").read_bytes())
    assert document["placement"] == "project/children" and document["initial_step"] == "child"
    assert document["parent"]["job_id"] == parent.job_id and document["parent"]["placement"] == "project"
    (observed,) = json.loads((done[parent.job_id].path / "run" / "children.json").read_text(encoding="utf-8"))
    assert observed["label"] == "kid" and observed["kind"] == "succeeded"
    assert observed["payload_path"] == child.path.relative_to(ws.root).as_posix()


def test_a_workdir_batch_replays_idempotently(tmp_path: Path) -> None:
    workdir = tmp_path / "run"
    workdir.mkdir()
    (workdir / "old").mkdir()
    (workdir / "old" / "stale").write_text("stale", encoding="utf-8")
    (workdir / "doomed").write_text("x", encoding="utf-8")
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_text("new", encoding="utf-8")
    batch = ReplayableWorkdirBatch.initialize(workdir)
    batch.transaction.make_dir("dir", "made/deep")
    batch.transaction.put_file("file", source / "file", "made/file")
    batch.transaction.put_tree("tree", source, "copied")
    batch.transaction.put_tree("replace", source, "old", replace=True)
    batch.transaction.remove("remove", "doomed")
    batch.transaction.remove("absent", "never", missing_ok=True)
    ready = batch.seal()
    # A crash after the replay applied everything but before the batch was retired: replaying again is a no-op.
    assert replay_transaction(ready, workdir, expected_generation=0)
    (applied,) = ReplayableWorkdirBatch.recover(workdir)
    assert applied.parent.name == "workdir-applied"
    assert (workdir / "made" / "deep").is_dir()
    assert (workdir / "made" / "file").read_text(encoding="utf-8") == "new"
    assert (workdir / "copied" / "file").read_text(encoding="utf-8") == "new"
    assert sorted(os.listdir(workdir / "old")) == ["file"]
    assert not (workdir / "doomed").exists()
    with pytest.raises(TransactionError, match="stale"):
        replay_transaction(applied, workdir, expected_generation=1)


def test_a_replay_refuses_a_corrupt_source(tmp_path: Path) -> None:
    workdir = tmp_path / "run"
    workdir.mkdir()
    source = tmp_path / "file"
    source.write_text("good", encoding="utf-8")
    batch = ReplayableWorkdirBatch.initialize(workdir)
    batch.transaction.put_file(str(uuid.uuid4().hex[:8]), source, "file")
    ready = batch.seal()
    (staged,) = (ready / "payload").iterdir()
    staged.write_text("tampered", encoding="utf-8")
    with pytest.raises(TransactionError, match="digest mismatch"):
        replay_transaction(ready, workdir, expected_generation=0)
    assert not (workdir / "file").exists()
