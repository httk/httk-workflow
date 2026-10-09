"""Labeled children, typed join observations, and runner-declared retries."""

import json
from pathlib import Path

import pytest

from attempt_fixtures import every_job, find, package, run_manager, state
from httk.workflow import Workspace, _kernel, _store
from httk.workflow.runtime_builders import JobSpec, prepare_job_payload

#: One SDK-free runner for parent and children: ``branch`` spawns the children ``parameters.labels`` lists
#: (label, mode) with a join that names no labels, ``gather`` records its context children, ``run`` is a
#: child that succeeds or declares a failure, and ``only`` declares a retryable failure.
_RUNNER = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
workdir = Path(os.environ["HTTK_WORKFLOW_WORKDIR"])
job = json.loads((Path(os.environ["HTTK_WORKFLOW_JOB_DIR"]) / "job.json").read_text())
step = context["step"]
temporary = control / "outcome.tmp.test"
temporary.mkdir()
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2)
if step == "gather":
    (workdir / "children.json").write_text(json.dumps(context["children"], sort_keys=True))
    outcome.update(action="succeed")
elif step == "run":
    if job["parameters"]["mode"] == "succeed":
        outcome.update(action="succeed")
    else:
        outcome.update(action="fail", failure={"code": "child.broken", "message": "the child declared a failure"})
elif step == "only":
    count_path = workdir / "attempts"
    count = int(count_path.read_text()) + 1 if count_path.exists() else 1
    count_path.write_text(str(count))
    outcome.update(
        action="fail",
        failure={
            "code": "vasp.nonconvergent",
            "message": "electronic minimization did not converge",
            "retryable": True,
        },
    )
else:
    entries = []
    for label, mode in job["parameters"]["labels"]:
        child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), label + mode))
        child_key = "child--" + child_id
        child_dir = temporary / "children" / "jobs" / child_key
        child_dir.mkdir(parents=True)
        child = dict(job, id=child_id, tag="child", name="Child " + label, placement="project/children")
        child.update(initial_step="run", parameters={"mode": mode})
        child["parent"] = {
            "workspace_id": context["workspace_id"],
            "job_id": context["job_id"],
            "job_key": context["job_key"],
            "placement": context["placement"],
            "activation_id": context["activation_id"],
            "spawn_id": str(uuid.uuid4()),
        }
        (child_dir / "job.json").write_text(json.dumps(child))
        entry = {"job_key": child_key, "placement": "project/children", "spawn_id": child["parent"]["spawn_id"]}
        if label:
            entry["label"] = label
        entries.append(entry)
    spawn = {"format": "httk-workflow-spawn", "format_version": 2, "children": entries}
    (temporary / "children" / "spawn.json").write_text(json.dumps(spawn))
    # The join names no labels: the manager must carry them over from the spawn set that registered them.
    references = [
        {
            "workspace_id": context["workspace_id"],
            "job_id": entry["job_key"].split("--")[1],
            "job_key": entry["job_key"],
            "placement_hint": entry["placement"],
        }
        for entry in entries
    ]
    outcome.update(action="wait", next_step="gather", join={"children": references, "condition": "all_terminal"})
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


@pytest.fixture()
def workspace(tmp_path: Path) -> Workspace:
    ws = Workspace.initialize(tmp_path / "workspace", durable=False)
    owner = _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})
    try:
        source = package(tmp_path / "labels", "tests.labels", ["branch", "gather", "run", "only"], runner=_RUNNER)
        _store.install(ws, owner, source)
    finally:
        owner.close()
    return ws


def _submit(
    workspace: Workspace,
    *,
    labels: tuple[tuple[str, str], ...] = (),
    initial_step: str = "branch",
    attempts_per_activation: int = 1,
) -> str:
    owner = _kernel.register_owner(workspace, kind="cli", label="test", allocation=None, advertised={})
    try:
        staging = owner.scratch("submit") / "job"
        spec = JobSpec(
            name="Labeled children parent",
            workflow_id="local:tests.labels",
            workflow_name="tests.labels",
            tag="parent",
            placement="project/parent",
            initial_step=initial_step,
            maximum_attempts_per_activation=attempts_per_activation,
            maximum_total_attempts=10,
            parameters={"labels": [list(item) for item in labels]},
        )
        job = prepare_job_payload(staging, spec)
        _kernel.submit(workspace, owner, staging)
    finally:
        owner.close()
    return job.id


def test_a_spawn_child_without_a_label_is_a_protocol_error(workspace: Workspace) -> None:
    job_id = _submit(workspace, labels=(("", "succeed"),))
    run_manager(workspace, timeout=60.0)
    failed = find(workspace, job_id)
    assert failed.state == "failed"
    failure = state(failed).failure
    assert failure is not None and failure["code"] == "protocol_error"
    assert "spawn child" in str(failure["message"])
    # Nothing was registered: an unusable spawn set never becomes work.
    assert [found.job_id for found in every_job(workspace)] == [job_id]


def test_duplicate_spawn_labels_are_a_protocol_error(workspace: Workspace) -> None:
    job_id = _submit(workspace, labels=(("alpha", "succeed"), ("alpha", "fail")))
    run_manager(workspace, timeout=60.0)
    failed = find(workspace, job_id)
    assert failed.state == "failed"
    failure = state(failed).failure
    assert failure is not None and failure["code"] == "protocol_error"
    assert "not unique" in str(failure["message"])


def test_gather_step_reads_labeled_child_observations_from_its_context(workspace: Workspace) -> None:
    job_id = _submit(workspace, labels=(("alpha", "succeed"), ("beta", "fail")))
    run_manager(workspace, timeout=60.0)

    parent = find(workspace, job_id)
    assert parent.state == "succeeded"
    observations = json.loads((parent.path / "run" / "children.json").read_text(encoding="utf-8"))
    assert [item["label"] for item in observations] == ["alpha", "beta"]
    by_label = {str(item["label"]): item for item in observations}

    succeeded = by_label["alpha"]
    child = find(workspace, str(succeeded["job_id"]))
    assert succeeded["kind"] == "succeeded"
    assert succeeded["failure"] is None
    # The manager located the child when it launched the gathering attempt.
    assert succeeded["payload_path"] == child.path.relative_to(workspace.root).as_posix()
    assert succeeded["workdir_path"] == f"{succeeded['payload_path']}/run"
    assert succeeded["data_path"] == f"{succeeded['payload_path']}/data"
    assert (workspace.root / str(succeeded["workdir_path"])).is_dir()

    failed = by_label["beta"]
    assert failed["kind"] == "failed"
    assert failed["failure"]["code"] == "child.broken"
    assert failed["failure"]["message"] == "the child declared a failure"


def test_a_retryable_declared_failure_retries_until_the_budget_is_exhausted(workspace: Workspace) -> None:
    job_id = _submit(workspace, initial_step="only", attempts_per_activation=3)
    run_manager(workspace, timeout=60.0)

    failed = find(workspace, job_id)
    assert failed.state == "failed"
    doc = state(failed)
    # The runner-declared failure is what an operator finally sees, and the
    # activation was repeated exactly as often as the budget permitted.
    assert doc.failure is not None and doc.failure["code"] == "vasp.nonconvergent"
    assert doc.failure["message"] == "electronic minimization did not converge"
    assert doc.attempt is not None and doc.attempt["ordinal"] == 3
    assert (failed.path / "run" / "attempts").read_text(encoding="utf-8") == "3"


def test_a_retryable_failure_is_not_retried_without_a_remaining_attempt(workspace: Workspace) -> None:
    job_id = _submit(workspace, initial_step="only", attempts_per_activation=1)
    run_manager(workspace, timeout=60.0)

    failed = find(workspace, job_id)
    assert failed.state == "failed"
    doc = state(failed)
    assert doc.failure is not None and doc.failure["code"] == "vasp.nonconvergent"
    assert (failed.path / "run" / "attempts").read_text(encoding="utf-8") == "1"
