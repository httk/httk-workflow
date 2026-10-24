"""Embedded workflows at run time: launch verification, unlock, calls, spawned children and their copies."""

import json
import os
import shutil
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

import v3_helpers as v3
from attempt_fixtures import package, sub_package
from fake_bwrap import write_wrapper
from httk.workflow import TaskManager, Workspace, _confine, _kernel, _requests, _store, removal, scaffold
from httk.workflow._embedded import embedded_digest
from httk.workflow._job import JobDefinition
from httk.workflow.introspection import explain_job
from httk.workflow.precheck import precheck_jobs
from httk.workflow.sdk import ChildSpec

#: The scriptable runner, also recording the runner root it ran from.
_RUNNER = v3.RUNNER.replace(
    'behavior = parameters["script"][step]\n',
    'behavior = parameters["script"][step]\n'
    'pathlib.Path("root-" + step).write_text(os.environ["HTTK_WORKFLOW_RUNNER_ROOT"])\n',
)


def _package(root: Path, name: str = "tests.embedded.run") -> Path:
    return package(root, name, list(v3.STEPS), runner=_RUNNER)


def _embedded_job(
    workspace: Workspace, source: Path, script: dict[str, str], *, placement: str = "project/c", **parameters: Any
) -> _kernel.JobRef:
    job = scaffold.new_job(
        workspace, source, embed_workflow=True, placement=placement, parameters={"script": script, **parameters}
    )
    return v3.find(workspace, job.job_id)


def _failure(ref: _kernel.JobRef) -> dict[str, Any]:
    failure = v3.state_of(ref).failure
    return {} if failure is None else dict(failure)


def _request(workspace: Workspace, job_id: str, action: str, **options: Any) -> str | None:
    with v3.cli_owner(workspace) as owner:
        return removal.request_now(workspace, owner, v3.find(workspace, job_id), action, "t", **options)


def test_an_embedded_job_runs_from_its_verified_workflow_directory(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "succeed"})
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "succeeded"
    job = JobDefinition.from_path(done.path / "job.json")
    root = (done.path / "run" / "root-start").read_text(encoding="utf-8")
    assert root.endswith("/workflow") and done.job_key in root
    assert job.embedded is not None
    assert v3.state_of(done).workflow_pin == {
        "id": job.workflow_id,
        "tree_sha256": job.embedded["tree_sha256"],
        "source": "embedded",
        "workflow_present": True,
    }
    assert embedded_digest(done.path / "workflow") == job.embedded["tree_sha256"]  # kept by the attempt end
    assert _store.list_installed(workspace) == []


@pytest.mark.parametrize(
    ("damage", "message"),
    [
        ("edit", "workflow/ changed since the job was created"),
        ("delete", "the job's workflow/ is missing"),
        ("pycache", "__pycache__"),
        ("symlink", "the job's workflow/ cannot be verified"),
    ],
)
def test_a_locked_embedded_job_refuses_a_changed_tree(tmp_path: Path, damage: str, message: str) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "succeed"})
    tree = ref.path / "workflow"
    if damage == "edit":
        (tree / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    elif damage == "delete":
        shutil.rmtree(tree)
    elif damage == "symlink":
        tree.rename(ref.path / "elsewhere")
        tree.symlink_to("elsewhere")
    else:
        (tree / "__pycache__").mkdir()
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "failed" and _failure(done)["code"] == "protocol_error"
    assert message in _failure(done)["message"]
    assert not (done.path / "run" / "root-start").exists()
    if damage == "delete":
        assert _request(workspace, ref.job_id, "unlock") == "the embedded workflow/ is missing; nothing to copy"


def test_an_unlocked_embedded_job_keeps_and_runs_its_edited_tree(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "fail"})
    v3.run(workspace)
    assert v3.find(workspace, ref.job_id).state == "failed"
    original = (v3.find(workspace, ref.job_id).path / "workflow" / "run").read_bytes()
    assert _request(workspace, ref.job_id, "unlock") is None
    unlocked = v3.find(workspace, ref.job_id)
    assert (unlocked.path / "workflow" / "run").read_bytes() == original  # never replaced
    assert "runner_copy_kept" in v3.events(unlocked)
    runner = unlocked.path / "workflow" / "run"
    runner.write_text(runner.read_text(encoding="utf-8").replace("behavior = parameters", "behavior = 'succeed' #"))
    assert _request(workspace, ref.job_id, "restart") is None
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "succeeded"
    pin = v3.state_of(done).workflow_pin
    assert pin is not None and pin["source"] == "copy" and pin["tree_sha256"] == embedded_digest(done.path / "workflow")


_CALLER = """#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("tests.embedded.caller")


@run.step
def start(a):
    a.call("sub", label="sub")
    a.gather("finish")


@run.step
def finish(a):
    if a.children["sub"].kind == "succeeded":
        a.succeed()
    else:
        a.fail("caller.dependency", "the called workflow did not succeed")


if __name__ == "__main__":
    raise SystemExit(run.main())
"""


def test_an_embedded_job_calls_an_installed_workflow(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    with v3.cli_owner(workspace) as owner:
        sub = _store.install(workspace, owner, sub_package(tmp_path / "sub"))
    caller = package(
        tmp_path / "caller",
        "tests.embedded.caller",
        ["start", "finish"],
        runner=_CALLER,
        extra='\n[workflow.calls]\nsub = "tests.sub"\n',
    )
    job = scaffold.new_job(workspace, caller, embed_workflow=True, placement="project/caller")
    v3.run(workspace)
    assert v3.find(workspace, job.job_id).state == "succeeded"
    (child,) = [ref for ref in _kernel.list_jobs(workspace, "succeeded") if ref.job_id != job.job_id]
    assert JobDefinition.from_path(child.path / "job.json").workflow_id == sub.id
    assert not (child.path / "workflow").exists()


_FAMILY = """#!/usr/bin/env python3
import os
from pathlib import Path

from httk.workflow import ChildSpec, Runner

run = Runner("tests.embedded.family")


@run.step
def start(a):
    a.spawn(ChildSpec(step="leaf", maximum_attempts_per_activation=1), label="kid")
    a.gather("finish")


@run.step
def leaf(a):
    (a.workdir / "root.txt").write_text(os.environ["HTTK_WORKFLOW_RUNNER_ROOT"], encoding="utf-8")
    a.succeed()


@run.step
def finish(a):
    if a.children["kid"].kind == "succeeded":
        a.succeed()
    else:
        a.fail("family.kid", "the child did not succeed")


if __name__ == "__main__":
    raise SystemExit(run.main())
"""


def test_a_spawned_child_of_an_embedded_parent_gets_a_verified_copy(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    source = package(tmp_path / "family", "tests.embedded.family", ["start", "leaf", "finish"], runner=_FAMILY)
    job = scaffold.new_job(workspace, source, embed_workflow=True, placement="project/family")
    v3.run(workspace)
    parent = v3.find(workspace, job.job_id)
    assert parent.state == "succeeded"
    (child,) = [ref for ref in _kernel.list_jobs(workspace, "succeeded") if ref.job_id != job.job_id]
    parent_job = JobDefinition.from_path(parent.path / "job.json")
    child_job = JobDefinition.from_path(child.path / "job.json")
    assert (child_job.workflow_id, child_job.embedded) == (parent_job.workflow_id, parent_job.embedded)
    assert parent_job.embedded is not None
    assert embedded_digest(child.path / "workflow") == parent_job.embedded["tree_sha256"]
    assert os.stat(child.path / "workflow" / "run").st_ino != os.stat(parent.path / "workflow" / "run").st_ino
    root = (child.path / "run" / "root.txt").read_text(encoding="utf-8")
    assert root.endswith("/workflow") and f"/{child.job_key}~" in root
    assert v3.state_of(child).workflow_pin["source"] == "embedded"  # type: ignore[index]
    assert not (child.path / "workflow.partial").exists()


def test_a_child_spec_embeds_only_its_parents_workflow(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "succeed"})
    parent = JobDefinition.from_path(ref.path / "job.json")
    own = ChildSpec(step="start")._job_spec(parent, (parent.workflow_id, parent.workflow_name), "kid")
    assert own.embedded == json.loads(json.dumps(parent.as_mapping()["embedded"]))
    other = ChildSpec(step="start")._job_spec(parent, ("local:other", "other"), "kid")
    assert other.embedded is None


def test_a_child_naming_an_embedded_workflow_its_parent_does_not_embed_is_refused(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    installed = v3.install(workspace, tmp_path / "demo")
    elsewhere = v3.workspace(tmp_path / "elsewhere")
    embedded = scaffold.new_job(elsewhere, _package(tmp_path / "pkg"), embed_workflow=True, parameters={"script": {}})
    foreign = JobDefinition.from_path(embedded.payload / "job.json").as_mapping()
    child = {
        "label": "kid",
        "script": {"start": "succeed"},
        "workflow": foreign["workflow"],
        "embedded": foreign["embedded"],
    }
    ref = v3.submit(
        workspace, installed, {"start": "spawn"}, placement="project/p", parameters={"spawn": {"children": [child]}}
    )
    v3.run(workspace)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "failed" and _failure(done)["code"] == "protocol_error"
    assert "names an embedded workflow its parent does not embed" in _failure(done)["message"]


@pytest.mark.parametrize("leftover", ["partial", "mismatch"])
def test_publication_replays_to_exactly_one_verified_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, leftover: str
) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(
        workspace,
        _package(tmp_path / "pkg"),
        {"start": "spawn", "gather": "succeed"},
        spawn={"children": [{"label": "kid", "script": {"start": "succeed"}}]},
    )
    original = _kernel.submit
    crashed: list[Path] = []

    def crash_once(workspace: Workspace, owner: _kernel.Owner, staged: Path, **options: Any) -> _kernel.JobRef:
        if not crashed and (staged / "workflow").is_dir():
            # A crash after the copy landed, with a half copy of an earlier try beside it, or with the landed copy
            # complete but not the parent's tree (it is discarded and copied again).
            crashed.append(staged)
            if leftover == "partial":
                (staged / "workflow.partial" / "junk").mkdir(parents=True)
            else:
                (staged / "workflow" / "stray.txt").write_text("not the parent's", encoding="utf-8")
            raise OSError("simulated crash during publication")
        return original(workspace, owner, staged, **options)

    monkeypatch.setattr(_kernel, "submit", crash_once)
    v3.run(workspace)
    v3.run(workspace)
    assert crashed
    parent = v3.find(workspace, ref.job_id)
    assert parent.state == "succeeded"
    (child,) = [item for item in _kernel.list_jobs(workspace, "succeeded") if item.job_id != ref.job_id]
    job = JobDefinition.from_path(parent.path / "job.json")
    assert job.embedded is not None and embedded_digest(child.path / "workflow") == job.embedded["tree_sha256"]
    assert not (child.path / "workflow.partial").exists() and not (child.path / "workflow" / "stray.txt").exists()


def test_a_confined_embedded_attempt_sees_its_workflow_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "succeed"})
    (tmp_path / "bin").mkdir()
    bwrap = write_wrapper(tmp_path / "bin" / "bwrap", namespaces=False)
    argvs: list[list[str]] = []
    prepare = _confine.prepare_attempt_sandbox

    def recording(settings: Any, **arguments: Any) -> Any:
        prepared = prepare(settings, **arguments)
        argvs.append(list(prepared.argv))
        return prepared

    monkeypatch.setattr(_confine, "probe_bwrap", lambda settings: True)
    monkeypatch.setattr(_confine, "prepare_attempt_sandbox", recording)
    settings = {"manager.confine": "bwrap", "confine.bwrap": str(bwrap), "confine.readonly_paths": "/usr"}
    with TaskManager(workspace, heartbeat_interval=0.01, setting_overrides=settings) as manager:
        manager.run_until_idle(timeout=120)
    done = v3.find(workspace, ref.job_id)
    assert done.state == "succeeded", _failure(done)
    (argv,) = argvs
    overlay = [index for index, item in enumerate(argv) if item.endswith("/workflow") and done.job_key in item]
    assert overlay and argv[overlay[0] - 2] == "--ro-bind-fd"


def test_children_of_an_unlocked_embedded_parent_copy_its_current_tree(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(
        workspace,
        _package(tmp_path / "pkg"),
        {"start": "spawn", "gather": "succeed"},
        spawn={"children": [{"label": "kid", "script": {"start": "succeed"}}]},
        # The unlocked (unconfined) attempt edits its tree too: its children get the tree as it is at publication.
        files={"start": {"../workflow/added.txt": "during the attempt"}},
    )
    assert _request(workspace, ref.job_id, "unlock") is None
    runner = v3.find(workspace, ref.job_id).path / "workflow" / "run"
    runner.write_text(runner.read_text(encoding="utf-8") + "# edited while unlocked\n", encoding="utf-8")
    v3.run(workspace)
    parent = v3.find(workspace, ref.job_id)
    assert parent.state == "succeeded"
    (child,) = [item for item in _kernel.list_jobs(workspace, "succeeded") if item.job_id != ref.job_id]
    edited = embedded_digest(parent.path / "workflow")
    assert embedded_digest(child.path / "workflow") == edited and (child.path / "workflow" / "added.txt").is_file()
    doc = v3.state_of(child)
    assert doc.unlocked is not None and doc.workflow_pin is not None
    assert (doc.workflow_pin["source"], doc.workflow_pin["tree_sha256"]) == ("copy", edited)


def test_inspection_never_reports_an_embedded_workflow_as_not_installed(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "succeed"})
    (finding,) = precheck_jobs(workspace)
    assert finding["runner"] == {"status": "ok", "ok": True} and finding["calls"] == []
    checks = {check.name: check for check in explain_job(workspace, ref).checks}
    assert checks["installed workflow"].satisfied is True


def test_a_damaged_embedded_job_keeps_its_workflow(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "fail"})
    v3.run(workspace)
    failed = v3.find(workspace, ref.job_id)
    digest = embedded_digest(failed.path / "workflow")
    # No readable pin: the tree stays (it is the only copy of the job's code).
    (failed.path / "state.json").write_text("{broken", encoding="utf-8")
    _requests.post(
        workspace,
        action="set_priority",
        job_id=ref.job_id,
        placement=PurePosixPath("project/c"),
        operator="t",
        reason="t",
        priority=7,
    )
    v3.run(workspace)
    damaged = v3.find(workspace, ref.job_id)
    assert damaged.state == "failed" and _failure(damaged)["code"] == "protocol_error"
    assert embedded_digest(damaged.path / "workflow") == digest
    assert _request(workspace, ref.job_id, "restart") is None
    v3.run(workspace)
    revived = v3.find(workspace, ref.job_id)
    # It ran again from its verified tree: its own failure, not a launch refusal.
    assert revived.state == "failed" and _failure(revived)["code"] == "converge"


def test_a_damaged_job_whose_job_json_is_unreadable_keeps_its_workflow(tmp_path: Path) -> None:
    workspace = v3.workspace(tmp_path / "workspace")
    ref = _embedded_job(workspace, _package(tmp_path / "pkg"), {"start": "succeed"})
    # Readable for the kernel's header, invalid as a definition: whether the job embeds its workflow is unknown.
    mapping = json.loads((ref.path / "job.json").read_text(encoding="utf-8"))
    (ref.path / "job.json").write_text(json.dumps({**mapping, "seal_succeeded": "maybe"}), encoding="utf-8")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        owned = _kernel.claim(workspace, manager.owner, ref)
        assert owned is not None
        manager._fail_damaged(owned, "job.json is damaged")
    damaged = v3.find(workspace, ref.job_id)
    assert damaged.state == "failed" and (damaged.path / "workflow" / "run").is_file()
