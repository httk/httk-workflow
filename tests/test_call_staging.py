"""``Attempt.call`` stages the callee's runner; the trusted manager publishes it.

A running step never writes the workspace runner store. A called runner file or
workflow directory is copied into the attempt's outcome draft at
``children/runners/<store name>``, and the manager, when it commits the outcome,
copies that entry into its own staging, verifies the copy against the digest the
child pins, and publishes it before the child exists.
"""

import json
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from httk.core.digests import sha256_file, tree_digest

from httk.workflow import Attempt, TaskManager, Workspace
from httk.workflow._logging import reset_logging
from httk.workflow._manager_commit import runner_staging_name
from httk.workflow.models import JobDefinition, Marker
from httk.workflow.scaffold import resolve_workflow

_SRC = str(Path(__file__).parents[1] / "src")

_PARENT_RUNNER = """#!/usr/bin/env python3
import json
import os
import sys

sys.path.insert(0, "@SRC@")

from pathlib import Path

from httk.workflow import Runner

run = Runner("tests.stager")
STORE = Path("@STORE@")


@run.step
def start(a):
    reference = a.call("@TARGET@", label="callee")
    listing = sorted(os.listdir(STORE)) if STORE.is_dir() else []
    (a.workdir / "during.json").write_text(json.dumps({"store": listing, "job_key": reference.job_key}))
    a.gather("finish", on_impossible="triage")


@run.step
def finish(a):
    a.succeed()


@run.step
def triage(a):
    a.fail("caller.dependency", "the called workflow did not succeed")


raise SystemExit(run.main())
"""

_SUB_RUNNER = """#!/usr/bin/env python3
import sys

sys.path.insert(0, "@SRC@")

from httk.workflow import Runner

run = Runner("tests.staged.sub")


@run.step
def run_sub(a):
    a.succeed()


raise SystemExit(run.main())
"""

_PACKAGE_RUNNER = """#!/usr/bin/env python3
import sys

sys.path.insert(0, "@SRC@")

from httk.workflow import Runner

run = Runner("tests.staged.package")


@run.step
def start(a):
    if @HOOKED@ and not (a.payload / "instantiated.txt").is_file():
        a.fail("tests.hook", "the instantiate hook did not run")
        return
    a.succeed()


raise SystemExit(run.main())
"""

_PYTHON_HOOK = """def instantiate(context):
    (context.payload / "instantiated.txt").write_text("python hook", encoding="utf-8")
"""

_EXECUTABLE_HOOK = """#!/usr/bin/env python3
import sys

sys.path.insert(0, "@SRC@")

from pathlib import Path

from httk.workflow.hookapi import instantiate_main


def instantiate(request):
    Path("instantiated.txt").write_text("executable hook", encoding="utf-8")
    return {"parameters": {}}


instantiate_main(instantiate)
"""


class _Killed(BaseException):
    """A manager interrupted mid-commit, as a crash would."""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    reset_logging()
    yield
    reset_logging()


def _executable(path: Path, text: str) -> Path:
    path.write_text(text.replace("@SRC@", _SRC), encoding="utf-8")
    path.chmod(0o755)
    return path


def _runner_file(tmp_path: Path) -> Path:
    return _executable(tmp_path / "sub_runner.py", _SUB_RUNNER)


def _package(tmp_path: Path, hook: str | None = None) -> Path:
    package = tmp_path / "staged_package"
    package.mkdir()
    manifest = '[workflow]\nname = "tests.staged.package"\n\n[workflow.runner]\nsteps = ["start"]\n'
    if hook == "python":
        manifest += '\n[workflow.instantiate]\nfile = "instantiate.py"\n'
        (package / "instantiate.py").write_text(_PYTHON_HOOK, encoding="utf-8")
    elif hook == "executable":
        manifest += '\n[workflow.instantiate]\nfile = "hook"\n'
        _executable(package / "hook", _EXECUTABLE_HOOK)
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (package / "lib").mkdir()
    (package / "lib" / "data.txt").write_text("package data\n", encoding="utf-8")
    _executable(package / "run", _PACKAGE_RUNNER.replace("@HOOKED@", repr(hook is not None)))
    return package


def _submit_parent(tmp_path: Path, workspace: Workspace, target: Path) -> tuple[Marker, str]:
    """Submit a parent whose first step calls *target* and records the store it saw."""

    job_id = str(uuid.uuid4())
    payload = tmp_path / "source" / "parent"
    (payload / "files").mkdir(parents=True)
    source = _PARENT_RUNNER.replace("@STORE@", str(workspace.runners)).replace("@TARGET@", str(target))
    _executable(payload / "files" / "runner", source)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": "stager",
        "name": "Stager",
        "workflow": "tests.stager",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": "none"},
        "initial_step": "start",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"maximum_attempts_per_activation": 1, "retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return workspace.submit(payload, "project/stager"), job_id


def _outcome(workspace: Workspace, job_id: str) -> tuple[str, str | None, str]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    if marker.kind != "failed":
        return marker.kind, None, ""
    failure = workspace.read_state(marker)["failure"]
    return marker.kind, failure["code"], failure["message"]


def _published_children(workspace: Workspace) -> list[Path]:
    return sorted(workspace.root.glob("project/stager/callee--*"))


def _store(workspace: Workspace) -> list[str]:
    return sorted(os.listdir(workspace.runners)) if workspace.runners.is_dir() else []


def _staging_leftovers(workspace: Workspace) -> list[str]:
    tmp = workspace.control / "tmp"
    return (
        sorted(entry.name for entry in tmp.iterdir() if entry.name.startswith(("runner.", "child.")))
        if (tmp.is_dir())
        else []
    )


def _staged_drafts(workspace: Workspace, parent: Marker) -> list[Path]:
    installed = workspace.payload_path(parent.placement, parent.job_key)
    return sorted(installed.glob("attempts/*/outcome.ready/children/runners/*"))


def _snapshot(directory: Path) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for current, directories, files in os.walk(directory):
        for name in (*directories, *files):
            path = Path(current, name)
            content = path.read_bytes() if path.is_file() and not path.is_symlink() else None
            state[str(path.relative_to(directory))] = (path.lstat().st_mode, content)
    return state


def _digest(path: Path) -> str:
    return tree_digest(path) if path.is_dir() else sha256_file(path)


def _run(workspace: Workspace) -> None:
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=90.0)


def _assert_published_and_ran(tmp_path: Path, workspace: Workspace, parent: Marker, job_id: str) -> Path:
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    installed = workspace.payload_path(parent.placement, parent.job_key)
    during = json.loads((installed / "run" / "during.json").read_text(encoding="utf-8"))
    # While the parent ran, the store did not hold the runner it called.
    assert during["store"] == []
    child = workspace.find_marker_at(during["job_key"], parent.placement)
    assert child is not None and child.kind == "succeeded"
    definition = JobDefinition.from_path(workspace.payload_path(child.placement, child.job_key) / "job.json")
    assert definition.runner_source == "workspace" and definition.runner_sha256 is not None
    stored = workspace.runner_store_path(definition.runner_path)
    # After the commit the store holds it under the pinned name and digest.
    assert _store(workspace) == [definition.runner_path.as_posix()]
    assert _digest(stored) == definition.runner_sha256
    assert not _staging_leftovers(workspace)
    return stored


def test_a_called_runner_file_is_published_by_the_manager_not_the_step(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    sub = _runner_file(tmp_path)
    parent, job_id = _submit_parent(tmp_path, workspace, sub)

    _run(workspace)

    stored = _assert_published_and_ran(tmp_path, workspace, parent, job_id)
    assert stored.name == resolve_workflow(sub).store_name
    assert stored.read_bytes() == sub.read_bytes()


def test_a_called_workflow_directory_is_published_by_the_manager_not_the_step(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _package(tmp_path)
    parent, job_id = _submit_parent(tmp_path, workspace, package)

    _run(workspace)

    stored = _assert_published_and_ran(tmp_path, workspace, parent, job_id)
    assert stored.name == resolve_workflow(package).store_name
    assert (stored / "lib" / "data.txt").read_text(encoding="utf-8") == "package data\n"


@pytest.mark.parametrize("hook", ["python", "executable"])
def test_an_instantiate_hook_runs_from_the_staged_copy_with_an_empty_store(tmp_path: Path, hook: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    package = _package(tmp_path, hook)
    parent, job_id = _submit_parent(tmp_path, workspace, package)
    assert _store(workspace) == []

    _run(workspace)

    _assert_published_and_ran(tmp_path, workspace, parent, job_id)
    installed = workspace.payload_path(parent.placement, parent.job_key)
    during = json.loads((installed / "run" / "during.json").read_text(encoding="utf-8"))
    child_payload = workspace.payload_path(parent.placement, during["job_key"])
    assert (child_payload / "instantiated.txt").read_text(encoding="utf-8") == f"{hook} hook"


def _before_commit(monkeypatch: pytest.MonkeyPatch, parent: Marker, change: Any) -> None:
    """Apply *change* to the parent's published draft just before the manager commits it."""

    real_process_committing = TaskManager._process_committing

    def change_the_draft_first(self: TaskManager, committing: Marker) -> None:
        if committing.job_key == parent.job_key and committing.kind == "committing":
            installed = self.workspace.payload_path(committing.placement, committing.job_key)
            (draft,) = installed.glob("attempts/*/outcome.ready")
            change(draft)
        real_process_committing(self, committing)

    monkeypatch.setattr(TaskManager, "_process_committing", change_the_draft_first)


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_a_staged_runner_changed_after_scaffolding_fails_the_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    target = _runner_file(tmp_path) if kind == "file" else _package(tmp_path)
    parent, job_id = _submit_parent(tmp_path, workspace, target)

    def swap(draft: Path) -> None:
        (staged,) = (draft / "children" / "runners").iterdir()
        changed = staged / "run" if staged.is_dir() else staged
        changed.write_text("#!/bin/sh\necho swapped\n", encoding="utf-8")

    _before_commit(monkeypatch, parent, swap)
    _run(workspace)

    kind_, code, message = _outcome(workspace, job_id)
    assert (kind_, code) == ("failed", "protocol_error")
    assert "but its child pins" in message
    assert _store(workspace) == []
    assert not _published_children(workspace)
    assert not [name for name in _staging_leftovers(workspace) if name.startswith("runner.")]


def test_a_different_runner_published_after_the_call_fails_the_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    sub = _runner_file(tmp_path)
    store_name = resolve_workflow(sub).store_name
    impostor = tmp_path / "impostor.py"
    impostor.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    parent, job_id = _submit_parent(tmp_path, workspace, sub)
    snapshots: list[dict[str, Any]] = []

    def another_job_publishes_meanwhile(draft: Path) -> None:
        # The call saw an empty store; another job takes the name before the commit.
        workspace.publish_runner(impostor, name=store_name)
        snapshots.append(_snapshot(workspace.runners))

    _before_commit(monkeypatch, parent, another_job_publishes_meanwhile)
    _run(workspace)
    (before,) = snapshots

    kind, code, message = _outcome(workspace, job_id)
    assert (kind, code) == ("failed", "protocol_error")
    assert f"cannot publish staged workspace runner {store_name}" in message
    assert "different digest" in message
    assert _snapshot(workspace.runners) == before
    assert not _published_children(workspace)


def test_a_different_runner_already_in_the_store_is_refused_inside_the_step(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    sub = _runner_file(tmp_path)
    store_name = resolve_workflow(sub).store_name
    impostor = tmp_path / "impostor.py"
    impostor.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    workspace.publish_runner(impostor, name=store_name)
    attempt = _in_process_attempt(tmp_path, workspace.root)

    with pytest.raises(FileExistsError, match=f"workspace runner {store_name} already holds a different digest"):
        attempt.call(sub, label="sub")

    # The step can catch it; nothing was staged and the store is unchanged.
    assert not list(attempt.control.glob("outcome.tmp.*/children"))
    assert _store(workspace) == [store_name]
    assert sha256_file(workspace.runner_store_path(store_name)) == sha256_file(impostor)


@pytest.mark.parametrize("where", ["entry", "runners directory", "inside a tree"])
def test_a_symlink_planted_in_the_staged_runners_fails_the_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, where: str
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_text("secret\n", encoding="utf-8")
    target = _package(tmp_path) if where == "inside a tree" else _runner_file(tmp_path)
    parent, job_id = _submit_parent(tmp_path, workspace, target)

    def plant(draft: Path) -> None:
        runners = draft / "children" / "runners"
        (staged,) = runners.iterdir()
        if where == "entry":
            staged.unlink()
            staged.symlink_to(outside / "secret")
        elif where == "runners directory":
            mirror = outside / "mirror"
            mirror.mkdir()
            (mirror / staged.name).write_bytes(staged.read_bytes())
            staged.unlink()
            runners.rmdir()
            runners.symlink_to(mirror, target_is_directory=True)
        else:
            (staged / "lib" / "data.txt").unlink()
            (staged / "lib" / "data.txt").symlink_to(outside / "secret")

    _before_commit(monkeypatch, parent, plant)
    before = _snapshot(outside) if where != "runners directory" else None
    _run(workspace)

    kind, code, message = _outcome(workspace, job_id)
    assert (kind, code) == ("failed", "protocol_error")
    assert "symlink" in message
    assert _store(workspace) == []
    assert not _published_children(workspace)
    assert not [name for name in _staging_leftovers(workspace) if name.startswith("runner.")]
    assert (outside / "secret").read_text(encoding="utf-8") == "secret\n"
    if before is not None:
        assert _snapshot(outside) == before


def test_a_commit_interrupted_after_publishing_a_staged_runner_completes_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    workspace = Workspace.initialize(root)
    sub = _runner_file(tmp_path)
    parent, job_id = _submit_parent(tmp_path, workspace, sub)
    real_publish_runner = Workspace.publish_runner

    def crash_after_publishing(self: Workspace, source: Any, **options: Any) -> dict[str, object]:
        reference = real_publish_runner(self, source, **options)
        if Path(source).name.startswith("runner."):
            raise _Killed()
        return reference

    monkeypatch.setattr(Workspace, "publish_runner", crash_after_publishing)
    with TaskManager(Workspace(root), heartbeat_interval=0.01) as dying:
        with pytest.raises(_Killed):
            dying.run_until_idle(timeout=90.0)
        for attempt in list(dying._running.values()):
            attempt.process.kill()
            attempt.process.wait(timeout=30)
        dying._running.clear()
    monkeypatch.undo()

    workspace = Workspace(root)
    interrupted = workspace.find_marker_by_id(job_id)
    assert interrupted is not None and interrupted.kind == "committing"
    # The runner is in the store, the child is not published yet, and the draft
    # still stages the runner for the replay.
    assert _store(workspace) == [resolve_workflow(sub).store_name]
    assert not _published_children(workspace)
    assert len(_staged_drafts(workspace, interrupted)) == 1

    _run(workspace)

    _assert_published_and_ran(tmp_path, workspace, parent, job_id)
    assert len(_published_children(workspace)) == 1


def _in_process_attempt(tmp_path: Path, workspace_root: Path) -> Attempt:
    """Bind an in-process attempt to a real workspace, without a manager."""

    control = tmp_path / "control"
    control.mkdir()
    (tmp_path / "run").mkdir()
    context = {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        "workspace_id": str(uuid.uuid4()),
        "job_id": str(uuid.uuid4()),
        "job_key": f"job--{uuid.uuid4()}",
        "placement": "project/a",
        "payload": str(tmp_path / "job"),
        "step": "start",
        "activation_id": str(uuid.uuid4()),
        "attempt_id": str(uuid.uuid4()),
        "data_generation": None,
    }
    return Attempt.initialize(
        {
            "HTTK_WORKFLOW_CONTEXT": json.dumps(context),
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_JOB_DIR": str(tmp_path / "job"),
            "HTTK_WORKFLOW_WORKDIR": str(tmp_path / "run"),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(workspace_root),
        }
    )


def test_a_call_stages_into_the_draft_and_a_failed_call_leaves_nothing_staged(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    sub = _runner_file(tmp_path)
    attempt = _in_process_attempt(tmp_path, workspace.root)

    with pytest.raises(ValueError, match="does not exist"):
        attempt.call(sub, label="broken", files={"input.txt": tmp_path / "absent.txt"})
    assert not list(attempt.control.glob("outcome.tmp.*/children"))

    reference = attempt.call(sub, label="sub")
    with pytest.raises(ValueError, match="does not exist"):
        attempt.call(sub, label="again", files={"input.txt": tmp_path / "absent.txt"})
    # The runner the first call staged is kept for its child; nothing reached the store.
    (staged,) = attempt.control.glob("outcome.tmp.*/children/runners/*")
    assert staged.name == resolve_workflow(sub).store_name
    assert staged.read_bytes() == sub.read_bytes()
    child = json.loads((staged.parents[1] / "jobs" / reference.job_key / "job.json").read_text(encoding="utf-8"))
    pinned = {key: child["runner"][key] for key in ("source", "path", "sha256")}
    assert pinned == {"source": "workspace", "path": staged.name, "sha256": sha256_file(sub)}
    assert _store(workspace) == []


def test_gc_keeps_a_runner_copy_of_an_unfinished_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow import gc

    workspace = Workspace.initialize(tmp_path / "workspace")
    tmp = workspace.control / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    committing, finished = str(uuid.uuid4()), str(uuid.uuid4())
    kept = tmp / runner_staging_name(committing, PurePosixPath("sub.py"))
    swept = tmp / runner_staging_name(finished, PurePosixPath("sub.py"))
    for entry in (kept, swept):
        entry.write_text("copy\n", encoding="utf-8")
        aged = time.time() - gc.TMP_MAXIMUM_AGE_SECONDS - 60
        os.utime(entry, (aged, aged))
    monkeypatch.setattr(gc._Collection, "_committing_attempt_ids", lambda self: {committing})

    report = workspace.collect_garbage(categories=("tmp_entries",))

    assert kept.exists() and not swept.exists()
    assert report.category("tmp_entries").removed == 1
