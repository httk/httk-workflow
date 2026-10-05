"""A job cannot redirect, stall or crash the manager through the draft it publishes.

The commit reads the published draft, registers its children and replays its
data transaction through descriptors pinned without following links. Child
bundles are verified in manager-owned staging before they are published, and a
symlink planted in the draft or below ``data/`` fails that job alone.
"""

import hashlib
import json
import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace
from httk.workflow._logging import reset_logging
from httk.workflow.models import Marker

_CHILD_RUNNER = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
(temporary / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(temporary, control / "outcome.ready")
"""

_SPAWNING_RUNNER = """#!/usr/bin/env python3
import json
import os
import time
import uuid
from pathlib import Path

CHILD = {child!r}
PLACEMENT = {placement!r}
OUTSIDE = Path({outside!r})
PLANT_LINK = {plant_link!r}
context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {{
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}}
if context["step"] == "gather":
    outcome = {{**base, "action": "succeed"}}
else:
    child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), "only"))
    child_key = "child--" + child_id
    child_dir = temporary / "children" / "jobs" / child_key
    (child_dir / "files").mkdir(parents=True)
    runner = child_dir / "files" / "runner"
    runner.write_text(CHILD)
    runner.chmod(0o755)
    if PLANT_LINK:
        (child_dir / "files" / "escape").symlink_to(OUTSIDE)
    (child_dir / "job.json").write_text(json.dumps({{
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": child_id,
        "tag": "child",
        "name": "Commit hardening child",
        "workflow": "tests.commit",
        "runner": {{"path": "files/runner", "arguments": []}},
        "workdir": {{"mode": "persistent", "path": "run"}},
        "data": {{"mode": "none"}},
        "initial_step": "run",
        "priority": 500,
        "claim": {{"pool": "default", "required_capabilities": []}},
        "retry_policy": {{"retry_on": []}},
        "resources": {{}},
        "parent": None,
    }}))
    entry = {{
        "workspace_id": context["workspace_id"],
        "job_id": child_id,
        "job_key": child_key,
        "placement": PLACEMENT,
        "label": "only",
    }}
    (temporary / "children" / "spawn.json").write_text(json.dumps({{"children": [entry]}}))
    outcome = {{
        **base,
        "action": "wait",
        "next_step": "gather",
        "join": {{
            "children": [
                {{
                    "workspace_id": entry["workspace_id"],
                    "job_id": child_id,
                    "job_key": child_key,
                    "placement_hint": PLACEMENT,
                }}
            ],
            "condition": "all_terminal",
        }},
    }}
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""

#: A transactional runner: it plants what the scenario asks for, then publishes
#: one transaction with one operation.
_TRANSACTION_RUNNER = """#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path

from httk.core.digests import tree_digest

OUTSIDE = Path({outside!r})
OPERATION = {operation!r}
LINKED_SOURCE = {linked_source!r}
context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
data = job / "data"
data.mkdir(exist_ok=True)
if not LINKED_SOURCE:
    (data / "link").symlink_to(OUTSIDE, target_is_directory=True)
temporary = control / "outcome.tmp.test"
payload = temporary / "transaction" / "payload"
payload.mkdir(parents=True)
operation = {{"id": "only", "op": OPERATION}}
if LINKED_SOURCE:
    (payload / "file").symlink_to(OUTSIDE / "secret")
    operation.update(path="installed", source="payload/file",
                     sha256=hashlib.sha256((OUTSIDE / "secret").read_bytes()).hexdigest())
elif OPERATION == "put-file":
    (payload / "file").write_text("new")
    operation.update(path="link/sentinel", source="payload/file", sha256=hashlib.sha256(b"new").hexdigest())
elif OPERATION in ("put-tree", "replace-tree"):
    (payload / "tree").mkdir()
    (payload / "tree" / "file").write_text("new")
    operation.update(path="link/tree", source="payload/tree", sha256=tree_digest(payload / "tree"))
else:
    operation.update(path="link/sentinel")
(temporary / "transaction" / "manifest.json").write_text(json.dumps({{
    "format": "httk-workflow-transaction",
    "format_version": 2,
    "expected_data_generation": context["data_generation"],
    "operations": [operation],
}}))
(temporary / "outcome.json").write_text(json.dumps({{
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
    "expected_data_generation": context["data_generation"],
}}))
os.rename(temporary, control / "outcome.ready")
"""


class _Killed(BaseException):
    """A manager interrupted mid-commit, as a crash would."""


@pytest.fixture(autouse=True)
def _isolated_logging() -> Iterator[None]:
    reset_logging()
    yield
    reset_logging()


def _payload(
    root: Path, tag: str, runner_source: str, *, initial_step: str = "branch", data_mode: str = "none"
) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = root / tag
    files = payload / "files"
    files.mkdir(parents=True)
    runner = files / "runner"
    runner.write_text(runner_source, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": tag,
        "name": f"Commit hardening {tag}",
        "workflow": "tests.commit",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": data_mode},
        "initial_step": initial_step,
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _spawner(outside: Path, *, placement: str = "project/children", plant_link: bool = False) -> str:
    return _SPAWNING_RUNNER.format(
        child=_CHILD_RUNNER, placement=placement, outside=str(outside), plant_link=plant_link
    )


def _outside(tmp_path: Path) -> Path:
    outside = tmp_path / "outside"
    (outside / "tree").mkdir(parents=True)
    (outside / "tree" / "kept").write_text("outside tree\n", encoding="utf-8")
    (outside / "sentinel").write_text("outside\n", encoding="utf-8")
    (outside / "secret").write_text("secret\n", encoding="utf-8")
    return outside


def _snapshot(directory: Path) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for current, directories, files in os.walk(directory):
        for name in (*directories, *files):
            path = Path(current, name)
            content = path.read_bytes() if path.is_file() and not path.is_symlink() else None
            state[str(path.relative_to(directory))] = (path.lstat().st_mode, content)
    return state


def _outcome(workspace: Workspace, job_id: str) -> tuple[str, str | None, str]:
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    if marker.kind != "failed":
        return marker.kind, None, ""
    failure = workspace.read_state(marker)["failure"]
    return marker.kind, failure["code"], failure["message"]


def _healthy(tmp_path: Path, workspace: Workspace) -> str:
    payload, job_id = _payload(tmp_path / "source", "healthy", _CHILD_RUNNER, initial_step="run")
    workspace.submit(payload, "project/healthy")
    return job_id


def _children(workspace: Workspace) -> list[Marker]:
    return [marker for marker in workspace.scan_markers() if marker.job_key.startswith("child--")]


def _staged(workspace: Workspace) -> list[str]:
    return sorted(entry.name for entry in (workspace.control / "tmp").iterdir() if entry.name.startswith("child."))


def test_a_symlink_in_a_child_bundle_fails_the_parent_and_the_manager_keeps_running(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    before = _snapshot(outside)
    payload, job_id = _payload(tmp_path / "source", "parent", _spawner(outside, plant_link=True))
    workspace.submit(payload, "project/parent")
    healthy = _healthy(tmp_path, workspace)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    kind, code, message = _outcome(workspace, job_id)
    assert (kind, code) == ("failed", "protocol_error")
    assert "symlink" in message
    assert _outcome(workspace, healthy)[:2] == ("succeeded", None)
    assert not _children(workspace)
    assert _snapshot(outside) == before


def test_a_refused_spawn_placement_leaves_no_spawn_record(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    # A placement naming a job directory would nest the child inside another job.
    nested = f"project/other--{uuid.uuid4()}"
    payload, job_id = _payload(tmp_path / "source", "parent", _spawner(outside, placement=nested))
    marker = workspace.submit(payload, "project/parent")
    healthy = _healthy(tmp_path, workspace)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    kind, code, message = _outcome(workspace, job_id)
    assert (kind, code) == ("failed", "protocol_error")
    assert "parses as a job key" in message
    installed = workspace.payload_path(marker.placement, marker.job_key)
    assert not (installed / ".httk-job" / "tree" / "spawns").exists()
    assert not _children(workspace) and not _staged(workspace)
    assert _outcome(workspace, healthy)[:2] == ("succeeded", None)


def test_a_child_bundle_changed_after_its_digest_was_recorded_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "parent", _spawner(outside))
    marker = workspace.submit(payload, "project/parent")
    real_process_committing = TaskManager._process_committing

    def change_the_draft_first(self: TaskManager, committing: Marker) -> None:
        if committing.job_key == marker.job_key and committing.kind == "committing":
            installed = workspace.payload_path(committing.placement, committing.job_key)
            for runner in installed.glob("attempts/*/outcome.ready/children/jobs/*/files/runner"):
                runner.write_text("#!/bin/sh\necho swapped\n", encoding="utf-8")
        real_process_committing(self, committing)

    monkeypatch.setattr(TaskManager, "_process_committing", change_the_draft_first)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    kind, code, message = _outcome(workspace, job_id)
    assert (kind, code) == ("failed", "protocol_error")
    assert "changed after outcome publication" in message
    assert not _children(workspace)
    assert not (workspace.root / "project" / "children").exists() or not list(
        (workspace.root / "project" / "children").iterdir()
    )
    # The refused bundle stays in the manager's staging as evidence, with the
    # content that was refused; aged tmp collection sweeps it later.
    staged = _staged(workspace)
    assert len(staged) == 1
    runner = workspace.control / "tmp" / staged[0] / "files" / "runner"
    assert runner.read_text(encoding="utf-8") == "#!/bin/sh\necho swapped\n"


@pytest.mark.parametrize("operation", ["put-file", "put-tree", "remove", "replace-tree"])
def test_a_symlink_below_data_never_redirects_a_replay(tmp_path: Path, operation: str) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    before = _snapshot(outside)
    runner = _TRANSACTION_RUNNER.format(outside=str(outside), operation=operation, linked_source=False)
    payload, job_id = _payload(tmp_path / "source", "data", runner, initial_step="run", data_mode="transactional")
    workspace.submit(payload, "project/data")
    healthy = _healthy(tmp_path, workspace)

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    assert _outcome(workspace, job_id)[:2] == ("failed", "protocol_error")
    assert _snapshot(outside) == before
    assert _outcome(workspace, healthy)[:2] == ("succeeded", None)


def test_a_symlinked_transaction_source_is_refused(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    before = _snapshot(outside)
    runner = _TRANSACTION_RUNNER.format(outside=str(outside), operation="put-file", linked_source=True)
    payload, job_id = _payload(tmp_path / "source", "source", runner, initial_step="run", data_mode="transactional")
    marker = workspace.submit(payload, "project/source")

    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)

    assert _outcome(workspace, job_id)[:2] == ("failed", "protocol_error")
    installed = workspace.payload_path(marker.placement, marker.job_key)
    assert not os.path.lexists(installed / "data" / "installed")
    assert _snapshot(outside) == before


def test_a_commit_interrupted_after_staging_a_child_publishes_it_exactly_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    outside = _outside(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "parent", _spawner(outside))
    Workspace(root).submit(payload, "project/parent")
    real_publish = Workspace._publish_path

    def crash_when_publishing_a_staged_child(self: Workspace, source: Path, destination: Path) -> None:
        if source.name.startswith("child."):
            raise _Killed()
        real_publish(self, source, destination)

    monkeypatch.setattr(Workspace, "_publish_path", crash_when_publishing_a_staged_child)
    with TaskManager(Workspace(root), heartbeat_interval=0.01) as dying:
        with pytest.raises(_Killed):
            dying.run_until_idle(timeout=60.0)
        for attempt in list(dying._running.values()):
            attempt.process.kill()
            attempt.process.wait(timeout=30)
        dying._running.clear()
    monkeypatch.undo()

    workspace = Workspace(root)
    interrupted = workspace.find_marker_by_id(job_id)
    assert interrupted is not None and interrupted.kind == "committing"
    staged = _staged(workspace)
    assert len(staged) == 1
    # The bundle has left the job-writable draft: only the staged copy remains.
    installed = workspace.payload_path(interrupted.placement, interrupted.job_key)
    assert not list(installed.glob("attempts/*/outcome.ready/children/jobs/*"))

    with TaskManager(workspace, heartbeat_interval=0.01) as fresh:
        fresh.run_until_idle(timeout=90.0)

    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    children = _children(workspace)
    assert len(children) == 1 and children[0].kind == "succeeded"
    assert not _staged(workspace)


def test_transaction_trash_collection_never_follows_a_planted_symlink(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    before = _snapshot(outside)
    payload, job_id = _payload(tmp_path / "source", "done", _CHILD_RUNNER, initial_step="run")
    marker = workspace.submit(payload, "project/done")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    installed = workspace.payload_path(marker.placement, marker.job_key)

    # A trash entry that is a symlink out of the job directory, a trash that is
    # itself a symlink, and a transaction directory that is a symlink.
    linked_entry = installed / "attempts" / str(uuid.uuid4()) / "outcome.ready" / "transaction" / "trash"
    linked_entry.mkdir(parents=True)
    (linked_entry / "only").symlink_to(outside / "tree", target_is_directory=True)
    (linked_entry / "real").mkdir()
    (linked_entry / "real" / "removed").write_text("old\n", encoding="utf-8")
    linked_trash = installed / "attempts" / str(uuid.uuid4()) / "outcome.ready" / "transaction"
    linked_trash.mkdir(parents=True)
    (linked_trash / "trash").symlink_to(outside / "tree", target_is_directory=True)
    linked_transaction = installed / "attempts" / str(uuid.uuid4()) / "outcome.ready"
    linked_transaction.mkdir(parents=True)
    (linked_transaction / "transaction").symlink_to(outside, target_is_directory=True)

    workspace.set_policy({"retention": {"trash_days": 0.0}})
    report = workspace.collect_garbage(categories=("transaction_trash",))

    assert _snapshot(outside) == before
    assert not os.path.lexists(linked_entry / "only")
    assert not (linked_entry / "real").exists()
    assert (linked_trash / "trash").is_symlink()
    assert (linked_transaction / "transaction").is_symlink()
    assert report.category("transaction_trash").removed == 2


def test_a_digest_of_the_staged_child_is_compared_with_the_recorded_one(tmp_path: Path) -> None:
    """A well-formed spawn still registers its child, verified in staging, and leaves no staging behind."""

    workspace = Workspace.initialize(tmp_path / "workspace")
    outside = _outside(tmp_path)
    payload, job_id = _payload(tmp_path / "source", "parent", _spawner(outside))
    workspace.submit(payload, "project/parent")
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=60.0)
    assert _outcome(workspace, job_id)[:2] == ("succeeded", None)
    children = _children(workspace)
    assert len(children) == 1 and children[0].kind == "succeeded"
    child_payload = workspace.payload_path(children[0].placement, children[0].job_key)
    assert hashlib.sha256((child_payload / "job.json").read_bytes()).hexdigest()
    assert not _staged(workspace)


def _batch(root: Path, operations: list[dict[str, object]]) -> Path:
    transaction = root / "batch"
    (transaction / "payload").mkdir(parents=True)
    (transaction / "manifest.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-transaction",
                "format_version": 2,
                "expected_data_generation": 0,
                "operations": operations,
            }
        ),
        encoding="utf-8",
    )
    return transaction


def test_a_runner_side_replay_with_paths_still_follows_a_symlinked_workdir_directory(tmp_path: Path) -> None:
    from httk.core.digests import tree_digest

    from httk.workflow._jobdir import JobDirectory, JobDirectoryError
    from httk.workflow.transactions import _replay_pinned, replay_transaction

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (scratch / "old").write_text("old\n", encoding="utf-8")
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    (workdir / "scratch").symlink_to(scratch, target_is_directory=True)
    operations: list[dict[str, object]] = [
        {
            "id": "file",
            "op": "put-file",
            "path": "scratch/out.txt",
            "source": "payload/out.txt",
            "sha256": hashlib.sha256(b"out\n").hexdigest(),
        },
        {"id": "tree", "op": "put-tree", "path": "scratch/tree", "source": "payload/tree"},
        {"id": "gone", "op": "remove", "path": "scratch/old"},
    ]
    transaction = _batch(tmp_path, operations)
    (transaction / "payload" / "out.txt").write_text("out\n", encoding="utf-8")
    (transaction / "payload" / "tree").mkdir()
    (transaction / "payload" / "tree" / "inner").write_text("inner\n", encoding="utf-8")
    operations[1]["sha256"] = tree_digest(transaction / "payload" / "tree")
    (transaction / "manifest.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-transaction",
                "format_version": 2,
                "expected_data_generation": 0,
                "operations": operations,
            }
        ),
        encoding="utf-8",
    )

    # The runner's own workdir: the replay follows its symlinked directory.
    assert replay_transaction(transaction, workdir, expected_generation=0, durable=True)
    assert (scratch / "out.txt").read_text(encoding="utf-8") == "out\n"
    assert (scratch / "tree" / "inner").read_text(encoding="utf-8") == "inner\n"
    assert not (scratch / "old").exists()
    assert (transaction / "trash" / "gone" / "removed").read_text(encoding="utf-8") == "old\n"
    # Idempotent on replay, as before.
    assert replay_transaction(transaction, workdir, expected_generation=0)

    # The manager's pinned handles refuse the same symlinked directory.
    second = _batch(tmp_path / "second", [operations[0]])
    (second / "payload" / "out.txt").write_text("out\n", encoding="utf-8")
    with (
        JobDirectory.at(second) as pinned_transaction,
        JobDirectory.at(workdir) as pinned_data,
        pytest.raises(JobDirectoryError),
    ):
        _replay_pinned(pinned_transaction, pinned_data, expected_generation=0)


@pytest.mark.parametrize("kind", ["failed", "cancelled"])
def test_the_newest_aged_attempt_control_of_a_failed_or_cancelled_job_survives_collection(
    tmp_path: Path, kind: str
) -> None:
    from httk.workflow.journal import JournalWriter

    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, _job_id = _payload(tmp_path / "source", "done", _CHILD_RUNNER, initial_step="run")
    marker = workspace.submit(payload, "project/done")
    frame: dict[str, object] = {
        "step": "run",
        "activation_id": str(uuid.uuid4()),
        "activation_ordinal": 1,
        "attempt_ordinal": 1,
        "total_attempts": 1,
        "data_generation": None,
        "reason": kind,
    }
    if kind == "failed":
        frame["failure"] = {"code": "process_failure", "message": "runner exited", "details": {}}
    with JournalWriter(workspace.control) as writer:
        workspace.transition(writer, marker, kind, frame)
    installed = workspace.payload_path(marker.placement, marker.job_key)
    controls = []
    for age_days in (30, 20, 10):
        control = installed / "attempts" / str(uuid.uuid4())
        (control / "outcome.ready").mkdir(parents=True)
        (control / "outcome.ready" / "outcome.json").write_text("{}", encoding="utf-8")
        aged = time.time() - age_days * 86400
        os.utime(control, (aged, aged))
        controls.append(control)

    workspace.set_policy({"retention": {"attempt_control_days": 1.0}})
    report = workspace.collect_garbage(categories=("attempt_control",))

    # Every control directory is aged; the newest still holds the evidence that
    # decided the job and is retained however old it is.
    assert [control.exists() for control in controls] == [False, False, True]
    assert report.category("attempt_control").removed == 2
