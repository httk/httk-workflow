"""One owner per commit: a takeover fences the previous owner without a false corruption.

Two real managers share one workspace. The first is stopped inside its replay
of a transaction at one of the interleavings an adversarial review found, the
second takes the commit over (the first manager's heartbeat is backdated, so
the second has evidence that it is gone) and completes it, and then the first
continues. It must stop at its next access to the renamed draft, record
nothing, and leave the data exactly as one sequential replay would.
"""

import json
import os
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
from httk.core.digests import tree_digest

from conftest import bury_manager
from httk.workflow import TaskManager, Workspace, _manager_commit, _txn
from httk.workflow import transactions as transactions_module
from httk.workflow._jobdir import JobDirectory
from httk.workflow._manager_commit import CommitFencedError, open_draft
from httk.workflow._manager_requests import _STATE_ENVELOPE_MEMBERS
from httk.workflow.journal import read_record
from httk.workflow.models import Marker, StateFrame
from httk.workflow.transactions import _DisplacedDataError, _replay_pinned

pytestmark = pytest.mark.xdist_group("commit-ownership")

#: Seeds the data in one commit, then changes it in a second one with a
#: put-file, a replace-tree and a remove: every rename kind the replay has.
_RUNNER = """#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
payload = temporary / "transaction" / "payload"
payload.mkdir(parents=True)
operations = []

def put_file(name, text):
    (payload / name).write_text(text)
    operations.append({
        "id": "put-" + name.replace(".", "-"),
        "op": "put-file",
        "source": "payload/" + name,
        "path": name,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
    })

def tree(op, text, digest):
    (payload / "tree").mkdir()
    (payload / "tree" / "inner").write_text(text)
    operations.append({"id": op, "op": op, "source": "payload/tree", "path": "tree", "sha256": digest})

base = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "expected_data_generation": context["data_generation"],
}
if context["step"] == "seed":
    put_file("keep.txt", "keep\\n")
    put_file("old.txt", "old\\n")
    tree("put-tree", "old\\n", "OLD_TREE")
    outcome = {**base, "action": "advance", "next_step": "change"}
else:
    put_file("new.txt", "new\\n")
    tree("replace-tree", "new\\n", "NEW_TREE")
    operations.append({"id": "remove", "op": "remove", "path": "old.txt"})
    outcome = {**base, "action": "succeed"}
(temporary / "transaction" / "manifest.json").write_text(json.dumps({
    "format": "httk-workflow-transaction",
    "format_version": 2,
    "expected_data_generation": context["data_generation"],
    "operations": operations,
}))
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


def _tree_digest(tmp_path: Path, text: str) -> str:
    """Return the digest of a ``tree`` holding one ``inner`` file with *text*."""

    tree = tmp_path / "digests" / str(uuid.uuid4())
    tree.mkdir(parents=True)
    (tree / "inner").write_text(text, encoding="utf-8")
    return tree_digest(tree)


def _payload(
    tmp_path: Path, runner_text: str, *, tag: str = "owned", data_mode: str = "transactional", step: str = "seed"
) -> tuple[Path, str]:
    job_id = str(uuid.uuid4())
    payload = tmp_path / "source" / tag
    (payload / "files").mkdir(parents=True)
    runner = payload / "files" / "runner"
    runner.write_text(runner_text, encoding="utf-8")
    runner.chmod(0o755)
    job = {
        "format": "httk-workflow-job",
        "format_version": 2,
        "id": job_id,
        "tag": tag,
        "name": "Commit ownership job",
        "workflow": "tests.ownership",
        "runner": {"path": "files/runner", "arguments": []},
        "workdir": {"mode": "persistent", "path": "run"},
        "data": {"mode": data_mode},
        "initial_step": step,
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": []},
        "retry_policy": {"retry_on": []},
        "resources": {},
        "parent": None,
    }
    (payload / "job.json").write_text(json.dumps(job), encoding="utf-8")
    return payload, job_id


def _backdate_heartbeat(workspace: Workspace, manager_id: str) -> None:
    """Make a live manager look long silent, which is evidence that it is gone."""

    updated = datetime.now(UTC) - timedelta(days=30)
    (workspace.control / "managers" / manager_id / "heartbeat.json").write_text(
        json.dumps(
            {"manager_id": manager_id, "updated_at": updated.isoformat(timespec="microseconds").replace("+00:00", "Z")}
        ),
        encoding="utf-8",
    )


#: Each interleaving names where the first owner is stopped, by the replay
#: primitive it is calling and its arguments, and whether it stops before the
#: call (the successor then runs while its draft root is open) or after it.
_Trigger = Callable[[str, tuple[Any, ...]], bool]


def _data_entry(name: str, *, present: bool | None = None) -> _Trigger:
    def matches(primitive: str, call: tuple[Any, ...]) -> bool:
        if primitive != "entry":
            return False
        root, relative, result = call
        return (
            root.path.name == "data"
            and relative == PurePosixPath(name)
            and (present is None or (result is not None) == present)
        )

    return matches


def _source_entry(primitive: str, call: tuple[Any, ...]) -> bool:
    if primitive != "entry":
        return False
    root, relative, _result = call
    return root.path.name == "transaction" and relative == PurePosixPath("payload/new.txt")


def _trash_rename(primitive: str, call: tuple[Any, ...]) -> bool:
    if primitive != "rename":
        return False
    _source, destination = call
    return destination.name == "old"


_INTERLEAVINGS: dict[str, _Trigger] = {
    # The reviewer's interleaving: the destination is observed absent, and the
    # successor moves the source in before the source is observed.
    "put-file-after-destination-observed": _data_entry("new.txt"),
    # The same, with the draft root already open: the source is observed
    # absent, and the destination is re-observed instead of concluding corruption.
    "put-file-source-in-flight": _source_entry,
    "replace-tree-between-its-two-renames": _trash_rename,
    "remove-before-its-trash-rename": _data_entry("old.txt", present=True),
}


@pytest.mark.timing
@pytest.mark.parametrize("interleaving", sorted(_INTERLEAVINGS))
def test_a_commit_taken_over_mid_replay_fences_its_old_owner_without_corruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interleaving: str
) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    payload, job_id = _payload(
        tmp_path,
        _RUNNER.replace("OLD_TREE", _tree_digest(tmp_path, "old\n")).replace(
            "NEW_TREE", _tree_digest(tmp_path, "new\n")
        ),
    )
    workspace = Workspace(root)
    workspace.submit(payload, "project/owned")
    trigger = _INTERLEAVINGS[interleaving]
    before_call = interleaving == "put-file-source-in-flight"

    with (
        TaskManager(Workspace(root), heartbeat_interval=0.01) as owner,
        TaskManager(Workspace(root), heartbeat_interval=0.01) as successor,
    ):
        armed = [False]
        fired: list[int] = []
        recorded: list[tuple[str, str]] = []
        fenced: list[tuple[str, type[BaseException]]] = []

        def run_successor() -> None:
            armed[0] = False
            fired.append(len(recorded))
            _backdate_heartbeat(workspace, owner.manager_id)
            deadline = time.monotonic() + 60.0
            while time.monotonic() < deadline:
                successor.tick()
                marker = workspace.find_marker_by_id(job_id)
                if marker is not None and marker.kind == "succeeded":
                    return
                time.sleep(0.01)
            raise AssertionError("the successor never committed")

        real_entry = transactions_module._entry
        real_rename = transactions_module._rename_verified

        def entry(root_dir: JobDirectory, relative: PurePosixPath, *, follow: bool) -> os.stat_result | None:
            if before_call and armed[0] and trigger("entry", (root_dir, relative, None)):
                run_successor()
            result = real_entry(root_dir, relative, follow=follow)
            if not before_call and armed[0] and trigger("entry", (root_dir, relative, result)):
                run_successor()
            return result

        def rename(
            source: transactions_module._Location,
            destination: transactions_module._Location,
            *,
            replace: bool = False,
            attempts: int = 7,
        ) -> None:
            real_rename(source, destination, replace=replace, attempts=attempts)
            if armed[0] and trigger("rename", (source, destination)):
                run_successor()

        real_transition = TaskManager._transition
        real_anomaly = TaskManager._report_anomaly
        real_process = TaskManager._process_committing

        def transition(self: TaskManager, marker: Marker, kind: str, *arguments: Any, **keywords: Any) -> Marker:
            recorded.append((self.manager_id, f"transition:{kind}"))
            return real_transition(self, marker, kind, *arguments, **keywords)

        def anomaly(self: TaskManager, key: str, *arguments: Any, **keywords: Any) -> None:
            recorded.append((self.manager_id, f"anomaly:{key}"))
            real_anomaly(self, key, *arguments, **keywords)

        def process(self: TaskManager, marker: Marker) -> None:
            try:
                real_process(self, marker)
            except Exception as exc:
                fenced.append((self.manager_id, type(exc)))
                raise

        monkeypatch.setattr(transactions_module, "_entry", entry)
        monkeypatch.setattr(transactions_module, "_rename_verified", rename)
        monkeypatch.setattr(TaskManager, "_transition", transition)
        monkeypatch.setattr(TaskManager, "_report_anomaly", anomaly)
        monkeypatch.setattr(TaskManager, "_process_committing", process)

        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            marker = workspace.find_marker_by_id(job_id)
            assert marker is not None
            if marker.kind == "succeeded":
                break
            # Only the second commit is interrupted, and only once.
            if marker.kind == "committing" and not fired:
                armed[0] = workspace.read_state(marker).get("step") == "change"
            owner.tick()
            time.sleep(0.01)
        monkeypatch.undo()

    assert len(fired) == 1, "the interleaving never happened"
    # The old owner was fenced at its next access to the draft, with an
    # error, and recorded nothing at all after the takeover.
    assert fenced == [(owner.manager_id, CommitFencedError)]
    assert [event for manager_id, event in recorded[fired[0] :] if manager_id == owner.manager_id] == []

    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    chain: list[dict[str, Any]] = []
    record_ref: str | None = final.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        chain.append(frame)
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)
    assert [frame["kind"] for frame in chain] == [
        "succeeded",
        "committing",
        "committing",
        "running",
        "claimed",
        "ready",
        "committing",
        "running",
        "claimed",
        "ready",
    ]
    takeover = chain[1]
    assert takeover["reason"] == "commit_takeover"
    assert takeover["previous_manager_id"] == owner.manager_id
    assert takeover["manager_id"] == successor.manager_id
    assert takeover["takeover_evidence"]["evidence"] == "lease_grace_expired"

    # The data is exactly what one sequential replay of both commits leaves.
    data = workspace.payload_path(final.placement, final.job_key) / "data"
    assert sorted(path.relative_to(data).as_posix() for path in data.rglob("*")) == [
        "keep.txt",
        "new.txt",
        "tree",
        "tree/inner",
    ]
    assert (data / "new.txt").read_text(encoding="utf-8") == "new\n"
    assert (data / "tree" / "inner").read_text(encoding="utf-8") == "new\n"
    assert workspace.read_state(final)["data_generation"] == 2

    # Nothing was created under the old owner's draft name; the draft carries
    # the successor's, with the trash that makes its replay idempotent.
    control = workspace.payload_path(final.placement, final.job_key) / str(takeover["attempt_control"])
    owned_generation = int(takeover["state_generation"]) - 1
    assert not (control / f"commit.{owned_generation}").exists()
    trash = control / f"commit.{owned_generation + 1}" / "transaction" / "trash"
    assert (trash / "replace-tree" / "old" / "inner").read_text(encoding="utf-8") == "old\n"
    assert (trash / "remove" / "removed").read_text(encoding="utf-8") == "old\n"
    assert workspace.check().ok


#: Spawns two children, then gathers them.
_SPAWNER = """#!/usr/bin/env python3
import json
import os
import uuid
from pathlib import Path

CHILD = '''#!/usr/bin/env python3
import json, os
from pathlib import Path
context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
(control / "outcome.tmp.child").mkdir()
(control / "outcome.tmp.child" / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome", "format_version": 2, "job_id": context["job_id"],
    "activation_id": context["activation_id"], "attempt_id": context["attempt_id"], "action": "succeed"}))
os.rename(control / "outcome.tmp.child", control / "outcome.ready")
'''
context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
temporary = control / "outcome.tmp.test"
temporary.mkdir()
base = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
}
if context["step"] == "gather":
    outcome = {**base, "action": "succeed"}
else:
    entries = []
    for label in ("first", "second"):
        child_id = str(uuid.uuid5(uuid.UUID(context["activation_id"]), label))
        child_key = "child--" + child_id
        bundle = temporary / "children" / "jobs" / child_key
        (bundle / "files").mkdir(parents=True)
        (bundle / "files" / "runner").write_text(CHILD)
        (bundle / "files" / "runner").chmod(0o755)
        (bundle / "job.json").write_text(json.dumps({
            "format": "httk-workflow-job",
            "format_version": 2,
            "id": child_id,
            "tag": "child",
            "name": "Spawned " + label,
            "workflow": "tests.ownership",
            "runner": {"path": "files/runner", "arguments": []},
            "workdir": {"mode": "persistent", "path": "run"},
            "data": {"mode": "none"},
            "initial_step": "run",
            "priority": 500,
            "claim": {"pool": "default", "required_capabilities": []},
            "retry_policy": {"retry_on": []},
            "resources": {},
            "parent": None,
        }))
        entries.append({
            "workspace_id": context["workspace_id"],
            "job_id": child_id,
            "job_key": child_key,
            "placement": "project/children",
            "label": label,
        })
    (temporary / "children" / "spawn.json").write_text(json.dumps({"children": entries}))
    outcome = {
        **base,
        "action": "wait",
        "next_step": "gather",
        "join": {
            "children": [
                {"workspace_id": entry["workspace_id"], "job_id": entry["job_id"], "job_key": entry["job_key"],
                 "placement_hint": entry["placement"]}
                for entry in entries
            ],
            "condition": "all_terminal",
        },
    }
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


class _Killed(BaseException):
    """A manager stopped dead, as a crash would stop it."""


def test_a_fenced_owner_never_removes_its_successors_staged_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    payload, job_id = _payload(tmp_path, _SPAWNER, tag="spawner", data_mode="none", step="spawn")
    workspace = Workspace(root)
    workspace.submit(payload, "project/spawner")

    # The first owner renames the draft to its name and stops right there.
    def stop_after_the_draft_rename(step: str) -> None:
        if step == "commit.draft_renamed":
            raise _Killed()

    monkeypatch.setattr(_txn, "_HOOK", stop_after_the_draft_rename)
    with TaskManager(Workspace(root), heartbeat_interval=0.01) as first:
        with pytest.raises(_Killed):
            first.run_until_idle(timeout=60.0)
        for attempt in list(first._running.values()):
            attempt.process.kill()
            attempt.process.wait(timeout=30)
        first._running.clear()
    monkeypatch.undo()
    bury_manager(workspace.control / "managers" / first.manager_id)
    committing = workspace.find_marker_by_id(job_id)
    assert committing is not None and committing.kind == "committing"
    old_name = f"commit.{committing.generation}"
    control_path = workspace.payload_path(committing.placement, committing.job_key) / str(
        workspace.read_state(committing)["attempt_control"]
    )

    # While the successor verifies each staged child, the fenced first owner
    # takes its next steps for that child: copy it (whose first act used to be
    # removing the shared staging name), and the fence before publication.
    real_verify = _manager_commit._verify_staged
    interleaved: list[str] = []

    def fenced_owner_steps_in(staging: JobDirectory, staged: str, job_key: str, *rest: Any) -> Any:
        with JobDirectory.at(control_path) as control:
            with pytest.raises(CommitFencedError):
                _manager_commit._copy_child(control, old_name, staging, job_key, staged)
            with pytest.raises(CommitFencedError):
                open_draft(control, old_name).close()
        assert staging.stat(staged) is not None
        interleaved.append(job_key)
        return real_verify(staging, staged, job_key, *rest)

    monkeypatch.setattr(_manager_commit, "_verify_staged", fenced_owner_steps_in)
    with TaskManager(workspace, heartbeat_interval=0.01) as successor:
        successor.run_until_idle(timeout=90.0)
        reported = dict(successor._reported)

    assert len(interleaved) == 2
    parent = workspace.find_marker_by_id(job_id)
    assert parent is not None and parent.kind == "succeeded"
    children = [marker for marker in workspace.scan_markers() if marker.job_key.startswith("child--")]
    assert sorted(marker.kind for marker in children) == ["succeeded", "succeeded"]
    assert not [key for key in reported if committing.job_key in key]
    record_ref: str | None = parent.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        assert frame["kind"] != "failed"
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)


def test_a_replace_tree_rename_onto_an_empty_set_aside_directory_is_detected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The syscall gap the stdlib cannot close: detected after the rename, with both paths named."""

    transaction = tmp_path / "control" / "commit.3" / "transaction"
    (transaction / "payload" / "tree").mkdir(parents=True)
    (transaction / "payload" / "tree" / "inner").write_text("new\n", encoding="utf-8")
    (transaction / "manifest.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-transaction",
                "format_version": 2,
                "expected_data_generation": 0,
                "operations": [
                    {
                        "id": "replace",
                        "op": "replace-tree",
                        "path": "tree",
                        "source": "payload/tree",
                        "sha256": tree_digest(transaction / "payload" / "tree"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    data = tmp_path / "data"
    (data / "tree").mkdir(parents=True)  # the old tree is empty
    real_rename = transactions_module._rename_verified

    def successor_in_the_gap(
        source: transactions_module._Location,
        destination: transactions_module._Location,
        *,
        replace: bool = False,
        attempts: int = 7,
    ) -> None:
        if destination.name == "old" and not (transaction / "trash" / "replace" / "old").exists():
            # Between this owner's check that the trash name is free and its
            # rename, a successor sets the empty old tree aside and installs the new one.
            os.rename(data / "tree", transaction / "trash" / "replace" / "old")
            os.rename(transaction / "payload" / "tree", data / "tree")
        real_rename(source, destination, replace=replace, attempts=attempts)

    monkeypatch.setattr(transactions_module, "_rename_verified", successor_in_the_gap)
    with (
        JobDirectory.at(tmp_path / "control") as control,
        JobDirectory.at(data) as pinned_data,
        pytest.raises(_DisplacedDataError) as raised,
    ):
        _replay_pinned(control, "commit.3", pinned_data, expected_generation=0)
    assert str(data / "tree") in str(raised.value)
    assert str(transaction / "trash" / "replace" / "old") in str(raised.value)
    # The displaced tree is where the message says it is.
    assert (transaction / "trash" / "replace" / "old" / "inner").read_text(encoding="utf-8") == "new\n"


_SUCCEED = """#!/usr/bin/env python3
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
(control / "outcome.tmp.test").mkdir()
(control / "outcome.tmp.test" / "outcome.json").write_text(json.dumps({
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "action": "succeed",
}))
os.rename(control / "outcome.tmp.test", control / "outcome.ready")
"""


@pytest.mark.parametrize("displaced", [True, False])
def test_a_fenced_commit_reports_only_displaced_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, displaced: bool
) -> None:
    workspace = Workspace.initialize(tmp_path / "workspace")
    payload, job_id = _payload(tmp_path, _SUCCEED, tag="fenced", data_mode="none", step="run")
    workspace.submit(payload, "project/fenced")
    fired: list[Marker] = []

    def fenced_mid_commit(self: TaskManager, marker: Marker) -> None:
        # What a takeover does to this commit's marker, and the error the
        # fenced replay then raises.
        state = self._read_frame(marker)
        frame = StateFrame(
            {name: value for name, value in state.members.items() if name not in _STATE_ENVELOPE_MEMBERS}
        )
        fired.append(self._transition(marker, "committing", frame))
        if displaced:
            raise _DisplacedDataError("replace-tree moved data/tree onto commit.3/transaction/trash/replace/old")
        raise CommitFencedError("commit draft commit.3 is gone")

    monkeypatch.setattr(TaskManager, "_process_committing", fenced_mid_commit)
    with TaskManager(workspace, heartbeat_interval=0.01) as manager:
        deadline = time.monotonic() + 60.0
        while not fired and time.monotonic() < deadline:
            manager.tick()
            time.sleep(0.01)
        assert fired
        reported = dict(manager._reported)
        monkeypatch.undo()
        manager.run_until_idle(timeout=60.0)

    key = f"displaced:{fired[0].job_key}"
    if displaced:
        assert "displaced data" in reported[key] and "trash/replace/old" in reported[key]
    else:
        assert key not in reported
    assert not [name for name in reported if name != key]
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
