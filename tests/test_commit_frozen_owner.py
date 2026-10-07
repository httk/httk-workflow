"""A frozen predecessor never changes data after a takeover.

A manager that is stopped anywhere inside its replay of a commit can wake up
long after a successor took the commit over, committed it, and the job moved
on. Each case freezes or kills real managers at one point of the replay
(through :func:`httk.workflow._txn._hook` or the rename primitive) and checks
that whatever the old owner still holds can no longer reach ``data/``.
"""

import errno
import json
import logging
import os
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from conftest import bury_manager
from httk.workflow import TaskManager, Workspace, _manager_launches, _txn
from httk.workflow import transactions as transactions_module
from httk.workflow._jobdir import JobDirectory
from httk.workflow.introspection import explain_job
from httk.workflow.journal import read_record
from httk.workflow.models import Marker
from test_commit_ownership import _backdate_heartbeat, _Killed, _payload, _tree_digest

pytestmark = pytest.mark.xdist_group("commit-ownership")

#: Runs the steps of ``files/plan.json``: each step publishes the transaction
#: its plan entry lists and advances to the next step, or succeeds.
_RUNNER = """#!/usr/bin/env python3
import hashlib
import json
import os
from pathlib import Path

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job_dir = Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
plan = json.loads((job_dir / "files" / "plan.json").read_text())[context["step"]]
temporary = control / "outcome.tmp.test"
payload = temporary / "transaction" / "payload"
payload.mkdir(parents=True)
operations = []
for entry in plan["operations"]:
    operation = {key: value for key, value in entry.items() if key != "text"}
    if entry["op"] == "put-file":
        source = payload / entry["id"]
        source.write_text(entry["text"])
        operation.update(source="payload/" + entry["id"], sha256=hashlib.sha256(entry["text"].encode()).hexdigest())
    elif entry["op"] in ("put-tree", "replace-tree"):
        source = payload / entry["id"]
        source.mkdir()
        (source / "inner").write_text(entry["text"])
        operation["source"] = "payload/" + entry["id"]
    operations.append(operation)
outcome = {
    "format": "httk-workflow-outcome",
    "format_version": 2,
    "job_id": context["job_id"],
    "activation_id": context["activation_id"],
    "attempt_id": context["attempt_id"],
    "expected_data_generation": context["data_generation"],
}
if plan.get("next"):
    outcome.update(action="advance", next_step=plan["next"])
else:
    outcome["action"] = "succeed"
(temporary / "transaction" / "manifest.json").write_text(json.dumps({
    "format": "httk-workflow-transaction",
    "format_version": 2,
    "expected_data_generation": context["data_generation"],
    "operations": operations,
}))
(temporary / "outcome.json").write_text(json.dumps(outcome))
os.rename(temporary, control / "outcome.ready")
"""


def _put(identifier: str, path: str, text: str) -> dict[str, object]:
    return {"id": identifier, "op": "put-file", "path": path, "text": text}


def _plan(tmp_path: Path) -> dict[str, dict[str, Any]]:
    """Seed some data, change it with every operation kind, then write the removed path again."""

    return {
        "seed": {
            "next": "change",
            "operations": [
                _put("keep", "keep.txt", "keep\n"),
                _put("old", "old.txt", "old\n"),
                {
                    "id": "tree",
                    "op": "put-tree",
                    "path": "tree",
                    "text": "old\n",
                    "sha256": _tree_digest(tmp_path, "old\n"),
                },
            ],
        },
        "change": {
            "next": "again",
            "operations": [
                _put("nested", "sub/dir/new.txt", "new\n"),
                {"id": "made", "op": "make-dir", "path": "made/deep"},
                {
                    "id": "swap",
                    "op": "replace-tree",
                    "path": "tree",
                    "text": "new\n",
                    "sha256": _tree_digest(tmp_path, "new\n"),
                },
                {"id": "drop", "op": "remove", "path": "old.txt"},
            ],
        },
        "again": {"next": None, "operations": [_put("again", "old.txt", "again\n")]},
    }


#: What ``data/`` holds once all three commits ran exactly once.
_FINAL = {
    "keep.txt": "keep\n",
    "made": None,
    "made/deep": None,
    "old.txt": "again\n",
    "sub": None,
    "sub/dir": None,
    "sub/dir/new.txt": "new\n",
    "tree": None,
    "tree/inner": "new\n",
}


def _submit(tmp_path: Path, plan: dict[str, dict[str, Any]], tag: str = "frozen") -> tuple[Path, str]:
    root = tmp_path / "workspace"
    Workspace.initialize(root)
    payload, job_id = _payload(tmp_path, _RUNNER, tag=tag)
    (payload / "files" / "plan.json").write_text(json.dumps(plan), encoding="utf-8")
    Workspace(root).submit(payload, f"project/{tag}")
    return root, job_id


def _data(workspace: Workspace, marker: Marker) -> dict[str, str | None]:
    data = workspace.payload_path(marker.placement, marker.job_key) / "data"
    return {
        path.relative_to(data).as_posix(): None if path.is_dir() else path.read_text(encoding="utf-8")
        for path in sorted(data.rglob("*"))
    }


def _kinds(workspace: Workspace, marker: Marker) -> list[str]:
    kinds: list[str] = []
    record_ref: str | None = marker.record_ref
    while record_ref is not None and record_ref != "init":
        frame = read_record(workspace.control, record_ref, deadline_seconds=workspace.visibility_deadline)
        kinds.append(str(frame["kind"]))
        previous = frame.get("previous_record_ref")
        record_ref = None if previous is None else str(previous)
    return kinds


def _committing_step(workspace: Workspace, job_id: str) -> str | None:
    marker = workspace.find_marker_by_id(job_id)
    if marker is None or marker.kind != "committing":
        return None
    return str(workspace.read_state(marker).get("step"))


class _Frozen:
    """Freezes the first manager at one point of its ``change`` commit and runs a successor to the end meanwhile.

    The successor takes the commit over (the first manager's heartbeat is
    backdated), commits it, runs and commits the ``again`` step, and only then
    does the first manager continue from where it stopped.
    """

    def __init__(self, workspace: Workspace, job_id: str, owner: TaskManager, successor: TaskManager) -> None:
        self.workspace = workspace
        self.job_id = job_id
        self.owner = owner
        self.successor = successor
        self.armed = False
        self.fired = 0
        self.generation = 0
        self.owner_events: list[str] = []

    def freeze(self) -> None:
        """Run the successor to the end while the first manager is held here."""

        self.armed = False
        self.fired += 1
        committing = self.workspace.find_marker_by_id(self.job_id)
        assert committing is not None and committing.kind == "committing"
        self.generation = committing.generation
        _backdate_heartbeat(self.workspace, self.owner.manager_id)
        deadline = time.monotonic() + 60.0
        while time.monotonic() < deadline:
            self.successor.tick()
            marker = self.workspace.find_marker_by_id(self.job_id)
            if marker is not None and marker.kind == "succeeded":
                return
            time.sleep(0.01)
        raise AssertionError("the successor never finished the job")

    def drive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tick the first manager until the job succeeds, arming the freeze for its ``change`` commit."""

        real_transition = TaskManager._transition
        real_anomaly = TaskManager._report_anomaly
        frozen = self

        def transition(self: TaskManager, marker: Marker, kind: str, *arguments: Any, **keywords: Any) -> Marker:
            if self is frozen.owner and frozen.fired:
                frozen.owner_events.append(f"transition:{kind}")
            return real_transition(self, marker, kind, *arguments, **keywords)

        def anomaly(self: TaskManager, key: str, *arguments: Any, **keywords: Any) -> None:
            if self is frozen.owner and frozen.fired:
                frozen.owner_events.append(f"anomaly:{key}")
            real_anomaly(self, key, *arguments, **keywords)

        monkeypatch.setattr(TaskManager, "_transition", transition)
        monkeypatch.setattr(TaskManager, "_report_anomaly", anomaly)
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline:
            marker = self.workspace.find_marker_by_id(self.job_id)
            assert marker is not None
            if marker.kind == "succeeded" and self.fired:
                break
            if not self.fired:
                self.armed = _committing_step(self.workspace, self.job_id) == "change"
            self.owner.tick()
            time.sleep(0.01)
        # The first manager gets a few more ticks after waking: it must stay silent.
        for _ in range(3):
            self.owner.tick()
        monkeypatch.undo()
        assert self.fired == 1, "the freeze never happened"


def _freeze_in_rename(frozen: _Frozen, monkeypatch: pytest.MonkeyPatch, destination_name: str) -> None:
    """Freeze the first manager inside the rename into its trash entry *destination_name*, before it happens."""

    real_rename = transactions_module._rename_verified

    def rename(
        source: transactions_module._Location,
        destination: transactions_module._Location,
        *,
        replace: bool = False,
        attempts: int = 7,
    ) -> None:
        if frozen.armed and destination.name == destination_name:
            frozen.freeze()
        real_rename(source, destination, replace=replace, attempts=attempts)

    monkeypatch.setattr(transactions_module, "_rename_verified", rename)


def _freeze_at_step(frozen: _Frozen, monkeypatch: pytest.MonkeyPatch, step: str, ordinal: int) -> None:
    """Freeze the first manager when its ``change`` commit reaches one hook step for the *ordinal*-th time."""

    reached_count = [0]

    def hook(reached: str) -> None:
        if frozen.armed and reached == step:
            reached_count[0] += 1
            if reached_count[0] == ordinal:
                frozen.freeze()

    monkeypatch.setattr(_txn, "_HOOK", hook)


def _run_frozen(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, arm: Callable[[_Frozen], None]) -> _Frozen:
    root, job_id = _submit(tmp_path, _plan(tmp_path))
    workspace = Workspace(root)
    with (
        TaskManager(Workspace(root), heartbeat_interval=0.01) as owner,
        TaskManager(Workspace(root), heartbeat_interval=0.01) as successor,
    ):
        frozen = _Frozen(workspace, job_id, owner, successor)
        arm(frozen)
        frozen.drive(monkeypatch)
    return frozen


def _assert_final(frozen: _Frozen) -> None:
    final = frozen.workspace.find_marker_by_id(frozen.job_id)
    assert final is not None and final.kind == "succeeded"
    assert _data(frozen.workspace, final) == _FINAL
    assert frozen.workspace.read_state(final)["data_generation"] == 3
    assert "failed" not in _kinds(frozen.workspace, final)
    # The old owner was fenced and recorded nothing once it woke.
    assert frozen.owner_events == []


@pytest.mark.timing
def test_an_owner_frozen_before_its_remove_rename_cannot_take_newer_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reported defect: a late rename into a trash the old owner still holds moved newer data away."""

    frozen = _run_frozen(tmp_path, monkeypatch, lambda frozen: _freeze_in_rename(frozen, monkeypatch, "removed"))
    _assert_final(frozen)


def _trash_entries(frozen: _Frozen) -> dict[str, list[str]]:
    """Return every transaction trash directory of the job, by name, with its entries."""

    final = frozen.workspace.find_marker_by_id(frozen.job_id)
    assert final is not None
    payload = frozen.workspace.payload_path(final.placement, final.job_key)
    return {
        path.name: sorted(entry.name for entry in path.iterdir())
        for path in payload.glob("attempts/*/commit.*/transaction/trash/*")
    }


#: Where the first manager is frozen in its ``change`` commit, and what it may
#: leave behind once it wakes: nothing, or one empty trash directory of its own.
#: The hook ordinals follow the plan: the pre-pass creates the trash of
#: ``nested``, ``made``, ``swap`` and ``drop`` in that order, then the replay
#: creates ``sub`` and ``sub/dir`` for ``nested`` and ``made`` and
#: ``made/deep`` for the make-dir.
_FREEZES: dict[str, tuple[Callable[[_Frozen, pytest.MonkeyPatch], None], str | None]] = {
    # After fence 1, before the mkdir of its own trash, which the successor had retired.
    "before-own-trash-mkdir": (lambda frozen, mp: _freeze_at_step(frozen, mp, "replay.trash_create", 5), "nested"),
    # After fence 2, before the non-creating open of the retired directory.
    "between-fence-and-trash-open": (lambda frozen, mp: _freeze_at_step(frozen, mp, "replay.trash_open", 1), None),
    "replace-tree-set-aside": (lambda frozen, mp: _freeze_in_rename(frozen, mp, "old"), None),
    "parent-creation": (lambda frozen, mp: _freeze_at_step(frozen, mp, "replay.directory_made", 1), None),
    "make-dir": (lambda frozen, mp: _freeze_at_step(frozen, mp, "replay.directory_made", 3), None),
}


@pytest.mark.timing
@pytest.mark.parametrize("point", sorted(_FREEZES))
def test_an_owner_frozen_anywhere_in_its_replay_leaves_data_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    arm, created = _FREEZES[point]
    frozen = _run_frozen(tmp_path, monkeypatch, lambda frozen: arm(frozen, monkeypatch))
    _assert_final(frozen)
    # The successor retired every directory of the old owner before it
    # replayed; a late wake adds at most one empty directory of its own.
    stale = {
        name: entries for name, entries in _trash_entries(frozen).items() if name.endswith(f".{frozen.generation}")
    }
    assert stale == ({} if created is None else {f"{created}.{frozen.generation}": []})


def _change_step(workspace: Workspace, job_id: str) -> bool:
    return _committing_step(workspace, job_id) == "change"


def _crash(root: Path, job_id: str, step: str, ordinal: int) -> int:
    """Run a manager until its ``change`` commit reaches *step* for the *ordinal*-th time, and kill it there.

    :return: The committing generation the killed manager held.
    """

    workspace = Workspace(root)
    reached_count = [0]
    held = [0]

    def hook(reached: str) -> None:
        if reached == step and _change_step(workspace, job_id):
            reached_count[0] += 1
            if reached_count[0] == ordinal:
                marker = workspace.find_marker_by_id(job_id)
                assert marker is not None
                held[0] = marker.generation
                raise _Killed()

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(_txn, "_HOOK", hook)
        with TaskManager(Workspace(root), heartbeat_interval=0.01) as dying:
            deadline = time.monotonic() + 60.0
            with pytest.raises(_Killed):
                while time.monotonic() < deadline:
                    dying.tick()
                    time.sleep(0.01)
            for attempt in list(dying._running.values()):
                attempt.process.kill()
                attempt.process.wait(timeout=30)
            dying._running.clear()
    bury_manager(workspace.control / "managers" / dying.manager_id)
    return held[0]


def _finish(root: Path, job_id: str, takeovers: int) -> None:
    workspace = Workspace(root)
    with TaskManager(workspace, heartbeat_interval=0.01) as fresh:
        fresh.run_until_idle(timeout=90.0)
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    assert _data(workspace, final) == _FINAL
    assert workspace.read_state(final)["data_generation"] == 3
    kinds = _kinds(workspace, final)
    assert "failed" not in kinds
    assert kinds.count("committing") == 3 + takeovers
    assert workspace.check().ok


#: Crashes at every new step of the replay, one or two managers in a row.
_CRASHES: dict[str, list[tuple[str, int]]] = {
    "first-own-trash-created": [("replay.trash_created", 1)],
    "every-own-trash-created": [("replay.trash_created", 4)],
    "removal-moved-not-deleted": [("replay.moved", 1)],
    "set-aside-deleted": [("replay.deleted", 1)],
    "removal-deleted": [("replay.deleted", 2)],
    "directory-made-not-renamed": [("replay.directory_made", 3)],
    "first-predecessor-retired": [("replay.trash_created", 2), ("replay.predecessor_retired", 1)],
    # Review H1: the removal happened and its owner died before deleting it;
    # the successor retired that owner's trash (carrying the evidence forward
    # in its own) and died too. The third owner must see the removal as done,
    # not as a missing target of a missing_ok=False remove.
    "chain-after-removal": [("replay.moved", 1), ("replay.predecessor_retired", 4)],
}


@pytest.mark.timing
@pytest.mark.parametrize("crash", sorted(_CRASHES))
def test_a_replay_killed_at_a_new_step_completes_exactly_once(tmp_path: Path, crash: str) -> None:
    root, job_id = _submit(tmp_path, _plan(tmp_path))
    for step, ordinal in _CRASHES[crash]:
        _crash(root, job_id, step, ordinal)
    _finish(root, job_id, takeovers=len(_CRASHES[crash]))


@pytest.mark.timing
def test_a_predecessor_trash_that_cannot_be_retired_defers_the_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    root, job_id = _submit(tmp_path, _plan(tmp_path))
    held = _crash(root, job_id, "replay.trash_created", 4)
    workspace = Workspace(root)
    marker = workspace.find_marker_by_id(job_id)
    assert marker is not None
    seeded = _data(workspace, marker)
    stuck = f"nested.{held}"
    real_remove_tree = JobDirectory.remove_tree

    def busy(self: JobDirectory, relative: str | PurePosixPath) -> None:
        if str(relative) == stuck:
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY), str(self.path / stuck))
        real_remove_tree(self, relative)

    monkeypatch.setattr(JobDirectory, "remove_tree", busy)
    caplog.set_level(logging.WARNING, logger="httk.workflow.manager")
    control = workspace.payload_path(marker.placement, marker.job_key) / str(
        workspace.read_state(marker)["attempt_control"]
    )
    wedge = control / "commit-wedge.json"
    with TaskManager(workspace, heartbeat_interval=0.01) as successor:
        # The first deferral only warns; the repeat is a wedge 'job why' shows.
        successor.tick()
        deferred = workspace.find_marker_by_id(job_id)
        assert deferred is not None and deferred.kind == "committing"
        assert deferred.generation == held + 1  # taken over, and nothing after that
        assert not wedge.exists()
        for _ in range(4):
            successor.tick()
            time.sleep(0.01)
        deferred = workspace.find_marker_by_id(job_id)
        assert deferred is not None and deferred.kind == "committing" and deferred.generation == held + 1
        assert _data(workspace, deferred) == seeded
        key = f"commit_deferred:{deferred.job_key}"
        assert stuck in successor._reported[key]
        warnings = [record for record in caplog.records if "deferring the commit" in record.getMessage()]
        assert len(warnings) == 1 and warnings[0].levelno == logging.WARNING
        assert not [name for name in successor._reported if name != key]
        assert stuck in json.loads(wedge.read_text(encoding="utf-8"))["error"]
        diagnosis = explain_job(workspace, deferred)
        assert diagnosis.blocked and stuck in diagnosis.summary

        monkeypatch.undo()
        # A tick that returns early without committing keeps the wedge state.
        monkeypatch.setattr(_manager_launches, "holds_commit", lambda manager, marker: True)
        successor.tick()
        assert wedge.exists() and key in successor._reported and key in successor._commit_wedge_recorded
        monkeypatch.undo()
        successor.run_until_idle(timeout=90.0)
        # The completed commit forgets its deferral: the record is gone, and a
        # later deferral would warn again.
        assert not wedge.exists()
        assert key not in successor._reported and key not in successor._commit_wedge_recorded
    final = workspace.find_marker_by_id(job_id)
    assert final is not None and final.kind == "succeeded"
    assert _data(workspace, final) == _FINAL
    assert "failed" not in _kinds(workspace, final)


@pytest.mark.timing
def test_a_successor_frozen_in_its_own_retirement_pass_leaves_data_alone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dies with its trash made; B, retiring it, freezes before its own mkdir; C takes over and finishes."""

    root, job_id = _submit(tmp_path, _plan(tmp_path))
    first = _crash(root, job_id, "replay.trash_created", 4)
    workspace = Workspace(root)
    with (
        TaskManager(Workspace(root), heartbeat_interval=0.01) as second,
        TaskManager(Workspace(root), heartbeat_interval=0.01) as third,
    ):
        frozen = _Frozen(workspace, job_id, second, third)
        # B's first trash creation is the first one of its retirement pass.
        _freeze_at_step(frozen, monkeypatch, "replay.trash_create", 1)
        frozen.drive(monkeypatch)
    assert frozen.generation == first + 1
    _assert_final(frozen)
    entries = _trash_entries(frozen)
    # C retired A's directories; B, waking, made at most one empty directory.
    assert not [name for name in entries if name.endswith(f".{first}")]
    assert {name: listed for name, listed in entries.items() if name.endswith(f".{frozen.generation}")} == {
        f"nested.{frozen.generation}": []
    }
