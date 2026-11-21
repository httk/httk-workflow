"""Pins of the fsync and ``syncfs`` sequences of the protocol paths L3 must not change (plan WORKFLOW_LOCAL_COMMANDS
§5 amendment 14, phase L3a).

Each pin records, in call order, the target of every ``fsync``, ``fdatasync`` and ``syncfs`` of one path: a
manager serving one request, the daemon's serving of one request (``removal.serve_requests`` with its owner), a
manager committing one attempt, and the dead-owner recovery of one held job. The CLI one-job cancel and delete are
pinned by ``test_local_commands.test_the_fsync_sequence_of_one_job_is_unchanged``.

The pins were recorded at HEAD d667de7 with ``HTTK_TEST_DUMP_PINS`` set to a directory, which makes them write what
they observe instead of comparing it. The recovery pins changed deliberately with L3a: the per-owner recovery seal of
amendments 15 and 27 comes first (``requests/``, ``owned/<dead>``, each held job directory, the dead owner's scratch
directories, ``tmp/``, by ``fsync``; one ``syncfs`` per device where the protocol predicate holds). The dead owner here
is serial (no batched marker), so no placement directory is sealed.
"""

import contextlib
import os
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

import v3_helpers as v3
from httk.workflow import TaskManager, Workspace, _fs, _kernel, _requests, removal
from httk.workflow.workflow_cli import command
from test_cli_job_request import _context, _new_workspace
from test_cli_job_request import _job as _cli_job
from test_local_commands import _identity, _normalized, _pinned  # noqa: F401  (_identity: an autouse fixture)


@pytest.fixture(autouse=True)
def _serial_paths_register_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every pinned path is serial: it registers no obligation and never latches its owner (L3a)."""

    def refused(*args: object, **kwargs: object) -> None:
        raise AssertionError("a serial path registered an obligation or latched its owner")

    monkeypatch.setattr(_kernel.Owner, "obliged", refused)
    monkeypatch.setattr(_kernel.Owner, "latch", refused)


@pytest.fixture
def workspace(tmp_path: Path) -> Workspace:
    """A durable workspace (every protocol step fsyncs) with a short visibility deadline."""

    return Workspace.initialize(tmp_path / "workspace", durable=True, policy={"visibility_deadline_seconds": 0.05})


class Journal:
    """The targets of every ``fsync``, ``fdatasync`` and ``syncfs`` while :attr:`on`, in call order."""

    def __init__(self) -> None:
        self.on = False
        self.entries: list[str] = []

    def record(self, descriptor: int, prefix: str = "") -> None:
        if not self.on:
            return
        try:
            self.entries.append(prefix + os.readlink(f"/proc/self/fd/{descriptor}"))
        except OSError:
            self.entries.append(prefix + "<unknown>")

    @contextlib.contextmanager
    def recording(self) -> Iterator[None]:
        self.on = True
        try:
            yield
        finally:
            self.on = False

    def normalized(self, root: Path) -> list[str]:
        """The entries relative to *root*, with ids and tokens normalized (``syncfs:`` kept as a prefix)."""

        syncfs = {index for index, entry in enumerate(self.entries) if entry.startswith("syncfs:")}
        paths = _normalized([entry.removeprefix("syncfs:") for entry in self.entries], root)
        return [f"syncfs:{path}" if index in syncfs else path for index, path in enumerate(paths)]


@pytest.fixture
def journal(monkeypatch: pytest.MonkeyPatch) -> Journal:
    """Record every ``fsync``, ``fdatasync`` and ``syncfs`` target while :attr:`Journal.on`."""

    recorded = Journal()
    for name in ("fsync", "fdatasync"):
        original = getattr(os, name)

        def wrapper(descriptor: int, _original: Callable[[int], None] = original) -> None:
            recorded.record(descriptor)
            _original(descriptor)

        monkeypatch.setattr(os, name, wrapper)
    syncfs = _fs._syncfs

    def syncing(descriptor: int) -> None:
        recorded.record(descriptor, "syncfs:")
        syncfs(descriptor)

    monkeypatch.setattr(_fs, "_syncfs", syncing)
    return recorded


def _job(tmp_path: Path, workspace: Workspace) -> _kernel.JobRef:
    installed = v3.install(workspace, tmp_path / "package")
    return v3.submit(workspace, installed, {"start": "succeed"}, tag="pin", placement="project/pin")


def _pause(workspace: Workspace, ref: _kernel.JobRef) -> Path:
    return _requests.post(
        workspace, action="pause", job_id=ref.job_id, placement="project/pin", operator="cli", reason="pin"
    )


_MANAGER_SERVE: list[str] = [
    'jobs/owned',
    'jobs/ready/project/pin',
    'jobs/owned/O0',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/.state.json.T.tmp',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    '.httk-workspace/requests',
    'jobs/paused',
    'jobs/paused/project',
    'jobs/owned/O0',
    'jobs/paused/project/pin',
]


def test_a_manager_serving_one_request_keeps_its_sequence(
    tmp_path: Path, workspace: Workspace, journal: Journal
) -> None:
    ref = _job(tmp_path, workspace)
    _pause(workspace, ref)
    with TaskManager(workspace, heartbeat_interval=3600.0) as manager:
        manager._cache = _kernel.ListingCache()
        with journal.recording():
            assert manager._requests_pass()
    assert v3.only(workspace, "paused").job_id == ref.job_id
    _pinned("manager-serve", journal.normalized(workspace.root), _MANAGER_SERVE)


_DAEMON_SERVE: list[str] = [
    'jobs/owned',
    'jobs/ready/project/pin',
    'jobs/owned/O0',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/.state.json.T.tmp',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    '.httk-workspace/requests',
    'jobs/paused',
    'jobs/paused/project',
    'jobs/owned/O0',
    'jobs/paused/project/pin',
]


def test_the_daemon_serving_one_request_keeps_its_sequence(
    tmp_path: Path, workspace: Workspace, journal: Journal
) -> None:
    ref = _job(tmp_path, workspace)
    path = _pause(workspace, ref)
    owner = _kernel.register_owner(workspace, kind="daemon", label="pin", allocation=None, advertised={})
    with owner, journal.recording():
        (outcome,) = removal.serve_requests(workspace, [(ref, path)], owner=owner)
    assert outcome.status == "applied"
    _pinned("daemon-serve", journal.normalized(workspace.root), _DAEMON_SERVE)


_COMMIT: list[str] = [
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/.state.json.T.tmp',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    'jobs/owned/O0/pin--U0~p500~T~ready/.httk-job/.seal.json.T.tmp',
    'jobs/owned/O0/pin--U0~p500~T~ready/.httk-job',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/attempts',
    '.httk-workspace/tmp/O0.trash.T',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/logs/runlog.jsonl',
    'jobs/owned/O0/pin--U0~p500~T~ready/.state.json.T.tmp',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    'jobs/succeeded',
    'jobs/succeeded/project',
    'jobs/owned/O0',
    'jobs/succeeded/project/pin',
]


def test_a_manager_committing_one_attempt_keeps_its_sequence(
    tmp_path: Path, workspace: Workspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _job(tmp_path, workspace)
    commit = TaskManager._commit_outcome

    def recorded(self: TaskManager, *args: object, **kwargs: object) -> None:
        with journal.recording():
            commit(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(TaskManager, "_commit_outcome", recorded)
    with TaskManager(workspace, heartbeat_interval=3600.0) as manager:
        manager.run_until_idle(timeout=120)
    assert v3.only(workspace, "succeeded").job_id == ref.job_id
    _pinned("manager-commit", journal.normalized(workspace.root), _COMMIT)


def _dead_owner_holding(workspace: Workspace, ref: _kernel.JobRef) -> _kernel.Owner:
    """An owner that claimed *ref* and was then declared dead, its handle abandoned."""

    dead = v3.cli_owner(workspace)
    owned = _kernel.claim(workspace, dead, ref)
    assert owned is not None
    owned.append_log("claimed", **{"from": ref.state, "to": _kernel.OWNED})
    _kernel.attest_dead(workspace, dead.owner_id, by="operator", evidence=[], operator="pin", reason="pin")
    return dead


#: Changed by L3a (amendments 15, 27): the per-owner recovery seal, by fsync, comes first (the first four entries:
#: ``requests/``, ``owned/O0``, the held job directory, ``tmp/``); HEAD d667de7 began with ``jobs/ready`` (the return's
#: move).
_RECOVERY_FSYNC: list[str] = [
    '.httk-workspace/requests',
    'jobs/owned/O0',
    'jobs/owned/O0/pin--U0~p500~T~ready',
    '.httk-workspace/tmp',
    'jobs/ready',
    'jobs/ready/project',
    'jobs/owned/O0',
    'jobs/ready/project/pin',
    '.httk-workspace/tmp',
    '.httk-workspace/owners/O0',
    '.httk-workspace/owners/O0',
]


def test_the_recovery_of_one_held_job_keeps_its_sequence_under_fsync(
    tmp_path: Path, workspace: Workspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _job(tmp_path, workspace)
    workspace.configure_settings({"io.sync": "fsync", "io.workers": 1})  # the seal's fsyncs in one order
    dead = _dead_owner_holding(workspace, ref)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    with v3.cli_owner(workspace) as recoverer, journal.recording():
        report = _kernel.recover(workspace, recoverer, dead.owner_id)
    assert [item.job_id for item in report.returned] == [ref.job_id]
    _pinned("recovery-fsync", journal.normalized(workspace.root), _RECOVERY_FSYNC)


#: Changed by L3a (amendments 15, 27): the recovery seal, one syncfs on the workspace's one filesystem, comes first; HEAD
#: d667de7 was the fsync pin's sequence from ``jobs/ready`` on.
_RECOVERY_SYNCFS: list[str] = [
    'syncfs:.httk-workspace/requests',
    'jobs/ready',
    'jobs/ready/project',
    'jobs/owned/O0',
    'jobs/ready/project/pin',
    '.httk-workspace/tmp',
    '.httk-workspace/owners/O0',
    '.httk-workspace/owners/O0',
]


def test_the_recovery_of_one_held_job_keeps_its_sequence_under_the_protocol_predicate(
    tmp_path: Path, workspace: Workspace, journal: Journal, monkeypatch: pytest.MonkeyPatch
) -> None:
    ref = _job(tmp_path, workspace)
    dead = _dead_owner_holding(workspace, ref)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    # Host-independent: the predicate holds and syncfs is recorded without being called.
    monkeypatch.setattr(_fs, "protocol_mode", lambda path, setting: "syncfs", raising=False)
    monkeypatch.setattr(_fs, "_syncfs", lambda descriptor: journal.record(descriptor, "syncfs:"))
    with v3.cli_owner(workspace) as recoverer, journal.recording():
        report = _kernel.recover(workspace, recoverer, dead.owner_id)
    assert [item.job_id for item in report.returned] == [ref.job_id]
    _pinned("recovery-syncfs", journal.normalized(workspace.root), _RECOVERY_SYNCFS)


def test_the_cli_one_job_cancel_and_delete_register_nothing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # Their sequences are pinned by test_local_commands; here, with obligations refused, they still succeed.
    workspace, name = _new_workspace(tmp_path)
    ref = _cli_job(tmp_path, workspace, "seq-1")
    context = _context(tmp_path)
    assert command(["job", "request", "cancel", "--workspace", name, "--reason", "r", ref.job_id], context) == 0
    assert command(["job", "delete", "--workspace", name, "--yes", ref.job_id], context) == 0
    capsys.readouterr()
