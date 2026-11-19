"""A failed recovery seal is a recovery that did not finish (plan WORKFLOW_LOCAL_COMMANDS amendment 27).

The manager and ``gc`` keep the dead owner, report it and retry at their next probe; the manager keeps ticking. Only
the seal, which comes before anything moves, is retried so: a failure after a return's rename propagates, as before.
"""

import errno
import logging
import os
from pathlib import Path

import pytest

import v3_helpers as v3
from httk.workflow import TaskManager, Workspace, _death, _fs, _kernel, gc


@pytest.fixture
def workspace(tmp_path: Path) -> Workspace:
    return Workspace.initialize(tmp_path / "workspace", durable=True, policy={"visibility_deadline_seconds": 0.05})


def _dead_owner_holding(tmp_path: Path, workspace: Workspace) -> tuple[_kernel.Owner, _kernel.JobRef]:
    """An owner of another (fictitious) process that holds a job and was attested dead."""

    installed = v3.install(workspace, tmp_path / "package")
    ref = v3.submit(workspace, installed, {"start": "succeed"}, tag="held", placement="project/held")
    dead = _kernel.register_owner(
        workspace,
        kind="cli",
        label="elsewhere",
        allocation=None,
        advertised={},
        execution_scope=_death.ProcessIdentity("elsewhere.invalid", None, 4242, None),
    )
    assert _kernel.claim(workspace, dead, ref) is not None
    _kernel.attest_dead(workspace, dead.owner_id, by="operator", evidence=[], operator="t", reason="t")
    return dead, ref


def _failing_seal(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls: list[int] = []

    def failing(directories: object, setting: object, *, workers: int = 1) -> None:
        calls.append(1)
        raise _fs.SyncFailed(errno.EIO, "cannot sync the recovery seal to disk: Input/output error")

    monkeypatch.setattr(_fs, "seal_directories", failing)
    return calls


def test_a_manager_keeps_the_dead_owner_reports_it_and_keeps_ticking(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    dead, ref = _dead_owner_holding(tmp_path, workspace)
    calls = _failing_seal(monkeypatch)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    with TaskManager(workspace, heartbeat_interval=3600.0) as manager:
        with caplog.at_level(logging.WARNING, logger="httk.workflow.manager"):
            assert manager._recover_dead_owners() is False
            manager.tick()  # the next probe fails again; the tick goes on
        assert len(calls) == 2
        assert "did not finish" in caplog.text
        assert (dead.path / "owner.json").is_file() and dead.owner_id in _kernel.recoverable_owners(workspace)
        assert v3.find(workspace, ref.job_id).state == _kernel.OWNED  # nothing exposed
        monkeypatch.setattr(_fs, "seal_directories", _REAL_SEAL)  # the disk recovered: the next probe finishes
        assert manager._recover_dead_owners() is True
    assert v3.find(workspace, ref.job_id).state == "ready"


def test_gc_keeps_the_dead_owner_and_reports_it(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    dead, ref = _dead_owner_holding(tmp_path, workspace)
    calls = _failing_seal(monkeypatch)
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    with v3.cli_owner(workspace) as owner, caplog.at_level(logging.WARNING, logger="httk.workflow.gc"):
        report = gc.collect_garbage(workspace, owner=owner, categories=("dead_owners",), sizes=False)
    assert calls and "did not finish" in caplog.text
    assert not any(category.removed for category in report.categories)
    assert (dead.path / "owner.json").is_file() and dead.owner_id in _kernel.recoverable_owners(workspace)
    assert v3.find(workspace, ref.job_id).state == _kernel.OWNED


def test_a_failure_after_a_return_rename_propagates_out_of_the_manager(
    tmp_path: Path, workspace: Workspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead, ref = _dead_owner_holding(tmp_path, workspace)
    destination = workspace.jobs / "ready" / "project" / "held"
    monkeypatch.setattr(_kernel, "_pause", lambda seconds: None)
    fsync = os.fsync

    def failing(descriptor: int) -> None:
        # The returned job's destination directory: its fsync follows the rename that exposed the job.
        if os.readlink(f"/proc/self/fd/{descriptor}") == str(destination):
            raise OSError(errno.EIO, "Input/output error")
        fsync(descriptor)

    with TaskManager(workspace, heartbeat_interval=3600.0) as manager:
        monkeypatch.setattr(os, "fsync", failing)
        with pytest.raises(OSError, match="Input/output") as raised:
            manager._recover_dead_owners()
        monkeypatch.setattr(os, "fsync", fsync)
        assert not isinstance(raised.value, _kernel.RecoverySealFailed)
    assert v3.find(workspace, ref.job_id).state == "ready"  # renamed, its fsync failed: as at HEAD
    assert (dead.path / "owner.json").is_file()


_REAL_SEAL = _fs.seal_directories
