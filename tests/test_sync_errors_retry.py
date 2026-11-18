"""``io.sync_errors`` and the one automatic retry of a failed sync commit (plan
WORKFLOW_SYNC_SEGMENTS_AND_TRANSFER_SESSION §7.1, §7.2, §7.4; phase P1).

Under ``ignore`` a barrier trusts the writeback error its ``syncfs`` commit reports (after one more ``syncfs``);
nothing else is ignored, and the workflow store always fails. Under ``fail`` a push refused by the far side's failed
commit is delivered once more with ``--sync fsync``, and a pull whose commit failed is pulled once more into a fresh
landing under a fresh barrier.
"""

import argparse
import contextlib
import errno
import io
import json
import logging
import os
import re
import sys
import tarfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _fs, _landing, _moving, _store
from httk.workflow import scaffold as scaffold_module
from httk.workflow._setting_catalog import classify_setting
from httk.workflow._state import read_state_unowned
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _job as job_cli
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_creation import install, raw_package, ready
from test_job_transfer import adapter_calls, jobs, outgoing, registered, selected, starts
from test_moving import assert_in, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_sync_landings import (
    Events,
    adopt_stream,
    in_process_far_side,
    landings,
    other_device,
    statuses,
    transfer,
)
from test_sync_publication import durable_workspace, ejected, prepared_payloads, staged_job, submit
from test_transfer_workflows import GIT_ID, shipped, workflows
from test_transfer_workflows import job as workflow_job
from v3_helpers import cli_owner

_FS_LOGGER = "httk.workflow._fs"


def receive_tar(workspace: Workspace, landing: Path, **options: Any) -> list[str]:
    """The far side's receiver reading standard input, as ``job adopt --stdin-tar`` hands it its stream."""

    return _landing.receive_tar(workspace, landing, sys.stdin.buffer, **options)


@pytest.fixture(autouse=True)
def _fresh_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_fs, "_syncfs_works", {})


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A project with two ``local``-adapter remotes, so ``cluster:NAME`` and ``other:NAME`` reach this machine."""

    root = tmp_path / "project"
    initialize_project(root, name="sync-errors")
    # The adapter path, which Far scripts (a session's retry: test_session_push).
    fake_remote(root, template="local", name="cluster", transfer_session="off")
    fake_remote(root, template="local", name="other", transfer_session="off")
    return root


class Syncfs:
    """A scripted ``syncfs``: each call records its descriptor and raises the next failure (``None``: succeeds)."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, failures: Sequence[int | None] = ()) -> None:
        self.failures = list(failures)
        self.descriptors: list[int] = []
        real = _fs._syncfs

        def syncfs(descriptor: int) -> None:
            self.descriptors.append(descriptor)
            failure = self.failures.pop(0) if self.failures else None
            if failure is not None:
                raise OSError(failure, os.strerror(failure))
            real(descriptor)

        monkeypatch.setattr(_fs, "_syncfs", syncfs)


def fs_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    # Only _fs's records: another logger's level may have been lowered by an earlier test in the worker.
    return [record.getMessage() for record in caplog.records if record.name == _FS_LOGGER]


def read_counter(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count every :meth:`Workspace.read_settings` call (one element per call)."""

    calls: list[int] = []
    real = Workspace.read_settings

    def counting(workspace: Workspace) -> dict[str, object]:
        calls.append(1)
        return real(workspace)

    monkeypatch.setattr(Workspace, "read_settings", counting)
    return calls


def empty_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    data = io.BytesIO()
    tarfile.open(fileobj=data, mode="w").close()
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data.getvalue())))


# -- the io.sync_errors setting -------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["IGNORE", "", "warn", 0, None, True])
def test_io_sync_errors_is_validated_before_anything_is_written(tmp_path: Path, value: object) -> None:
    workspace, _ = registered(tmp_path, "a")
    before = (workspace.control / "format.json").read_bytes()
    with pytest.raises(ValueError, match="io.sync_errors"):
        workspace.configure_settings({"io.sync_errors": value})
    with pytest.raises(ValueError, match="io.sync_errors"):
        workspace.seed_settings({"io.sync_errors": value})
    assert (workspace.control / "format.json").read_bytes() == before
    assert workspace.io_sync_policy() == ("auto", "fail")


def test_io_sync_errors_takes_fail_and_ignore_and_is_a_recognized_engine_setting(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workspace, name = registered(tmp_path, "a")
    reader = Workspace(workspace.root)  # long-lived: reads afresh
    assert reader.io_sync_policy() == ("auto", "fail")
    workspace.configure_settings({"io.sync_errors": "ignore", "io.sync": "syncfs"})
    assert reader.io_sync_policy() == ("syncfs", "ignore")
    workspace.configure_settings({"io.sync_errors": "fail"})
    assert reader.io_sync_policy() == ("syncfs", "fail")
    workspace.unset_setting("io.sync_errors")
    assert reader.io_sync_policy() == ("syncfs", "fail")
    assert reader.io_sync() == "syncfs"  # io_sync() callers keep working
    assert workspace.seed_settings({"io.sync_errors": "ignore"})["io.sync_errors"] == "ignore"
    assert classify_setting("io.sync_errors", "ignore").state == "recognized"
    argv = ["workspace", "settings", "set", "--key", "io.sync_errors", "--value", "never", name]
    assert command(argv, CLIContext("httk", tmp_path)) != 0
    assert "io.sync_errors must be fail or ignore" in capsys.readouterr().err
    assert reader.io_sync_policy() == ("syncfs", "ignore")


@pytest.mark.parametrize("stored", ["IGNORE", "warn", 1, True, [], {}])
def test_an_invalid_stored_io_sync_errors_is_fail_with_one_warning(
    tmp_path: Path, stored: object, caplog: pytest.LogCaptureFixture
) -> None:
    workspace, _ = registered(tmp_path, "a")
    path = workspace.control / "format.json"
    document = json.loads(path.read_bytes())
    document["settings"]["io.sync_errors"] = stored
    document["settings"]["io.sync"] = "syncfs"
    path.write_text(json.dumps(document), encoding="utf-8")
    with caplog.at_level("WARNING", logger="httk.workflow.workspace"):
        policy = workspace.io_sync_policy()
    warnings = [record.getMessage() for record in caplog.records if record.name == "httk.workflow.workspace"]
    assert len(warnings) == 1 and warnings[0].startswith("io.sync")  # relayable from a far side
    if isinstance(stored, str | int) and not isinstance(stored, bool):
        assert policy == ("syncfs", "fail") and warnings[0].startswith(f"io.sync_errors is {stored!r}, not fail")
    else:  # not a JSON scalar: the settings themselves are invalid, so io.sync is fsync too
        assert policy == ("fsync", "fail")


def test_unreadable_settings_give_fsync_and_fail_with_one_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    workspace, _ = registered(tmp_path, "a")
    (workspace.control / "format.json").write_text("{", encoding="utf-8")
    with caplog.at_level("WARNING", logger="httk.workflow.workspace"):
        assert workspace.io_sync_policy() == ("fsync", "fail")
    warnings = [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING]
    expected = "io.sync is fsync and io.sync_errors is fail, since the settings cannot be read"
    assert [message.startswith(expected) for message in warnings] == [True]


def test_each_barrier_reads_its_settings_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    far, _ = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    durable = Workspace(far.root)
    calls = read_counter(monkeypatch)
    seen: list[tuple[object, str]] = []
    real_init = _fs.Barrier.__init__

    def init(barrier: _fs.Barrier, anchor: Path, setting: object, **options: Any) -> None:
        real_init(barrier, anchor, setting, **options)
        seen.append((setting, barrier.errors))

    monkeypatch.setattr(_fs.Barrier, "__init__", init)
    # The far side of a push.
    empty_stream(monkeypatch)
    assert receive_tar(durable, far.control / "transfers" / "incoming" / "a", barrier=True) == []
    assert (len(calls), seen) == (1, [("syncfs", "ignore")])
    # A transfer landing (pull, copies).
    calls.clear()
    with transfer_cli._sync_barrier(durable, far.control / "tmp", argparse.Namespace(quiet=True), inline=False):
        pass
    assert (len(calls), seen[-1]) == (1, ("syncfs", "ignore"))
    # Job creation and job submit.
    calls.clear()
    with scaffold_module._sync_barrier(durable, far.control / "tmp"):
        pass
    assert (len(calls), seen[-1]) == (1, ("syncfs", "ignore"))
    # Operators' bundles.
    calls.clear()
    bundle = far.control / "tmp" / "bundle"
    bundle.mkdir()
    _moving.sync_operator_bundles(durable, [bundle])
    assert (len(calls), seen[-1]) == (1, ("syncfs", "ignore"))


# -- the barrier under ignore ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("second", "said"),
    [(None, "(a second flush reported no further error)"), (errno.EIO, "so the flush may be incomplete")],
    ids=["second-syncfs-ok", "second-syncfs-fails"],
)
def test_ignore_trusts_a_commit_error_after_a_second_syncfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, second: int | None, said: str
) -> None:
    calls = Syncfs(monkeypatch, [None, errno.EDQUOT, second])
    with (
        caplog.at_level("WARNING", logger=_FS_LOGGER),
        _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True) as barrier,
    ):
        descriptor = barrier._descriptor
        barrier.commit()  # returns: the landing is published
        barrier.commit()  # a barrier still commits again (S2's job creation)
    # The prime, the failed commit, the second syncfs on the same descriptor, and the next commit.
    assert calls.descriptors == [descriptor] * 4
    (warning,) = fs_warnings(caplog)
    assert "reported a writeback error, which io.sync_errors=ignore trusts" in warning and said in warning
    assert os.strerror(errno.EDQUOT) in warning


def test_fail_raises_on_a_commit_error_without_a_second_syncfs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = Syncfs(monkeypatch, [None, errno.EDQUOT])
    with _fs.Barrier(tmp_path, "syncfs", quiet=True) as barrier, pytest.raises(_fs.SyncFailed, match="not durable"):
        barrier.commit()
    assert len(calls.descriptors) == 2
    with pytest.raises(ValueError, match="io.sync_errors"):
        _fs.Barrier(tmp_path, "syncfs", errors="warn")  # type: ignore[arg-type]


@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EPERM, errno.EINVAL], ids=["ENOSYS", "EPERM", "EINVAL"])
def test_ignore_never_ignores_an_unsupported_syncfs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: int
) -> None:
    # A primed barrier whose commit says syncfs is not supported: it fails as under fail, with no second call.
    calls = Syncfs(monkeypatch, [None, error])
    with (
        _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed),
    ):
        barrier.commit()
    assert len(calls.descriptors) == 2
    # A no-prime barrier keeps its unsupported fallback: the tree is walked instead (and nothing is trusted).
    walked: list[int] = []
    real = _fs.fsync_landing

    def walk(anchor: int, **options: Any) -> int:
        walked.append(real(anchor, **options))
        return walked[-1]

    monkeypatch.setattr(_fs, "fsync_landing", walk)
    calls = Syncfs(monkeypatch, [error])
    with _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True, prime=False) as barrier:
        barrier.commit()
        assert barrier.strategy == "fsync"
    assert len(calls.descriptors) == 1 and len(walked) == 1


def test_ignore_leaves_the_prime_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    calls = Syncfs(monkeypatch, [errno.EIO, None])
    with (
        caplog.at_level("WARNING", logger=_FS_LOGGER),
        _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True) as barrier,
    ):
        barrier.commit()
    # The prime's error is logged and ignored as under fail: no second syncfs for it, no "trusts" warning.
    assert len(calls.descriptors) == 2
    (warning,) = fs_warnings(caplog)
    assert "first syncfs failed before anything was landed" in warning
    # And an unsupported prime still makes the strategy fsync.
    Syncfs(monkeypatch, [errno.ENOSYS])
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    with _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True) as barrier:
        assert barrier.strategy == "fsync"


def test_ignore_has_no_effect_on_the_walk_of_strategy_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "landed").write_bytes(b"x")

    def failing(anchor: int, **options: Any) -> int:
        raise _fs.SyncFailed(errno.EIO, "injected walk failure")

    monkeypatch.setattr(_fs, "fsync_landing", failing)
    with (
        _fs.Barrier(tmp_path, "fsync", errors="ignore", quiet=True) as barrier,
        pytest.raises(_fs.SyncFailed, match="injected walk failure"),
    ):
        barrier.commit()


def test_ignore_has_no_effect_on_an_unreadable_walk(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("root opens a file of mode 000")
    (tmp_path / "landed").write_bytes(b"x")
    (tmp_path / "landed").chmod(0)
    with (
        _fs.Barrier(tmp_path, "fsync", errors="ignore", quiet=True) as barrier,
        pytest.raises(_fs.SyncUnreadable, match="cannot read"),
    ):
        barrier.commit()


def test_ignore_trusts_a_no_prime_sync_of_operators_trees(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    tree = tmp_path / "bundle"
    tree.mkdir()
    Syncfs(monkeypatch, [errno.EIO, None])
    with caplog.at_level("WARNING", logger=_FS_LOGGER):
        assert _fs.sync_written(tmp_path, [tree], "syncfs", errors="ignore") == [tree]
    assert any(
        "io.sync_errors=ignore trusts (a second flush reported no further error)" in warning
        for warning in fs_warnings(caplog)
    )
    Syncfs(monkeypatch, [errno.EIO])
    with pytest.raises(_fs.SyncFailed):
        _fs.sync_written(tmp_path, [tree], "syncfs")


# -- the sites under ignore ------------------------------------------------------------------------------------------


def test_a_pushed_stream_under_ignore_is_adopted_after_a_commit_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    calls = Syncfs(monkeypatch, [None, errno.EDQUOT, None])
    with caplog.at_level("WARNING", logger=_FS_LOGGER):
        assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 0
    document = json.loads(capsys.readouterr().out)
    assert [entry["status"] for entry in document["bundles"]] == ["adopted"] * 2 and "sync_failed" not in document
    assert len(calls.descriptors) == 3
    assert any("trusts (a second flush reported no further error)" in warning for warning in fs_warnings(caplog))
    assert_in(far, plan)


def test_an_inline_fsync_error_under_ignore_still_refuses_the_stream_without_sync_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "fsync", "io.sync_errors": "ignore"})
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    real = os.fsync

    def fsync(descriptor: int) -> None:
        if os.path.isfile(f"/proc/self/fd/{descriptor}") and not os.path.isdir(f"/proc/self/fd/{descriptor}"):
            raise OSError(errno.EIO, "injected inline fsync failure")
        real(descriptor)

    monkeypatch.setattr(os, "fsync", fsync)
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 2
    document = json.loads(capsys.readouterr().out)
    assert "injected inline fsync failure" in document["refused"] and "sync_failed" not in document
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan) and landings(far) == []


def test_job_creation_under_ignore_publishes_after_a_commit_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    ws.configure_settings({"io.sync_errors": "ignore"})
    create = staged_job(ws, tmp_path)
    calls = Syncfs(monkeypatch, [None, errno.EIO, None])
    with caplog.at_level("WARNING", logger=_FS_LOGGER):
        job = create()
    assert (job.payload / "job.json").is_file() and len(calls.descriptors) == 3
    assert any("trusts (a second flush reported no further error)" in warning for warning in fs_warnings(caplog))


def test_adopt_path_under_ignore_takes_the_operator_s_bundle_after_a_sync_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    target.configure_settings({"io.sync_errors": "ignore"})
    calls = Syncfs(monkeypatch, [errno.EIO, None])
    argv = ["job", "adopt", "--json", "--workspace", target_name, str(bundle)]
    assert command(argv, CLIContext("httk", tmp_path)) == 0
    assert [entry["status"] for entry in json.loads(capsys.readouterr().out)["bundles"]] == ["adopted"]
    assert len(calls.descriptors) == 2
    assert_in(target, plan)


def test_the_workflow_store_ignores_ignore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    ws.configure_settings({"io.sync_errors": "ignore"})
    calls = Syncfs(monkeypatch, [None, errno.EIO, None])
    with pytest.raises(_fs.SyncFailed, match="the workflow local:tests.raw was not published, nothing changed"):
        install(ws, raw_package(tmp_path / "package"))
    assert len(calls.descriptors) == 2  # no second syncfs: nothing trusted
    assert _store.list_installed(ws) == []


# -- the far side of a push: sync_failed and --sync fsync ------------------------------------------------------------


def test_only_a_failed_commit_marks_the_refusal_sync_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, _ = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    Syncfs(monkeypatch, [None, errno.EDQUOT])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 2
    assert json.loads(capsys.readouterr().out)["sync_failed"] is True
    # A malformed stream, a refused member, a failed prime: refusals, never sync_failed.
    Syncfs(monkeypatch, [errno.EIO])
    assert adopt_stream(monkeypatch, tmp_path, data[:700], "--workspace", far_name) == 2
    document = json.loads(capsys.readouterr().out)
    assert "refused" in document and "sync_failed" not in document
    evil = tarfile.TarInfo("../escape")
    evil.size = 1
    Syncfs(monkeypatch, [])
    assert adopt_stream(monkeypatch, tmp_path, stream(Path("."), [], [evil]), "--workspace", far_name) == 2
    document = json.loads(capsys.readouterr().out)
    assert "refused" in document and "sync_failed" not in document


def test_a_failed_commit_without_json_hints_at_the_remedy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, _ = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    Syncfs(monkeypatch, [None, errno.EDQUOT])
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    argv = ["job", "adopt", "--stdin-tar", "--workspace", far_name]
    assert command(argv, CLIContext("httk", tmp_path)) != 0
    err = capsys.readouterr().err
    assert "refused the stream, nothing of it was used" in err and "deliver the stream again" in err
    assert "io.sync_errors" in err and "--sync" not in err  # the hidden option is not advertised


def test_sync_fsync_forces_inline_fsync_whatever_io_sync_says(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    durable: list[bool] = []
    real = _fs.extract_tar

    def extract(*arguments: Any, **options: Any) -> int:
        durable.append(options["durable"])
        return real(*arguments, **options)

    monkeypatch.setattr(_fs, "extract_tar", extract)
    calls = Syncfs(monkeypatch)
    assert adopt_stream(monkeypatch, tmp_path, data, "--sync", "fsync", "--workspace", far_name) == 0
    assert [entry["status"] for entry in json.loads(capsys.readouterr().out)["bundles"]] == ["adopted"] * 2
    assert durable == [True] and calls.descriptors == []
    assert_in(far, plan)
    # No effect in a non-durable workspace: no barrier, no fsync.
    holds, plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    durable.clear()
    assert adopt_stream(monkeypatch, tmp_path, data, "--sync", "fsync", "--no-durable", "--workspace", far_name) == 0
    assert durable == [False]
    assert_in(far, plan)


def test_sync_is_refused_without_stdin_tar_and_takes_only_fsync(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _, far_name = registered(tmp_path, "far")
    argv = ["job", "adopt", "--sync", "fsync", "--workspace", far_name, str(tmp_path / "bundle")]
    assert command(argv, CLIContext("httk", tmp_path)) != 0
    assert "--sync applies to the landing of --stdin-tar" in capsys.readouterr().err
    argv = ["job", "adopt", "--stdin-tar", "--sync", "syncfs", "--workspace", far_name]
    assert command(argv, CLIContext("httk", tmp_path)) == 2
    assert "invalid choice: 'syncfs'" in capsys.readouterr().err


def test_receive_tar_raises_commit_failed_only_for_the_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    far, _ = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    durable = Workspace(far.root)
    Syncfs(monkeypatch, [None, errno.EIO])
    empty_stream(monkeypatch)
    with pytest.raises(job_cli._CommitFailed, match="refused the stream, nothing of it was used: .*syncfs of"):
        receive_tar(durable, far.control / "transfers" / "incoming" / "c", barrier=True)
    # Under --sync fsync nothing is committed by syncfs: no commit can fail so.
    Syncfs(monkeypatch, [None, errno.EIO])
    empty_stream(monkeypatch)
    assert receive_tar(durable, far.control / "transfers" / "incoming" / "d", barrier=True, force_fsync=True) == []


# -- the client of a push: one retry ---------------------------------------------------------------------------------


type Answer = tuple[int, str, str]


class Far:
    """A recording, scripted far side of ``deliver``: each call's argv and source; each answer from *answers*
    (``None``: run ``job adopt`` in this process on the delivered stream; a callable: the in-process answer, as
    that callable changes it). Any other delivery (``workflow install``) goes through the real adapter."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, answers: Sequence[Answer | Callable[[Answer], Answer] | None] = ()
    ) -> None:
        self.answers = list(answers)
        self.calls: list[list[str]] = []
        self.sources: list[str] = []
        self.present: list[bool] = []
        adapter = transfer_cli._deliver_call
        in_process_far_side(monkeypatch)
        real = transfer_cli._deliver_call

        def deliver_call(
            end: Any, context: CLIContext, arguments: Any, source: str, files: list[str], argv: list[str]
        ) -> tuple[int, str, str]:
            self.calls.append(list(argv))
            if "adopt" not in argv:
                return adapter(end, context, arguments, source, files, argv)
            self.sources.append(source)
            self.present.append(all((Path(source) / name).is_dir() for name in files))
            answer = self.answers.pop(0) if self.answers else None
            if answer is None:
                return real(end, context, arguments, source, files, argv)
            if callable(answer):
                return answer(real(end, context, arguments, source, files, argv))
            return answer

        monkeypatch.setattr(transfer_cli, "_deliver_call", deliver_call)

    def tokens(self) -> list[str]:
        return [argv[argv.index("--token") + 1] for argv in self.calls]

    def strengthened(self) -> list[bool]:
        return ["--sync" in argv and argv[argv.index("--sync") + 1] == "fsync" for argv in self.calls]


def sync_failed(workspace_id: str, **extra: Any) -> tuple[int, str, str]:
    refusal = {"bundles": [], "workspace_id": workspace_id, "refused": "refused the stream: syncfs failed"}
    return 2, json.dumps({**refusal, "sync_failed": True, **extra}), ""


def test_a_failed_commit_refusal_is_delivered_once_more_with_a_fresh_token_and_sync_fsync(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    plan = jobs(near, 3)
    far_side = Far(monkeypatch)
    calls = Syncfs(monkeypatch, [None, errno.EDQUOT])
    assert transfer(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    assert far_side.strengthened() == [False, True]
    first, second = far_side.tokens()
    assert first != second and re.fullmatch(r"[0-9a-z]{16,64}", second)
    assert [argv[:4] for argv in far_side.calls] == [far_side.calls[0][:4]] * 2  # the same job adopt --json --stdin-tar
    assert "delivering 3 bundle(s) again, with an fsync of every file" in captured.err
    assert len(calls.descriptors) == 2  # the first delivery's prime and failed commit; the retry calls no syncfs
    assert_in(far, plan)
    assert outgoing(near) == [] and landings(far) == []


def test_the_retry_announcement_is_quiet_with_q(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    plan = jobs(near, 1)
    far_side = Far(monkeypatch)
    Syncfs(monkeypatch, [None, errno.EDQUOT])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert "again" not in capsys.readouterr().err and len(far_side.calls) == 2


def test_a_refused_retry_takes_a_local_source_s_holds_back(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 2)
    refused = {"bundles": [], "workspace_id": far.workspace_id, "refused": "refused the stream: injected"}
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id), (2, json.dumps(refused), "")])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["not_moved"] * 2
    assert all("refused the stream: injected" in outcome["message"] for outcome in outcomes)
    assert far_side.strengthened() == [False, True]
    assert_in(near, plan)
    assert outgoing(near) == []


def test_a_retry_is_never_retried(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 2)
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id)] * 3)
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    # The second refusal is the answer: a local source takes its holds back.
    assert [outcome["status"] for outcome in statuses(capsys)] == ["not_moved"] * 2
    assert far_side.strengthened() == [False, True]
    assert_in(near, plan)


def test_an_unreadable_retry_answer_keeps_the_holds_and_a_resume_delivers(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 2)
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id), (255, "", "ssh: connection reset")])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["failed"] * 2
    assert all("whether the jobs arrived is unknown" in outcome["message"] for outcome in outcomes)
    assert len(far_side.calls) == 2 and sorted(outgoing(near)) == sorted(o["transfer_id"] for o in outcomes)
    assert transfer(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert_in(far, plan)


def test_a_retry_answered_by_another_workspace_keeps_the_holds(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 1)
    other = "00000000-0000-4000-8000-000000000001"
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id), sync_failed(other)])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    (outcome,) = statuses(capsys)
    # Not taken back, though the second answer is a refusal: the holds stay for --resume (at least once).
    assert outcome["status"] == "failed" and "whether the jobs arrived is unknown" in outcome["message"]
    assert f"answered the second delivery as workspace {other}" in outcome["message"]
    assert "httk job transfer --resume" in outcome["message"]
    assert len(far_side.calls) == 2 and outgoing(near) == [outcome["transfer_id"]]


@pytest.mark.parametrize("marker", ["true", 1, None, [True]], ids=["string", "int", "null", "list"])
def test_a_sync_failed_marker_that_is_not_a_boolean_is_an_unreadable_answer(
    project: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    marker: object,
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 1)
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id, sync_failed=marker)])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    (outcome,) = statuses(capsys)
    assert outcome["status"] == "failed" and "whether the jobs arrived is unknown" in outcome["message"]
    assert len(far_side.calls) == 1 and outgoing(near) == [outcome["transfer_id"]]


def test_other_refusals_and_false_markers_are_not_retried(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 1)
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id, sync_failed=False)])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    assert [outcome["status"] for outcome in statuses(capsys)] == ["not_moved"]
    assert len(far_side.calls) == 1
    # A real refusal of another kind (a member already there would be a bundle refusal; a malformed stream is not):
    plan = jobs(near, 1)
    far_side = Far(monkeypatch, [(2, json.dumps({"bundles": [], "workspace_id": None, "refused": "no such"}), "")])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    assert [outcome["status"] for outcome in statuses(capsys)] == ["not_moved"]
    assert len(far_side.calls) == 1


def test_no_retry_under_ignore(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    plan = jobs(near, 2)
    far_side = Far(monkeypatch)
    calls = Syncfs(monkeypatch, [None, errno.EDQUOT, None])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert far_side.strengthened() == [False] and len(calls.descriptors) == 3
    assert_in(far, plan)


def test_a_remote_source_s_retry_reuses_the_relay(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 2)
    pulls: list[str] = []
    real = transfer_cli._adapter

    def adapter(end: Any, context: Any, arguments: Any, operation: str, *rest: Any) -> None:
        if operation == "pull":
            pulls.append(str(rest[1]))
        real(end, context, arguments, operation, *rest)

    monkeypatch.setattr(transfer_cli, "_adapter", adapter)
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id)])
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{near_name}", f"other:{far_name}") == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    # One pull into the relay; both deliveries from that same, still present, relay.
    assert len(pulls) == 1 and far_side.sources == [pulls[0]] * 2 and far_side.present == [True, True]
    assert far_side.strengthened() == [False, True]
    assert_in(far, plan)
    assert outgoing(near) == []


def test_a_remote_source_s_refused_retry_keeps_its_holds(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 1)
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id)] * 2)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{near_name}", f"other:{far_name}") == 1
    (outcome,) = statuses(capsys)
    assert outcome["status"] == "failed" and "stays held" in outcome["message"]
    assert len(far_side.calls) == 2 and outgoing(near) == [outcome["transfer_id"]]


# -- the counts: +1 connection and +1 remote start only when a retry happens ----------------------------------------

_FAILING_HTTK = '''import errno
import os
import runpy
import sys

argv = sys.argv[1:]
if os.environ.get("HTTK_TEST_FAIL_ADOPT_COMMIT") and "adopt" in argv and "--stdin-tar" in argv and "--sync" not in argv:
    from httk.workflow import _fs

    real = _fs._syncfs
    calls = []

    def syncfs(descriptor):
        calls.append(descriptor)
        if len(calls) == 2:  # the barrier's commit, after its prime
            raise OSError(errno.EDQUOT, os.strerror(errno.EDQUOT))
        real(descriptor)

    _fs._syncfs = syncfs
sys.argv = ["httk", *argv]
runpy.run_module("httk.core.cli", run_name="__main__", alter_sys=True)
'''


def failing_far_commits(tmp_path: Path, remote: Remote) -> None:
    """Make the stand-in cluster's ``httk`` fail the sync commit of every ``job adopt --stdin-tar`` without
    ``--sync`` while ``HTTK_TEST_FAIL_ADOPT_COMMIT`` is set."""

    wrapper = tmp_path / "failing_httk.py"
    wrapper.write_text(_FAILING_HTTK, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')


@pytest.mark.parametrize("retry", [False, True], ids=["no-retry", "retry"])
def test_local_to_remote_over_ssh_costs_one_more_call_only_for_a_retry(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], retry: bool
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="ssh-retry")
    fake_remote(project, transfer_session="off")  # the adapter path's count (a session's: test_session_push)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    far.configure_settings({"io.sync": "syncfs"})
    failing_far_commits(tmp_path, remote)
    if retry:
        monkeypatch.setenv("HTTK_TEST_FAIL_ADOPT_COMMIT", "1")
    calls = adapter_calls(monkeypatch)
    before = len(remote.commands())
    plan = jobs(near, 3)
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 3
    commands = remote.commands()[before:]
    assert calls == ["deliver job adopt"] * (2 if retry else 1)
    assert (len(commands), starts(commands)) == ((2, 2) if retry else (1, 1))
    assert ["--sync fsync" in item for item in commands] == ([False, True] if retry else [False])
    assert_in(far, plan)
    assert outgoing(near) == []


@pytest.mark.parametrize("retry", [False, True], ids=["no-retry", "retry"])
def test_remote_to_remote_over_ssh_costs_one_more_call_only_for_a_retry(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], retry: bool
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="ssh-relay-retry")
    # The adapter path on both ends, (A, A): its count (the sessions' retry: test_remote_to_remote).
    fake_remote(project, transfer_session="off")
    fake_remote(project, name="other", transfer_session="off")
    near, near_name = registered(remote.root, "near")
    far, far_name = registered(remote.root, "far")
    far.configure_settings({"io.sync": "syncfs"})
    failing_far_commits(tmp_path, remote)
    if retry:
        monkeypatch.setenv("HTTK_TEST_FAIL_ADOPT_COMMIT", "1")
    calls = adapter_calls(monkeypatch)
    before = len(remote.commands())
    plan = jobs(near, 3)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{near_name}", f"other:{far_name}") == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 3
    adoptions = ["deliver job adopt"] * (2 if retry else 1)
    # No second pull: the retry delivers the relay's copies again.
    assert calls == ["job transfer", "pull", *adoptions, "job transfer"]
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == ((5, 4) if retry else (4, 3))
    assert_in(far, plan)
    assert outgoing(near) == []


# -- the local side of a pull: one re-pull --------------------------------------------------------------------------


def recorded_pulls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The landing directory of every adapter ``pull`` (installed after :class:`Events`, which wraps it too)."""

    pulls: list[str] = []
    real = transfer_cli._adapter

    def adapter(end: Any, context: Any, arguments: Any, operation: str, *rest: Any) -> None:
        if operation == "pull":
            if pulls and Path(pulls[0]).parent.exists():
                pulls.append("the earlier landing was not discarded before this pull")
            pulls.append(str(rest[1]))
        real(end, context, arguments, operation, *rest)

    monkeypatch.setattr(transfer_cli, "_adapter", adapter)
    return pulls


def test_a_failed_pull_commit_is_pulled_once_more_into_a_fresh_landing_under_a_fresh_barrier(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs"})
    plan = jobs(far, 3)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    pulls = recorded_pulls(monkeypatch)
    assert transfer(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    # Two pulls into two landings, each under its own barrier, primed before its pull, committed after it.
    assert len(pulls) == 2 and pulls[0] != pulls[1] and Path(pulls[0]).parent != Path(pulls[1]).parent
    kinds = events.kinds()
    assert kinds.count("enter") == 2 and kinds.count("syncfs") == 4
    (first, _), (second, _) = events.barriers
    assert first == second == "syncfs"
    descriptors = events.syncfs_descriptors()
    assert descriptors[0] == descriptors[1] and descriptors[2] == descriptors[3]
    syncfs = [index for index, kind in enumerate(kinds) if kind == "syncfs"]
    pull_starts = [index for index, kind in enumerate(kinds) if kind == "pull-start"]
    pull_ends = [index for index, kind in enumerate(kinds) if kind == "pull-end"]
    assert syncfs[0] < pull_starts[0] < pull_ends[0] < syncfs[1] < syncfs[2] < pull_starts[1] < pull_ends[1]
    assert pull_ends[1] < syncfs[3] < events.first("take")
    assert "again" in captured.err
    assert not Path(pulls[0]).exists()  # the failed landing was discarded
    assert_in(near, plan)
    assert outgoing(far) == [] and not list((near.control / "tmp").glob("*.landing.*"))


def test_a_second_failed_pull_commit_fails_the_chunk_and_a_resume_adopts(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs"})
    plan = jobs(far, 2)
    Events(monkeypatch, syncfs_failures=[None, errno.EIO, None, errno.EDQUOT])
    pulls = recorded_pulls(monkeypatch)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["failed"] * 2
    assert all("not durable" in outcome["message"] and "stays held" in outcome["message"] for outcome in outcomes)
    assert len(pulls) == 2
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert sorted(outgoing(far)) == sorted(outcome["transfer_id"] for outcome in outcomes)
    assert not list((near.control / "tmp").glob("*.landing.*"))
    pulls.clear()
    assert transfer(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2 and len(pulls) == 1
    assert_in(near, plan)


def test_a_failed_walk_of_an_fsync_pull_is_not_pulled_again(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "fsync"})
    plan = jobs(far, 1)

    def failing(anchor: int, **options: Any) -> int:
        raise _fs.SyncFailed(errno.EIO, "injected walk failure")

    monkeypatch.setattr(_fs, "fsync_landing", failing)
    pulls = recorded_pulls(monkeypatch)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    (outcome,) = statuses(capsys)
    assert outcome["status"] == "failed" and "injected walk failure" in outcome["message"]
    assert len(pulls) == 1 and outgoing(far) == [outcome["transfer_id"]]


def test_a_pull_under_ignore_is_adopted_without_a_re_pull(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    plan = jobs(far, 2)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO, None])
    pulls = recorded_pulls(monkeypatch)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert len(pulls) == 1 and events.kinds().count("syncfs") == 3 and events.kinds().count("enter") == 1
    assert_in(near, plan)


# -- review corrections ----------------------------------------------------------------------------------------------


def leftover_landing(far: Workspace, near: Workspace, token: str) -> tuple[Path, str]:
    """A landing ``transfers/incoming/<token>/`` holding an unsynced bundle of an interrupted earlier delivery."""

    holds, _ = hold_jobs(near, 1)
    landing = far.control / "transfers" / "incoming" / token
    landing.mkdir(parents=True)
    (landing / holds[0].transfer_id).mkdir()
    (landing / holds[0].transfer_id / "partial").write_bytes(b"x")
    return landing, holds[0].transfer_id


def test_sync_fsync_refuses_an_existing_landing_before_changing_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    token = "0123456789abcdef0123"
    landing, leftover = leftover_landing(far, near, token)
    holds, plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    argv = ["--sync", "fsync", "--token", token, "--workspace", far_name]
    assert adopt_stream(monkeypatch, tmp_path, data, *argv) == 2
    document = json.loads(capsys.readouterr().out)
    assert "--sync fsync needs a fresh landing" in document["refused"] and "sync_failed" not in document
    # Nothing adopted, the leftover neither adopted nor discarded.
    assert sorted(os.listdir(landing)) == [leftover] and (landing / leftover / "partial").read_bytes() == b"x"
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    # receive_tar itself refuses it the same way.
    empty_stream(monkeypatch)
    with pytest.raises(ValueError, match="needs a fresh landing"):
        receive_tar(Workspace(far.root), landing, barrier=True, force_fsync=True)
    assert sorted(os.listdir(landing)) == [leftover]
    # Without --sync a delivery may reuse the token, but the leftover is still never returned (so never adopted).
    empty_stream(monkeypatch)
    assert receive_tar(Workspace(far.root), landing, barrier=True) == []
    assert sorted(os.listdir(landing)) == [leftover]


@pytest.mark.parametrize("durable", [True, False], ids=["durable", "non-durable"])
def test_sync_fsync_refuses_an_existing_landing_in_any_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], durable: bool
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    token = "0123456789abcdef0124"
    landing, leftover = leftover_landing(far, near, token)
    holds, plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    argv = ["--sync", "fsync", "--token", token, *([] if durable else ["--no-durable"]), "--workspace", far_name]
    assert adopt_stream(monkeypatch, tmp_path, data, *argv) == 2
    assert "--sync fsync needs a fresh landing" in json.loads(capsys.readouterr().out)["refused"]
    assert sorted(os.listdir(landing)) == [leftover]
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)


def test_sync_fsync_refuses_a_landing_created_concurrently(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    far, _ = registered(tmp_path, "far")
    landing = far.control / "transfers" / "incoming" / "0123456789abcdef0125"
    real = _fs.make_dirs

    def racing(path: Path, **options: Any) -> None:
        real(path, **options)
        if path == landing.parent:
            # Another delivery creates the landing between the parents' creation and the exclusive mkdir.
            landing.mkdir()
            (landing / "theirs").write_bytes(b"x")

    monkeypatch.setattr(_fs, "make_dirs", racing)
    empty_stream(monkeypatch)
    with pytest.raises(ValueError, match="needs a fresh landing"):
        receive_tar(Workspace(far.root), landing, barrier=True, force_fsync=True)
    assert os.listdir(landing) == ["theirs"] and (landing / "theirs").read_bytes() == b"x"


def test_a_reused_token_adopts_only_the_bundles_its_stream_carries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    token = "0123456789abcdef0126"
    landing, leftover = leftover_landing(far, near, token)  # an unrelated bundle of an interrupted delivery
    holds, plan = hold_jobs(near, 2)
    resent = holds[0].transfer_id
    (landing / resent).mkdir()  # a re-sent bundle that the interrupted delivery had begun
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    assert adopt_stream(monkeypatch, tmp_path, data, "--token", token, "--workspace", far_name) == 0
    captured = capsys.readouterr()
    entries = json.loads(captured.out)["bundles"]
    assert sorted(entry["bundle"] for entry in entries) == sorted(hold.transfer_id for hold in holds)
    assert [entry["status"] for entry in entries] == ["adopted"] * 2
    assert_in(far, plan)
    # The leftover was neither adopted nor discarded, and a warning says so.
    assert os.listdir(landing) == [leftover] and (landing / leftover / "partial").read_bytes() == b"x"
    assert f"warning: left 1 entr(ies) in {landing}" in captured.err and leftover in captured.err


def test_a_top_level_non_directory_member_still_refuses_the_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _, far_name = registered(tmp_path, "far")
    member = tarfile.TarInfo("loose-file")
    member.size = 1
    assert adopt_stream(monkeypatch, tmp_path, stream(Path("."), [], [member]), "--workspace", far_name) == 2
    assert "not a directory: 'loose-file'" in json.loads(capsys.readouterr().out)["refused"]


def test_extract_tar_reports_the_top_level_names_of_its_stream(tmp_path: Path) -> None:
    (tmp_path / "earlier").mkdir()
    members = []
    for name, kind in (
        ("./a/x", tarfile.REGTYPE),
        ("b", tarfile.DIRTYPE),
        ("/c/d/e", tarfile.REGTYPE),
        (".", tarfile.DIRTYPE),
    ):
        member = tarfile.TarInfo(name)
        member.type = kind
        member.size = 1 if kind == tarfile.REGTYPE else 0
        members.append(member)
    names: set[str] = set()
    assert _fs.extract_tar(io.BytesIO(stream(Path("."), [], members)), tmp_path, top_level=names) == 2
    assert names == {"a", "b", "c"}
    names.clear()
    assert _fs.extract_tar(io.BytesIO(stream(Path("."), [], members)), tmp_path, workers=4, top_level=names) == 2
    assert names == {"a", "b", "c"}


def test_a_retry_adopted_by_another_workspace_says_so_and_keeps_the_holds(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 1)
    expected = "00000000-0000-4000-8000-000000000002"
    far_side = Far(monkeypatch, [sync_failed(expected)])  # the second delivery is adopted for real, by far
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    (outcome,) = statuses(capsys)
    assert outcome["status"] == "failed" and len(far_side.calls) == 2
    message = outcome["message"]
    assert f"adopted the jobs into workspace {far.workspace_id}, not the expected workspace {expected}" in message
    assert "the holds are kept" in message and "httk job transfer --resume" in message
    assert outgoing(near) == [outcome["transfer_id"]]
    assert_in(far, plan)
    # The resume finds them there and releases the holds.
    assert transfer(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert outgoing(near) == []


def test_sync_fsync_names_itself_as_the_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, _ = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    assert adopt_stream(monkeypatch, tmp_path, data, "--sync", "fsync", "--workspace", far_name) == 0
    err = capsys.readouterr().err
    assert "landed file(s) durable (fsync: --sync fsync)" in err and "io.sync is fsync" not in err


def test_under_ignore_an_unsupported_syncfs_at_the_commit_refuses_without_sync_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    holds, _ = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    Syncfs(monkeypatch, [None, errno.ENOSYS])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 2
    document = json.loads(capsys.readouterr().out)
    assert "not durable" in document["refused"] and "sync_failed" not in document
    # The same under fail is marked.
    far.configure_settings({"io.sync_errors": "fail"})
    Syncfs(monkeypatch, [None, errno.ENOSYS])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 2
    assert json.loads(capsys.readouterr().out)["sync_failed"] is True


@pytest.mark.parametrize("policy", ["fail", "ignore"])
def test_an_unsupported_syncfs_at_a_pull_commit_is_pulled_again_only_under_fail(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], policy: str
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs", "io.sync_errors": policy})
    plan = jobs(far, 1)
    Events(monkeypatch, syncfs_failures=[None, errno.ENOSYS])
    pulls = recorded_pulls(monkeypatch)
    code = transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name)
    (outcome,) = statuses(capsys)
    if policy == "fail":
        assert code == 0 and outcome["status"] == "adopted" and len(pulls) == 2
        assert_in(near, plan)
    else:
        assert code == 1 and outcome["status"] == "failed" and len(pulls) == 1
        assert outgoing(far) == [outcome["transfer_id"]]


def test_the_retry_repeats_the_first_delivery_and_its_answer_ships_the_workflows(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    job_id = workflow_job(near, workflows(near, tmp_path))
    far_side = Far(monkeypatch, [sync_failed(far.workspace_id)])
    argv = ["job", "transfer", "--json", "-q", "--job", job_id, near_name, f"cluster:{far_name}"]
    assert command(argv, CLIContext("httk", project)) == 0
    (outcome,) = json.loads(capsys.readouterr().out)["transfers"]
    assert outcome["status"] == "adopted"
    # From the second answer's workflows report: both were missing there, so both were shipped.
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    first, second, install_call = far_side.calls
    assert install_call[1:3] == ["workflow", "install"]
    index = first.index("--token")
    expected = [*first[: index + 2], "--sync", "fsync", *first[index + 2 :]]
    expected[index + 1] = second[index + 1]
    assert second == expected and second[index + 1] != first[index + 1]
    assert first.count("--built-for") == 2 and {GIT_ID, "local:caller"} <= set(first)
    assert {found.id for found in _store.list_installed(far)} == {GIT_ID, "local:caller"}
    assert find(far, job_id) is not None and outgoing(near) == []


def test_an_adoption_document_marked_sync_failed_is_not_retried(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 2)

    def marked(answer: Answer) -> Answer:
        code, out, err = answer
        return code, json.dumps({**json.loads(out), "sync_failed": True}), err

    far_side = Far(monkeypatch, [marked])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert len(far_side.calls) == 1
    assert_in(far, plan)


def test_keep_with_a_retry_records_the_second_answer_s_workspace(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 2)
    # A first refusal that names no workspace (it could not be opened): the second answer's id is used.
    refusal: dict[str, object] = {
        "bundles": [],
        "workspace_id": None,
        "refused": "refused the stream: syncfs failed",
        "sync_failed": True,
    }
    far_side = Far(monkeypatch, [(2, json.dumps(refusal), "")])
    assert transfer(project, "--json", "-q", "--keep", *selected(plan), near_name, f"cluster:{far_name}") == 0
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["adopted"] * 2 and all(o["kept"] for o in outcomes)
    assert far_side.strengthened() == [False, True]
    for mapping, _, _ in plan:
        ref = find(near, mapping["id"])
        assert ref is not None and ref.state == "away"
        doc = read_state_unowned(ref.path / "state.json")[0]
        assert doc is not None and doc.away is not None and doc.away["workspace_id"] == far.workspace_id
    assert_in(far, plan)


def test_landing_copies_under_ignore_are_not_redone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    plan = jobs(source, 2)
    other_device(monkeypatch, target)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO, None])
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 2
    assert events.copies == [False, False] and events.kinds().count("syncfs") == 3
    assert "again" not in captured.err
    assert_in(target, plan)


def test_job_submit_copies_and_moves_under_ignore_publish_after_a_sync_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    ws.configure_settings({"io.sync_errors": "ignore"})
    first, second, third = prepared_payloads(ws, tmp_path, 3)
    # Copies: the prime, then one commit for the batch (plan 7.3; this was one commit per payload before batching),
    # which reports an error, trusted after a re-sync.
    monkeypatch.setattr(scaffold_module, "_clock", lambda: 0.0)  # no time cap: one batch however slow the copies
    calls = Syncfs(monkeypatch, [None, errno.EIO, None])
    assert submit(tmp_path, ws, str(first), str(second)) == 0
    assert len(ready(ws)) == 2 and len(calls.descriptors) == 3
    # --move: one no-prime syncfs for the batch, trusted after a re-sync.
    calls = Syncfs(monkeypatch, [errno.EIO, None])
    assert submit(tmp_path, ws, "--move", str(third)) == 0
    assert len(ready(ws)) == 3 and not third.exists() and len(calls.descriptors) == 2
    capsys.readouterr()


def test_a_store_build_under_ignore_still_fails_on_a_sync_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_runner_builds import _tool_package

    ws = durable_workspace(tmp_path / "ws", "syncfs")
    ws.configure_settings({"io.sync_errors": "ignore"})
    installed = install(ws, _tool_package(tmp_path / "tool"), build=False)
    calls = Syncfs(monkeypatch, [None, errno.EIO, None])
    with cli_owner(ws) as owner, pytest.raises(_fs.SyncFailed):
        _store.build(ws, owner, installed.id)
    assert len(calls.descriptors) == 2 and _store.built(_store.lookup(ws, installed.id)) is not True  # type: ignore[arg-type]


def test_the_far_side_s_sync_setting_warnings_are_relayed() -> None:
    err = (
        "io.sync_errors is 'never', not fail or ignore: using fail\n"
        "io.sync is fsync and io.sync_errors is fail, since the settings cannot be read: bad\n"
        "using something else\n"
    )
    out = io.StringIO()
    with contextlib.redirect_stderr(out):
        transfer_cli._relay(transfer_cli._End("cluster:far"), argparse.Namespace(quiet=False), err)
    assert out.getvalue().splitlines() == [
        "cluster:far: io.sync_errors is 'never', not fail or ignore: using fail",
        "cluster:far: io.sync is fsync and io.sync_errors is fail, since the settings cannot be read: bad",
    ]


def test_a_push_relays_the_far_side_s_invalid_io_sync_errors_warning(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    path = far.control / "format.json"
    document = json.loads(path.read_bytes())
    document["settings"]["io.sync_errors"] = "never"
    path.write_text(json.dumps(document), encoding="utf-8")
    plan = jobs(near, 1)
    # The real local adapter: a far-side httk process renders its logged warnings on its standard error.
    assert transfer(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    err = capsys.readouterr().err
    assert f"cluster:{far_name}: io.sync_errors is 'never', not fail or ignore: using fail" in err.splitlines()
    assert_in(far, plan)
