"""Bulk landings are durable before they are published (plan WORKFLOW_SYNC_BARRIER §8, phase S1).

Each site that lands payload a transfer brings (the far side of a push, the local side of a pull, a local transfer
across filesystems) enters a sync barrier before the first byte lands and commits it before the first adoption
takes anything; a failed commit publishes nothing. The recorded call order is the property under test.
"""

import argparse
import errno
import io
import json
import logging
import os
import shutil
import stat
import sys
import tarfile
import tempfile
import threading
from collections.abc import Iterator, Sequence
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import fake_remote
from httk.workflow import Workspace, _fs, _kernel, _landing, _moving
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_transfer import jobs, outgoing, registered, selected
from test_moving import assert_in, find, io_workers, place, tree  # noqa: F401 (io_workers: fixture)


def receive_tar(workspace: Workspace, landing: Path, **options: Any) -> list[str]:
    """The far side's receiver reading standard input, as ``job adopt --stdin-tar`` hands it its stream."""

    return _landing.receive_tar(workspace, landing, sys.stdin.buffer, **options)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A project with a ``local``-adapter remote, so ``cluster:NAME`` reaches this machine."""

    root = tmp_path / "project"
    initialize_project(root, name="sync")
    # The adapter path, whose far side in_process_far_side runs in this process to observe its barrier.
    fake_remote(root, template="local", name="cluster", transfer_session="off")
    return root


@pytest.fixture(autouse=True)
def _fresh_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_fs, "_syncfs_works", {})


class Events:
    """One ordered record of barrier entries, ``syncfs`` calls, landed writes, copies, adapter pulls and takes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, syncfs_failures: Sequence[int | None] = ()) -> None:
        self.lock = threading.Lock()
        self.events: list[tuple[str, object]] = []
        self.failures = list(syncfs_failures)
        self.copies: list[bool] = []
        #: Each barrier entered: its strategy and the descriptor it opened.
        self.barriers: list[tuple[str, int | None]] = []
        #: Each trusted adoption's ``copied`` (adoption's own cross-filesystem copy).
        self.adopted_copied: list[bool] = []
        #: Set while an extraction runs.
        self.extracting = False
        real_enter, real_syncfs, real_write = _fs.Barrier.__enter__, _fs._syncfs, _fs.write_all
        real_take, real_copy, real_adapter = _kernel.take, _fs.copy_tree, transfer_cli._adapter
        real_landing, real_extract, real_adopt = _fs.fsync_landing, _fs.extract_tar, _moving.adopt

        def enter(barrier: _fs.Barrier) -> _fs.Barrier:
            self.add("entering", None)
            entered = real_enter(barrier)
            with self.lock:
                self.barriers.append((barrier.strategy, barrier._descriptor))
            self.add("enter", len(self.barriers) - 1)
            return entered

        def adopt(*arguments: Any, **options: Any) -> Any:
            report = real_adopt(*arguments, **options)
            if report is not None:
                with self.lock:
                    self.adopted_copied.append(report.copied)
            return report

        def syncfs(descriptor: int) -> None:
            self.add("syncfs", descriptor)
            failure = self.failures.pop(0) if self.failures else None
            if failure is not None:
                raise OSError(failure, os.strerror(failure))
            real_syncfs(descriptor)

        def write_all(descriptor: int, data: bytes) -> None:
            self.add("write", descriptor)
            real_write(descriptor, data)

        def take(*arguments: Any, **options: Any) -> Any:
            self.add("take", None)
            return real_take(*arguments, **options)

        def copy_tree(src: Any, dst: Path, *, durable: bool, **options: Any) -> None:
            self.add("copy", durable)
            with self.lock:
                self.copies.append(durable)
            real_copy(src, dst, durable=durable, **options)
            self.add("copied", durable)

        def adapter(end: Any, context: Any, arguments: Any, operation: str, *rest: Any) -> None:
            self.add(f"{operation}-start", None)
            real_adapter(end, context, arguments, operation, *rest)
            self.add(f"{operation}-end", None)

        def extract_tar(*arguments: Any, **options: Any) -> int:
            self.add("extract-start", None)
            self.extracting = True
            try:
                count = real_extract(*arguments, **options)
            finally:
                self.extracting = False
            self.add("extract-end", count)
            return count

        def fsync_landing(anchor: int, **options: Any) -> int:
            count = real_landing(anchor, **options)
            self.add("walk", count)
            return count

        monkeypatch.setattr(_fs.Barrier, "__enter__", enter)
        monkeypatch.setattr(_fs, "_syncfs", syncfs)
        monkeypatch.setattr(_fs, "write_all", write_all)
        monkeypatch.setattr(_kernel, "take", take)
        monkeypatch.setattr(_fs, "copy_tree", copy_tree)
        monkeypatch.setattr(transfer_cli, "_adapter", adapter)
        monkeypatch.setattr(_fs, "fsync_landing", fsync_landing)
        monkeypatch.setattr(_fs, "extract_tar", extract_tar)
        monkeypatch.setattr(_moving, "adopt", adopt)

    def add(self, kind: str, detail: object) -> None:
        with self.lock:
            self.events.append((kind, detail))

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.events]

    def first(self, kind: str) -> int:
        return self.kinds().index(kind)

    def last(self, kind: str) -> int:
        return len(self.events) - 1 - self.kinds()[::-1].index(kind)

    def syncfs_descriptors(self) -> list[object]:
        return [detail for kind, detail in self.events if kind == "syncfs"]


def statuses(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    return list(json.loads(capsys.readouterr().out)["transfers"])


def transfer(cwd: Path, *argv: str) -> int:
    """A durable ``job transfer`` (the transfer suites run ``--no-durable``)."""

    return command(["job", "transfer", "--no-workflows", *argv], CLIContext("httk", cwd))


def assert_syncfs_barrier(events: Events, start: str, end: str, published: str = "take") -> None:
    """One barrier, entered and primed before the landing's first *start*, and committed after its last *end* and
    before the first *published*, both ``syncfs`` on the descriptor the entry opened."""

    kinds = events.kinds()
    assert kinds.count("enter") == 1 and kinds.count("syncfs") == 2, kinds
    enter = events.first("enter")
    ((strategy, descriptor),) = events.barriers
    assert strategy == "syncfs"
    prime, commit = (index for index, kind in enumerate(kinds) if kind == "syncfs")
    assert events.first("entering") < prime < enter < events.first(start)
    assert events.last(end) < commit < events.first(published)
    assert events.syncfs_descriptors() == [descriptor, descriptor]


# -- the far side of a push: job adopt --stdin-tar ---------------------------------------------------------------


def adopt_stream(monkeypatch: pytest.MonkeyPatch, cwd: Path, data: bytes, *argv: str) -> int:
    """A durable ``job adopt --json --stdin-tar`` reading *data*."""

    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    return command(["job", "adopt", "--json", "--stdin-tar", *argv], CLIContext("httk", cwd))


def landings(workspace: Workspace) -> list[str]:
    directory = workspace.control / "transfers" / "incoming"
    return sorted(os.listdir(directory)) if directory.exists() else []


def test_a_pushed_stream_is_synced_before_its_first_adoption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, plan = hold_jobs(near, 3)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    events = Events(monkeypatch)
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 0
    captured = capsys.readouterr()
    assert [entry["status"] for entry in json.loads(captured.out)["bundles"]] == ["adopted"] * 3
    assert_syncfs_barrier(events, "extract-start", "extract-end")
    # Every member write (the reader's and the workers') happened inside the extraction.
    writes = [index for index, kind in enumerate(events.kinds()) if kind == "write"]
    landed = [index for index in writes if events.first("extract-start") < index < events.first("extract-end")]
    assert len(landed) >= 3 * 2
    assert "syncing " in captured.err and "landed file(s) to disk (syncfs: forced, " in captured.err
    assert_in(far, plan)
    assert landings(far) == []


def test_a_failed_commit_refuses_the_pushed_stream_and_adopts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    assert adopt_stream(monkeypatch, tmp_path, data, "--workspace", far_name) == 2
    document = json.loads(capsys.readouterr().out)
    # sync_failed (P1 of WORKFLOW_SYNC_SEGMENTS_AND_TRANSFER_SESSION §7.2): the sender delivers once more.
    assert set(document) == {"bundles", "workspace_id", "refused", "sync_failed"}
    assert document["sync_failed"] is True
    assert document["bundles"] == [] and document["workspace_id"] == far.workspace_id
    assert document["refused"].startswith("refused the stream, nothing of it was used: ")
    assert "not durable" in document["refused"]
    assert "take" not in events.kinds()
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    assert landings(far) == []


def in_process_far_side(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run a push's ``job adopt --stdin-tar`` in this process, so that its barrier can be observed."""

    def deliver_call(
        end: Any, context: CLIContext, arguments: Any, source: str, files: list[str], argv: list[str]
    ) -> tuple[int, str, str]:
        data = stream(Path(source), files)
        out, err = io.StringIO(), io.StringIO()
        with monkeypatch.context() as patch, redirect_stdout(out), redirect_stderr(err):
            patch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
            code = command(list(argv[1:]), CLIContext("httk", context.cwd))
        return code, out.getvalue(), err.getvalue()

    monkeypatch.setattr(transfer_cli, "_deliver_call", deliver_call)


def test_a_push_refused_by_a_failed_commit_is_delivered_once_more_with_fsync(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    plan = jobs(near, 2)
    in_process_far_side(monkeypatch)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EDQUOT])
    assert transfer(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    outcomes = statuses(capsys)
    # Before P1 the refusal took the holds back (not_moved); now its one retry, with --sync fsync, adopts them
    # (the refused retry's take-back is in test_sync_errors_retry.py).
    assert [outcome["status"] for outcome in outcomes] == ["adopted"] * 2
    assert_in(far, plan)
    assert outgoing(near) == [] and landings(far) == []
    # The failed commit's two syncfs share one descriptor; the retry's fsync barrier calls none.
    assert len(events.syncfs_descriptors()) == 2
    assert events.syncfs_descriptors()[0] == events.syncfs_descriptors()[1]
    assert [strategy for strategy, _ in events.barriers] == ["syncfs", "fsync"]


def test_a_long_lived_workspace_reads_io_sync_afresh_at_each_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    far, _ = registered(tmp_path, "far")
    durable = Workspace(far.root)
    seen: list[object] = []
    real = _fs.resolve_sync

    def resolve(path: Path, setting: object) -> tuple[_fs.SyncStrategy, str]:
        seen.append(setting)
        return real(path, setting)

    monkeypatch.setattr(_fs, "resolve_sync", resolve)
    empty = io.BytesIO()
    tarfile.open(fileobj=empty, mode="w").close()
    for value in ("fsync", "syncfs", None):
        if value is None:
            Workspace(far.root).unset_setting("io.sync")
        else:
            Workspace(far.root).configure_settings({"io.sync": value})  # another process's change
        monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(empty.getvalue())))
        assert receive_tar(durable, far.control / "tmp" / f"landing-{len(seen)}", barrier=True) == []
    assert seen == ["fsync", "syncfs", "auto"]


def test_an_fsync_strategy_extracts_durably_and_commits_nothing_more(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, _ = registered(tmp_path, "near")
    far, _ = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "fsync"})
    holds, _ = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    durable: list[bool] = []
    real = _fs.extract_tar

    def extract(*arguments: Any, **options: Any) -> int:
        durable.append(options["durable"])
        return real(*arguments, **options)

    monkeypatch.setattr(_fs, "extract_tar", extract)
    events = Events(monkeypatch)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    landing = far.control / "transfers" / "incoming" / "x"
    assert receive_tar(Workspace(far.root), landing, barrier=True) == sorted(hold.transfer_id for hold in holds)
    assert durable == [True]
    assert events.kinds().count("syncfs") == 0 and "walk" not in events.kinds()


def test_a_non_durable_or_install_landing_takes_no_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])

    def refuse(*_arguments: Any, **_options: Any) -> None:
        raise AssertionError("a barrier was taken")

    monkeypatch.setattr(_fs.Barrier, "__init__", refuse)
    assert adopt_stream(monkeypatch, tmp_path, data, "--no-durable", "--workspace", far_name) == 0
    assert_in(far, plan)
    capsys.readouterr()
    empty = io.BytesIO()
    tarfile.open(fileobj=empty, mode="w").close()
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(empty.getvalue())))
    # workflow install --stdin-tar: its landing is copied into the store, not published (phase S2 covers that).
    assert command(["install", "--json", "--stdin-tar", "--workspace", far_name], CLIContext("httk", tmp_path)) == 0
    assert json.loads(capsys.readouterr().out)["workflows"] == []


# -- the local side of a pull ---------------------------------------------------------------------------------------


def test_a_pull_is_synced_before_its_first_adoption(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs"})
    plan = jobs(far, 3)
    events = Events(monkeypatch)
    assert transfer(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    assert_syncfs_barrier(events, "pull-start", "pull-end")
    assert "syncing the landed files to disk (syncfs: forced, " in captured.err
    assert_in(near, plan)
    assert outgoing(far) == []


def test_a_failed_commit_fails_the_pulled_chunk_and_keeps_every_hold(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs"})
    plan = jobs(far, 2)
    # Both commits fail: the first chunk's and that of its one re-pull (P1); before P1 the first alone failed it.
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO, None, errno.EIO])
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["failed"] * 2
    assert all("not durable" in outcome["message"] and "stays held" in outcome["message"] for outcome in outcomes)
    assert "take" not in events.kinds()
    assert events.kinds().count("pull-start") == 2 and events.kinds().count("syncfs") == 4
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert sorted(outgoing(far)) == sorted(outcome["transfer_id"] for outcome in outcomes)
    assert not list((near.control / "tmp").glob("*.landing.*"))
    assert transfer(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert_in(near, plan)


def test_an_fsync_pull_walks_the_landing_before_its_first_adoption(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "fsync"})
    plan = jobs(far, 2)
    events = Events(monkeypatch)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    walks = [detail for kind, detail in events.events if kind == "walk"]
    assert len(walks) == 1 and isinstance(walks[0], int) and walks[0] >= 2 * 2  # job.json, input.txt, ... a job
    assert events.last("pull-end") < events.first("walk") < events.first("take")
    assert "syncfs" not in events.kinds()


def test_an_fsync_pull_whose_landing_cannot_be_opened_fails_the_chunk(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root opens a file of mode 000")
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "fsync"})
    plan = jobs(far, 1)
    real = transfer_cli._adapter

    def unreadable(end: Any, context: Any, arguments: Any, operation: str, *rest: Any) -> None:
        real(end, context, arguments, operation, *rest)
        if operation == "pull":
            next(Path(rest[1]).rglob("input.txt")).chmod(0)

    monkeypatch.setattr(transfer_cli, "_adapter", unreadable)
    assert transfer(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    (outcome,) = statuses(capsys)
    assert outcome["status"] == "failed" and "cannot read" in outcome["message"]
    assert "input.txt to sync it to disk" in outcome["message"]
    assert find(near, plan[0][0]["id"]) is None and outgoing(far) == [outcome["transfer_id"]]


# -- a local transfer across filesystems ----------------------------------------------------------------------------


def other_device(monkeypatch: pytest.MonkeyPatch, workspace: Workspace) -> None:
    """Make *workspace*'s control directory report another ``st_dev`` to the transfer's filesystem check."""

    control = workspace.control.resolve()

    def stat(path: Any) -> Any:
        info = os.stat(path)
        if Path(path).resolve() == control:
            return SimpleNamespace(st_dev=info.st_dev + 1)
        return info

    monkeypatch.setattr(transfer_cli, "os", SimpleNamespace(stat=stat))


def test_holds_on_another_filesystem_are_copied_and_synced_before_their_first_adoption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs"})
    plan = jobs(source, 3)
    other_device(monkeypatch, target)  # st_dev patched: the real second filesystem is the /dev/shm test below
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    captured = capsys.readouterr()
    outcomes = json.loads(captured.out)["transfers"]
    assert [outcome["status"] for outcome in outcomes] == ["adopted"] * 3
    # No per-file fsync under syncfs, and adopted from the landing by a rename (adoption's own copy adds none).
    assert events.copies == [False] * 3 and events.adopted_copied == [False] * 3
    assert_syncfs_barrier(events, "copy", "copied")
    assert "syncing 3 landed bundle(s) to disk (syncfs: forced, " in captured.err
    assert_in(target, plan)
    assert outgoing(source) == [] and all(find(source, mapping["id"]) is None for mapping, _, _ in plan)
    assert not list((target.control / "tmp").glob("*.landing.*"))


def test_a_failed_commit_of_copies_redoes_them_once_with_an_fsync_of_every_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs"})
    plan = jobs(source, 2)
    other_device(monkeypatch, target)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 2
    assert sorted(events.copies) == [False, False, True, True]
    commit = [index for index, kind in enumerate(events.kinds()) if kind == "syncfs"][1]
    redo = [index for index, (kind, durable) in enumerate(events.events) if kind == "copy" and durable]
    assert commit < min(redo) and events.last("copied") < events.first("take")
    assert "copying 2 bundle(s) again, with an fsync of every file" in captured.err
    assert_in(target, plan)
    assert outgoing(source) == []


def test_a_failed_redo_fails_the_chunk_and_keeps_every_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs"})
    plan = jobs(source, 2)
    other_device(monkeypatch, target)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    real = _fs.copy_tree

    def failing(src: Any, dst: Path, *, durable: bool, **options: Any) -> None:
        if durable:
            raise OSError(errno.ENOSPC, "injected: no space left")
        real(src, dst, durable=durable, **options)

    monkeypatch.setattr(_fs, "copy_tree", failing)
    assert transfer(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 1
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["failed"] * 2
    assert all("no space left" in outcome["message"] for outcome in outcomes)
    assert "take" not in events.kinds()
    assert sorted(outgoing(source)) == sorted(outcome["transfer_id"] for outcome in outcomes)
    assert all(find(target, mapping["id"]) is None for mapping, _, _ in plan)
    assert not list((target.control / "tmp").glob("*.landing.*"))


def test_a_refused_landed_copy_says_the_source_hold_is_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)
    place(target, plan[3][0], "succeeded")  # one member already there: the bundle is refused
    other_device(monkeypatch, target)
    root_id = str(plan[0][0]["id"])
    assert transfer(tmp_path, "--json", "-q", "--tree", "--job", root_id, source_name, target_name) == 1
    (outcome,) = statuses(capsys)
    assert outcome["status"] == "refused"
    assert "landed copy of a held bundle and was discarded (the source hold is untouched)" in outcome["message"]
    assert outcome["message"].endswith(f"it stays held on {source.root}")
    assert outgoing(source) == [outcome["transfer_id"]]
    assert not list((target.control / "tmp").iterdir())


def test_one_filesystem_moves_holds_in_without_a_barrier_or_a_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert "enter" not in events.kinds() and events.copies == []
    assert_in(target, plan)


def test_a_non_durable_transfer_across_filesystems_copies_without_a_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = jobs(source, 2)
    other_device(monkeypatch, target)
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", "--no-durable", *selected(plan), source_name, target_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert "enter" not in events.kinds() and "syncfs" not in events.kinds()
    assert events.copies == [False, False]


def test_keep_copies_through_the_barrier_on_one_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs"})
    plan = jobs(source, 2)
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", "--keep", *selected(plan), source_name, target_name) == 0
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["adopted"] * 2
    assert all(outcome["kept"] for outcome in outcomes)
    assert_syncfs_barrier(events, "copy", "copied")


@pytest.fixture
def shm_root() -> Iterator[Path]:
    """A small directory on ``/dev/shm`` (tmpfs), removed afterwards; skipped when it is not another filesystem."""

    shm = Path("/dev/shm")
    if not shm.is_dir() or not os.access(shm, os.W_OK):
        pytest.skip("no writable /dev/shm")
    root = Path(tempfile.mkdtemp(prefix="httk-sync-", dir=shm))
    try:
        if os.stat(root).st_dev == os.stat(tempfile.gettempdir()).st_dev:
            pytest.skip("/dev/shm is not another filesystem here")
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_a_transfer_to_a_real_second_filesystem_lands_synced_copies(
    tmp_path: Path, shm_root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(shm_root, "b")  # tmpfs: auto resolves to syncfs on Linux 5.8 and later
    plan = jobs(source, 2)
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert events.copies == [False, False] or events.copies == [True, True]
    ((strategy, _descriptor),) = events.barriers
    assert events.adopted_copied == [False, False]
    if strategy == "syncfs":
        assert_syncfs_barrier(events, "copy", "copied")
    assert_in(target, plan)
    assert outgoing(source) == []


def test_the_cross_device_backstop_of_adoption_still_copies(
    tmp_path: Path, shm_root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(shm_root, "b")
    plan = jobs(source, 1)
    # Equal devices that still refuse a rename (a bind mount): the transfer moves, adoption copies.
    monkeypatch.setattr(transfer_cli, "_other_filesystem", lambda *_arguments: False)
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"]
    assert "enter" not in events.kinds() and events.copies == [True]  # adoption's own inline-fsynced copy
    assert events.adopted_copied == [True]
    assert_in(target, plan)
    assert outgoing(source) == []


# -- the io.sync setting --------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["SYNCFS", "", "sync", 1, None, True])
def test_io_sync_is_validated_before_anything_is_written(tmp_path: Path, value: object) -> None:
    workspace, _ = registered(tmp_path, "a")
    before = (workspace.control / "format.json").read_bytes()
    with pytest.raises(ValueError, match="io.sync"):
        workspace.configure_settings({"io.sync": value})
    with pytest.raises(ValueError, match="io.sync"):
        workspace.seed_settings({"io.sync": value})
    assert (workspace.control / "format.json").read_bytes() == before
    assert workspace.io_sync() == "auto"


def test_io_sync_takes_its_three_values_and_is_a_recognized_engine_setting(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.workflow._setting_catalog import classify_setting

    workspace, name = registered(tmp_path, "a")
    reader = Workspace(workspace.root)
    for value in ("fsync", "syncfs", "auto"):
        workspace.configure_settings({"io.sync": value})
        assert reader.io_sync() == value  # read afresh by a long-lived object
    workspace.unset_setting("io.sync")
    assert reader.io_sync() == "auto"
    assert classify_setting("io.sync", "auto").state == "recognized"
    argv = ["workspace", "settings", "set", "--key", "io.sync", "--value", "never", name]
    assert command(argv, CLIContext("httk", tmp_path)) != 0
    assert "io.sync must be auto, syncfs or fsync" in capsys.readouterr().err
    assert reader.io_sync() == "auto"


def test_an_invalid_stored_io_sync_resolves_to_fsync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    workspace, _ = registered(tmp_path, "a")
    document = json.loads((workspace.control / "format.json").read_bytes())
    document["settings"]["io.sync"] = "always"
    (workspace.control / "format.json").write_text(json.dumps(document), encoding="utf-8")
    assert workspace.io_sync() == "always"
    with (
        caplog.at_level("WARNING", logger="httk.workflow._fs"),
        _fs.Barrier(workspace.control / "tmp", workspace.io_sync(), quiet=True) as barrier,
    ):
        assert barrier.reason == "fsync (io.sync 'always' is not auto, syncfs or fsync)"
    # Only _fs's records: an earlier test in the worker may have lowered the "httk" logger's level, admitting the
    # INFO record of registered()'s policy update.
    records = [record.getMessage() for record in caplog.records if record.name == "httk.workflow._fs"]
    assert records == ["io.sync is 'always', not one of auto, syncfs or fsync: using fsync"]


def test_unreadable_settings_resolve_to_fsync(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    workspace, _ = registered(tmp_path, "a")
    (workspace.control / "format.json").write_text("{", encoding="utf-8")
    with caplog.at_level("WARNING", logger="httk.workflow.workspace"):
        assert workspace.io_sync() == "fsync"
    warnings = [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING]
    assert [message.startswith("using io.sync = fsync") for message in warnings] == [True]


def test_an_invalid_stored_settings_value_makes_io_sync_fsync_until_fixed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    workspace, _ = registered(tmp_path, "a")
    reader = Workspace(workspace.root)  # long-lived
    path = workspace.control / "format.json"
    invalid: list[object] = [True, [], {}]
    document = json.loads(path.read_bytes())
    for stored in invalid:
        document["settings"]["io.sync"] = stored
        path.write_text(json.dumps(document), encoding="utf-8")
        caplog.clear()
        with caplog.at_level("WARNING", logger="httk.workflow.workspace"):
            assert reader.io_sync() == "fsync"
        assert [record.getMessage().startswith("using io.sync = fsync") for record in caplog.records] == [True]
    document["settings"]["io.sync"] = "syncfs"
    path.write_text(json.dumps(document), encoding="utf-8")
    assert reader.io_sync() == "syncfs"


# -- review additions: strategy fsync copies, relayed far-side lines, tests that must be able to fail ----------------


def test_holds_on_another_filesystem_under_fsync_are_copied_with_an_fsync_of_every_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "fsync"})  # what auto gives on zfs, nfs, btrfs or below Linux 5.8
    plan = jobs(source, 3)
    other_device(monkeypatch, target)
    events = Events(monkeypatch)
    assert transfer(tmp_path, "--json", *selected(plan), source_name, target_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    assert events.copies == [True] * 3 and events.adopted_copied == [False] * 3
    assert "syncfs" not in events.kinds() and "walk" not in events.kinds()
    assert events.barriers == [("fsync", events.barriers[0][1])]
    assert events.first("enter") < events.first("copy") and events.last("copied") < events.first("take")
    assert "made 3 landed bundle(s) durable (fsync: io.sync is fsync)" in captured.err
    assert_in(target, plan)


def test_a_plain_copy_failure_fails_the_chunk_without_a_redo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs"})
    plan = jobs(source, 2)
    other_device(monkeypatch, target)
    events = Events(monkeypatch)
    real = _fs.copy_tree

    def failing(src: Any, dst: Path, *, durable: bool, **options: Any) -> None:
        if not durable:
            raise OSError(errno.ENOSPC, "injected: no space left")
        real(src, dst, durable=durable, **options)

    monkeypatch.setattr(_fs, "copy_tree", failing)
    assert transfer(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 1
    outcomes = statuses(capsys)
    assert [outcome["status"] for outcome in outcomes] == ["failed"] * 2
    assert all("no space left" in outcome["message"] for outcome in outcomes)
    assert events.kinds().count("syncfs") == 1  # the prime; no commit, no redo with an fsync of every file
    assert sorted(outgoing(source)) == sorted(outcome["transfer_id"] for outcome in outcomes)
    assert all(find(target, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_non_durable_pull_takes_no_barrier(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    near.configure_settings({"io.sync": "syncfs"})
    plan = jobs(far, 2)
    events = Events(monkeypatch)
    assert transfer(project, "--json", "--no-durable", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in statuses(capsys)] == ["adopted"] * 2
    assert "pull-end" in events.kinds() and "enter" not in events.kinds() and "syncfs" not in events.kinds()
    assert_in(near, plan)


def test_the_push_landing_is_created_durably_before_anything_lands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    far, _ = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    events = Events(monkeypatch)
    real = os.fsync

    def fsync(descriptor: int) -> None:
        info = os.fstat(descriptor)
        if stat.S_ISDIR(info.st_mode):
            events.add("dirsync", info.st_ino)
        real(descriptor)

    monkeypatch.setattr(os, "fsync", fsync)
    empty = io.BytesIO()
    tarfile.open(fileobj=empty, mode="w").close()
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(empty.getvalue())))
    incoming = far.control / "transfers" / "incoming"
    assert receive_tar(Workspace(far.root), incoming / "token", barrier=True) == []
    synced = [
        index
        for index, (kind, inode) in enumerate(events.events)
        if kind == "dirsync" and inode == os.stat(incoming).st_ino
    ]
    assert synced and synced[0] < events.first("entering") < events.first("extract-start")


def test_a_failed_directory_fsync_of_an_fsync_landing_refuses_the_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, _ = registered(tmp_path, "near")
    far, _ = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "fsync"})
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    events = Events(monkeypatch)
    real = os.fsync
    files: list[int] = []

    landing = far.control / "transfers" / "incoming" / "x"

    def fsync(descriptor: int) -> None:
        info = os.fstat(descriptor)
        # A bundle's directory, never the landing itself.
        if events.extracting and stat.S_ISDIR(info.st_mode) and info.st_ino != os.stat(landing).st_ino:
            raise OSError(errno.EIO, "injected directory fsync failure")
        real(descriptor)
        if events.extracting:
            files.append(descriptor)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data)))
    with pytest.raises(ValueError, match="refused the stream, nothing of it was used: .*injected directory fsync"):
        receive_tar(Workspace(far.root), landing, barrier=True)
    assert files and not landing.exists()
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_push_client_shows_the_far_side_sync_line_unless_quiet(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    in_process_far_side(monkeypatch)
    for quiet in (False, True):
        plan = jobs(near, 2)
        flags = ["-q"] if quiet else []
        assert transfer(project, "--json", *flags, *selected(plan), near_name, f"cluster:{far_name}") == 0
        captured = capsys.readouterr()
        assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 2
        relayed = [line for line in captured.err.splitlines() if line.startswith(f"cluster:{far_name}: ")]
        if quiet:
            assert relayed == []
        else:
            # The far side's sync line; its warning about uninstalled workflows is the client's to report.
            assert len(relayed) == 1, captured.err
            assert relayed[0].startswith(f"cluster:{far_name}: syncing ")
            assert " landed file(s) to disk (syncfs: forced, " in relayed[0]
        assert_in(far, plan)


FAR_SIDE_STDERR = """\
Warning: Permanently added 'cluster.example.org' (ED25519) to the list of known hosts.
Lmod has detected the following error: The following module(s) are unknown: "vasp/6.4"
Traceback (most recent call last):
  File "/opt/httk/bin/httk", line 8, in <module>
0123456789abcdef0123456789abcdef: bundle refused: the job is already here
  and a continuation line of that message
syncing 12 landed file(s) to disk (syncfs: lustre, Linux 5.14)
warning: something the operator should see
warning: these workflows are not installed, so their jobs wait: demo--0123456789abcdef
io.sync is syncfs, but Linux 4.18 is older than 5.8: using fsync
"""


@pytest.mark.parametrize("quiet", [False, True], ids=["verbose", "quiet"])
def test_only_the_far_side_sync_line_and_warnings_are_relayed(capsys: pytest.CaptureFixture[str], quiet: bool) -> None:
    transfer_cli._relay(transfer_cli._End("cluster:far"), argparse.Namespace(quiet=quiet), FAR_SIDE_STDERR)
    expected = [
        "cluster:far: syncing 12 landed file(s) to disk (syncfs: lustre, Linux 5.14)",
        "cluster:far: warning: something the operator should see",
        "cluster:far: io.sync is syncfs, but Linux 4.18 is older than 5.8: using fsync",
    ]
    assert capsys.readouterr().err.splitlines() == ([] if quiet else expected)


def test_a_refused_adoption_relays_nothing(
    project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    plan = jobs(near, 1)
    refused = {"bundles": [], "workspace_id": far.workspace_id, "refused": "refused the stream: injected"}

    def deliver_call(*_arguments: Any) -> tuple[int, str, str]:
        return 2, json.dumps(refused), FAR_SIDE_STDERR

    monkeypatch.setattr(transfer_cli, "_deliver_call", deliver_call)
    assert transfer(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 1
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["not_moved"]
    assert not [line for line in captured.err.splitlines() if line.startswith(f"cluster:{far_name}: ")]
    assert_in(near, plan)
