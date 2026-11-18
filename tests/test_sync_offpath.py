"""Sync off the critical path, low-risk reductions (plan WORKFLOW_TRANSFER_SESSION §7.11, T6a items 1-6).

1. The landings of one transfer prime once per filesystem (a set of the session connection or of the command's
   transfer), and an unprimed barrier fails as a primed one does; store and creation barriers keep their prime.
2. Landing directory fsyncs and the late walk run pooled over ``io.workers``, each task through its own anchored
   descriptors, a failure draining the pool before it is raised.
3. The scratch discards of a hold's delivery, an adoption's publication and a release's take are not durable; an
   adoption scratch that a crash brings back is discarded only when a walk proves it an empty skeleton.
4. A durable move fsyncs one parent directory once when both parents are provably the same.
5. Job creation's scratches leave their ``tmp/`` entry to the batch commit, except those receiving the only copy of
   something (moved inputs, a moved payload).
6. A failed ``Stage.close`` after a validated adoption is reported, never turned into an unknown outcome; the pair
   stops, and the session makes one final attempt to close it at its end.
"""

import contextlib
import errno
import io
import json
import os
import shutil
import stat
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import pytest
from httk.core.cli import CLIContext

import test_input_moves
from conftest import Remote
from httk.workflow import Workspace, _fs, _kernel, _moving
from httk.workflow import scaffold as scaffold_module
from httk.workflow._bundles import BundleError
from httk.workflow._frames import DATA, DATA_LIMIT, END, REQUEST
from httk.workflow.scaffold import new_job, new_jobs, submit_payload
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import _transfer_session, command
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_transfer import jobs, outgoing, recorded_requests, registered, run, selected
from test_moving import WORKFLOW, claim_root, client_bundle, die_and_recover, find, job_ids_below, place, single
from test_paged_holds import paged
from test_session_push import adopts, ssh  # noqa: F401 (ssh: fixture)
from test_sync_publication import durable_moves_workspace, durable_workspace, prepared_payloads
from test_transfer_session import _Peer
from v3_helpers import cli_owner, job_mapping
from v3_helpers import workspace as v3_workspace


@pytest.fixture(autouse=True)
def _fresh(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    yield
    _fs.set_fault_injector(None)


class Syncfs:
    """Record every ``syncfs`` (primes included) and every barrier entry; fail the calls *failures* names."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, failures: dict[int, int] | None = None) -> None:
        self.calls = 0
        self.failures = dict(failures or {})
        self.barriers: list[_fs.Barrier] = []
        self.lock = threading.Lock()
        real_syncfs, real_enter = _fs._syncfs, _fs.Barrier.__enter__

        def syncfs(descriptor: int) -> None:
            with self.lock:
                self.calls += 1
                failure = self.failures.get(self.calls)
            if failure is not None:
                raise OSError(failure, os.strerror(failure))
            real_syncfs(descriptor)

        def enter(barrier: _fs.Barrier) -> _fs.Barrier:
            entered = real_enter(barrier)
            with self.lock:
                self.barriers.append(barrier)
            return entered

        monkeypatch.setattr(_fs, "_syncfs", syncfs)
        monkeypatch.setattr(_fs.Barrier, "__enter__", enter)

    def primed(self) -> list[bool]:
        return [barrier.primed for barrier in self.barriers]


# -- 1. prime once per filesystem, for transfer landings only -------------------------------------------------------


def test_only_the_first_barrier_per_filesystem_of_a_transfer_primes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = Syncfs(monkeypatch)
    device = os.stat(tmp_path).st_dev
    devices: set[int] = set()
    for _ in range(3):
        with _fs.Barrier(tmp_path, "syncfs", quiet=True, primed_devices=devices) as barrier:
            barrier.commit()
    assert calls.primed() == [True, False, False] and devices == {device}
    assert calls.calls == 4  # one prime, three commits
    # Another transfer (its own set), and a barrier of no transfer, prime again.
    with _fs.Barrier(tmp_path, "syncfs", quiet=True, primed_devices=set()) as barrier:
        barrier.commit()
    with _fs.Barrier(tmp_path, "syncfs", quiet=True) as barrier:
        barrier.commit()
    assert calls.primed()[3:] == [True, True] and calls.calls == 8


def test_only_a_clean_probed_prime_is_recorded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # A prime whose syncfs failed (logged, ignored) records nothing: the next landing primes again.
    calls = Syncfs(monkeypatch, {1: errno.EIO})
    devices: set[int] = set()
    with _fs.Barrier(tmp_path, "syncfs", quiet=True, primed_devices=devices) as barrier:
        assert barrier.primed
        barrier.commit()
    assert devices == set()
    with _fs.Barrier(tmp_path, "syncfs", quiet=True, primed_devices=devices) as barrier:
        barrier.commit()
    assert calls.primed() == [True, True] and devices == {os.stat(tmp_path).st_dev}
    # A prime finding syncfs unsupported records nothing either (the strategy becomes fsync).
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    Syncfs(monkeypatch, {1: errno.ENOSYS})
    devices = set()
    with _fs.Barrier(tmp_path, "syncfs", quiet=True, primed_devices=devices) as barrier:
        assert barrier.strategy == "fsync" and not barrier.primed
    assert devices == set()


@pytest.mark.parametrize("errors", ["fail", "ignore"])
@pytest.mark.parametrize("error", [errno.ENOSYS, errno.EPERM, errno.EINVAL], ids=["ENOSYS", "EPERM", "EINVAL"])
def test_an_unprimed_transfer_barrier_fails_on_an_unsupported_syncfs_as_a_primed_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, errors: Any, error: int
) -> None:
    walked: list[int] = []

    def walk(anchor: int, **options: Any) -> int:
        walked.append(anchor)
        return 0

    monkeypatch.setattr(_fs, "fsync_landing", walk)
    device = os.stat(tmp_path).st_dev
    # Primed (its syncfs calls: the prime, then the failing commit), then unprimed (its filesystem primed already:
    # the failing commit only). Both fail the same way, and neither walks.
    for devices, failing in ((set(), 2), ({device}, 1)):
        calls = Syncfs(monkeypatch, {failing: error})
        with (
            _fs.Barrier(tmp_path, "syncfs", errors=errors, quiet=True, primed_devices=devices) as barrier,
            pytest.raises(_fs.SyncFailed, match="not durable"),
        ):
            barrier.commit()
        assert barrier.strategy == "syncfs" and barrier.primed == (failing == 2) and calls.calls == failing
    assert walked == []


def test_a_tree_written_before_its_barrier_is_never_primed() -> None:
    with pytest.raises(ValueError, match="never primed"):
        _fs.Barrier(Path("/"), "syncfs", written=True)


def test_under_ignore_an_unprimed_barrier_says_the_error_may_predate_the_landing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    Syncfs(monkeypatch, {1: errno.EIO})
    with (
        caplog.at_level("WARNING", logger="httk.workflow._fs"),
        _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True, primed_devices={os.stat(tmp_path).st_dev}) as b,
    ):
        b.commit()
    (warning,) = [record.getMessage() for record in caplog.records]
    assert "io.sync_errors=ignore trusts" in warning and "may predate the landing" in warning
    caplog.clear()
    Syncfs(monkeypatch, {2: errno.EIO})
    with (
        caplog.at_level("WARNING", logger="httk.workflow._fs"),
        _fs.Barrier(tmp_path, "syncfs", errors="ignore", quiet=True) as barrier,
    ):
        barrier.commit()
    (warning,) = [record.getMessage() for record in caplog.records]
    assert "io.sync_errors=ignore trusts" in warning and "predate" not in warning


def _send_upload(peer: _Peer, request_id: int, header: dict[str, Any], data: bytes) -> dict[str, Any]:
    """One request with an upload to an in-process far side; the upload is written by a thread of its own."""

    def write() -> None:
        peer.writer.send_json(REQUEST, request_id, {**header, "upload": True})
        for start in range(0, len(data), DATA_LIMIT):
            peer.writer.send(DATA, request_id, data[start : start + DATA_LIMIT])
        peer.writer.send_json(END, request_id, {"ok": True})

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    answer = peer.answer()
    thread.join(timeout=30)
    return answer


def _request(peer: _Peer, request_id: int, header: dict[str, Any]) -> dict[str, Any]:
    peer.writer.send_json(REQUEST, request_id, header)
    return peer.answer()


def _stage(peer: _Peer, request_id: int, far_name: str, near: Workspace, holds: list[_moving.Hold]) -> dict[str, Any]:
    ids = [hold.transfer_id for hold in holds]
    declared = sum(transfer_cli._measure(Path(hold.path))[0] for hold in holds)
    header = {"op": "stage", "workspace": far_name, "transfer_ids": ids, "bytes": declared}
    return _send_upload(peer, request_id, header, stream(outgoing_dir(near), ids))


def _adopt_staged(peer: _Peer, request_id: int, far_name: str, staged: dict[str, Any]) -> dict[str, Any]:
    header = {"op": "adopt_staged", "workspace": far_name, "token": staged["token"]}
    return _request(peer, request_id, {**header, "transfer_ids": staged["transfer_ids"]})


def _owners(workspace: Workspace) -> list[str]:
    owners = workspace.control / "owners"
    return sorted(os.listdir(owners)) if owners.exists() else []


def _scratch(workspace: Workspace) -> list[str]:
    tmp = workspace.control / "tmp"
    return sorted(name for name in os.listdir(tmp) if not name.startswith("trash.")) if tmp.exists() else []


def test_a_session_primes_only_its_first_landing_and_another_session_primes_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, plan = hold_jobs(near, 4)
    calls = Syncfs(monkeypatch)
    peer = _Peer(dict(_transfer_session.OPS))
    staged = _stage(peer, 1, far_name, near, holds[:1])
    assert staged["ok"] is True
    assert _adopt_staged(peer, 2, far_name, staged)["ok"] is True
    assert _stage(peer, 3, far_name, near, holds[1:2])["ok"] is True
    # The T3 adopt op is a transfer landing of the session too.
    header = {"op": "adopt", "workspace": far_name, "token": _fs.fresh_token()}
    adopted = _send_upload(peer, 4, header, stream(outgoing_dir(near), [holds[2].transfer_id]))
    assert adopted["ok"] is True
    assert _request(peer, 5, {"op": "bye"})["ok"] is True
    assert peer.join() == 0
    assert calls.primed() == [True, False, False] and calls.calls == 4
    # A new connection has its own set: its first landing primes.
    other = _Peer(dict(_transfer_session.OPS))
    staged = _stage(other, 1, far_name, near, holds[3:])
    assert _adopt_staged(other, 2, far_name, staged)["ok"] is True
    assert other.end() == 0
    assert calls.primed()[3:] == [True] and calls.calls == 6
    assert find(far, plan[3][0]["id"]) is not None


def test_store_and_creation_barriers_keep_their_prime_after_a_transfer_primed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from test_job_creation import install, raw_package

    ws = durable_workspace(tmp_path / "ws", "syncfs")
    calls = Syncfs(monkeypatch)
    devices: set[int] = set()
    with _fs.Barrier(ws.control, "syncfs", quiet=True, primed_devices=devices) as barrier:
        barrier.commit()
    assert devices == {os.stat(ws.control).st_dev}
    installed = install(ws, raw_package(tmp_path / "package"))  # the store's barrier
    new_job(ws, installed.id)  # job creation's barrier
    assert len(calls.barriers) >= 3 and all(calls.primed()), calls.primed()


def test_an_error_before_an_unprimed_landing_is_sync_failed_and_the_segment_retried_with_fsync(
    ssh: tuple[Path, Workspace, str, Workspace, str],  # noqa: F811
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The far side's syncfs calls: 1 the first segment's prime, 2 its commit, 3 the commit of the second segment,
    # whose barrier did not prime: an error recorded before its open fails it as one after the open does.
    project, near, near_name, far, far_name = ssh
    far.configure_settings({"io.sync": "syncfs"})
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "3")
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 6
    # The second segment refused as sync_failed, staged once more with fsync, which then sticks.
    assert [header.get("sync") for header in adopts(requests)] == [None, None, "fsync", "fsync"]
    assert captured.err.count("could not make the delivery durable") == 1
    assert outgoing(near) == []


# -- 2. pooled directory fsyncs --------------------------------------------------------------------------------------


class Fsyncs:
    """Record the inode of every fsynced descriptor (and whether it is a directory); optionally fail one."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, fail: Path | None = None) -> None:
        self.files: list[int] = []
        self.directories: list[int] = []
        self.active = 0
        self.lock = threading.Lock()
        failing = None if fail is None else os.stat(fail).st_ino
        real = os.fsync

        def fsync(descriptor: int) -> None:
            info = os.fstat(descriptor)
            with self.lock:
                self.active += 1
                (self.directories if stat.S_ISDIR(info.st_mode) else self.files).append(info.st_ino)
            try:
                time.sleep(0.002)
                if info.st_ino == failing:
                    raise OSError(errno.EIO, "injected")
                real(descriptor)
            finally:
                with self.lock:
                    self.active -= 1

        monkeypatch.setattr(os, "fsync", fsync)


def _tree(root: Path) -> Path:
    for top in range(4):
        for sub in range(3):
            directory = root / f"bundle-{top}" / "jobs" / f"p{sub}" / "deeper"
            directory.mkdir(parents=True)
            for index in range(5):
                (directory / f"file-{index}").write_text(f"{top} {sub} {index}")
    return root


@pytest.mark.parametrize("workers", [1, 8], ids=["sequential", "pooled"])
def test_the_late_walk_fsyncs_the_same_files_and_directories_pooled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    tree = _tree(tmp_path / "tree")
    (tree / "link").symlink_to("bundle-0")
    expected_files = {path.stat().st_ino for path in tree.rglob("file-*")}
    expected_directories = {path.stat().st_ino for path in tree.rglob("*") if path.is_dir() and not path.is_symlink()}
    expected_directories.add(tree.stat().st_ino)
    fsyncs = Fsyncs(monkeypatch)
    with _fs.Barrier(tree, "fsync", quiet=True, workers=workers) as barrier:
        barrier.commit()
    assert sorted(fsyncs.files) == sorted(expected_files)
    assert sorted(fsyncs.directories) == sorted(expected_directories)
    assert fsyncs.directories[-1] == tree.stat().st_ino  # the anchor last, once every task finished


def test_a_failed_pooled_fsync_fails_the_walk_after_draining_the_pool(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = _tree(tmp_path / "tree")
    fsyncs = Fsyncs(monkeypatch, fail=tree / "bundle-1" / "jobs" / "p1")
    with (
        _fs.Barrier(tree, "fsync", quiet=True, workers=8) as barrier,
        pytest.raises(_fs.SyncFailed, match="bundle-1/jobs/p1 to disk"),
    ):
        try:
            barrier.commit()
        finally:
            assert fsyncs.active == 0  # nothing still running when the failure is raised
    assert tree.stat().st_ino not in fsyncs.directories  # the anchor is never fsynced after a failure


@pytest.mark.parametrize("workers", [1, 8], ids=["sequential", "pooled"])
def test_a_durable_extraction_fsyncs_the_same_directories_pooled_and_a_failure_fails_the_landing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    source = _tree(tmp_path / "source")
    data = stream(source, sorted(os.listdir(source)))
    target = tmp_path / "landing"
    target.mkdir()
    fsyncs = Fsyncs(monkeypatch)
    assert _fs.extract_tar(io.BytesIO(data), target, workers=workers, durable=True) == 60
    directories = {path.stat().st_ino for path in target.rglob("*") if path.is_dir()} | {target.stat().st_ino}
    assert sorted(fsyncs.directories) == sorted(directories) and fsyncs.directories[-1] == target.stat().st_ino
    # A failed directory fsync fails the extraction, once no fsync runs any more.
    shutil.rmtree(target)
    target.mkdir()
    real = os.fsync
    monkeypatch.setattr(os, "fsync", real)
    started: list[float] = []

    def failing(descriptor: int) -> None:
        info = os.fstat(descriptor)
        if stat.S_ISDIR(info.st_mode) and info.st_ino != target.stat().st_ino:
            started.append(time.monotonic())
            raise OSError(errno.EIO, "injected")
        real(descriptor)

    monkeypatch.setattr(os, "fsync", failing)
    with pytest.raises(OSError) as raised:
        _fs.extract_tar(io.BytesIO(data), target, workers=workers, durable=True)
    assert raised.value.errno == errno.EIO
    ended = time.monotonic()
    assert started and max(started) <= ended
    assert len(started) <= (1 if workers == 1 else 8)  # the pool stops at the first failure


# -- 3. non-durable scratch discards and the empty-skeleton proof ----------------------------------------------------


class Discards:
    """Record each scratch discard of an owner: its purpose and whether it was durable."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.seen: list[tuple[str, bool]] = []
        real = _kernel.Owner.discard_scratch

        def discard(owner: _kernel.Owner, path: Path, *, durable: bool = True) -> None:
            self.seen.append((path.name.split(".")[1], durable))
            real(owner, path, durable=durable)

        monkeypatch.setattr(_kernel.Owner, "discard_scratch", discard)


def test_only_a_hold_s_delivery_an_adoption_and_a_release_discard_their_scratch_without_a_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = Workspace.initialize(tmp_path / "a", durable=True, policy={"visibility_deadline_seconds": 0})
    target = Workspace.initialize(tmp_path / "b", durable=True, policy={"visibility_deadline_seconds": 0})
    held, ejected, released = single(source), single(source), single(source)
    discards = Discards(monkeypatch)
    with cli_owner(source) as owner:
        root = claim_root(source, owner, held[0]["id"])
        hold = _moving.hold(source, owner, root, tree=True, destination_locator="b")
        root = claim_root(source, owner, ejected[0]["id"])
        _moving.eject(source, owner, root, destination=tmp_path / "out", tree=True)
    # A hold's delivery, never an eject's.
    assert discards.seen == [("hold", False), ("eject", True)]
    (bundle,) = (tmp_path / "out").iterdir()
    copy = tmp_path / "again" / bundle.name
    shutil.copytree(bundle, copy)
    discards.seen.clear()
    with cli_owner(target) as owner:
        _moving.adopt(target, owner, Path(hold.path), untrusted=False, synced=True)
        _moving.adopt(target, owner, bundle, untrusted=False)
        # An operator's copy of a bundle adopted already is refused back to its path: a refusal's discard is durable.
        with pytest.raises(BundleError, match="already adopted"):
            _moving.adopt(target, owner, copy, untrusted=False)
    assert discards.seen == [("adopt", False), ("adopt", False), ("adopt", True)]
    assert copy.is_dir()
    discards.seen.clear()
    with cli_owner(source) as owner:
        root = claim_root(source, owner, released[0]["id"])
        second = _moving.hold(source, owner, root, tree=True, destination_locator="b")
        assert _moving.release_hold(source, owner, second.transfer_id)
    assert discards.seen == [("hold", False), ("release", False)]


def _adoption_left_behind(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Workspace, _kernel.Owner, Path]:
    """A trusted operator's bundle adopted, its scratch's non-durable discard undone by a crash: the whole scratch
    is back (records and the bundle's skeleton), its owner dead."""

    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    mapping = single(source)[0]
    with cli_owner(source) as owner:
        root = claim_root(source, owner, mapping["id"])
        _moving.eject(source, owner, root, destination=tmp_path / "out", tree=True)
    (bundle,) = (tmp_path / "out").iterdir()
    owner = cli_owner(target)
    real = _kernel.Owner.discard_scratch

    def lost(self: _kernel.Owner, path: Path, *, durable: bool = True) -> None:
        if durable:
            real(self, path, durable=durable)

    with monkeypatch.context() as patch:
        patch.setattr(_kernel.Owner, "discard_scratch", lost)
        report = _moving.adopt(target, owner, bundle, untrusted=False)
    assert report is not None and len(report.published) == 1
    (scratch,) = (target.control / "tmp").glob("*.adopt.*")
    assert {"source.json", "plan.json", bundle.name} <= set(os.listdir(scratch))
    assert (scratch / bundle.name / "bundle.json").is_file() and job_ids_below(scratch) == []
    return target, owner, scratch


@pytest.mark.parametrize(
    "lost",
    [(), ("plan.json",), ("source.json",), ("plan.json", "source.json"), ("plan.json", "source.json", "bundle.json")],
    ids=["whole", "plan-gone", "source-gone", "both-gone", "only-directories"],
)
def test_an_adoption_scratch_brought_back_as_an_empty_skeleton_is_discarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lost: tuple[str, ...]
) -> None:
    target, owner, scratch = _adoption_left_behind(tmp_path, monkeypatch)
    (bundle,) = [name for name in os.listdir(scratch) if not name.endswith(".json")]
    for name in lost:
        (scratch / (f"{bundle}/{name}" if name == "bundle.json" else name)).unlink()
    assert _moving._empty_skeleton(scratch)
    published = job_ids_below(target.jobs)
    die_and_recover(target, owner)
    assert not scratch.exists() and not list((target.control / "tmp").glob("*.adopt.*"))
    assert job_ids_below(target.jobs) == published  # nothing published twice
    assert not (tmp_path / "out").exists() or os.listdir(tmp_path / "out") == []  # never refused back to its path


def test_an_interrupted_untrusted_rekey_is_never_taken_for_a_skeleton(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    client_bundle(tmp_path / "inbox" / "entry")
    owner = cli_owner(ws)

    def crash_before_the_manifest(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        # Members renamed to their new keys, bundle.json still naming the old ones, no plan.
        if op == "write" and phase == "before_replace" and dst is not None and dst.path.name == "bundle.json":
            raise _CrashError("before the rekeyed manifest")

    _fs.set_fault_injector(crash_before_the_manifest)
    with pytest.raises(_CrashError):
        _moving.adopt(ws, owner, tmp_path / "inbox" / "entry", untrusted=True)
    _fs.set_fault_injector(None)
    (scratch,) = (ws.control / "tmp").glob("*.adopt-untrusted.*")
    assert (scratch / "rekey.json").is_file() and not (scratch / "plan.json").exists()
    assert not _moving._empty_skeleton(scratch)
    die_and_recover(ws, owner)
    assert len(job_ids_below(ws.jobs)) == 3  # resumed, every member published once


def test_an_isolation_partial_is_never_taken_for_a_skeleton(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, scratch = _adoption_left_behind(tmp_path, monkeypatch)
    (scratch / ".partial" / "entry").mkdir(parents=True)
    assert not _moving._empty_skeleton(scratch)
    shutil.rmtree(scratch / ".partial")
    assert _moving._empty_skeleton(scratch)
    # A member directory under any key, a file, a symlink or an unreadable directory is no skeleton either.
    (bundle,) = [name for name in os.listdir(scratch) if not name.endswith(".json")]
    jobs_dir = scratch / bundle / "jobs"
    member = jobs_dir / "proj" / "solo" / "solo--00000000-0000-4000-8000-000000000000"
    member.mkdir(parents=True)
    assert not _moving._empty_skeleton(scratch)
    member.rmdir()
    (jobs_dir / "stray").write_text("x")
    assert not _moving._empty_skeleton(scratch)
    (jobs_dir / "stray").unlink()
    (jobs_dir / "link").symlink_to("proj")
    assert not _moving._empty_skeleton(scratch)
    (jobs_dir / "link").unlink()
    (scratch / "other").mkdir()
    assert not _moving._empty_skeleton(scratch)  # a second bundle
    (scratch / "other").rmdir()
    if os.geteuid() != 0:
        (jobs_dir / "proj").chmod(0)
        try:
            assert not _moving._empty_skeleton(scratch)
        finally:
            (jobs_dir / "proj").chmod(0o755)
    assert _moving._empty_skeleton(scratch)


class _CrashError(BaseException):
    """A simulated process death."""


# -- 4. one parent fsync for one parent ------------------------------------------------------------------------------


def test_a_move_fsyncs_one_parent_once_only_when_it_is_provably_the_same(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory, other = tmp_path / "d", tmp_path / "e"
    directory.mkdir()
    other.mkdir()
    (tmp_path / "alias").symlink_to("d")
    fsynced: list[int] = []
    real = os.fsync

    def fsync(descriptor: int) -> None:
        fsynced.append(os.fstat(descriptor).st_ino)
        real(descriptor)

    monkeypatch.setattr(os, "fsync", fsync)

    def parents(src: _fs.Loc, dst: _fs.Loc) -> list[int]:
        fsynced.clear()
        _fs._fsync_parents(src, dst)
        return list(fsynced)

    inode = directory.stat().st_ino
    assert parents(_fs.loc(directory / "a"), _fs.loc(directory / "b")) == [inode]
    assert parents(_fs.loc(directory / "a"), _fs.loc(other / "b")) == [inode, other.stat().st_ino]
    # Lexically different paths of one directory: two fsyncs (no resolution).
    assert parents(_fs.loc(directory / "a"), _fs.loc(tmp_path / "alias" / "b")) == [inode, inode]
    first, second = _fs.open_dir(directory), _fs.open_dir(directory)
    try:
        assert parents(_fs.anchored(first, "a"), _fs.anchored(first, "b")) == [inode]
        # Two descriptors of one directory, or one anchored and one plain location: two fsyncs.
        assert parents(_fs.anchored(first, "a"), _fs.anchored(second, "b")) == [inode, inode]
        assert parents(_fs.anchored(first, "a"), _fs.loc(directory / "b")) == [inode, inode]
    finally:
        os.close(first)
        os.close(second)
    (directory / "a").mkdir()
    fsynced.clear()
    _fs.move_owned(_fs.loc(directory / "a"), _fs.loc(directory / "b"), durable=True)
    assert fsynced == [inode]


# -- 5. creation scratches -------------------------------------------------------------------------------------------


class Scratches:
    """Record which kind of scratch each ``submit`` scratch was made with."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.made: list[str] = []
        real_scratch, real_creation = _kernel.Owner.scratch, _kernel.Owner.creation_scratch

        def scratch(owner: _kernel.Owner, purpose: str) -> Path:
            if purpose == "submit":
                self.made.append("scratch")
            return real_scratch(owner, purpose)

        def creation(owner: _kernel.Owner, purpose: str) -> Path:
            self.made.append("creation")
            return real_creation(owner, purpose)

        monkeypatch.setattr(_kernel.Owner, "scratch", scratch)
        monkeypatch.setattr(_kernel.Owner, "creation_scratch", creation)


@pytest.mark.parametrize("sync", ["fsync", "syncfs"])
def test_job_creation_folds_its_scratch_s_tmp_fsync_into_the_batch_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sync: str
) -> None:
    from test_job_creation import install, raw_package

    ws = durable_workspace(tmp_path / "ws", sync)
    workflow = install(ws, raw_package(tmp_path / "package")).id
    scratches = Scratches(monkeypatch)
    tmp = (ws.control / "tmp").stat().st_ino
    order: list[str] = []
    real_fsync, real_rename = os.fsync, os.rename

    def fsync(descriptor: int) -> None:
        if os.fstat(descriptor).st_ino == tmp:
            order.append("fsync-tmp")
        real_fsync(descriptor)

    def rename(src: Any, dst: Any, **options: Any) -> None:
        if Path(dst).name == "job":
            order.append("job")
        real_rename(src, dst, **options)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "rename", rename)
    made = list(new_jobs(ws, workflow, [{"tag": f"t{index}"} for index in range(3)], batch=3))
    assert len(made) == 3 and scratches.made == ["creation"] * 3
    if sync == "fsync":
        # One fsync of tmp/ for the batch, after its commit and before the first job is named complete.
        assert order[: order.index("job")].count("fsync-tmp") == 1
    else:
        assert "fsync-tmp" not in order[: order.index("job")]  # the commit's syncfs covers the entries
    # job submit's copies too.
    scratches.made.clear()
    payloads = prepared_payloads(ws, tmp_path / "prepared", 2)
    status = command(["job", "submit", "--workspace", str(ws.root), *map(str, payloads)], CLIContext("httk", tmp_path))
    assert status == 0 and scratches.made == ["creation"] * 2


def test_moved_inputs_and_a_moved_payload_keep_the_early_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_moves_workspace(tmp_path, monkeypatch)
    try:
        origin = test_input_moves._origin(ws)
        scratches = Scratches(monkeypatch)
        new_job(
            ws,
            test_input_moves._WORKFLOW,
            parameters={"script": {"start": "succeed"}},
            move_inputs={"trajectory": origin / "run" / "traj.dat"},
        )
        assert scratches.made == ["scratch"]
    finally:
        scaffold_module._WORKFLOW_PROVIDERS.pop(test_input_moves._WORKFLOW, None)
    other = durable_workspace(tmp_path / "other", "syncfs")
    moved, copied = prepared_payloads(other, tmp_path / "p", 2)
    scratches.made.clear()
    with cli_owner(other) as owner:
        submit_payload(other, owner, moved, move=True)
        submit_payload(other, owner, copied, move=False)
    assert scratches.made == ["scratch", "scratch"]  # submit_payload is unchanged either way


# -- 6. a failed close after a validated adoption -------------------------------------------------------------------


def _failing_close(monkeypatch: pytest.MonkeyPatch, times: int = 1) -> list[_moving.Stage]:
    """Make the first *times* calls of ``Stage.close`` fail before they do anything; return the stages tried."""

    tried: list[_moving.Stage] = []
    real = _moving.Stage.close

    def close(stage: _moving.Stage) -> None:
        tried.append(stage)
        if len(tried) <= times:
            raise OSError(errno.EIO, "injected close failure")
        real(stage)

    monkeypatch.setattr(_moving.Stage, "close", close)
    return tried


@pytest.mark.parametrize("end", ["bye", "eof"])
def test_a_failed_close_answers_the_adoption_refuses_later_stages_and_is_retried_at_the_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, end: str
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    tried = _failing_close(monkeypatch)
    peer = _Peer(dict(_transfer_session.OPS))
    staged = _stage(peer, 1, far_name, near, holds[:1])
    adopted = _adopt_staged(peer, 2, far_name, staged)
    # The validated adoption is answered, the close failure reported in its stderr.
    assert adopted["ok"] is True and adopted.get("fatal") is None
    assert [entry["status"] for entry in adopted["document"]["bundles"]] == ["adopted"]
    assert "warning: the stage could not be closed after its adoption" in adopted["stderr"]
    assert find(far, plan[0][0]["id"]) is not None and len(_owners(far)) == 1  # its owner is still registered
    # No further stage is admitted, and not for capacity: nothing of it is used.
    refused = _stage(peer, 3, far_name, near, holds[1:])
    assert refused["ok"] is False and "capacity" not in refused and "document" not in refused
    assert refused["unclosed"] is True and ";" not in refused["error"]  # the client's stop reason is not cut short
    assert "could not be closed" in refused["error"] and find(far, plan[1][0]["id"]) is None
    if end == "bye":
        assert _request(peer, 4, {"op": "bye"})["ok"] is True
        assert peer.join() == 0
    else:
        assert peer.end() == 0
    # One final attempt at the session's end closed it.
    assert len(tried) == 2 and tried[0] is tried[1]
    assert _owners(far) == [] and _scratch(far) == []


def test_a_failed_close_in_a_push_keeps_the_adoption_and_stops_the_pair_with_later_holds_kept(
    ssh: tuple[Path, Workspace, str, Workspace, str],  # noqa: F811
    remote: Remote,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    far.set_policy({"visibility_deadline_seconds": 0})  # each segment adopted before the next is staged
    wrapper = tmp_path / "close_failing_httk.py"
    wrapper.write_text(_CLOSE_FAILING_HTTK, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    plan = jobs(near, 3)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 1
    captured = capsys.readouterr()
    transfers = {outcome["transfer_id"]: outcome for outcome in json.loads(captured.out)["transfers"]}
    statuses = sorted(outcome["status"] for outcome in transfers.values())
    assert statuses == ["adopted", "failed", "failed"]
    # Known outcomes: the refused segment was not delivered (nothing of it uploaded), the last never sent.
    failed = [outcome["message"] for outcome in transfers.values() if outcome["status"] == "failed"]
    assert sum("stages nothing more on this transfer session" in message for message in failed) == 1
    assert all("--resume" in message and "is unknown" not in message for message in failed)
    assert "warning: the stage could not be closed after its adoption" in captured.err
    adopted = [mapping for mapping, _, _ in plan if find(far, mapping["id"]) is not None]
    assert len(adopted) == 1
    # The later segments' holds stay held on SRC (the second was refused before its data was used).
    assert sorted(outgoing(near)) == sorted(key for key, outcome in transfers.items() if outcome["status"] == "failed")
    # The far side's final attempt at the session's end closed the stage.
    deadline = time.monotonic() + 30
    while (_owners(far) or _scratch(far)) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _owners(far) == [] and _scratch(far) == []


#: The stand-in cluster's ``httk``: the real one, whose transfer session fails the first ``Stage.close``.
_CLOSE_FAILING_HTTK = '''import errno
import runpy
import sys

argv = sys.argv[1:]
if argv[:2] == ["workflow", "transfer-session"]:
    from httk.workflow import _moving

    real = _moving.Stage.close
    calls = []

    def close(stage):
        calls.append(stage)
        if len(calls) == 1:
            raise OSError(errno.EIO, "injected close failure")
        real(stage)

    _moving.Stage.close = close
sys.argv = ["httk", *argv]
runpy.run_module("httk.core.cli", run_name="__main__", alter_sys=True)
'''


def test_a_failed_local_close_keeps_the_adoption_and_stops_the_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 0})
    plan = jobs(source, 3)
    paged(monkeypatch, 1)
    tried = _failing_close(monkeypatch)
    assert run(tmp_path, "--json", "-q", *selected(plan), source_name, target_name) == 1
    captured = capsys.readouterr()
    transfers = json.loads(captured.out)["transfers"]
    assert [outcome["status"] for outcome in transfers][:1] == ["adopted"]
    assert "warning: an adopted stage in" in captured.err and "could not be closed" in captured.err
    assert len(tried) == 1  # nothing more was staged for the pair
    adopted = [mapping for mapping, _, _ in plan if find(target, mapping["id"]) is not None]
    assert len(adopted) == 1
    # The jobs of the later pages were never moved: still in SRC.
    assert all(find(source, mapping["id"]) is not None for mapping in (m for m, _, _ in plan) if mapping not in adopted)


# -- correction round 1 (the Fable and Codex reviews of T6a) --------------------------------------------------------


@contextlib.contextmanager
def _lost_discards(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Non-durable scratch discards that a crash undid: they do nothing."""

    real = _kernel.Owner.discard_scratch

    def lost(self: _kernel.Owner, path: Path, *, durable: bool = True) -> None:
        if durable:
            real(self, path, durable=durable)

    with monkeypatch.context() as patch:
        patch.setattr(_kernel.Owner, "discard_scratch", lost)
        yield


def _directories_only(entry: Path) -> None:
    (entry / "jobs" / "a" / "b").mkdir(parents=True)
    (entry / "jobs" / "c").mkdir(parents=True)


@pytest.mark.parametrize("records", ["source", "none"])
def test_an_unisolated_untrusted_skeleton_is_rejected_not_discarded(tmp_path: Path, records: str) -> None:
    # C1: the client may still change a bundle that was not isolated; without a record of isolation, never proof.
    ws = v3_workspace(tmp_path / "ws")
    entry = tmp_path / "inbox" / "entry"
    _directories_only(entry)
    owner = cli_owner(ws)
    scratch = _kernel.take(ws, owner, _fs.loc(entry), "adopt-untrusted")
    assert scratch is not None
    rejected = tmp_path / "rejected" if records == "source" else _moving._rejected(ws)
    if records == "source":
        record = _moving._Record(None, str(rejected), False, _fs.fresh_token(), True)
        _moving._write_record(scratch, record, durable=False)
    assert _moving._empty_skeleton(scratch)  # the walk alone would take it
    die_and_recover(ws, owner)
    assert not scratch.exists()
    (unique,) = rejected.iterdir()
    assert (unique / "entry" / "jobs" / "a" / "b").is_dir()  # rejected back to the client, as before T6a


@pytest.mark.parametrize("lost", ["plan.json", "none"])
def test_an_isolated_untrusted_skeleton_is_discarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lost: str
) -> None:
    ws = v3_workspace(tmp_path / "ws")
    client_bundle(tmp_path / "inbox" / "entry")
    owner = cli_owner(ws)
    with _lost_discards(monkeypatch):
        report = _moving.adopt(ws, owner, tmp_path / "inbox" / "entry", untrusted=True, refused_to=tmp_path / "x")
    assert report is not None and len(report.published) == 3
    (scratch,) = (ws.control / "tmp").glob("*.adopt-untrusted.*")
    if lost != "none":
        (scratch / lost).unlink()  # source.json still records the isolation
    published = sorted(job_ids_below(ws.jobs))
    die_and_recover(ws, owner)
    assert not scratch.exists() and not (tmp_path / "x").exists()
    assert sorted(job_ids_below(ws.jobs)) == published


def test_the_skeleton_walk_is_bounded_by_its_entries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, _, scratch = _adoption_left_behind(tmp_path, monkeypatch)
    total = sum(1 for _ in scratch.rglob("*"))
    assert _moving._empty_skeleton(scratch)
    monkeypatch.setattr(_fs, "DEFAULT_LIMITS", _fs.WalkLimits(entries=total))
    assert _moving._empty_skeleton(scratch)
    monkeypatch.setattr(_fs, "DEFAULT_LIMITS", _fs.WalkLimits(entries=total - 1))
    assert not _moving._empty_skeleton(scratch)
    # R2: one directory far larger than the budget is never listed whole: the listing stops past the budget.
    (bundle,) = [name for name in os.listdir(scratch) if not name.endswith(".json")]
    for index in range(200):
        (scratch / bundle / "jobs" / "proj" / f"wide-{index}").mkdir()
    monkeypatch.setattr(_fs, "DEFAULT_LIMITS", _fs.WalkLimits(entries=total + 10))
    yielded: list[str] = []
    real_scandir = os.scandir

    class Counted:
        def __init__(self, target: Any) -> None:
            self.listing = real_scandir(target)

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *exc_info: object) -> None:
            self.listing.close()

        def __iter__(self) -> Iterator[os.DirEntry[str]]:
            for entry in self.listing:
                yielded.append(entry.name)
                yield entry

    monkeypatch.setattr(os, "scandir", Counted)
    assert not _moving._empty_skeleton(scratch)
    assert len(yielded) <= total + 11 < 200


@pytest.mark.parametrize("then", ["retried-close", "recovery"])
def test_a_failed_seal_of_tmp_is_completed_before_a_retried_close_removes_the_owner_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, then: str
) -> None:
    # C2: the trash's rename out of tmp/ happened, its fsync of tmp/ failed; the retry must seal tmp/ first. R1: so
    # must the recovery of an owner that died there (no scratch of it is left to take).
    ws = Workspace.initialize(tmp_path / "ws", durable=True, policy={"visibility_deadline_seconds": 0})
    owner = cli_owner(ws)
    scratch = owner.scratch("landing")
    (scratch / "copy").write_text("x")
    owner.discard_scratch(scratch, durable=False)
    tmp = ws.control / "tmp"
    tmp_inode = tmp.stat().st_ino
    events: list[str] = []
    real = os.fsync

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        renamed = op == "rename" and phase == "after" and dst is not None and dst.at is None
        if renamed and dst is not None and dst.path.parent == tmp and dst.path.name.startswith("trash."):
            events.append("trash-rename")
        if op == "remove" and src is not None and src.path == owner.path / "owner.json":
            events.append("unlink-owner")

    def fsync(descriptor: int) -> None:
        if os.fstat(descriptor).st_ino == tmp_inode:
            if events[-1:] == ["trash-rename"] and "fsync-tmp-failed" not in events:
                events.append("fsync-tmp-failed")
                raise OSError(errno.EIO, "injected")
            real(descriptor)
            events.append("fsync-tmp")
            return
        real(descriptor)

    _fs.set_fault_injector(fault)
    monkeypatch.setattr(os, "fsync", fsync)
    with pytest.raises(OSError, match="injected"):
        owner.close()
    assert events == ["trash-rename", "fsync-tmp-failed"] and (owner.path / "owner.json").exists()
    if then == "retried-close":
        owner.close()
    else:
        # The process died there: the owner is declared dead, and another recovers it.
        _kernel.attest_dead(ws, owner.owner_id, by="operator", evidence=[], reason="test")
        with cli_owner(ws) as rescuer:
            events.append("recovering")
            report = _kernel.recover(ws, rescuer, owner.owner_id)
            assert report.scratch == ()  # nothing of it left to take
    after = events[events.index("recovering") + 1 :] if then == "recovery" else events[2:]
    assert "unlink-owner" in after and "fsync-tmp" in after[: after.index("unlink-owner")]
    assert not (owner.path / "owner.json").exists()


def test_a_placement_component_ending_in_a_uuid_is_no_member(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # C3: runs/mat-<uuid> is a legal placement; its skeleton is discarded, never refused back into a hold.
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    mapping = job_mapping(WORKFLOW, {"start": "succeed"}, tag="solo", placement=f"runs/mat-{uuid.uuid4()}")
    place(source, mapping, "succeeded")
    with cli_owner(source) as owner:
        hold = _moving.hold(source, owner, claim_root(source, owner, mapping["id"]), tree=True, destination_locator="b")
    owner = cli_owner(target)
    with _lost_discards(monkeypatch):
        assert _moving.adopt(target, owner, Path(hold.path), untrusted=False, synced=True) is not None
    (scratch,) = (target.control / "tmp").glob("*.adopt.*")
    (scratch / "plan.json").unlink()
    assert _moving._empty_skeleton(scratch)
    die_and_recover(target, owner)
    assert not scratch.exists()
    assert not Path(hold.path).exists() and _moving.held(source) == []  # no hold resurrected
    assert find(target, mapping["id"]) is not None


def test_a_copied_adoption_leaves_a_skeleton_that_is_discarded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # C4: the copy's emptied .partial/ is removed, so the skeleton is recognized.
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    mapping = single(source)[0]
    with cli_owner(source) as owner:
        _moving.eject(source, owner, claim_root(source, owner, mapping["id"]), destination=tmp_path / "out", tree=True)
    (bundle,) = (tmp_path / "out").iterdir()
    real_take = _kernel.take

    def take(*arguments: Any, **options: Any) -> Any:
        monkeypatch.setattr(_kernel, "take", real_take)
        raise _fs.CrossDevice("injected: another filesystem")

    monkeypatch.setattr(_kernel, "take", take)
    owner = cli_owner(target)
    with _lost_discards(monkeypatch):
        report = _moving.adopt(target, owner, bundle, untrusted=False)
    assert report is not None and report.copied and bundle.is_dir()  # the source stays in place
    (scratch,) = (target.control / "tmp").glob("*.adopt.*")
    assert not (scratch / ".partial").exists()
    (scratch / "plan.json").unlink()
    (scratch / "source.json").unlink()
    die_and_recover(target, owner)
    assert list((target.control / "tmp").glob("*.adopt.*")) == []
    assert not (target.control / "owners" / owner.owner_id / "owner.json").exists()  # no owner left


def test_a_hold_whose_delivery_copied_keeps_a_durable_discard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # C5: across filesystems the delivery copies, and the original stays in the hold's scratch.
    source = v3_workspace(tmp_path / "a")
    held, copied = single(source), single(source)
    real_deliver = _fs.deliver
    calls: list[int] = []

    def deliver(*arguments: Any, **options: Any) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise _fs.CrossDevice("injected: another filesystem")
        return real_deliver(*arguments, **options)

    discards = Discards(monkeypatch)
    with cli_owner(source) as owner:
        monkeypatch.setattr(_fs, "deliver", deliver)
        _moving.hold(source, owner, claim_root(source, owner, copied[0]["id"]), tree=True, destination_locator="b")
        _moving.hold(source, owner, claim_root(source, owner, held[0]["id"]), tree=True, destination_locator="b")
    assert len(calls) == 3 and discards.seen == [("hold", True), ("hold", False)]
    assert len(_moving.held(source)) == 2


def test_a_lost_owner_while_closing_an_adopted_stage_stays_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # C7: OwnerLost is no cleanup failure: the far side fails, as for any lost owner.
    near, _ = registered(tmp_path, "near")
    _, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(near, 1)

    def close(stage: _moving.Stage) -> None:
        raise _kernel.OwnerLost("injected: a recoverer took this owner")

    monkeypatch.setattr(_moving.Stage, "close", close)
    peer = _Peer(dict(_transfer_session.OPS))
    staged = _stage(peer, 1, far_name, near, holds)
    answer = _adopt_staged(peer, 2, far_name, staged)
    assert answer["ok"] is False and answer["fatal"] is True and "OwnerLost" in answer["error"]
    assert peer.join() == 1


def test_pooled_directory_fsyncs_open_each_parent_chain_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # C8: one task per parent directory: its chain opened once, each child relative to it.
    source = tmp_path / "source"
    for name in ("a", "b", "c", "d", "e"):
        (source / "top" / "mid" / name).mkdir(parents=True)
        (source / "top" / "mid" / name / "file").write_text(name)
    data = stream(source, ["top"])
    opened: list[str] = []
    real_open = os.open

    def counting(path: Any, flags: int, *arguments: Any, **options: Any) -> int:
        if flags & os.O_DIRECTORY and threading.current_thread() is not threading.main_thread():
            opened.append(os.fspath(path))
        return real_open(path, flags, *arguments, **options)

    target = tmp_path / "landing"
    target.mkdir()
    monkeypatch.setattr(os, "open", counting)
    _fs.extract_tar(io.BytesIO(data), target, workers=8, durable=True)
    # Directories below the target: top, mid and five leaves, in three parents (the target, top, mid). Each task
    # opens its parent's chain (0, 1 and 2 components) and then each child once: 3 + 7 opens, not one chain each.
    assert len(opened) == 3 + 7, opened
