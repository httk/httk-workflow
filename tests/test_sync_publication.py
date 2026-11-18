"""Payload written outside the transfer paths is durable before it is published (plan WORKFLOW_SYNC_BARRIER §8.4,
phase S2): job creation, the workflow store and ``job adopt PATH`` of a bundle on the workspace's filesystem.

Each site enters one sync barrier before the first byte of the tree it publishes is written and commits it before
the rename that names the tree complete: ``scratch/job`` for a job (the submit scratch's reconciler publishes a
``job/``), the rename into ``workflows/`` for a package, the take for an adopted bundle. A failed commit publishes
nothing. The recorded call order is the property under test.
"""

import dataclasses
import errno
import fcntl
import gc
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import threading
from collections.abc import Iterator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

import test_input_moves
from httk.workflow import Workspace, _fs, _kernel, _moving, _store
from httk.workflow import scaffold as scaffold_module
from httk.workflow._bundles import BundleError
from httk.workflow.scaffold import new_job, new_jobs, submit_payload
from httk.workflow.seals import read_job_releases
from httk.workflow.workflow_cli import command
from test_cli_job_eject_adopt import registered
from test_job_creation import assert_nothing_left, install, raw_package, ready
from test_moving import assert_in, find, single
from test_runner_builds import _tool_package
from v3_helpers import cli_owner

pytestmark = pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="names descriptors through /proc")

#: An executable instantiate hook: it writes ``hook.txt`` into the payload (its working directory).
_HOOK = "#!/bin/sh\ncat >/dev/null\nprintf 'hooked\\n' > hook.txt\nprintf '{\"parameters\": {}}'\n"


class Crash(Exception):
    """A simulated crash."""


@pytest.fixture(autouse=True)
def _fresh(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_fs, "_syncfs_works", {})
    yield
    _fs.set_fault_injector(None)


class Events:
    """One ordered record of barrier entries, ``syncfs`` calls, fsyncs, copies, walks, renames, submits and takes."""

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        syncfs_failures: Sequence[int | None] = (),
        crash: str | None = None,
        fsync_failure: str | None = None,
    ) -> None:
        self.lock = threading.Lock()
        self.events: list[tuple[str, object]] = []
        self.failures = list(syncfs_failures)
        self.barriers: list[_fs.Barrier] = []
        #: Each fsync: the path of its descriptor and whether it was opened for writing.
        self.fsynced: list[tuple[str, bool]] = []
        #: The size of each fsynced file as its descriptor saw it at the fsync.
        self.sizes: dict[str, int] = {}
        real_enter, real_syncfs, real_walk = _fs.Barrier.__enter__, _fs._syncfs, _fs.fsync_landing
        real_copy, real_copy_tree, real_fsync = _fs.copy_file, _fs.copy_tree, os.fsync
        real_submit, real_take = _kernel.submit, _kernel.take
        real_hook, real_prepare = scaffold_module._run_executable_instantiate, scaffold_module.prepare_job_payload
        real_build = _store.execute_build

        def enter(barrier: _fs.Barrier) -> _fs.Barrier:
            self.add("entering", str(barrier.anchor))
            entered = real_enter(barrier)
            with self.lock:
                self.barriers.append(barrier)
            self.add("enter", barrier._descriptor)
            return entered

        def syncfs(descriptor: int) -> None:
            self.add("syncfs", descriptor)
            failure = self.failures.pop(0) if self.failures else None
            if failure is not None:
                raise OSError(failure, os.strerror(failure))
            real_syncfs(descriptor)

        def walk(anchor: int, **options: Any) -> int:
            self.add("walk-start", os.readlink(f"/proc/self/fd/{anchor}"))
            count = real_walk(anchor, **options)
            self.add("walk", count)
            return count

        def copy_file(src: Any, dst: Any, *, durable: bool, metadata: bool = False) -> None:
            self.add("copy", (os.fspath(dst), durable))
            real_copy(src, dst, durable=durable, metadata=metadata)

        def copy_tree(src: Any, dst: Path, *, durable: bool, **options: Any) -> None:
            self.add("copy-tree", (str(dst), durable))
            real_copy_tree(src, dst, durable=durable, **options)

        def fsync(descriptor: int) -> None:
            path = os.readlink(f"/proc/self/fd/{descriptor}")
            writing = fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY
            with self.lock:
                self.fsynced.append((path, writing))
                if writing:
                    self.sizes[path] = os.fstat(descriptor).st_size
            self.add("fsync", path)
            if fsync_failure is not None and path.endswith(fsync_failure) and not writing:
                raise OSError(errno.EIO, os.strerror(errno.EIO))
            real_fsync(descriptor)

        def submit(*arguments: Any, **options: Any) -> Any:
            self.add("submit", None)
            return real_submit(*arguments, **options)

        def take(*arguments: Any, **options: Any) -> Any:
            self.add("take", None)
            return real_take(*arguments, **options)

        def hook(*arguments: Any, **options: Any) -> Any:
            result = real_hook(*arguments, **options)
            self.add("hook", None)
            return result

        def build(*arguments: Any, **options: Any) -> Any:
            self.add("build-start", None)
            result = real_build(*arguments, **options)
            self.add("build-end", None)
            return result

        def prepare(*arguments: Any, **options: Any) -> Any:
            result = real_prepare(*arguments, **options)
            self.add("job.json", None)
            return result

        def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
            if op != "rename" or dst is None or dst.at is not None:
                return
            if phase == "before":
                self.add("rename", str(dst.path))
            if crash is not None and phase == crash and dst.path.name == "job":
                raise Crash(f"rename {phase} {dst.path}")

        monkeypatch.setattr(_fs.Barrier, "__enter__", enter)
        monkeypatch.setattr(_fs, "_syncfs", syncfs)
        monkeypatch.setattr(_fs, "fsync_landing", walk)
        monkeypatch.setattr(_fs, "copy_file", copy_file)
        monkeypatch.setattr(_fs, "copy_tree", copy_tree)
        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(_kernel, "submit", submit)
        monkeypatch.setattr(_store, "execute_build", build)
        monkeypatch.setattr(_kernel, "take", take)
        monkeypatch.setattr(scaffold_module, "_run_executable_instantiate", hook)
        monkeypatch.setattr(scaffold_module, "prepare_job_payload", prepare)
        _fs.set_fault_injector(fault)

    def add(self, kind: str, detail: object) -> None:
        with self.lock:
            self.events.append((kind, detail))

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.events]

    def indices(self, kind: str) -> list[int]:
        return [index for index, (seen, _) in enumerate(self.events) if seen == kind]

    def first(self, kind: str) -> int:
        return self.indices(kind)[0]

    def last(self, kind: str) -> int:
        return self.indices(kind)[-1]

    def details(self, kind: str) -> list[Any]:
        return [detail for seen, detail in self.events if seen == kind]

    def renames(self, predicate: Any) -> list[int]:
        """The indices of the renames whose destination path satisfies *predicate*."""

        return [
            index
            for index, (kind, detail) in enumerate(self.events)
            if kind == "rename" and predicate(Path(str(detail)))
        ]


def durable_workspace(path: Path, sync: str | None = None) -> Workspace:
    """A durable workspace (the job-creation suites use non-durable ones), with ``io.sync`` when given."""

    ws = Workspace.initialize(path, durable=True, policy={"visibility_deadline_seconds": 0.05})
    if sync is not None:
        ws.configure_settings({"io.sync": sync})
    return ws


def hooked_package(root: Path, *, extra: str = "") -> Path:
    """A package whose executable instantiate hook writes ``hook.txt`` into each payload."""

    package = raw_package(root, "tests.sync", extra='\n[workflow.instantiate]\nfile = "hook"\n' + extra)
    (package / "hook").write_text(_HOOK, encoding="utf-8")
    (package / "hook").chmod(0o755)
    return package


def job_rename(path: Path) -> bool:
    """A rename naming a staged payload complete: ``tmp/<owner>.submit.<token>/job``."""

    return path.name == "job" and ".submit." in path.parent.name


def assert_syncfs_barrier(events: Events, barrier: int, first_write: int, last_write: int, published: int) -> None:
    """Barrier *barrier* was entered and primed before *first_write*, and committed after *last_write* and before
    *published*, both ``syncfs`` on the descriptor it opened."""

    entering = events.indices("entering")[barrier]
    descriptor = events.details("enter")[barrier]
    calls = [index for index, (kind, detail) in enumerate(events.events) if kind == "syncfs" and index > entering]
    prime, commit = calls[0], calls[1]
    assert events.events[prime][1] == events.events[commit][1] == descriptor
    assert entering < prime < first_write
    assert last_write < commit < published


# -- job creation -------------------------------------------------------------------------------------------------


def staged_job(ws: Workspace, tmp_path: Path, **options: Any) -> Any:
    install(ws, hooked_package(tmp_path / "package"))
    data = tmp_path / "INPUT"
    data.write_text("input\n", encoding="utf-8")
    return lambda **more: new_job(ws, "tests.sync", files={"INPUT": data}, **{**options, **more})


def test_a_job_is_synced_once_before_its_payload_is_named_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    create = staged_job(ws, tmp_path)
    events = Events(monkeypatch)
    job = create()
    assert (job.payload / "hook.txt").read_text(encoding="utf-8") == "hooked\n"
    ((barrier,),) = [events.barriers]
    assert barrier.strategy == "syncfs" and barrier.anchor == ws.control / "tmp" and barrier.prime
    assert events.kinds().count("syncfs") == 2
    (renamed,) = events.renames(job_rename)
    # The staged copy, then the hook's file, then job.json: all inside the barrier.
    assert events.first("copy") < events.first("hook") < events.first("job.json")
    assert_syncfs_barrier(events, 0, events.first("copy"), events.last("job.json"), renamed)
    assert renamed < events.first("submit")
    # Under syncfs nothing is fsynced per file, and the staged copy carries no fsync of its own.
    assert [durable for _, durable in events.details("copy")] == [False]
    assert "walk" not in events.kinds()


def test_under_fsync_copies_fsync_on_their_writing_descriptor_and_the_walk_covers_hook_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    create = staged_job(ws, tmp_path)
    events = Events(monkeypatch)
    create()
    ((barrier,),) = [events.barriers]
    assert barrier.strategy == "fsync" and "syncfs" not in events.kinds()
    (renamed,) = events.renames(job_rename)
    walked = events.last("walk")
    assert events.last("hook") < events.last("job.json") < walked < renamed
    staging = Path(events.details("walk-start")[-1])
    # The command's barrier is anchored on tmp/; the job's commit walks its own submit scratch only.
    assert barrier.anchor == ws.control / "tmp" and staging.parent == barrier.anchor and ".submit." in staging.name
    assert events.details("walk-start") == [str(staging)]
    # The staged copy and job.json fsynced on the descriptor that wrote them.
    (copy,) = events.details("copy")
    assert copy == (str(staging / "build" / "files" / "INPUT"), True)
    for name in ("files/INPUT", "job.json"):
        assert (str(staging / "build" / name), True) in events.fsynced
    # Every byte of job.json had reached its descriptor when it was fsynced (a buffered stream is flushed first).
    assert events.sizes[str(staging / "build" / "job.json")] == (job_payload(ws) / "job.json").stat().st_size
    # The hook's file, written by another process, fsynced by the late walk (a read-only descriptor), then the
    # payload's directories, all before the rename.
    hook_file = str(staging / "build" / "hook.txt")
    assert (hook_file, False) in events.fsynced
    fsyncs = [(index, str(detail)) for index, (kind, detail) in enumerate(events.events) if kind == "fsync"]
    assert all(index < renamed for index, path in fsyncs if path.startswith(str(staging / "build")))
    for directory in (staging / "build" / "files", staging / "build", staging):
        assert (str(directory), False) in events.fsynced


def test_an_embedded_workflow_is_copied_with_fsync_under_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    package = hooked_package(tmp_path / "package")
    events = Events(monkeypatch)
    job = new_job(ws, package, embed_workflow=True)
    (renamed,) = events.renames(job_rename)
    (staging,) = events.details("walk-start")
    assert events.details("copy-tree") == [(str(Path(staging) / "build" / "workflow"), True)]
    assert events.first("copy-tree") < events.last("walk") < renamed
    assert (job.payload / "workflow" / "hook").is_file()


def test_a_failed_commit_submits_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    create = staged_job(ws, tmp_path)
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    with pytest.raises(_fs.SyncFailed, match="was not submitted, nothing was published: syncfs of .* not durable"):
        create()
    assert events.renames(job_rename) == [] and "submit" not in events.kinds()
    assert ready(ws) == []
    # The staged copy is discardable: the owner's close discarded the scratch.
    assert list((ws.control / "tmp").iterdir()) == []


@pytest.mark.parametrize("phase", ["before", "after"])
def test_only_a_committed_payload_is_ever_named_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    create = staged_job(ws, tmp_path)
    # A crash right after the commit (before the rename to job/), or right after that rename.
    events = Events(monkeypatch, crash=phase)
    with pytest.raises(Crash):
        create()
    (renamed,) = events.renames(job_rename)
    commit = events.indices("syncfs")[1]
    assert commit < renamed
    if phase == "before":
        # No job/: the build is discarded, nothing published.
        assert ready(ws) == [] and "submit" not in events.kinds()
    else:
        # job/ named after the commit: the scratch reconciler published it as the dead owner would have.
        (published,) = ready(ws)
        assert (published.path / "hook.txt").is_file() and commit < events.first("submit")
    assert list((ws.control / "tmp").iterdir()) == []


def job_payload(ws: Workspace) -> Path:
    (ref,) = ready(ws)
    return ref.path


def test_a_walk_failure_at_job_creation_publishes_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    create = staged_job(ws, tmp_path)
    # The hook's file cannot be fsynced by the late walk.
    events = Events(monkeypatch, fsync_failure="hook.txt")
    with pytest.raises(_fs.SyncFailed, match="was not submitted, nothing was published: cannot sync .*hook.txt"):
        create()
    assert events.renames(job_rename) == [] and ready(ws) == []
    assert list((ws.control / "tmp").iterdir()) == []


@pytest.mark.parametrize("ending", ["close", "abandon"])
def test_a_closed_or_abandoned_campaign_closes_its_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ending: str
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    install(ws, hooked_package(tmp_path / "package"))
    events = Events(monkeypatch)
    jobs = new_jobs(ws, "tests.sync", [{"tag": f"j{index}"} for index in range(3)])
    next(jobs)
    ((barrier,),) = [events.barriers]
    descriptor = barrier._descriptor
    assert descriptor is not None and os.fstat(descriptor)
    if ending == "close":
        jobs.close()
    else:
        del jobs
        gc.collect()
    assert barrier._descriptor is None and len(ready(ws)) == 1
    assert list((ws.control / "tmp").iterdir()) == []


def test_prepared_documents_and_saved_inputs_are_fsynced_where_they_are_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import httk.core
    import httk.core.register

    ws = durable_workspace(tmp_path / "ws", "fsync")
    extra = '\n[workflow.inputs.thing]\ndestination = "THING"\n\n[workflow.inputs.object]\nrole = "object"\n'
    package = hooked_package(tmp_path / "package", extra=extra)
    installed = install(ws, package)

    def save(value: object, path: Any) -> None:
        Path(path).write_text(f"saved {value!r}\n", encoding="utf-8")
        events.add("saved", str(path))

    monkeypatch.setattr(httk.core, "save", save)
    monkeypatch.setattr(httk.core, "has_writer_for", lambda _name: True)
    monkeypatch.setattr(httk.core.register, "known_writers", lambda: {".txt": None})
    prepared = scaffold_module._prepare(installed, step=None)
    prepared = dataclasses.replace(prepared, documents={"doc.json": "{}\n", "files/raw.bin": b"raw"})
    staging = tmp_path / "staging"
    staging.mkdir()
    events = Events(monkeypatch)
    scaffold_module._build_payload(
        ws,
        prepared,
        staging,
        inputs={"thing": Decimal(1), "object": Decimal(2)},
        files=None,
        parameters=None,
        environment=None,
        tag=None,
        priority=None,
        name=None,
        durable=True,
    )
    # Each prepared document fsynced on the descriptor that wrote it, with all its bytes.
    for name, size in (("files/doc.json", 3), ("files/raw.bin", 3)):
        path = str(staging / name)
        assert (path, True) in events.fsynced and events.sizes[path] == size
    # Each save's output fsynced right after the save returned, in this process.
    saved = [(index, str(path)) for index, (kind, path) in enumerate(events.events) if kind == "saved"]
    assert sorted(Path(path).name for _, path in saved) == ["THING", "object.txt"]
    for index, path in saved:
        assert events.events[index + 1] == ("fsync", path)


def test_a_campaign_takes_one_barrier_primed_once_and_commits_before_each_job_s_rename(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    install(ws, hooked_package(tmp_path / "package"))
    events = Events(monkeypatch)
    submitted = list(new_jobs(ws, "tests.sync", [{"tag": f"j{index}"} for index in range(3)]))
    assert len(submitted) == 3 and len(ready(ws)) == 3
    # N jobs cost N + 1 syncfs calls, all on the one descriptor the barrier opened.
    ((barrier,),) = [events.barriers]
    assert barrier.anchor == ws.control / "tmp"
    assert events.details("syncfs") == events.details("enter") * 4
    renamed = events.renames(job_rename)
    hooks = events.indices("hook")
    prime, *commits = events.indices("syncfs")
    assert events.first("entering") < prime < hooks[0]
    for index, commit in enumerate(commits):
        # Each job's commit follows its own last write and precedes its own rename, and the next job's staging.
        assert hooks[index] < commit < renamed[index]
        if index:
            assert renamed[index - 1] < hooks[index]


def test_a_failed_commit_stops_the_campaign_at_that_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    install(ws, hooked_package(tmp_path / "package"))
    # The prime and the first job's commit succeed; the second job's commit reports an error since the first.
    events = Events(monkeypatch, syncfs_failures=[None, None, errno.EIO])
    jobs = new_jobs(ws, "tests.sync", [{"tag": f"j{index}"} for index in range(3)])
    first = next(jobs)
    with pytest.raises(_fs.SyncFailed, match="the job j1--.* was not submitted, nothing was published: .*") as raised:
        next(jobs)
    assert "the 1 job(s) before it were submitted: create the rest with --duplicate-tag skip" in str(raised.value)
    assert "io.sync fsync" in str(raised.value) and "io.sync_errors ignore" in str(raised.value)
    assert first.tag == "j0" and [ref.path for ref in ready(ws)] == [first.payload]
    assert len(events.renames(job_rename)) == 1 and events.kinds().count("syncfs") == 3
    assert events.kinds().count("hook") == 2  # the third job was never staged
    assert list((ws.control / "tmp").iterdir()) == []


def durable_moves_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Workspace:
    """The ``--input-move-from-job`` suite's workspace, made durable with ``io.sync`` forced to ``syncfs``."""

    def durable(root: Path) -> Workspace:
        return durable_workspace(root, "syncfs")

    monkeypatch.setattr(test_input_moves, "workspace", durable)
    return test_input_moves._workspace(tmp_path)


@pytest.mark.parametrize("back", [True, False])
def test_a_failed_commit_with_moved_inputs_moves_them_back_or_keeps_the_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, back: bool
) -> None:
    ws = durable_moves_workspace(tmp_path, monkeypatch)
    try:
        origin = test_input_moves._origin(ws)
        source = origin / "run" / "traj.dat"
        if not back:
            monkeypatch.setattr(scaffold_module, "_undo_moves", lambda *_arguments, **_options: False)
        events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
        with pytest.raises(_fs.SyncFailed, match="was not submitted, nothing was published"):
            new_job(
                ws,
                test_input_moves._WORKFLOW,
                parameters={"script": {"start": "succeed"}},
                move_inputs={"trajectory": source},
            )
        assert events.renames(job_rename) == [] and "submit" not in events.kinds() and ready(ws) == []
        if back:
            # As for any failure before submission: the file is home again and its release withdrawn.
            assert source.read_bytes() == test_input_moves._FRAMES and read_job_releases(origin) == ()
            assert list((ws.control / "tmp").iterdir()) == []
        else:
            # A moved file that cannot go back keeps the scratch holding it, as today.
            (kept,) = (ws.control / "tmp").iterdir()
            assert (kept / "build" / "files" / "trajectory.dat").read_bytes() == test_input_moves._FRAMES
    finally:
        scaffold_module._WORKFLOW_PROVIDERS.pop(test_input_moves._WORKFLOW, None)


def test_a_submitted_copy_is_synced_before_it_is_named_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    scaffold_module.scaffold_job(ws, install(ws, raw_package(tmp_path / "package")).id, prepared)
    events = Events(monkeypatch)
    with cli_owner(ws) as owner:
        ref = submit_payload(ws, owner, prepared, move=False, changes={"tag": "copied"})
    assert ref.state == "ready" and prepared.is_dir()
    ((barrier,),) = [events.barriers]
    assert ".submit." in barrier.anchor.name and barrier.inline
    (renamed,) = events.renames(job_rename)
    copied = events.first("copy-tree")
    assert events.details("copy-tree") == [(str(barrier.anchor / "copy"), False)]
    assert_syncfs_barrier(events, 0, copied, copied, renamed)


def prepared_payloads(ws: Workspace, tmp_path: Path, count: int) -> list[Path]:
    """*count* payloads prepared outside the workspace (``prepared-<n>``), as an operator would."""

    installed = install(ws, raw_package(tmp_path / "package")).id
    payloads = []
    for index in range(count):
        payload = tmp_path / f"prepared-{index}"
        payload.mkdir()
        scaffold_module.scaffold_job(ws, installed, payload, tag=f"p{index}")
        payloads.append(payload)
    return payloads


def submit(tmp_path: Path, ws: Workspace, *argv: str) -> int:
    return command(["job", "submit", "--workspace", str(ws.root), *argv], CLIContext("httk", tmp_path))


def test_a_moved_payload_is_synced_in_place_without_a_prime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    (prepared,) = prepared_payloads(ws, tmp_path, 1)
    events = Events(monkeypatch)
    with cli_owner(ws) as owner:
        ref = submit_payload(ws, owner, prepared, move=True)
    assert ref.state == "ready" and not prepared.exists()
    ((barrier,),) = [events.barriers]
    assert barrier.anchor == ws.control and not barrier.prime
    # One syncfs, on the descriptor opened on the control directory, before the payload is renamed to job/.
    assert events.details("syncfs") == events.details("enter")
    (renamed,) = events.renames(job_rename)
    assert events.first("enter") < events.first("syncfs") < renamed


def test_a_moved_payload_whose_sync_fails_stays_in_place(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    (prepared,) = prepared_payloads(ws, tmp_path, 1)
    events = Events(monkeypatch, syncfs_failures=[errno.EIO])
    with cli_owner(ws) as owner, pytest.raises(_fs.SyncFailed, match="re-create the payload from its source"):
        submit_payload(ws, owner, prepared, move=True)
    assert (prepared / "job.json").is_file() and ready(ws) == []
    assert events.renames(job_rename) == [] and events.kinds().count("syncfs") == 1


def test_job_submit_move_syncs_all_payloads_once_and_a_failure_fails_them_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    first, second, third, fourth = prepared_payloads(ws, tmp_path, 4)
    events = Events(monkeypatch)
    assert submit(tmp_path, ws, "--move", str(first), str(second)) == 0
    # One syncfs, no prime, for both, before either is renamed to job/.
    ((barrier,),) = [events.barriers]
    assert barrier.anchor == ws.control and events.details("syncfs") == events.details("enter")
    assert events.first("syncfs") < min(events.renames(job_rename)) and len(ready(ws)) == 2
    capsys.readouterr()
    events = Events(monkeypatch, syncfs_failures=[errno.EIO])
    assert submit(tmp_path, ws, "--move", str(third), str(fourth)) == 1
    err = capsys.readouterr().err
    assert err.count("re-create the payload from its source") == 2
    assert events.renames(job_rename) == [] and (third / "job.json").is_file() and (fourth / "job.json").is_file()


@pytest.mark.parametrize("failing", [0, 1])
def test_job_submit_stops_at_a_failed_commit_keeping_those_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], failing: int
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    payloads = prepared_payloads(ws, tmp_path, 3)
    # Batches of one payload (plan 7.3 batches them; this keeps the per-payload scenario): the prime, then one
    # commit per payload: the commit of payload *failing* reports an error.
    monkeypatch.setattr(scaffold_module, "_BATCH_JOBS", 1)
    events = Events(monkeypatch, syncfs_failures=[None] * (failing + 1) + [errno.EIO])
    assert submit(tmp_path, ws, *map(str, payloads)) == 1
    err = capsys.readouterr().err
    assert "was not submitted, nothing was published" in err
    assert f"stopped: the {2 - failing} payload(s) after {payloads[failing]} were not submitted" in err
    # Only the payloads before it were copied and submitted; nothing after it was even copied.
    assert len(ready(ws)) == failing and len(events.details("copy-tree")) == failing + 1
    assert_nothing_left(ws)


def test_job_submit_copies_under_one_barrier_with_a_commit_per_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    payloads = prepared_payloads(ws, tmp_path, 2)
    monkeypatch.setattr(scaffold_module, "_clock", lambda: 0.0)  # no time cap: one batch however slow the copies
    events = Events(monkeypatch)
    assert submit(tmp_path, ws, *map(str, payloads)) == 0
    ((barrier,),) = [events.barriers]
    assert barrier.anchor == ws.control / "tmp" and barrier.prime and barrier.inline
    # One batch, one commit (plan 7.3; this pinned one commit per payload before batching).
    prime, commit = events.indices("syncfs")
    renamed = events.renames(job_rename)
    copies = events.indices("copy-tree")
    assert set(events.details("syncfs")) == {events.details("enter")[0]}
    assert prime < copies[0] < copies[1] < commit < renamed[0] < renamed[1]
    assert all(payload.is_dir() for payload in payloads) and len(ready(ws)) == 2


def test_a_submitted_copy_under_fsync_is_fsynced_as_it_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    (prepared,) = prepared_payloads(ws, tmp_path, 1)
    events = Events(monkeypatch)
    with cli_owner(ws) as owner:
        submit_payload(ws, owner, prepared, move=False)
    ((barrier,),) = [events.barriers]
    assert barrier.inline and ".submit." in barrier.anchor.name
    (copied, durable) = events.details("copy-tree")[0]
    assert durable and "syncfs" not in events.kinds() and "walk" not in events.kinds()
    (renamed,) = events.renames(job_rename)
    # Each file on its writing descriptor, each directory too, all before the rename.
    assert (str(Path(copied) / "job.json"), True) in events.fsynced and (copied, False) in events.fsynced
    assert events.sizes[str(Path(copied) / "job.json")] == (prepared / "job.json").stat().st_size
    last = max(
        index for index, (kind, path) in enumerate(events.events) if kind == "fsync" and str(path).startswith(copied)
    )
    assert last < renamed


def test_a_non_durable_workspace_takes_no_barrier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = Workspace.initialize(tmp_path / "ws", durable=False, policy={"visibility_deadline_seconds": 0.05})
    ws.configure_settings({"io.sync": "syncfs"})

    def refuse(*_arguments: Any, **_options: Any) -> None:
        raise AssertionError("a barrier was taken")

    monkeypatch.setattr(_fs.Barrier, "__init__", refuse)
    install(ws, hooked_package(tmp_path / "package"))
    install(ws, _tool_package(tmp_path / "tool"))
    with cli_owner(ws) as owner:
        _store.build(ws, owner, "local:tests.tool")
    data = tmp_path / "INPUT"
    data.write_text("input\n", encoding="utf-8")
    job = new_job(ws, "tests.sync", files={"INPUT": data})
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    scaffold_module.scaffold_job(ws, "tests.sync", prepared)
    with cli_owner(ws) as owner:
        submit_payload(ws, owner, prepared, move=True)
    assert (job.payload / "hook.txt").is_file() and len(ready(ws)) == 2


# -- the workflow store -------------------------------------------------------------------------------------------


def store_rename(ws: Workspace) -> Any:
    """A rename into ``workflows/`` (an installation) or into an installation's ``builds/``."""

    workflows = ws.root / "workflows"
    return lambda path: (
        path.parent == workflows or (path.parent.name == "builds" and path.parent.parent.parent == workflows)
    )


def test_an_install_is_synced_before_it_is_renamed_into_workflows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    events = Events(monkeypatch)
    installed = install(ws, _tool_package(tmp_path / "tool"))
    assert _store.built(installed) is True
    tree, builds = events.barriers
    assert (
        tree.anchor.name == "tree" and ".build." in tree.anchor.parent.name and builds.anchor == tree.anchor / "builds"
    )
    assert events.kinds().count("syncfs") == 4
    (renamed,) = events.renames(store_rename(ws))
    # The package's files under the first barrier; the build outside any barrier; its artifact under the second.
    copies = events.indices("copy")
    targets = [Path(path).relative_to(tree.anchor).as_posix() for path, _ in events.details("copy")]
    assert sorted(targets[:4]) == ["package/build.sh", "package/httk_workflow.toml", "package/payload", "package/run"]
    assert targets[4].startswith("builds/") and targets[4].endswith("/artifacts/out/tool")
    assert not any(durable for _, durable in events.details("copy"))
    _, first_commit, second_prime, _ = events.indices("syncfs")
    assert_syncfs_barrier(events, 0, copies[0], copies[3], renamed)
    assert_syncfs_barrier(events, 1, copies[4], copies[4], renamed)
    assert first_commit < events.first("build-start") < events.first("build-end") < events.indices("entering")[1]
    assert second_prime < copies[4]


def test_under_fsync_an_install_fsyncs_every_copy_and_walks_its_tree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    events = Events(monkeypatch)
    install(ws, _tool_package(tmp_path / "tool"))
    tree, builds = events.barriers
    assert "syncfs" not in events.kinds()
    (renamed,) = events.renames(store_rename(ws))
    copies = events.details("copy")
    assert len(copies) == 5 and all(durable for _, durable in copies)
    for path, _ in copies:
        assert (path, True) in events.fsynced
    assert events.details("walk-start") == [str(tree.anchor), str(builds.anchor)]
    assert events.last("copy") < events.last("walk") < renamed
    assert (str(tree.anchor / "package"), False) in events.fsynced
    # install.json and build.json fsynced as they were written.
    # (write_file fsyncs its temporary before renaming it into place.)
    assert any(
        Path(path).parent == tree.anchor and "install.json" in path and writing for path, writing in events.fsynced
    )
    assert any("build.json" in path and writing for path, writing in events.fsynced)


def test_a_reinstall_commits_before_the_old_installation_is_moved_aside(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    package = raw_package(tmp_path / "package")
    install(ws, package)
    (package / "extra").write_text("changed\n", encoding="utf-8")
    events = Events(monkeypatch)
    install(ws, package)
    (aside,) = events.renames(lambda path: ".old." in path.name and path.parent == ws.root / "workflows")
    (renamed,) = events.renames(lambda path: ".old." not in path.name and path.parent == ws.root / "workflows")
    assert events.indices("syncfs")[1] < aside < renamed


def test_a_store_build_under_fsync_fsyncs_its_artifacts_and_walks_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "fsync")
    installed = install(ws, _tool_package(tmp_path / "tool"), build=False)
    events = Events(monkeypatch)
    with cli_owner(ws) as owner:
        _store.build(ws, owner, installed.id)
    ((barrier,),) = [events.barriers]
    assert barrier.anchor.name == "builds" and events.first("build-end") < events.first("entering")
    ((artifact, durable),) = events.details("copy")
    assert durable and (artifact, True) in events.fsynced
    (renamed,) = events.renames(store_rename(ws))
    assert events.details("walk-start") == [str(barrier.anchor)] and events.last("walk") < renamed


def test_a_failed_install_commit_installs_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    with pytest.raises(_fs.SyncFailed, match="the workflow local:tests.raw was not published, nothing changed"):
        install(ws, raw_package(tmp_path / "package"))
    assert events.renames(store_rename(ws)) == []
    assert _store.list_installed(ws) == [] and list((ws.control / "tmp").iterdir()) == []


def test_a_job_new_install_takes_its_own_barrier_before_the_job_s(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    package = raw_package(tmp_path / "package")
    events = Events(monkeypatch)
    new_job(ws, package, install=True)
    tree, staging = events.barriers
    assert tree.anchor.name == "tree" and staging.anchor == ws.control / "tmp"
    (installed,) = events.renames(store_rename(ws))
    (renamed,) = events.renames(job_rename)
    syncfs = events.indices("syncfs")
    assert len(syncfs) == 4 and syncfs[1] < installed < events.indices("entering")[1] < syncfs[3] < renamed


def test_install_copy_and_build_commit_before_their_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = Workspace.initialize(tmp_path / "source", durable=False, policy={"visibility_deadline_seconds": 0.05})
    entry = install(source, _tool_package(tmp_path / "tool"), build=False)
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    events = Events(monkeypatch)
    with cli_owner(ws) as owner:
        status, copied = _store.install_copy(ws, owner, entry.directory, source_label="test")
        assert status == "installed"
        _store.build(ws, owner, copied.id)
    tree, builds = events.barriers
    assert tree.anchor.name == "tree" and builds.anchor.name == "builds" and ".build." in builds.anchor.parent.name
    installed, built = events.renames(store_rename(ws))
    syncfs = events.indices("syncfs")
    assert len(syncfs) == 4
    copies = events.indices("copy")
    assert_syncfs_barrier(events, 0, copies[0], copies[3], installed)
    assert_syncfs_barrier(events, 1, copies[4], copies[4], built)
    assert _store.built(_store.lookup(ws, copied.id)) is True  # type: ignore[arg-type]


def test_a_failed_install_copy_commit_installs_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = Workspace.initialize(tmp_path / "source", durable=False, policy={"visibility_deadline_seconds": 0.05})
    entry = install(source, raw_package(tmp_path / "package"))
    ws = durable_workspace(tmp_path / "ws", "syncfs")
    Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    with cli_owner(ws) as owner, pytest.raises(_fs.SyncFailed, match="the copy of workflow local:tests.raw was not"):
        _store.install_copy(ws, owner, entry.directory, source_label="test")
    assert _store.list_installed(ws) == [] and list((ws.control / "tmp").iterdir()) == []


def test_workflow_install_stdin_tar_syncs_the_store_copy_not_its_landing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Workspace.initialize(tmp_path / "source", durable=False, policy={"visibility_deadline_seconds": 0.05})
    entry = install(source, raw_package(tmp_path / "package"))
    target, target_name = registered(tmp_path, "target")
    target.configure_settings({"io.sync": "syncfs"})
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name in ("install.json", "package"):
            archive.add(entry.directory / name, arcname=f"{entry.directory.name}/{name}")
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data.getvalue())))
    events = Events(monkeypatch)
    argv = ["install", "--json", "--stdin-tar", "--workspace", target_name]
    assert command(argv, CLIContext("httk", tmp_path)) == 0
    assert [row["status"] for row in json.loads(capsys.readouterr().out)["workflows"]] == ["installed"]
    # One barrier: the store copy's, none on the landing the stream was extracted into.
    ((barrier,),) = [events.barriers]
    assert barrier.anchor.name == "tree" and ".build." in barrier.anchor.parent.name
    (renamed,) = events.renames(store_rename(target))
    copies = events.indices("copy")
    assert_syncfs_barrier(events, 0, copies[0], copies[-1], renamed)


def test_a_store_copy_that_cannot_be_synced_is_failed_not_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Workspace.initialize(tmp_path / "source", durable=False, policy={"visibility_deadline_seconds": 0.05})
    entry = install(source, raw_package(tmp_path / "package"))
    target, target_name = registered(tmp_path, "target")
    target.configure_settings({"io.sync": "syncfs"})
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name in ("install.json", "package"):
            archive.add(entry.directory / name, arcname=f"{entry.directory.name}/{name}")
    monkeypatch.setattr(sys, "stdin", io.TextIOWrapper(io.BytesIO(data.getvalue())))
    Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    argv = ["install", "--json", "--stdin-tar", "--workspace", target_name]
    assert command(argv, CLIContext("httk", tmp_path)) == 1
    (row,) = json.loads(capsys.readouterr().out)["workflows"]
    assert row["status"] == "failed" and "was not published, nothing changed" in row["reason"]
    assert _store.list_installed(target) == []


# -- job adopt PATH -----------------------------------------------------------------------------------------------


def ejected(tmp_path: Path, capsys: pytest.CaptureFixture[str], sync: str) -> tuple[Workspace, str, Path, Any]:
    """A job ejected to ``out/`` (an operator's bundle on the target's filesystem), and the target forced to *sync*."""

    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": sync})
    plan = [single(source, "succeeded", 10)]
    argv = ["job", "eject", "--workspace", source_name, str(plan[0][0]["id"]), str(tmp_path / "out"), "--json"]
    assert command(argv, CLIContext("httk", tmp_path)) == 0
    bundle = Path(json.loads(capsys.readouterr().out)["destination"])
    return target, target_name, bundle, plan


def adopt(tmp_path: Path, target_name: str, bundle: Path) -> int:
    return command(["job", "adopt", "--json", "--workspace", target_name, str(bundle)], CLIContext("httk", tmp_path))


def test_adopt_path_syncs_the_bundle_without_a_prime_before_the_take(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    events = Events(monkeypatch)
    assert adopt(tmp_path, target_name, bundle) == 0
    assert [entry["status"] for entry in json.loads(capsys.readouterr().out)["bundles"]] == ["adopted"]
    ((barrier,),) = [events.barriers]
    assert barrier.anchor == target.control and not barrier.prime and barrier.strategy == "syncfs"
    # Exactly one syncfs, on the descriptor opened on the control directory, before the take.
    assert events.details("syncfs") == events.details("enter")
    assert events.first("enter") < events.first("syncfs") < events.first("take")
    assert_in(target, plan)


def test_adopt_path_syncs_a_batch_once_and_a_failure_fails_every_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.sync": "syncfs"})
    plan = [single(source, "succeeded", 10 + index) for index in range(4)]
    bundles = []
    for mapping, _, _ in plan:
        argv = ["job", "eject", "--workspace", source_name, str(mapping["id"]), str(tmp_path / "out"), "--json"]
        assert command(argv, CLIContext("httk", tmp_path)) == 0
        bundles.append(json.loads(capsys.readouterr().out)["destination"])
    events = Events(monkeypatch, syncfs_failures=[errno.EIO])
    argv = ["job", "adopt", "--json", "--workspace", target_name, *bundles[:2]]
    assert command(argv, CLIContext("httk", tmp_path)) == 1
    entries = json.loads(capsys.readouterr().out)["bundles"]
    assert [entry["status"] for entry in entries] == ["failed", "failed"]
    assert all("re-create the bundle from its source" in entry["message"] for entry in entries)
    assert "take" not in events.kinds() and all(Path(bundle).is_dir() for bundle in bundles[:2])
    events = Events(monkeypatch)
    argv = ["job", "adopt", "--json", "--workspace", target_name, *bundles[2:]]
    assert command(argv, CLIContext("httk", tmp_path)) == 0
    assert [entry["status"] for entry in json.loads(capsys.readouterr().out)["bundles"]] == ["adopted", "adopted"]
    # One syncfs for the batch, before the first take.
    assert events.kinds().count("syncfs") == 1 and events.first("syncfs") < events.first("take")
    assert_in(target, plan[2:])


def test_adopt_path_under_fsync_walks_the_bundle_before_the_take(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "fsync")
    files = [str(path) for path in bundle.rglob("*") if path.is_file() and not path.is_symlink()]
    events = Events(monkeypatch)
    assert adopt(tmp_path, target_name, bundle) == 0
    assert "syncfs" not in events.kinds()
    assert events.details("walk-start") == [str(bundle)] and events.first("walk") < events.first("take")
    take = events.first("take")
    synced = {path for index, (kind, path) in enumerate(events.events) if kind == "fsync" and index < take}
    assert files and set(files) <= synced and str(bundle) in synced
    assert_in(target, plan)


def test_adopt_path_whose_sync_fails_leaves_the_bundle_in_place(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    events = Events(monkeypatch, syncfs_failures=[errno.EIO])
    assert adopt(tmp_path, target_name, bundle) == 1
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    # A retry would find the error seen and the failed pages clean, so it could publish lost data: the bundle
    # is to be re-created from its source, never merely adopted again.
    assert entry["status"] == "failed" and "may not be intact on disk" in entry["message"]
    assert "re-create the bundle from its source" in entry["message"]
    assert "take" not in events.kinds() and (bundle / "bundle.json").is_file()
    assert all(find(target, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_lookalike_transfer_path_is_synced_as_an_operator_s_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    # A path that looks like another workspace's hold, with no such workspace: still an operator's bundle.
    lookalike = tmp_path / "other" / ".httk-workspace" / "transfers" / "outgoing" / bundle.name
    lookalike.parent.mkdir(parents=True)
    bundle.rename(lookalike)
    events = Events(monkeypatch)
    assert adopt(tmp_path, target_name, lookalike) == 0
    assert events.kinds().count("syncfs") == 1 and events.first("syncfs") < events.first("take")
    assert_in(target, plan)


@pytest.mark.parametrize("sync", ["syncfs", "fsync", "failing"])
def test_an_anchored_trusted_adoption_is_synced_before_its_take(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], sync: str
) -> None:
    target, _, bundle, plan = ejected(tmp_path, capsys, "syncfs" if sync == "failing" else sync)
    durable = Workspace(target.root)
    events = Events(monkeypatch, syncfs_failures=[errno.EIO] if sync == "failing" else [])
    directory = _fs.open_dir(bundle.parent)
    try:
        with cli_owner(durable) as owner:
            source = _fs.anchored(directory, bundle.name)
            if sync == "failing":
                with pytest.raises(_fs.SyncFailed, match="re-create the bundle from its source"):
                    _moving.adopt(durable, owner, source, untrusted=False)
            else:
                assert _moving.adopt(durable, owner, source, untrusted=False) is not None
    finally:
        os.close(directory)
    if sync == "failing":
        assert "take" not in events.kinds() and (bundle / "bundle.json").is_file()
        return
    if sync == "syncfs":
        assert events.kinds().count("syncfs") == 1 and events.first("syncfs") < events.first("take")
    else:
        # The anchored walk of the bundle, from its directory's descriptor.
        assert events.details("walk-start") == [str(bundle)] and events.last("walk") < events.first("take")
    assert_in(target, plan)


def test_a_bundle_the_batch_sync_cannot_reach_fails_alone_and_no_owner_is_left(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    afile = tmp_path / "afile"
    afile.write_text("not a directory\n", encoding="utf-8")
    events = Events(monkeypatch)
    argv = ["job", "adopt", "--json", "--workspace", target_name, str(bundle), str(afile / "sub")]
    assert command(argv, CLIContext("httk", tmp_path)) == 1
    entries = json.loads(capsys.readouterr().out)["bundles"]
    # The unreachable path is skipped by the sync and reported by its own take, as before (nothing to take).
    assert [entry["status"] for entry in entries] == ["adopted", "taken"]
    assert events.kinds().count("syncfs") == 1
    assert_in(target, plan)
    assert_nothing_left(target)


def test_an_error_of_the_batch_sync_fails_the_batch_and_leaves_no_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, _ = ejected(tmp_path, capsys, "syncfs")

    def broken(*_arguments: Any, **_options: Any) -> None:
        raise PermissionError(errno.EACCES, "Permission denied", str(target.control))

    monkeypatch.setattr(_fs, "sync_written", broken)
    assert adopt(tmp_path, target_name, bundle) == 1
    (entry,) = json.loads(capsys.readouterr().out)["bundles"]
    assert entry["status"] == "failed" and "could not be synced to disk" in entry["message"]
    assert bundle.is_dir()
    assert_nothing_left(target)


def test_an_entry_the_walk_cannot_read_is_named_without_claiming_lost_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    if os.geteuid() == 0:
        pytest.skip("root reads every file")
    target, target_name, bundle, _ = ejected(tmp_path, capsys, "fsync")
    hidden = bundle / "hidden"
    hidden.write_text("x", encoding="utf-8")
    hidden.chmod(0)
    try:
        assert adopt(tmp_path, target_name, bundle) == 1
        (entry,) = json.loads(capsys.readouterr().out)["bundles"]
        assert entry["status"] == "failed" and "cannot read" in entry["message"] and "hidden" in entry["message"]
        assert "re-create" not in entry["message"] and "may not be intact" not in entry["message"]
    finally:
        hidden.chmod(0o644)
    assert_nothing_left(target)


def test_an_unsynced_library_adoption_syncs_and_a_transfer_s_does_not(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, _, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    durable = Workspace(target.root)
    copy = tmp_path / "copy" / bundle.name
    copy.parent.mkdir()
    shutil.copytree(bundle, copy, symlinks=True)
    events = Events(monkeypatch)
    with cli_owner(durable) as owner:
        assert _moving.adopt(durable, owner, bundle, untrusted=False, synced=True) is not None
    assert "syncfs" not in events.kinds()
    with cli_owner(durable) as owner, pytest.raises(BundleError, match="already adopted"):
        _moving.adopt(durable, owner, copy, untrusted=False)
    assert events.kinds().count("syncfs") == 1 and events.first("syncfs") < events.indices("take")[1]
    assert_in(target, plan)


@pytest.mark.skipif(not Path("/dev/shm").is_dir(), reason="needs a second filesystem")
def test_adopt_path_from_another_filesystem_takes_no_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    target, target_name, bundle, plan = ejected(tmp_path, capsys, "syncfs")
    other = Path(tempfile.mkdtemp(dir="/dev/shm", prefix="httk-s2-"))
    try:
        if os.stat(other).st_dev == os.stat(target.control).st_dev:
            pytest.skip("/dev/shm is on the workspace's filesystem")
        moved = other / bundle.name
        shutil.copytree(bundle, moved, symlinks=True)
        events = Events(monkeypatch)
        assert adopt(tmp_path, target_name, moved) == 0
        # No sync of the other filesystem: the adoption's copy is fsynced as it is written.
        assert "syncfs" not in events.kinds() and "walk" not in events.kinds()
        assert [durable for _, durable in events.details("copy-tree")] == [True]
        assert_in(target, plan)
    finally:
        shutil.rmtree(other, ignore_errors=True)


# -- the pieces: a barrier with no prime, and the single-file copy ---------------------------------------------------


def test_a_barrier_without_a_prime_calls_syncfs_once_and_never_attributes_its_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "sub" / "file").write_text("x", encoding="utf-8")
    events = Events(monkeypatch, syncfs_failures=[None, errno.EIO])
    with _fs.Barrier(tree, "syncfs", quiet=True, prime=False) as barrier:
        assert "syncfs" not in events.kinds()
        barrier.commit()
    with _fs.Barrier(tree, "syncfs", quiet=True, prime=False) as barrier, pytest.raises(_fs.SyncFailed):
        barrier.commit()
    assert events.kinds().count("syncfs") == 2 and "walk" not in events.kinds()


def test_a_barrier_without_a_prime_walks_its_tree_where_syncfs_is_not_supported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tree = tmp_path / "tree"
    (tree / "sub").mkdir(parents=True)
    (tree / "sub" / "file").write_text("x", encoding="utf-8")
    events = Events(monkeypatch, syncfs_failures=[errno.EPERM])
    with _fs.Barrier(tree, "syncfs", quiet=True, prime=False, written=True) as barrier:
        barrier.commit()
        assert barrier.strategy == "fsync"
    assert events.details("walk") == [1] and (str(tree / "sub" / "file"), False) in events.fsynced
    # Recorded for the filesystem: the next barrier there resolves to fsync without calling syncfs.
    assert _fs._syncfs_works[os.stat(tree).st_dev] is False
    with _fs.Barrier(tree, "syncfs", quiet=True, prime=False, written=True) as barrier:
        assert barrier.strategy == "fsync"
    assert events.kinds().count("syncfs") == 1


def test_copy_file_copies_as_shutil_and_fsyncs_on_its_writing_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"data" * 1000)
    source.chmod(0o751)
    os.utime(source, ns=(1_000_000_000, 2_000_000_000))
    link = tmp_path / "link"
    link.symlink_to(source)
    events = Events(monkeypatch)
    _fs.copy_file(link, tmp_path / "plain", durable=False)
    _fs.copy_file(source, tmp_path / "kept", durable=True, metadata=True)
    assert (tmp_path / "plain").read_bytes() == (tmp_path / "kept").read_bytes() == source.read_bytes()
    # copyfile keeps no mode: a new file (0o666 less the umask) is never executable.
    assert (tmp_path / "plain").stat().st_mode & 0o111 == 0
    kept = (tmp_path / "kept").stat()
    assert kept.st_mode & 0o777 == 0o751 and kept.st_mtime_ns == 2_000_000_000
    assert events.fsynced == [(str(tmp_path / "kept"), True)]
    # An existing file is truncated, as shutil.copyfile does; a symlink there is never written through.
    (tmp_path / "plain").write_bytes(b"longer than the source" * 1000)
    _fs.copy_file(source, tmp_path / "plain", durable=False)
    assert (tmp_path / "plain").read_bytes() == source.read_bytes()
    (tmp_path / "trap").symlink_to(tmp_path / "victim")
    with pytest.raises(OSError) as raised:
        _fs.copy_file(source, tmp_path / "trap", durable=False)
    assert raised.value.errno == errno.ELOOP and not (tmp_path / "victim").exists()
    os.mkfifo(tmp_path / "fifo")
    with pytest.raises(_fs.UnsafePath):
        _fs.copy_file(tmp_path / "fifo", tmp_path / "from-fifo", durable=False)


def test_a_commit_under_fsync_walks_only_its_tree(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    anchor = tmp_path / "anchor"
    for name in ("one", "two"):
        (anchor / name).mkdir(parents=True)
        (anchor / name / "file").write_text(name, encoding="utf-8")
    events = Events(monkeypatch)
    with _fs.Barrier(anchor, "fsync", quiet=True) as barrier:
        barrier.commit(tree=anchor / "one")
        assert events.details("walk-start") == [str(anchor / "one")]
        barrier.commit()
    synced = {path for path, _ in events.fsynced}
    assert str(anchor / "one" / "file") in synced and events.details("walk") == [1, 2]
    assert events.details("walk-start") == [str(anchor / "one"), str(anchor)]
    with (
        _fs.Barrier(anchor, "fsync", quiet=True) as barrier,
        pytest.raises(_fs.SyncUnreadable, match="cannot read .*missing"),
    ):
        barrier.commit(tree=anchor / "missing")


def test_copy_file_refuses_to_copy_a_file_onto_itself(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"precious")
    alias = tmp_path / "alias"
    os.link(source, alias)
    for target in (source, alias):
        with pytest.raises(shutil.SameFileError):
            _fs.copy_file(source, target, durable=True, metadata=True)
    assert source.read_bytes() == b"precious"


def test_copy_file_keeps_extended_attributes_with_its_metadata(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"data")
    try:
        os.setxattr(source, "user.httk-test", b"kept")
    except (AttributeError, OSError):
        pytest.skip("this filesystem takes no user extended attributes")
    _fs.copy_file(source, tmp_path / "with", durable=True, metadata=True)
    _fs.copy_file(source, tmp_path / "without", durable=True)
    assert os.getxattr(tmp_path / "with", "user.httk-test") == b"kept"
    assert "user.httk-test" not in os.listxattr(tmp_path / "without")


def test_copy_file_never_writes_into_a_special_destination(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.write_bytes(b"data")
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    # A FIFO with no reader is refused at once rather than blocking the copy.
    with pytest.raises(OSError) as raised:
        _fs.copy_file(source, fifo, durable=False)
    assert raised.value.errno == errno.ENXIO
    if Path("/dev/null").exists():
        with pytest.raises(shutil.SpecialFileError):
            _fs.copy_file(source, "/dev/null", durable=False)
