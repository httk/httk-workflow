"""``job new --input-move-from-job``: a file moved (not copied) out of a succeeded job, and collecting both jobs."""

import errno
import hashlib
import json
import logging
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from httk.core import FileRecord
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _fs, _store, collect
from httk.workflow import scaffold as scaffold_module
from httk.workflow._job import JobDefinition
from httk.workflow.scaffold import new_job
from httk.workflow.seals import (
    is_job_sealed,
    job_releases_path,
    job_seal_path,
    read_job_releases,
    read_seal,
    record_job_release,
    seal_job,
    verify_job_seal,
)
from httk.workflow.workflow_cli import command
from test_collect_inputs import _stored
from test_workflow_packages import _package
from v3_helpers import RUNNER, cli_owner, find, run, workspace

_WORKFLOW = "tests.moves"
_MANIFEST = f'''
[workflow]
name = "{_WORKFLOW}"
description = "Moves a file between jobs."
declaration_uri = "https://example.test/workflows/moves"

[workflow.runner]
steps = ["start"]

[workflow.collect]
file = "collect"

[workflow.inputs.trajectory]
destination = "trajectory.dat"
entry_type = "files"
role = "trajectory"
required = false

[workflow.outputs.frames]
entry_type = "files"
role = "frames"
'''
_FRAMES = b"frame 1\nframe 2\n"
_SHA256 = hashlib.sha256(_FRAMES).hexdigest()


@pytest.fixture(autouse=True)
def _unregister() -> Any:
    yield
    scaffold_module._WORKFLOW_PROVIDERS.pop(_WORKFLOW, None)


def _workspace(tmp_path: Path) -> Workspace:
    """Install the package: its runner writes ``traj.dat`` in the origin's workdir, its collector names it."""

    source = Path(__file__).parents[1] / "src"
    hook = (
        "#!/usr/bin/env python3\n"
        f"import sys; sys.path.insert(0, {str(source)!r})\n"
        "from httk.workflow.hookapi import collect_main\n"
        "collect_main(lambda record: {'frames': {'file': 'traj.dat'}} if record['job_key'].startswith('origin') "
        "else {})\n"
    )
    package = _package(tmp_path / "package", _MANIFEST)
    (package / "run").write_text(RUNNER, encoding="utf-8")
    (package / "run").chmod(0o755)
    (package / "collect").write_text(hook, encoding="utf-8")
    (package / "collect").chmod(0o755)
    ws = workspace(tmp_path / "workspace")
    with cli_owner(ws) as owner:
        _store.install(ws, owner, package)
    return ws


def _origin(
    ws: Workspace, *, tag: str = "origin", sealed: bool = True, script: str = "succeed", extra: str | None = None
) -> Path:
    """Run one job writing ``run/traj.dat`` (and ``run/<extra>``) to *script*'s end and return its payload."""

    written = {"traj.dat": _FRAMES.decode(), **({extra: "other\n"} if extra else {})}
    job = new_job(ws, _WORKFLOW, parameters={"script": {"start": script}, "files": {"start": written}}, tag=tag)
    run(ws)
    ref = find(ws, job.job_id)
    if sealed and not is_job_sealed(ref.path):
        assert seal_job(ws, ref) is None
    return find(ws, job.job_id).path


def _no_hashing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail any hashing of a payload file during job creation."""

    def refuse(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the moved file was hashed")

    monkeypatch.setattr(scaffold_module, "sha256_file", refuse)
    monkeypatch.setattr(_fs, "digest_regular", refuse)


def test_a_sealed_file_moves_into_the_new_job_without_hashing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    source = origin / "run" / "traj.dat"
    inode = source.stat().st_ino
    _no_hashing(monkeypatch)

    job = new_job(ws, _WORKFLOW, parameters={"script": {"start": "succeed"}}, move_inputs={"trajectory": source})

    moved = job.payload / "files" / "trajectory.dat"
    assert not source.exists() and moved.read_bytes() == _FRAMES and moved.stat().st_ino == inode
    staged = JobDefinition.from_path(job.payload / "job.json").as_mapping()["staged_files"]
    assert staged == [{"path": "files/trajectory.dat", "size": len(_FRAMES), "sha256": _SHA256}]
    (release,) = read_job_releases(origin)
    assert (release.path, release.to_job_key, release.to_path) == ("run/traj.dat", job.job_key, "files/trajectory.dat")
    assert verify_job_seal(origin).discrepancies == ()
    # No submit scratch is left behind.
    assert list((ws.control / "tmp").iterdir()) == []


def test_a_file_never_moves_out_of_an_unlocked_job(tmp_path: Path) -> None:
    from httk.workflow import removal

    ws = _workspace(tmp_path)
    job_id = JobDefinition.from_path(_origin(ws) / "job.json").id
    with cli_owner(ws) as owner:
        for action in ("unseal", "unlock"):
            assert removal.request_now(ws, owner, find(ws, job_id), action, "t") is None
    source = find(ws, job_id).path / "run" / "traj.dat"
    with pytest.raises(ValueError, match="the source job is unlocked; copy the file with --input-from instead"):
        new_job(ws, _WORKFLOW, parameters={"script": {"start": "succeed"}}, move_inputs={"trajectory": source})
    assert source.read_bytes() == _FRAMES


def test_the_cli_moves_a_file_and_refuses_it_with_input_for_the_same_name(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ws = _workspace(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "moves")
    source = _origin(ws) / "run" / "traj.dat"
    argv = ["job", "new", "--workspace", name, "--workflow", _WORKFLOW, "--parameter", 'script={"start":"succeed"}']

    refused = [*argv, "--input", f"trajectory={tmp_path}", "--input-move-from-job", f"trajectory={source}"]
    assert command(refused, context) != 0
    assert "both supplied and moved" in capsys.readouterr().err
    assert source.exists()
    assert command([*argv, "--tag", "moved", "--input-move-from-job", f"trajectory={source}"], context) == 0

    assert not source.exists()
    assert (next((ws.root / "jobs").rglob("moved--*")) / "files" / "trajectory.dat").read_bytes() == _FRAMES


def test_an_unsealed_origin_is_hashed_with_a_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.scaffold")
    ws = _workspace(tmp_path)
    origin = _origin(ws, sealed=False)
    if is_job_sealed(origin):
        (origin / ".httk-job" / "seal.json").unlink()

    job = new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": origin / "run" / "traj.dat"},
    )

    assert "is hashed" in caplog.text
    staged = JobDefinition.from_path(job.payload / "job.json").as_mapping()["staged_files"]
    assert staged == [{"path": "files/trajectory.dat", "size": len(_FRAMES), "sha256": _SHA256}]
    assert not job_releases_path(origin).exists()


def _refused(ws: Workspace, source: Path, match: str) -> None:
    jobs = len(list((ws.root / "jobs").rglob("job.json")))
    with pytest.raises(ValueError, match=match):
        new_job(ws, _WORKFLOW, parameters={"script": {"start": "succeed"}}, move_inputs={"trajectory": source})
    assert len(list((ws.root / "jobs").rglob("job.json"))) == jobs


def test_moves_are_refused_outside_succeeded_jobs_and_for_reserved_members_and_symlinks(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    outside = tmp_path / "outside.dat"
    outside.write_bytes(_FRAMES)
    (origin / "run" / "link.dat").symlink_to(origin / "run" / "traj.dat")
    (origin / "workflow").mkdir()
    (origin / "workflow" / "run").write_bytes(_FRAMES)

    _refused(ws, outside, "not a regular file inside a succeeded job")
    _refused(ws, origin / "job.json", "reserved member")
    _refused(ws, origin / ".httk-job" / "seal.json", "reserved member")
    _refused(ws, origin / "workflow" / "run", "reserved member")
    _refused(ws, origin / "run" / "link.dat", "not a regular file inside a succeeded job")
    _refused(ws, origin / "run", "not a regular file inside a succeeded job")
    assert (origin / "run" / "traj.dat").read_bytes() == _FRAMES and not job_releases_path(origin).exists()


def test_a_file_of_a_failed_job_is_refused_like_one_outside(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    failed = _origin(ws, tag="failing", sealed=False, script="fail")
    assert "failed" in failed.parts

    _refused(ws, failed / "run" / "traj.dat", "not a regular file inside a succeeded job")


def test_a_cross_filesystem_move_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    real_rename = os.rename

    def exdev(src: Any, dst: Any, **kwargs: Any) -> None:
        if str(src) == "traj.dat":
            raise OSError(errno.EXDEV, "Invalid cross-device link")
        real_rename(src, dst, **kwargs)

    monkeypatch.setattr(os, "rename", exdev)

    _refused(ws, origin / "run" / "traj.dat", "another filesystem")
    assert (origin / "run" / "traj.dat").read_bytes() == _FRAMES
    assert read_job_releases(origin) == ()
    assert verify_job_seal(origin).discrepancies == ()


def test_a_different_device_is_refused_before_anything_moves(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    real_stat = os.stat

    def other_device(path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        result = real_stat(path, *args, **kwargs)
        if str(path) == "traj.dat":
            values = list(result)
            values[2] += 1  # st_dev
            return os.stat_result(values)
        return result

    monkeypatch.setattr(os, "stat", other_device)

    _refused(ws, origin / "run" / "traj.dat", "another filesystem")
    assert (origin / "run" / "traj.dat").exists() and not job_releases_path(origin).exists()


def test_a_failure_after_the_move_puts_the_file_back_and_withdraws_the_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    real_submit = scaffold_module._submit_staged

    def failing(*args: Any) -> Any:
        assert not (origin / "run" / "traj.dat").exists() and len(read_job_releases(origin)) == 1
        raise RuntimeError("submission failed")

    monkeypatch.setattr(scaffold_module, "_submit_staged", failing)

    with pytest.raises(RuntimeError, match="submission failed"):
        new_job(
            ws,
            _WORKFLOW,
            parameters={"script": {"start": "succeed"}},
            move_inputs={"trajectory": origin / "run" / "traj.dat"},
        )

    assert (origin / "run" / "traj.dat").read_bytes() == _FRAMES
    assert read_job_releases(origin) == ()
    assert verify_job_seal(origin).discrepancies == ()
    assert list((ws.control / "tmp").iterdir()) == []
    monkeypatch.setattr(scaffold_module, "_submit_staged", real_submit)


def test_a_build_holding_a_moved_file_that_cannot_go_back_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.scaffold")
    ws = _workspace(tmp_path)
    origin = _origin(ws)

    def failing(*args: Any) -> Any:
        raise RuntimeError("submission failed")

    def stuck(*args: Any) -> bool:
        return False

    monkeypatch.setattr(scaffold_module, "_submit_staged", failing)
    monkeypatch.setattr(scaffold_module, "_undo_moves", stuck)

    with pytest.raises(RuntimeError):
        new_job(
            ws,
            _WORKFLOW,
            parameters={"script": {"start": "succeed"}},
            move_inputs={"trajectory": origin / "run" / "traj.dat"},
        )

    (kept,) = (ws.control / "tmp").iterdir()
    assert (kept / "build" / "files" / "trajectory.dat").read_bytes() == _FRAMES
    assert "holds files moved out of other jobs" in caplog.text


# -- collecting and storing --------------------------------------------------


def _moved(ws: Workspace, origin: Path) -> str:
    """Move the origin's ``run/traj.dat`` into a new ``receiver`` job and return its job id."""

    job = new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": origin / "run" / "traj.dat"},
        tag="receiver",
    )
    return job.job_id


def _collected(ws: Workspace, prefix: str) -> Any:
    return next(item for item in collect(ws, allow_job_collector=True) if item.record.job_key.startswith(prefix))


def test_collecting_a_released_output_records_its_new_location(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    _moved(ws, origin)
    run(ws)
    caplog.set_level(logging.INFO, logger="httk.workflow.collecting")

    item = _collected(ws, "origin")
    receiver = _collected(ws, "receiver")

    frames = item.outputs["frames"]
    assert isinstance(frames, FileRecord) and item.missing_collector is None
    new_url = f"data/workspace/{receiver.record.payload_path.as_posix()}/files/trajectory.dat"
    assert (frames.url, frames.sha256, frames.size) == (new_url, _SHA256, len(_FRAMES))
    assert any("was moved to job receiver--" in record.getMessage() for record in caplog.records)
    (edge,) = [edge for edge in item.run.outputs if edge.label == "frames"]
    assert edge.entry_type == "files"
    old_url = f"data/workspace/{item.record.payload_path.as_posix()}/run/traj.dat"
    assert item.relocations == ((old_url, new_url, _SHA256),)
    staged = receiver.inputs["trajectory"]
    assert isinstance(staged, FileRecord) and (staged.url, staged.sha256) == (new_url, _SHA256)


def test_a_released_staged_input_is_recorded_at_its_new_location(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ws = _workspace(tmp_path)
    first = _moved(ws, _origin(ws))
    run(ws)
    if not is_job_sealed(find(ws, first).path):
        assert seal_job(ws, find(ws, first)) is None
    middle = find(ws, first).path
    # The receiver's own staged input moves on to a third job.
    third = new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": middle / "files" / "trajectory.dat"},
        tag="third",
    )
    caplog.set_level(logging.INFO, logger="httk.workflow.collecting")

    item = _collected(ws, "receiver")

    staged = item.inputs["trajectory"]
    assert isinstance(staged, FileRecord)
    # The third job is queued: its location still changes, so none is recorded yet.
    assert (staged.url, staged.sha256) == (None, _SHA256)
    assert "which has not succeeded" in caplog.text and "recorded by its staged digest only" not in caplog.text
    run(ws)
    staged = _collected(ws, "receiver").inputs["trajectory"]
    third_path = find(ws, third.job_id).path.relative_to(ws.root).as_posix()
    assert staged.url == f"data/workspace/{third_path}/files/trajectory.dat"
    assert "was moved to job third--" in caplog.text


def test_a_stored_file_is_repointed_when_collected_again_after_the_move(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("httk.store")
    from httk.core import Run
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    ws = _workspace(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "moves")
    origin = _origin(ws)
    store_path = tmp_path / "results.sqlite"
    argv = ["collect", "--workspace", name, "--allow-job-collector", "--into", str(store_path), "--id-base", "httk.t"]

    def stored() -> list[dict[str, Any]]:
        capsys.readouterr()
        assert command(argv, context) == 0
        *reports, _summary = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert all(report["stored"] for report in reports), reports
        return reports

    def rows() -> list[tuple[str, str | None]]:
        with closing(sqlite3.connect(store_path)) as connection:
            return connection.execute("SELECT id, url FROM core_file").fetchall()

    stored()
    ((file_id, url),) = rows()
    assert url is not None and url.endswith("/run/traj.dat")

    # Collected again while the receiver is queued: the row loses its old location, and gets no transient one.
    receiver = _moved(ws, origin)
    stored()
    assert rows() == [(file_id, None)]

    # The receiver's own collect fills the location in; the origin's relocation then finds it settled.
    run(ws)
    reports = stored()

    receiver_path = find(ws, receiver).path.relative_to(ws.root).as_posix()
    new_url = f"data/{name}/{receiver_path}/files/trajectory.dat"
    assert rows() == [(file_id, new_url)]
    with Backend.sqlite(store_path) as database:
        store = SqlStore(database)
        runs = {report["job_key"].split("--")[0]: _stored(store, Run, report["stored"]["run"]) for report in reports}
    assert [edge.entry_id for edge in runs["origin"].outputs if edge.label == "frames"] == [file_id]
    assert [edge.entry_id for edge in runs["receiver"].inputs if edge.label == "trajectory"] == [file_id]


def test_a_hook_that_retags_the_job_leaves_one_release_naming_the_final_key(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    package = tmp_path / "retagging"
    _package(
        package,
        _MANIFEST.replace('name = "tests.moves"', 'name = "tests.retag"').replace(
            "[workflow.collect]", '[workflow.instantiate]\nfile = "instantiate.py"\n\n[workflow.collect]'
        ),
    )
    (package / "instantiate.py").write_text(
        "def instantiate(context):\n    context.tag = 'retagged'\n", encoding="utf-8"
    )
    (package / "collect").write_text("#!/bin/sh\n", encoding="utf-8")
    (package / "collect").chmod(0o755)
    with cli_owner(ws) as owner:
        _store.install(ws, owner, package)
    try:
        job = new_job(ws, "tests.retag", move_inputs={"trajectory": origin / "run" / "traj.dat"})
    finally:
        scaffold_module._WORKFLOW_PROVIDERS.pop("tests.retag", None)

    assert job.job_key.startswith("retagged--")
    (release,) = read_job_releases(origin)
    assert release.to_job_key == job.job_key
    assert verify_job_seal(origin).discrepancies == ()


def test_a_seal_its_manager_did_not_write_vouches_for_nothing(tmp_path: Path) -> None:
    from httk.workflow.seals import seal_payload

    ws = _workspace(tmp_path)
    origin = _origin(ws)
    job = JobDefinition.from_path(origin / "job.json")
    # A rewritten seal (as a job could plant one) no longer matches the digest state.json records.
    (origin / "planted").write_text("planted\n", encoding="utf-8")
    seal_payload(origin, job_id=job.id, job_key=job.job_key, keys=(), durable=False)
    (origin / "planted").unlink()

    _refused(ws, origin / "run" / "traj.dat", "not the one its manager wrote")
    assert (origin / "run" / "traj.dat").exists() and not job_releases_path(origin).exists()


def test_origin_collected_while_the_receiver_is_queued_records_no_location(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    _moved(ws, origin)

    item = _collected(ws, "origin")

    frames = item.outputs["frames"]
    assert isinstance(frames, FileRecord) and (frames.url, frames.sha256) == (None, _SHA256)
    old_url = f"data/workspace/{item.record.payload_path.as_posix()}/run/traj.dat"
    assert item.relocations == ((old_url, None, _SHA256),)


def _claim(ws: Workspace, origin: Path, job_key: str) -> None:
    """Record the release of ``run/traj.dat`` to *job_key* without moving it."""

    (sealed,) = [item for item in read_seal(job_seal_path(origin)).records if item["path"] == "run/traj.dat"]
    record_job_release(
        ws,
        origin,
        path="run/traj.dat",
        size=int(sealed["size"]),  # type: ignore[call-overload]
        sha256=str(sealed["sha256"]),
        released_to={"job_key": job_key, "path": "files/x"},
        keys=(),
    )


def test_a_file_released_to_an_existing_job_is_refused_naming_the_record(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    other = new_job(ws, _WORKFLOW, parameters={"script": {"start": "succeed"}}, tag="other")
    _claim(ws, origin, other.job_key)

    _refused(
        ws,
        origin / "run" / "traj.dat",
        r"already released, or being moved, to job other--.*releases/[0-9a-f]{64}\.json",
    )

    (release,) = read_job_releases(origin)
    assert release.to_job_key == other.job_key and (origin / "run" / "traj.dat").exists()


def test_a_stale_record_of_a_crashed_move_is_replaced(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.scaffold")
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    # A move that crashed between its record and its rename: the file is here, the job never existed.
    _claim(ws, origin, "crashed--00000000-0000-4000-8000-000000000000")

    job = new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": origin / "run" / "traj.dat"},
    )

    (release,) = read_job_releases(origin)
    assert release.to_job_key == job.job_key
    assert verify_job_seal(origin).discrepancies == ()
    assert "replacing the stale release record" in caplog.text


def test_a_record_of_a_move_in_flight_is_not_stale(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    real = scaffold_module._claim_release

    def contested(workspace: Any, ref: Any, member: Any, source: Any, seal: Any, marker: Path, **kwargs: Any) -> Any:
        # Another job new holds the same file: its marker is in its own scratch and its record is written.
        (workspace.control / "tmp" / "other.submit.x").mkdir()
        (workspace.control / "tmp" / "other.submit.x" / marker.name).write_text("{}\n", encoding="utf-8")
        _claim(ws, origin, "inflight--00000000-0000-4000-8000-000000000000")
        return real(workspace, ref, member, source, seal, marker, **kwargs)

    scaffold_module._claim_release = contested
    try:
        _refused(ws, origin / "run" / "traj.dat", "already released, or being moved, to job inflight--")
    finally:
        scaffold_module._claim_release = real
    (release,) = read_job_releases(origin)
    assert release.to_job_key.startswith("inflight--")


def test_a_vanished_source_is_not_a_move(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)

    def fault(op: str, phase: str, src: Any, dst: Any) -> None:
        # Another actor renames the file away right before ours: move_owned sees the source gone.
        if op == "rename" and phase == "before" and src.at is not None and src.name() == "traj.dat":
            os.rename("traj.dat", str(tmp_path / "taken"), src_dir_fd=src.at)

    _fs.set_fault_injector(fault)
    try:
        _refused(ws, origin / "run" / "traj.dat", "not a regular file inside a succeeded job")
    finally:
        _fs.set_fault_injector(None)
    assert read_job_releases(origin) == ()
    # Nothing landed in the scratch, so it is not kept.
    assert list((ws.control / "tmp").iterdir()) == []


def test_two_files_of_one_origin_move_to_two_jobs(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws, extra="other.dat")

    for name in ("traj.dat", "other.dat"):
        new_job(
            ws,
            _WORKFLOW,
            parameters={"script": {"start": "succeed"}},
            move_inputs={"trajectory": origin / "run" / name},
        )

    assert sorted(release.path for release in read_job_releases(origin)) == ["run/other.dat", "run/traj.dat"]
    assert verify_job_seal(origin).discrepancies == ()


def test_a_rollback_keeps_the_record_of_a_file_that_is_not_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING, logger="httk.workflow.scaffold")
    ws = _workspace(tmp_path)
    origin = _origin(ws)

    def failing(workspace: Any, owner: Any, scratch: Path, staging: Path) -> Any:
        # The moved file is gone from the build by the time the job fails.
        (staging / "files" / "trajectory.dat").rename(tmp_path / "elsewhere")
        raise RuntimeError("submission failed")

    monkeypatch.setattr(scaffold_module, "_submit_staged", failing)

    with pytest.raises(RuntimeError):
        new_job(
            ws,
            _WORKFLOW,
            parameters={"script": {"start": "succeed"}},
            move_inputs={"trajectory": origin / "run" / "traj.dat"},
        )

    assert not (origin / "run" / "traj.dat").exists()
    assert len(read_job_releases(origin)) == 1
    assert "its release record is kept" in caplog.text


def test_a_stale_release_of_a_present_file_changes_nothing(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    receiver = _moved(ws, _origin(ws))
    run(ws)
    payload = find(ws, receiver).path
    seal = read_seal(job_seal_path(payload))
    (sealed,) = [item for item in seal.records if item["path"] == "files/trajectory.dat"]
    # A record whose rename never happened (a crash in between).
    record_job_release(
        ws,
        payload,
        path="files/trajectory.dat",
        size=int(sealed["size"]),  # type: ignore[call-overload]
        sha256=str(sealed["sha256"]),
        released_to={"job_key": "never--00000000-0000-4000-8000-000000000000", "path": "files/x"},
        keys=(),
    )

    item = _collected(ws, "receiver")

    staged = item.inputs["trajectory"]
    assert isinstance(staged, FileRecord)
    assert staged.url == f"data/workspace/{item.record.payload_path.as_posix()}/files/trajectory.dat"
    assert item.relocations == ()


def test_a_signed_origin_moves_only_with_a_signing_key(tmp_path: Path) -> None:
    import base64

    from httk.core.crypto import ed25519_generate_seed

    ws = _workspace(tmp_path)
    seed = tmp_path / "seal.seed"
    seed.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    ws.set_setting("seal.keys", str(seed))
    origin = _origin(ws, extra="other.dat")
    assert read_seal(job_seal_path(origin)).signatures

    job = new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": origin / "run" / "traj.dat"},
    )
    (release,) = read_job_releases(origin)
    assert release.signatures and release.to_job_key == job.job_key
    assert verify_job_seal(origin).discrepancies == ()

    seed.unlink()
    _refused(ws, origin / "run" / "other.dat", "seal.keys")
    assert (origin / "run" / "other.dat").exists() and len(read_job_releases(origin)) == 1


def test_a_planted_releases_file_refuses_the_move(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    job_releases_path(origin).write_text("planted\n", encoding="utf-8")

    _refused(ws, origin / "run" / "traj.dat", "cannot record its release")
    assert (origin / "run" / "traj.dat").exists()
    assert _collected(ws, "origin").missing_collector is None


def test_an_unknown_moved_input_is_reported_as_unknown(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)

    with pytest.raises(ValueError, match="unknown workflow input 'nope'"):
        new_job(
            ws,
            _WORKFLOW,
            parameters={"script": {"start": "succeed"}},
            move_inputs={"nope": origin / "run" / "traj.dat"},
        )


class _Store:
    """The two store calls ``_relocate`` makes after finding the row."""

    def __init__(self, swapped: bool, url: str | None, incremental: bool = True) -> None:
        self.swapped, self.url, self.calls = swapped, url, 0
        self.backend_facts = SimpleNamespace(supports_incremental_save=incremental)

    def sid_of(self, value: object) -> int:
        return 7

    def relocate_file(self, sid: int, url: str | None, *, expected: str | None) -> bool:
        self.calls += 1
        return self.swapped

    def fetch(self, kind: type, sid: int) -> Any:
        return SimpleNamespace(url=self.url)


@pytest.mark.parametrize(
    ("store", "logged"),
    [
        (_Store(True, None), False),
        (_Store(False, "data/new"), False),
        (_Store(False, "data/someone-else"), True),
        (_Store(True, None, incremental=False), False),
    ],
)
def test_relocate_is_a_compare_and_swap(store: _Store, logged: bool, caplog: pytest.LogCaptureFixture) -> None:
    from httk.workflow.storing import _relocate

    caplog.set_level(logging.DEBUG, logger="httk.workflow.storing")
    record = FileRecord("data/old", "traj.dat", sha256=_SHA256)
    item = cast(Any, SimpleNamespace(outputs={"frames": record}, inputs={}, record=SimpleNamespace(job_key="origin")))

    _relocate(store, item, "data/old", "data/new", _SHA256)

    assert ("leaving its location as it is" in caplog.text) is logged
    assert store.calls == (1 if store.backend_facts.supports_incremental_save else 0)
    _relocate(
        _Store(True, None), cast(Any, SimpleNamespace(outputs={}, inputs={}, record=item.record)), "a", "b", "0" * 64
    )
    assert "no stored file" in caplog.text


def test_a_move_with_a_rotated_seal_key_is_refused(tmp_path: Path) -> None:
    import base64

    from httk.core.crypto import ed25519_generate_seed

    ws = _workspace(tmp_path)
    seeds = [tmp_path / f"k{index}.seed" for index in (1, 2)]
    for seed in seeds:
        seed.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    ws.set_setting("seal.keys", str(seeds[0]))
    origin = _origin(ws)
    ws.set_setting("seal.keys", str(seeds[1]))

    _refused(ws, origin / "run" / "traj.dat", "signed by sha256:.*seal.keys")

    assert (origin / "run" / "traj.dat").exists() and read_job_releases(origin) == ()
    assert verify_job_seal(origin).discrepancies == ()


def test_a_crash_between_marker_and_rename_is_reconciled_away(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    job_id = JobDefinition.from_path(origin / "job.json").id
    _claim(ws, origin, "crashed--00000000-0000-4000-8000-000000000000")
    with cli_owner(ws) as owner:
        scratch = owner.scratch("submit")
        (scratch / "build").mkdir()
        name = f"moved-{hashlib.sha256(f'{job_id}/run/traj.dat'.encode()).hexdigest()}.json"
        to = scratch / "build" / "files" / "trajectory.dat"
        (scratch / name).write_text(json.dumps({"from": str(origin / "run" / "traj.dat"), "to": str(to)}))
    # Closing the owner reconciled the scratch: the file never moved, so nothing is kept.
    assert list((ws.control / "tmp").iterdir()) == []

    new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": origin / "run" / "traj.dat"},
    )

    assert not (origin / "run" / "traj.dat").exists() and verify_job_seal(origin).discrepancies == ()


def test_an_unreadable_record_does_not_block_the_move(tmp_path: Path) -> None:
    ws = _workspace(tmp_path)
    origin = _origin(ws)
    job_releases_path(origin).mkdir()
    (job_releases_path(origin) / (hashlib.sha256(b"run/traj.dat").hexdigest() + ".json")).write_text("junk")

    job = new_job(
        ws,
        _WORKFLOW,
        parameters={"script": {"start": "succeed"}},
        move_inputs={"trajectory": origin / "run" / "traj.dat"},
    )

    (release,) = read_job_releases(origin)
    assert release.to_job_key == job.job_key and verify_job_seal(origin).discrepancies == ()
