"""A push over one transfer session (plan WORKFLOW_TRANSFER_SESSION §7, T1b; sessions required since T7, §7.12).

The far side's ``stage``, ``adopt_staged`` and ``install`` ops (over a real session), and the client's segmented
push: segments, per-segment outcomes, the retry of a failed commit, workflow shipping on the same session, and the
stop before anything is held when the far side's ``httk`` cannot serve the session.
"""

import argparse
import io
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO, cast

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _frames, _landing, _moving, _store, registry
from httk.workflow._frames import REQUEST, RESPONSE, FrameReader, encode_frame, encode_json
from httk.workflow._state import read_state_unowned
from httk.workflow.adapters import (
    TRANSFER_PROTOCOL,
    TRANSFER_SESSION_FORMAT,
    RequestAborted,
    SessionFailed,
    TransferSession,
)
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import _transfer_session, command
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_transfer import (
    adapter_calls,
    jobs,
    outcomes,
    outgoing,
    recorded_requests,
    registered,
    run,
    selected,
    starts,
)
from test_moving import assert_in, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_transfer_takeback import assert_kept_held, refuse, refusing  # noqa: F401 (refusing: fixture)
from test_transfer_workflows import GIT_ID, _entries_tar, shipped, workflows
from test_transfer_workflows import job as workflow_job

#: The stand-in cluster's ``httk``: the real one, with faults the environment asks for injected into a transfer session.
PATCHED_HTTK = '''import errno
import json
import os
import runpy
import sys
import time

argv = sys.argv[1:]
if argv[:2] == ["workflow", "transfer-session"]:
    if os.environ.get("HTTK_TEST_OLD_FAR_SIDE"):
        print("httk workflow: error: argument GROUP: invalid choice: 'transfer-session' (choose from 'run', 'session')", file=sys.stderr)
        sys.exit(2)
    from httk.workflow import _frames, _fs, _moving
    from httk.workflow.workflow_cli import _job, _transfer_session

    if os.environ.get("HTTK_TEST_TICK"):
        _frames.TICK = float(os.environ["HTTK_TEST_TICK"])
    if os.environ.get("HTTK_TEST_PROTOCOL"):  # an httk of another version: its hello and its check
        _transfer_session.TRANSFER_PROTOCOL = int(os.environ["HTTK_TEST_PROTOCOL"])
    failing = {int(item) for item in os.environ.get("HTTK_TEST_FAIL_SYNCFS", "").split(",") if item}
    real_syncfs = _fs._syncfs
    calls = []

    def syncfs(descriptor):
        calls.append(descriptor)
        if len(calls) in failing:
            raise OSError(errno.EDQUOT, os.strerror(errno.EDQUOT))
        real_syncfs(descriptor)

    _fs._syncfs = syncfs
    # Seconds of delay injected into a step: the barrier's prime, the extraction's directory times, the settling.
    delays = json.loads(os.environ.get("HTTK_TEST_DELAYS", "{}"))

    def delayed(module, name):
        real = getattr(module, name)

        def run(*arguments, **options):
            time.sleep(delays[name])
            return real(*arguments, **options)

        setattr(module, name, run)

    for name in delays:
        delayed(_moving if name == "settle_absent" else _fs, name)
    if os.environ.get("HTTK_TEST_DELAY_LANDING_DIRS"):
        # The landing's own directories, made (and fsynced) before its first byte.
        real_make_dirs = _fs.make_dirs

        def make_dirs(path, **options):
            if os.path.basename(str(path)) == "outgoing":  # the stage's landing in its owner scratch
                time.sleep(float(os.environ["HTTK_TEST_DELAY_LANDING_DIRS"]))
            return real_make_dirs(path, **options)

        _fs.make_dirs = make_dirs
    order = os.environ.get("HTTK_TEST_ORDER_LOG")
    if order:
        # The order in which the upload's terminal is validated and the landing committed.
        def logged(owner, name, label):
            real = getattr(owner, name)

            def run(*arguments, **options):
                result = real(*arguments, **options)
                with open(order, "a") as stream:
                    stream.write(label + "\\n")
                return result

            setattr(owner, name, run)

        logged(_frames.StreamReader, "finish", "terminal")
        logged(_fs.Barrier, "commit", "commit")
    if os.environ.get("HTTK_TEST_CONSOLE_LOG"):
        # A console handler made at start, as reporting configures one: it holds the process's real stderr.
        from httk.core.report import configure_reporting

        configure_reporting(logger="httk")
    if os.environ.get("HTTK_TEST_BROKEN_BUILD"):
        from httk.workflow.workflow_cli import _describe

        def broken(*arguments):
            raise RuntimeError("the build machinery broke")

        _describe._build_reason = broken
    if os.environ.get("HTTK_TEST_STATUS_OSERROR"):
        # An OSError once the jobs are adopted, before the answer is made.
        def unreadable(*arguments):
            raise OSError(errno.EIO, "the adoption's status could not be read")

        _job._adoption_status = unreadable
    if os.environ.get("HTTK_TEST_CRASH_ADOPT"):
        def crash(*arguments, **options):
            raise KeyError("an unexpected failure inside the adoption")

        _moving.adopt = crash
    for op in os.environ.get("HTTK_TEST_EXIT_ON_OP", "").split(","):
        if op:
            _transfer_session.OPS[op] = lambda request: os._exit(3)
    exit_after = int(os.environ.get("HTTK_TEST_EXIT_AFTER_ADOPT", "0"))
    other_from = int(os.environ.get("HTTK_TEST_OTHER_ID_FROM_ADOPT", "0"))
    # Scripted answers by stage request number: "refuse" (a refusal document with these members), "garble" (an
    # answer that is no adoption document), "ok_refused" (a refusal answered ok), "after" (members changed in the
    # real answer's document).
    script = json.loads(os.environ.get("HTTK_TEST_SCRIPT", "{}"))
    real_stage = _transfer_session.OPS["stage"]
    real_adopt_staged = _transfer_session.OPS["adopt_staged"]
    # One count of deliveries: a stage request (whose adoption is its adopt_staged).
    adopts = []
    delivery_of = {}

    def scripted(request, step):
        if "refuse" in step:
            document = {"bundles": [], "workspace_id": None, "refused": "a scripted refusal", **step["refuse"]}
            raise _transfer_session.Refused(document["refused"], document=document, returncode=2, stderr="")
        if "garble" in step:
            raise _transfer_session.Refused("garbled", document={"garbled": True}, returncode=2, stderr="")
        if "ok_refused" in step:  # a refusal document answered ok, over a whole upload
            request.upload.finish()
            document = {"bundles": [], "workspace_id": None, "refused": "a refusal answered ok"}
            return {"document": document, "returncode": 2, "stderr": ""}
        return None

    def adopted(response, number):
        if number == exit_after:
            os._exit(3)  # adopted, but the answer is lost
        if other_from and number >= other_from:
            response["document"]["workspace_id"] = "00000000-0000-4000-8000-000000000000"
        response["document"].update(script.get(str(number), {}).get("after", {}))
        return response

    def stage(request):
        adopts.append(1)
        number = len(adopts)
        answer = scripted(request, script.get(str(number), {}))
        if answer is not None:
            return answer
        response = real_stage(request)
        delivery_of[response["token"]] = number
        return response

    def adopt_staged(request):
        return adopted(real_adopt_staged(request), delivery_of.get(request.header.get("token"), 0))

    _transfer_session.OPS["stage"] = stage
    _transfer_session.OPS["adopt_staged"] = adopt_staged
sys.argv = ["httk", *argv]
runpy.run_module("httk.core.cli", run_name="__main__", alter_sys=True)
'''


def patched_httk(tmp_path: Path, remote: Remote) -> None:
    """Make the stand-in cluster's ``httk`` (the ``local`` adapter's too) :data:`PATCHED_HTTK`."""

    wrapper = tmp_path / "patched_httk.py"
    wrapper.write_text(PATCHED_HTTK, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')


def local_project(tmp_path: Path, **settings: object) -> tuple[Path, Path]:
    """A project whose ``cluster`` remote is the ``local`` adapter: the project and the remote's bundle."""

    root = tmp_path / "project"
    initialize_project(root, name="session-push")
    return root, fake_remote(root, template="local", name="cluster", **settings)


def opened(bundle: Path) -> TransferSession:
    session = TransferSession(bundle, timeout=60)
    session.open()
    return session


def staged_adoption(
    session: TransferSession,
    workspace: str,
    transfer_ids: list[str],
    upload: Callable[[BinaryIO], None],
    *,
    sync: str | None = None,
) -> dict[str, Any]:
    """A push's delivery of one segment over *session*: its ``stage``, then, once staged, its ``adopt_staged``. The
    result is the adoption's response (with both requests' standard error), or the stage's refusal.

    :raises RequestAborted: When this side's upload failed (its response is the stage's answer).
    """

    header: dict[str, Any] = {"workspace": workspace, "transfer_ids": transfer_ids, "bytes": 0}
    if sync is not None:
        header["sync"] = sync
    staged = session.request("stage", header, upload=upload)
    if staged.get("ok") is not True:
        return staged
    request = {"workspace": workspace, "token": staged["token"], "transfer_ids": transfer_ids}
    adopted = session.request("adopt_staged", request)
    return {**adopted, "stderr": str(staged.get("stderr", "")) + str(adopted.get("stderr", ""))}


def writer(data: bytes, *, then: BaseException | None = None) -> Callable[[BinaryIO], None]:
    def write(target: BinaryIO) -> None:
        target.write(data)
        if then is not None:
            raise then

    return write


def landings(workspace: Workspace) -> list[str]:
    directory = workspace.control / "transfers" / "incoming"
    return sorted(os.listdir(directory)) if directory.exists() else []


def normalized(document: dict[str, Any]) -> dict[str, Any]:
    """An adoption document without what differs between two adopting workspaces (their id, the jobs' paths)."""

    entries = [
        {**entry, "published": [{k: v for k, v in item.items() if k != "path"} for item in entry["published"]]}
        for entry in document["bundles"]
    ]
    return {**document, "bundles": entries, "workspace_id": None}


# -- the far side: the adopt op ----------------------------------------------------------------------------------


def test_stage_and_adopt_staged_answer_the_adoption_document(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), ids)
    with opened(bundle) as session:
        response = staged_adoption(session, far_name, ids, writer(data))
    assert response["ok"] is True and response["returncode"] == 0
    document = response["document"]
    assert document["workspace_id"] == far.workspace_id
    assert [entry["status"] for entry in document["bundles"]] == ["adopted"] * 2
    # The far side's barrier line is in the stage's captured standard error.
    assert " landed file(s) " in response["stderr"]
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)
    assert landings(far) == []


def test_an_aborted_complete_archive_is_refused_and_nothing_adopted(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), ids)
    with opened(bundle) as session:
        with pytest.raises(RequestAborted) as aborted:
            staged_adoption(session, far_name, ids, writer(data, then=OSError("the source went away")))
        response = aborted.value.response
        assert response["ok"] is False and response["returncode"] == 2
        document = response["document"]
        assert document["bundles"] == [] and "the source went away" in document["refused"]
        assert "bundle" not in document and "sync_failed" not in document and document["stream_aborted"] is True
        # The session goes on: the same stream, whole, is adopted.
        response = staged_adoption(session, far_name, ids, writer(data))
        assert [entry["status"] for entry in response["document"]["bundles"]] == ["adopted"] * 2
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)
    assert landings(far) == []


@pytest.mark.parametrize("cut", ["inside the archive", "after the archive"])
def test_an_input_ending_inside_the_upload_refuses_it_and_ends_the_far_side(
    tmp_path: Path, remote: Remote, cut: str
) -> None:
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    sent = data[: len(data) // 2] if cut == "inside the archive" else data
    process = subprocess.Popen(
        ["httk", "workflow", "transfer-session"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    assert process.stdin is not None and process.stdout is not None
    hello = json.loads(process.stdout.readline())
    assert hello == {"format": TRANSFER_SESSION_FORMAT, "format_version": TRANSFER_PROTOCOL}
    ids = [hold.transfer_id for hold in holds]
    header = {"op": "stage", "upload": True, "protocol": TRANSFER_PROTOCOL, "workspace": far_name}
    header.update(transfer_ids=ids, bytes=0)
    process.stdin.write(encode_json(REQUEST, 1, header))
    for offset in range(0, len(sent), _frames.DATA_LIMIT):
        process.stdin.write(encode_frame(_frames.DATA, 1, sent[offset : offset + _frames.DATA_LIMIT]))
    process.stdin.close()  # no terminal: the input ends inside the upload
    frames = []
    reader = FrameReader(process.stdout)
    while (frame := reader.read_frame()) is not None:
        frames.append(frame)
    assert process.stderr is not None
    errors = process.stderr.read()
    assert process.wait(timeout=60) != 0, errors
    (response,) = [frame.document for frame in frames if frame.kind == RESPONSE]
    assert response is not None and response["ok"] is False and response.get("fatal") is not True
    assert "refused the stream, nothing of it was used" in response["document"]["refused"]
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    assert landings(far) == []


def test_a_refused_member_names_its_bundle(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    bad = holds[1].transfer_id
    fifo = tarfile.TarInfo(f"{bad}/pipe")
    fifo.type = tarfile.FIFOTYPE
    ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), ids, [fifo])
    with opened(bundle) as session:
        response = staged_adoption(session, far_name, ids, writer(data))
    assert response["ok"] is False and response["returncode"] == 2
    document = response["document"]
    assert document["bundle"] == bad and "is a special file" in document["refused"]
    assert "stream_aborted" not in document
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    assert landings(far) == []


@pytest.mark.parametrize(
    ("name", "kind", "expected"),
    [
        ("b/pipe", tarfile.FIFOTYPE, "b"),  # a special file: the member rides on the exception
        ("/./b/x/link", tarfile.LNKTYPE, "b"),  # a hard link to nothing: only the message names the member
        ("../escape", tarfile.REGTYPE, None),  # outside the landing: no bundle
        ("./", tarfile.SYMTYPE, None),  # the landing itself: no bundle
    ],
)
def test_the_offending_bundle_is_the_first_component_of_the_refused_member(
    tmp_path: Path, name: str, kind: bytes, expected: str | None
) -> None:
    member = tarfile.TarInfo(name)
    member.type = kind
    member.linkname = "nowhere" if kind in (tarfile.LNKTYPE, tarfile.SYMTYPE) else ""
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        archive.addfile(member, io.BytesIO(b"") if member.isreg() else None)
    workspace = Workspace.initialize(tmp_path / "workspace")
    landing = workspace.control / "transfers" / "incoming" / "landing"
    with pytest.raises(_landing.StreamRefused) as refused:
        _landing.receive_tar(workspace, landing, io.BytesIO(data.getvalue()), barrier=True)
    assert refused.value.bundle == expected
    assert not landing.exists()


def test_a_failed_commit_is_refused_as_sync_failed_and_a_fresh_fsync_landing_adopts(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first barrier's commit, after its prime
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})
    holds, plan = hold_jobs(near, 2)
    ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), ids)
    with opened(bundle) as session:
        response = staged_adoption(session, far_name, ids, writer(data))
        assert response["ok"] is False and response["document"]["sync_failed"] is True
        assert response["returncode"] == 2 and "job adopt: refused the stream" in response["stderr"]
        assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
        response = staged_adoption(session, far_name, ids, writer(data), sync="fsync")
    assert response["ok"] is True and [entry["status"] for entry in response["document"]["bundles"]] == ["adopted"] * 2
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)
    assert landings(far) == []


def test_a_crash_inside_the_adoption_is_fatal_and_ends_the_far_side(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setenv("HTTK_TEST_CRASH_ADOPT", "1")
    near, _ = registered(tmp_path, "near")
    _, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(near, 1)
    ids = [holds[0].transfer_id]
    data = stream(outgoing_dir(near), ids)
    session = opened(bundle)
    with pytest.raises(SessionFailed, match="an unexpected failure inside the adoption") as failed:
        staged_adoption(session, far_name, ids, writer(data))
    assert failed.value.response is not None and failed.value.response["fatal"] is True
    assert session._status not in (None, 0)  # the far side exited
    with pytest.raises(SessionFailed):
        staged_adoption(session, far_name, ids, writer(data))
    session.close()


# -- the far side: the install op --------------------------------------------------------------------------------


def test_the_install_op_installs_and_builds_store_entries_and_builds_alone(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    workflows(near, tmp_path)
    entries = [_store.lookup(near, GIT_ID), _store.lookup(near, "local:caller")]
    assert all(entries)
    data = _entries_tar(entries)  # type: ignore[arg-type]
    header = {"workspace": far_name, "source_label": "transfer from near", "build": True}
    with opened(bundle) as session:
        response = session.request("install", header, upload=writer(data))
        assert response["ok"] is True and response["returncode"] == 0
        rows = {row["id"]: row for row in response["document"]["workflows"]}
        assert rows[GIT_ID]["status"] == "installed" and rows[GIT_ID]["build_status"] == "built"
        assert rows["local:caller"]["status"] == "installed" and "built" not in response["document"]
        assert _store.closure(far, "local:caller", check_builds=True).ok
        copy = _store.lookup(far, GIT_ID)
        assert copy is not None and copy.record["source"] == "transfer from near"
        assert not [path for path in (far.control / "tmp").iterdir() if path.name.startswith("push.")]
        # Only builds: no upload.
        response = session.request("install", {"workspace": far_name, "also_build": [GIT_ID, "local:absent"]})
        assert response["ok"] is True and response["returncode"] == 1
        assert response["document"]["workflows"] == []
        built = response["document"]["built"]
        assert built[0] == {"id": GIT_ID, "status": "built"} and built[1]["status"] == "failed"
        # Refused before anything is installed: an unknown workspace, an upload that is no tar stream.
        response = session.request("install", {"workspace": "no-such-workspace", "also_build": [GIT_ID]})
        assert response["ok"] is False and "no-such-workspace" in response["error"]
        response = session.request("install", {"workspace": far_name}, upload=writer(b"not a tar stream" * 64))
        assert response["ok"] is False and "refused the stream" in response["error"]


def test_an_install_error_after_the_first_install_is_fatal_not_a_refusal(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setenv("HTTK_TEST_BROKEN_BUILD", "1")
    near, _ = registered(tmp_path, "near")
    _, far_name = registered(tmp_path, "far")
    workflows(near, tmp_path)
    entry = _store.lookup(near, GIT_ID)
    assert entry is not None
    session = opened(bundle)
    header = {"workspace": far_name, "source_label": "transfer from near", "build": True}
    # Installed, then the build fails unexpectedly: the far side ends (its owner is left for recovery).
    with pytest.raises(SessionFailed, match="the build machinery broke") as failed:
        session.request("install", header, upload=writer(_entries_tar([entry])))
    assert failed.value.response is not None and failed.value.response["fatal"] is True
    session.close()


def test_the_workflows_op_reports_present_unbuilt_and_missing(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    workflows(far, tmp_path, built=False)
    caller = _store.lookup(far, "local:caller")
    assert caller is not None
    with opened(bundle) as session:
        ids = [GIT_ID, "local:caller", "local:absent"]
        response = session.request("workflows", {"workspace": far_name, "ids": ids})
        assert response["ok"] is True
        assert response["workflows"] == [
            {"id": GIT_ID, "status": "unbuilt", "tree_sha256": _store.lookup(far, GIT_ID).record["tree_sha256"]},  # type: ignore[union-attr]
            {"id": "local:caller", "status": "present", "tree_sha256": caller.record["tree_sha256"]},
            {"id": "local:absent", "status": "missing", "tree_sha256": None},
        ]
        response = session.request("workflows", {"workspace": "no-such-workspace", "ids": ids})
        assert response["ok"] is False and "no-such-workspace" in response["error"]


def test_slow_steps_without_moving_data_report_their_phases_to_the_watchdog(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A bound of 1 second on the far side's liveness; the landing's directories, the barrier's prime, the
    # extraction's directory times and the settling each stand still for 2 seconds, in the sync and settle phases,
    # which the bound exempts.
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    delays = {"_prime": 2.0, "_restore_directory_times": 2.0, "settle_absent": 2.0}
    monkeypatch.setenv("HTTK_TEST_DELAYS", json.dumps(delays))
    monkeypatch.setenv("HTTK_TEST_DELAY_LANDING_DIRS", "2.0")
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.sync": "syncfs"})  # a prime, then a commit
    holds, plan = hold_jobs(near, 2)
    ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), ids)
    session = opened(bundle)
    session._limit = 1.0
    with session:
        response = staged_adoption(session, far_name, ids, writer(data))
    assert response["ok"] is True, response
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)


def test_a_refused_member_s_slow_drain_and_discard_report_sync_and_the_refusal_arrives(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A pooled write still running when a later member is refused, and the landing's discard, each stand still for
    # 2 seconds against a bound of 1 second: in the sync phase, so the refusal is answered, not an unknown outcome.
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    monkeypatch.setenv("HTTK_TEST_DELAYS", json.dumps({"_write_member": 2.0, "discard": 2.0}))
    far, far_name = registered(tmp_path, "far")
    far.configure_settings({"io.workers": 4})
    name = "a" * 32  # a transfer id, as a stage requires
    directory = tarfile.TarInfo(name)
    directory.type = tarfile.DIRTYPE
    slow = tarfile.TarInfo(f"{name}/slow")
    slow.size = 10
    fifo = tarfile.TarInfo(f"{name}/pipe")
    fifo.type = tarfile.FIFOTYPE
    data = stream(tmp_path, [], [directory, slow, fifo])
    session = opened(bundle)
    session._limit = 1.0
    with session:
        response = staged_adoption(session, far_name, [name], writer(data))
    assert response["ok"] is False and response["document"]["bundle"] == name
    assert "is a special file" in response["document"]["refused"]
    assert landings(far) == []


@pytest.mark.parametrize("workers", [1, 4])
def test_the_receiver_reports_its_phases_with_a_moving_extract_count(tmp_path: Path, workers: int) -> None:
    near, _ = registered(tmp_path, "near")
    far, _ = registered(tmp_path, "far")
    far.configure_settings({"io.workers": workers})
    holds, _ = hold_jobs(near, 3)
    data = stream(outgoing_dir(near), [hold.transfer_id for hold in holds])
    seen: list[tuple[str, int | None]] = []
    workspace = Workspace(far.root)  # durable: the barrier is entered and committed
    landing = workspace.control / "transfers" / "incoming" / "phases"
    names = _landing.receive_tar(
        workspace, landing, io.BytesIO(data), barrier=True, progress=lambda phase, count: seen.append((phase, count))
    )
    assert names == sorted(hold.transfer_id for hold in holds)
    phases = [phase for phase, _ in seen]
    # sync (the barrier's entry), extract counting up, then sync (the tail and the commit).
    first_extract, last_extract = phases.index("extract"), len(phases) - 1 - phases[::-1].index("extract")
    assert phases[0] == "sync" and phases[last_extract + 1 :] == ["sync"]
    assert set(phases[first_extract : last_extract + 1]) == {"extract"}
    counts = [count or 0 for phase, count in seen if phase == "extract"]
    assert counts[0] == 0 and counts == sorted(counts) and counts[-1] >= len(data) - 2 * 10240


def test_the_upload_s_terminal_is_validated_before_the_landing_is_committed(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    order = tmp_path / "order"
    monkeypatch.setenv("HTTK_TEST_ORDER_LOG", str(order))
    near, _ = registered(tmp_path, "near")
    _, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(near, 1)
    ids = [holds[0].transfer_id]
    data = stream(outgoing_dir(near), ids)
    with opened(bundle) as session:
        response = staged_adoption(session, far_name, ids, writer(data))
    assert response["ok"] is True
    # The opening echo's terminal, then the stage's terminal, validated before its landing's commit.
    assert order.read_text().splitlines() == ["terminal", "terminal", "commit"]


def test_an_error_after_the_adoption_began_is_fatal_even_when_it_is_an_oserror(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setenv("HTTK_TEST_STATUS_OSERROR", "1")
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    ids = [holds[0].transfer_id]
    data = stream(outgoing_dir(near), ids)
    session = opened(bundle)
    with pytest.raises(SessionFailed, match="the adoption's status could not be read") as failed:
        staged_adoption(session, far_name, ids, writer(data))
    assert failed.value.response is not None and failed.value.response["fatal"] is True
    session.close()
    # Adopted before the error: never claimed as a refusal.
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)


def test_a_request_s_log_records_land_once_in_its_capture_and_nowhere_else(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_httk(tmp_path, remote)
    monkeypatch.setenv("HTTK_TEST_CONSOLE_LOG", "1")  # the far side's own console handler, on its real stderr
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    path = far.control / "format.json"
    document = json.loads(path.read_bytes())
    document["settings"]["io.sync_errors"] = "warn"  # read once by the landing's barrier, which warns
    path.write_text(json.dumps(document), encoding="utf-8")
    holds, _ = hold_jobs(near, 1)
    ids = [holds[0].transfer_id]
    data = stream(outgoing_dir(near), ids)
    session = opened(bundle)
    with session:
        response = staged_adoption(session, far_name, ids, writer(data))
        assert response["ok"] is True
        lines = response["stderr"].splitlines()
        # Formatted by the far side's console handler, once, and only in the response.
        assert len([line for line in lines if "io.sync_errors is 'warn', not fail" in line]) == 1
    assert "io.sync_errors" not in "".join(session._errors)


class _Quiet(logging.Filter):
    """A reporting policy: a record whose message says ``quiet`` is suppressed."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "quiet" not in record.getMessage()


class _Kept(logging.Handler):
    """Another consumer of the records (a file, say): it keeps their messages."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_the_capture_takes_console_output_once_keeping_its_filters_and_other_handlers(
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, below = logging.getLogger("httk"), logging.getLogger("httk.workflow.test_capture")
    console = logging.StreamHandler(sys.stderr)  # a console handler as reporting configures one
    console.addFilter(_Quiet())
    console.setFormatter(logging.Formatter("console: %(message)s"))
    elsewhere = _Kept()
    saved = (root.handlers[:], root.propagate, below.handlers[:], below.propagate)
    root.handlers, root.propagate = [console, elsewhere], False
    try:
        with _transfer_session._captured() as capture:
            below.warning("io.sync is 'x': one record")
            below.warning("a quiet one, suppressed by the console's policy")
        assert capture.getvalue() == "console: io.sync is 'x': one record\n"
        assert capsys.readouterr().err == ""  # this process's own stderr
        assert elsewhere.messages == ["io.sync is 'x': one record", "a quiet one, suppressed by the console's policy"]
        # Restored: the console writes to this process's stderr again, with its filter.
        assert console.stream is sys.stderr and root.handlers == [console, elsewhere]
        below.warning("after the request")
        assert capsys.readouterr().err == "console: after the request\n"
    finally:
        root.handlers, root.propagate, below.handlers, below.propagate = saved


def test_an_active_report_collection_still_receives_a_request_s_records() -> None:
    from httk.core.report import collect_reports

    logger = logging.getLogger("httk.workflow.test_collect")
    with collect_reports() as collection, _transfer_session._captured() as capture:
        logger.warning("io.sync is 'x': collected")
    assert [record.getMessage() for record in collection.records] == ["io.sync is 'x': collected"]
    assert capture.getvalue() == ""  # collected, not shown: as on a command line inside the same collection


def test_an_aborted_install_upload_installs_nothing(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    workflows(near, tmp_path)
    entry = _store.lookup(near, GIT_ID)
    assert entry is not None
    header = {"workspace": far_name, "source_label": "transfer from near", "build": True}
    with opened(bundle) as session, pytest.raises(RequestAborted) as aborted:
        session.request("install", header, upload=writer(_entries_tar([entry]), then=OSError("gone")))
    response = aborted.value.response
    assert response["ok"] is False and "refused the stream" in response["error"] and "gone" in response["error"]
    assert _store.lookup(far, GIT_ID) is None
    assert not [path for path in (far.control / "tmp").iterdir() if path.name.startswith("push.")]


# -- the client: a segmented push over one session ------------------------------------------------------------------


@pytest.fixture
def ssh(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster (its ``httk`` :data:`PATCHED_HTTK`), a local SRC and a DST there."""

    project = tmp_path / "project"
    initialize_project(project, name="session-push-ssh")
    fake_remote(project)
    patched_httk(tmp_path, remote)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


def adopts(requests: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    """The headers of the deliveries: ``stage`` requests (plan §7.10)."""

    return [header for op, header in requests if op == "stage"]


def ops(requests: list[tuple[str, dict[str, Any]]]) -> list[str]:
    return [op for op, _ in requests]


def pushed(requests: list[tuple[str, dict[str, Any]]]) -> list[str]:
    """The ops without the ``adopt_staged`` requests, whose place among the stages depends on when each staged segment
    matured (plan §7.10): :func:`adoptions` counts them."""

    return [op for op, _ in requests if op != "adopt_staged"]


def adoptions(requests: list[tuple[str, dict[str, Any]]]) -> int:
    return sum(1 for op, _ in requests if op == "adopt_staged")


def statuses_by_id(capsys: pytest.CaptureFixture[str]) -> dict[str, dict[str, Any]]:
    return {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}


def test_a_push_is_one_connection_and_one_start_whatever_its_segments(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 4)
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    measured = []
    for count, segments in ((1, 1), (30, 8)):
        calls.clear()
        requests.clear()
        before = len(remote.commands())
        plan = jobs(near, count)
        assert run(project, "--json", "--keep", *selected(plan), near_name, f"cluster:{far_name}") == 0
        assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * count
        # No adapter call at all: one session asks for the workflows (installed nowhere: nothing to install), then
        # carries every segment, each its own request and landing.
        assert calls == []
        # Staged, each adopted with its own adopt_staged (plan §7.10).
        assert pushed(requests) == ["workflows", *["stage"] * segments] and adoptions(requests) == segments
        assert {header["workspace"] for header in adopts(requests)} == {far_name}
        commands = remote.commands()[before:]
        measured.append((len(commands), starts(commands)))
        assert "workflow transfer-session" in commands[0]
        assert_in(far, plan)
        assert outgoing(near) == [] and landings(far) == []
        # The away copies record the workspace the adoptions reported.
        for mapping, _, _ in plan:
            ref = find(near, mapping["id"])
            assert ref is not None and ref.state == "away"
            doc = read_state_unowned(ref.path / "state.json")[0]
            assert doc is not None and doc.away is not None and doc.away["workspace_id"] == far.workspace_id
    assert measured == [(1, 1), (1, 1)]


def test_segments_close_at_bundles_bytes_or_members_and_never_split_a_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sizes: dict[str, tuple[int, int]] = {"a": (10, 3), "b": (10, 3), "c": (50, 3), "d": (1, 40), "e": (1, 1)}
    holds = [_moving.Hold(name, name, (name,), None, None, "2026-11-17T00:00:00+00:00") for name in sizes]
    measure = transfer_cli._measure
    monkeypatch.setattr(transfer_cli, "_measure", lambda path: sizes[str(path)])
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 3)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BYTES", 30)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_MEMBERS", 20)
    names = [[hold.transfer_id for hold in segment] for segment in transfer_cli._segments(holds)]
    # c alone exceeds the bytes, d the members: each is a segment of its own.
    assert names == [["a", "b"], ["c"], ["d"], ["e"]]
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BYTES", 1000)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_MEMBERS", 1000)
    names = [[hold.transfer_id for hold in segment] for segment in transfer_cli._segments(holds)]
    assert names == [["a", "b", "c"], ["d", "e"]]
    # One walk counts every entry as a member and every regular file's bytes, a hard link with its target's.
    hold = tmp_path / "hold"
    (hold / "job").mkdir(parents=True)
    (hold / "job" / "data").write_bytes(b"x" * 100)
    os.link(hold / "job" / "data", hold / "job" / "linked")
    (hold / "job" / "link").symlink_to("data")
    assert measure(hold) == (200, 5)


def test_workflows_are_installed_and_built_before_the_first_segment_in_the_one_start(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    job_id = workflow_job(near, workflows(near, tmp_path / "near"), state="ready")
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    # Asked, installed and built, and only then adopted: the job can start as soon as it arrives.
    assert calls == [] and pushed(requests) == ["workflows", "install", "stage"] and adoptions(requests) == 1
    assert sorted(requests[0][1]["ids"]) == sorted([GIT_ID, "local:caller"])
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert _store.closure(far, "local:caller", check_builds=True).ok
    assert find(far, job_id) is not None
    # A destination that has the workflows unbuilt only builds them, still before the adoption and in the one start.
    other, other_name = registered(remote.root, "other")
    workflows(other, tmp_path / "other", built=False)
    requests.clear()
    before = len(remote.commands())
    job_id = workflow_job(near, workflows(near, tmp_path / "again"), state="ready")
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{other_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "present", "local:caller": "present"}
    assert pushed(requests) == ["workflows", "install", "stage"] and requests[1][1]["also_build"] == [GIT_ID]
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (1, 1)
    assert _store.closure(other, "local:caller", check_builds=True).ok


def test_a_failing_build_is_reported_and_the_push_goes_on(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    requests = recorded_requests(monkeypatch)
    failing = '\n[workflow.build]\ncommand = "sh -c \'exit 3\'"\nartifacts = ["out.bin"]\n'
    job_id = workflow_job(near, workflows(near, tmp_path, build=failing, built=False), state="ready")
    assert run(project, "--json", "-q", "--job", job_id, near_name, f"cluster:{far_name}") == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "adopted" and find(far, job_id) is not None
    (callee,) = [entry for entry in outcome["workflows"] if entry["id"] == GIT_ID]
    assert callee["status"] == "unbuilt"
    assert f"httk workflow build --workspace cluster:{far_name} {GIT_ID}" in callee["reason"]
    assert pushed(requests) == ["workflows", "install", "stage"] and adoptions(requests) == 1


def test_a_session_failing_in_the_workflow_step_adopts_nothing(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, _far_name = ssh
    requests = recorded_requests(monkeypatch)
    installed = workflows(near, tmp_path)
    for op, expected in (("workflows", ["workflows"]), ("install", ["workflows", "install"])):
        far, far_name = registered(Path(str(_far.root)).parent, f"far-{op}")
        job_id = workflow_job(near, installed, state="ready")
        monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", op)
        requests.clear()
        assert run(project, "--json", "-q", "--job", job_id, near_name, f"cluster:{far_name}") == 1
        (outcome,) = outcomes(capsys)["transfers"]
        assert outcome["status"] == "failed" and "failed before anything was adopted" in outcome["message"]
        assert ops(requests) == expected
        assert find(far, job_id) is None and outcome["transfer_id"] in outgoing(near)  # held, for a later run


def test_an_older_far_side_stops_the_push_before_anything_is_held(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_OLD_FAR_SIDE", "1")
    calls = adapter_calls(monkeypatch)
    plan = jobs(near, 3)
    before = sorted(path.name for path in near.root.rglob("*"))
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    assert document["transfers"] == []
    (error,) = document["errors"]
    assert (
        "the httk on cluster is too old (or too new) for this transfer: install the same httk version on both ends"
        in (error)
    )
    assert "3 selected job(s) were not attempted" in error
    # One refused start, nothing else: no adapter operation, no hold, nothing changed on either end.
    assert calls == [] and len(remote.commands()) == 1 and "workflow transfer-session" in remote.commands()[0]
    assert outgoing(near) == [] and sorted(path.name for path in near.root.rglob("*")) == before
    assert_in(near, plan)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan) and outgoing(far) == []


@pytest.mark.parametrize("held", [False, True], ids=["selection", "resume"])
def test_a_newer_far_side_stops_the_push_and_an_earlier_hold_stays_held(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    held: bool,
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_PROTOCOL", str(TRANSFER_PROTOCOL + 1))
    plan = jobs(near, 2)
    if held:  # a hold an earlier run made (today's on-disk format), driven by --resume
        assert run(project, "--hold", "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
        capsys.readouterr()
    earlier = sorted(outgoing(near))
    argv = ["--resume"] if held else selected(plan)
    assert run(project, "--json", *argv, near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    error = " ".join(document["errors"]) + " ".join(str(item.get("message")) for item in document["transfers"])
    assert "the httk on cluster is too old (or too new) for this transfer" in error
    assert f"its transfer protocol is {TRANSFER_PROTOCOL + 1}, this side's {TRANSFER_PROTOCOL}" in error
    assert sorted(outgoing(near)) == earlier  # an earlier hold stays held, untouched; no new one
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan) and outgoing(far) == []
    if not held:
        assert_in(near, plan)
    # Once both ends run the same httk, the hold made by the earlier run is resumed over a session.
    monkeypatch.delenv("HTTK_TEST_PROTOCOL")
    if held:
        assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
        assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
        assert_in(far, plan)
        assert outgoing(near) == []


@pytest.mark.parametrize("direction", ["push", "pull"])
def test_an_adapter_without_the_session_operation_stops_before_anything_is_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], direction: str
) -> None:
    from test_adapter_refusal import _unrecognized

    project = tmp_path / "project"
    initialize_project(project, name="session-refused")
    _unrecognized(project, "cluster")  # an adapter kind that refuses every operation, session included
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    calls = adapter_calls(monkeypatch)
    plan = jobs(near if direction == "push" else far, 2)
    ends = [near_name, f"cluster:{far_name}"] if direction == "push" else [f"cluster:{far_name}", near_name]
    assert run(project, "--json", *selected(plan), *ends) == 1
    document = outcomes(capsys)
    assert document["transfers"] == []
    (error,) = document["errors"]
    assert "the adapter of cluster cannot open a transfer session" in error and "'torque' is not implemented" in error
    # Nothing held or changed on either end, and no adapter operation tried in its place.
    assert calls == [] and outgoing(near) == [] and outgoing(far) == []
    assert_in(near if direction == "push" else far, plan)


def test_the_removed_setting_is_refused_by_remote_configure(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="session-setting")
    fake_remote(project)
    argv = ["remote", "configure", "--set", "transfer_session=off", "cluster"]
    assert command(argv, CLIContext("httk", project)) != 0
    assert "unknown remote setting 'transfer_session'" in capsys.readouterr().err


def test_ssh_failing_to_open_the_session_holds_nothing(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="session-open")
    fake_remote(project)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    monkeypatch.setenv("HTTK_FAKE_SSH_REFUSE", "1")
    calls = adapter_calls(monkeypatch)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    # Plan §7.8: the session opens before the first hold, so one that cannot open leaves nothing held.
    assert document["transfers"] == []
    (error,) = document["errors"]
    assert "transfer session did not open" in error and "ssh connection failed" in error
    assert "2 selected job(s) were not attempted" in error and "--resume" in error
    assert calls == [] and len(remote.commands()) == 1
    assert outgoing(near) == [] and all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    assert_in(near, plan)


def test_a_failed_commit_is_retried_in_its_segment_and_later_segments_fsync_directly(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    far.configure_settings({"io.sync": "syncfs"})
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first barrier's commit, after its prime
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 6
    assert [header.get("sync") for header in adopts(requests)] == [None, "fsync", "fsync", "fsync"]
    assert len({header["token"] for op, header in requests if op == "adopt_staged"}) == 3  # the refused stage: none
    assert captured.err.count("could not make the delivery durable") == 1
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(far, plan)
    assert outgoing(near) == [] and landings(far) == []


def test_no_retry_under_ignore(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Under io.sync_errors ignore the far side trusts a failed commit after a second syncfs: the segment is
    # adopted, never refused as sync_failed, so it is delivered once, without fsync.
    project, near, near_name, far, far_name = ssh
    far.configure_settings({"io.sync": "syncfs", "io.sync_errors": "ignore"})
    monkeypatch.setenv("HTTK_TEST_FAIL_SYNCFS", "2")  # the first barrier's commit, after its prime
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    assert [header.get("sync") for header in adopts(requests)] == [None]
    assert_in(far, plan)


def scripted(monkeypatch: pytest.MonkeyPatch, steps: dict[int, dict[str, Any]]) -> None:
    """Script the far side's answers to stage requests by number (see :data:`PATCHED_HTTK`)."""

    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({str(number): step for number, step in steps.items()}))


def test_a_refused_retry_takes_this_run_s_holds_back(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    failed = {"refuse": {"sync_failed": True, "workspace_id": far.workspace_id}}
    scripted(monkeypatch, {1: failed, 2: {"refuse": {"workspace_id": far.workspace_id}}})
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * 2
    assert all("a scripted refusal" in outcome["message"] for outcome in transfers)
    assert [header.get("sync") for header in adopts(requests)] == [None, "fsync"]
    assert_in(near, plan)
    assert outgoing(near) == []


@pytest.mark.parametrize(
    ("second", "said"),
    [
        ({"after": {"workspace_id": "00000000-0000-4000-8000-000000000000"}}, "not the expected workspace"),
        ({"garble": True}, "gave no usable adoption report"),
    ],
    ids=["another-workspace", "unreadable"],
)
def test_a_retry_answered_unusably_keeps_the_holds(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    second: dict[str, Any],
    said: str,
) -> None:
    project, near, near_name, far, far_name = ssh
    scripted(monkeypatch, {1: {"refuse": {"sync_failed": True, "workspace_id": far.workspace_id}}, 2: second})
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all(said in outcome["message"] for outcome in transfers)
    assert len(outgoing(near)) == 2  # never taken back


def test_a_non_boolean_sync_failed_marker_is_an_unreadable_answer(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    scripted(monkeypatch, {1: {"refuse": {"sync_failed": "yes", "workspace_id": far.workspace_id}}})
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 1)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "failed" and "whether the jobs arrived is unknown" in outcome["message"]
    assert len(adopts(requests)) == 1 and len(outgoing(near)) == 1


def test_an_adoption_document_marked_sync_failed_is_an_adoption(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, far_name = ssh
    scripted(monkeypatch, {1: {"after": {"sync_failed": True}}})
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    assert len(adopts(requests)) == 1 and outgoing(near) == []


def test_keep_with_a_retry_records_the_second_answer_s_workspace(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    scripted(monkeypatch, {1: {"refuse": {"sync_failed": True}}})  # the first answer names no workspace
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", "--keep", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert all(outcome["kept"] for outcome in outcomes(capsys)["transfers"])
    for mapping, _, _ in plan:
        ref = find(near, mapping["id"])
        assert ref is not None and ref.state == "away"
        doc = read_state_unowned(ref.path / "state.json")[0]
        assert doc is not None and doc.away is not None and doc.away["workspace_id"] == far.workspace_id


def test_quiet_silences_the_segment_announcements(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    lines = capsys.readouterr().err.splitlines()
    assert not [
        line for line in lines if line.startswith(("holding", "delivering", "releasing", f"cluster:{far_name}"))
    ]
    assert run(project, "--json", *selected(jobs(near, 4)), near_name, f"cluster:{far_name}") == 0
    lines = capsys.readouterr().err.splitlines()
    assert len([line for line in lines if line.startswith("delivering")]) == 2


def test_a_refused_earlier_segment_stops_the_push_before_any_new_hold(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    refusing: list[Workspace],  # noqa: F811 (the fixture)
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    earlier = jobs(near, 2)
    # Run 1: the far side adopts, then dies before it answers: the holds stay.
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_ADOPT", "1")
    assert run(project, "--json", "-q", *selected(earlier), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("whether the jobs arrived is unknown" in outcome["message"] for outcome in transfers)
    held = sorted(outcome["transfer_id"] for outcome in transfers)
    assert_in(far, earlier)
    monkeypatch.delenv("HTTK_TEST_EXIT_AFTER_ADOPT")
    # Run 2 pushes new jobs, and DST refuses the earlier holds' segment as a whole (no landing can be made).
    refuse(refusing, far)
    new = jobs(near, 2)
    requests = recorded_requests(monkeypatch)
    before = len(remote.commands())
    assert run(project, "--json", "-q", *selected(new), near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert sorted(outcome["transfer_id"] for outcome in transfers) == held
    assert_kept_held(transfers, near, far, far_name)
    # Plan §7.8: the earlier holds are driven first, and the refusal of the whole destination stops the pair before
    # any new hold is made (T1b held them first and took them back).
    (error,) = document["errors"]
    assert "stopped" in error and "2 selected job(s) were not attempted" in error
    assert len(adopts(requests)) == 1
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(near, new)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in new)
    assert sorted(outgoing(near)) == held


def test_a_destination_that_refuses_everything_gets_one_segment_not_all(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, _far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    assert run(project, "--json", "-q", *selected(plan), near_name, "cluster:no-such-workspace") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * 6
    assert all("no-such-workspace" in outcome["message"] for outcome in transfers)
    assert len(adopts(requests)) == 1
    assert_in(near, plan)
    assert outgoing(near) == []


def offending(monkeypatch: pytest.MonkeyPatch, positions: list[int]) -> list[str]:
    """Put a FIFO, which the far side refuses, into the holds at *positions* of the push's first segmenting."""

    real = transfer_cli._segments
    named: list[str] = []

    def segments(holds: list[_moving.Hold], **options: Any) -> list[list[_moving.Hold]]:
        if not named:
            for position in positions:
                (job,) = Path(holds[position].path).glob("jobs/**/job.json")
                os.mkfifo(job.parent / "pipe")  # among the job's own files, which a take-back accepts
                named.append(holds[position].transfer_id)
        return real(holds, **options)

    monkeypatch.setattr(transfer_cli, "_segments", segments)
    return named


def test_a_fresh_bundle_whose_member_is_refused_is_taken_back_and_its_segment_goes_again(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(near, 3)
    named = offending(monkeypatch, [1])
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    (bad,) = named
    by_id = statuses_by_id(capsys)
    offender = by_id.pop(bad)
    assert offender["status"] == "not_moved" and "is a special file" in offender["message"]
    assert (
        "refused before anything was adopted" in offender["message"] and "the jobs are back in" in offender["message"]
    )
    assert [outcome["status"] for outcome in by_id.values()] == ["adopted"] * 2
    assert len(adopts(requests)) == 2
    assert outgoing(near) == []
    returned = offender["members"][0].split("--")[-1]  # the job key's UUID
    for mapping, _, _ in plan:
        assert (find(near, mapping["id"]) is not None, find(far, mapping["id"]) is not None) == (
            (True, False) if mapping["id"] == returned else (False, True)
        )


def test_an_earlier_run_s_bundle_whose_member_is_refused_stays_held_with_the_conditional_remedy(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(near, 3)
    # Run 1: adopted, the answer lost: every hold stays, the jobs are at DST.
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_ADOPT", "1")
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    capsys.readouterr()
    monkeypatch.delenv("HTTK_TEST_EXIT_AFTER_ADOPT")
    bad = _moving.held(near)[1]
    os.mkfifo(Path(bad.path) / "pipe")
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 1
    by_id = statuses_by_id(capsys)
    offender = by_id.pop(bad.transfer_id)
    assert_kept_held([offender], near, far, far_name)
    assert "is a special file" in offender["message"]
    assert [outcome["status"] for outcome in by_id.values()] == ["already_adopted"] * 2
    assert len(adopts(requests)) == 2
    assert outgoing(near) == [bad.transfer_id]
    assert_in(far, plan)


def test_a_segment_goes_again_at_most_once_without_a_refused_bundle(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(near, 4)
    named = offending(monkeypatch, [0, 2])
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    first, second = named
    by_id = statuses_by_id(capsys)
    # The first refusal takes its bundle back; the second is final for the whole rest of the segment, which is this
    # run's: taken back too, the second refused bundle with it.
    assert len(adopts(requests)) == 2
    assert [outcome["status"] for outcome in by_id.values()] == ["not_moved"] * 4
    assert f"'{first}/jobs/" in by_id[first]["message"] and f"'{second}/jobs/" in by_id[second]["message"]
    others = [outcome for key, outcome in by_id.items() if key not in (first, second)]
    assert all(f"'{second}/jobs/" in outcome["message"] for outcome in others)
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    assert_in(near, plan)
    assert outgoing(near) == []


def test_a_sync_failed_refusal_naming_a_bundle_is_retried_and_never_resent_without_it(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(near, 2)
    calls: list[bool] = []

    def session_stage(session: Any, src: Any, dst: Any, arguments: Any, holds: Any, size: int, fsync: bool) -> Any:
        calls.append(fsync)  # a stub answer, both times: a failed commit that also names a bundle
        refusal = {"bundles": [], "workspace_id": far.workspace_id, "refused": "syncfs failed", "sync_failed": True}
        return {**refusal, "bundle": holds[0].transfer_id}, ""

    monkeypatch.setattr(transfer_cli, "_session_stage", session_stage)  # a push stages its segments (plan §7.10)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    # Retried once with fsync; the second failed commit is the answer for the whole segment, never one bundle's.
    assert calls == [False, True]
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["not_moved"] * 2
    assert_in(near, plan)


def test_an_aborted_upload_s_answer_counts_only_as_a_refusal(tmp_path: Path) -> None:
    holds = [_moving.Hold("a" * 32, str(tmp_path / ("a" * 32)), ("job",), None, None, "2026-11-17T00:00:00+00:00")]
    src = transfer_cli._End(str(tmp_path))
    arguments = argparse.Namespace(keep=False)
    adopted = {
        "bundle": "a" * 32,
        "status": "adopted",
        "already_adopted": False,
        "published": [],
        "missing_workflows": [],
    }
    workspace_id = "00000000-0000-4000-8000-000000000001"

    class Stub:
        def __init__(self, response: dict[str, Any]) -> None:
            self.response = response

        def request(self, *_arguments: Any, **_options: Any) -> dict[str, Any]:
            raise RequestAborted("adopt was aborted", response=self.response)

    def answer(response: dict[str, Any]) -> tuple[dict[str, Any], str]:
        return transfer_cli._session_stage(cast(Any, Stub(response)), src, dst, arguments, holds, 0, False)

    dst = transfer_cli._End("cluster:far", binding=registry.WorkspaceBinding("cluster:far", "cluster", None))
    # A stage claimed for an upload this side aborted is no answer, nor is an adoption so claimed.
    claimed = {"ok": True, "token": "t", "transfer_ids": ["a" * 32], "workspace_id": workspace_id, "matures_in": 0}
    with pytest.raises(RuntimeError, match="whether the jobs arrived is unknown"):
        answer({**claimed, "bytes": 0})
    with pytest.raises(RuntimeError, match="whether the jobs arrived is unknown"):
        answer({"ok": False, "document": {"bundles": [adopted], "workspace_id": workspace_id}, "error": "x"})
    refusal = {"bundles": [], "workspace_id": workspace_id, "refused": "aborted"}
    document, _err = answer({"ok": False, "document": refusal, "error": "aborted"})
    assert document["refused"] == "aborted"
    # A stream_aborted marker that is no boolean is no answer either.
    with pytest.raises(RuntimeError, match="whether the jobs arrived is unknown"):
        answer({"ok": False, "document": {**refusal, "stream_aborted": "yes"}, "error": "aborted"})


def test_a_hold_only_hold_is_never_taken_back_by_a_refused_resume(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    refusing: list[Workspace],  # noqa: F811 (the fixture)
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(near, 2)
    assert run(project, "--hold", *selected(plan), near_name, f"cluster:{far_name}") == 0
    capsys.readouterr()
    refuse(refusing, far)
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert_kept_held(transfers, near, far, far_name)
    assert len(outgoing(near)) == 2


def test_another_workspace_answering_a_later_segment_stops_the_push(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    # Each staged segment matures at once, so it is adopted before the next is staged (plan §7.10): with a positive
    # deadline the third segment may be staged before the second's adoption stops the push (a race that pooled
    # directory fsyncs, T6a, made faster staging expose).
    far.set_policy({"visibility_deadline_seconds": 0})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_OTHER_ID_FROM_ADOPT", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 4
    assert all("not the expected workspace" in outcome["message"] for outcome in transfers[2:4])
    for outcome in transfers[4:]:
        assert "not delivered, since the transfer session stopped" in outcome["message"]
        assert f"httk job transfer --resume {near.root} cluster:{far_name}" in outcome["message"]
    assert len(adopts(requests)) == 2
    assert sorted(outgoing(near)) == sorted(outcome["transfer_id"] for outcome in transfers[2:])


def test_another_workspace_answering_with_later_segments_staged_keeps_every_hold(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # The positive-deadline variant of the test above: the later segments may be staged before the first adoption
    # answers another workspace; they are abandoned, never adopted, and every hold stays held (sent, so never taken
    # back).
    project, near, near_name, far, far_name = ssh
    far.set_policy({"visibility_deadline_seconds": 2})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_OTHER_ID_FROM_ADOPT", "1")
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 6
    assert all("not the expected workspace" in outcome["message"] for outcome in transfers[:2])
    assert all("--resume" in outcome["message"] for outcome in transfers[2:])
    # One adoption (the first segment's, answered by another workspace); the others were never adopted there.
    assert len([op for op, _ in requests if op == "adopt_staged"]) == 1
    assert len([op for op, _ in requests if op == "stage"]) >= 2  # at least one stage abandoned unadopted
    assert [find(far, mapping["id"]) is not None for mapping, _, _ in plan] == [True] * 2 + [False] * 4
    # Every hold kept on SRC, none taken back.
    assert sorted(outgoing(near)) == sorted(outcome["transfer_id"] for outcome in transfers)
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_far_side_dying_in_a_segment_keeps_it_and_the_later_ones_held_for_resume(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    # Each staged segment matures at once, so it is adopted before the next is staged (plan §7.10).
    far.set_policy({"visibility_deadline_seconds": 0})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_ADOPT", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2 + ["failed"] * 4
    for outcome in transfers[2:4]:
        assert "the transfer session to" in outcome["message"]
        assert "whether the jobs arrived is unknown" in outcome["message"]
    assert all("not delivered, since the transfer session stopped" in outcome["message"] for outcome in transfers[4:])
    # Nothing is sent after the stop, no install either.
    assert ops(requests) == ["workflows", "stage", "adopt_staged", "stage", "adopt_staged"]
    # The first segment was released at once; the others stay held.
    assert sorted(outgoing(near)) == sorted(outcome["transfer_id"] for outcome in transfers[2:])
    monkeypatch.delenv("HTTK_TEST_EXIT_AFTER_ADOPT")
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    statuses = {outcome["transfer_id"]: outcome["status"] for outcome in outcomes(capsys)["transfers"]}
    assert [statuses[outcome["transfer_id"]] for outcome in transfers[2:]] == ["already_adopted"] * 2 + ["adopted"] * 2
    assert outgoing(near) == []
    assert_in(far, plan)


def test_a_hold_vanishing_under_the_upload_is_answered_as_a_refusal(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 3)
    plan = jobs(near, 6)
    real = tarfile.TarFile.add
    tops: list[str] = []

    def add(archive: tarfile.TarFile, name: Any, arcname: Any = None, *rest: Any, **options: Any) -> None:
        if arcname is not None and "/" not in str(arcname):
            tops.append(str(arcname))
            if len(tops) == 2:
                shutil.rmtree(name)  # the second hold vanishes as the writer reaches it
        real(archive, name, arcname, *rest, **options)

    monkeypatch.setattr(tarfile.TarFile, "add", add)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    by_id = statuses_by_id(capsys)
    gone = by_id.pop(tops[1])
    assert gone["status"] == "failed" and "taking it back failed" in gone["message"]
    assert "refused the stream, nothing of it was used" in gone["message"]
    # This side's failure is no refusal of the destination: the next segment is delivered.
    assert sorted(outcome["status"] for outcome in by_id.values()) == ["adopted"] * 3 + ["not_moved"] * 2
    assert landings(far) == [] and outgoing(near) == []


def test_the_far_side_s_sync_lines_and_warnings_are_relayed_for_each_segment(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    path = far.control / "format.json"
    document = json.loads(path.read_bytes())
    document["settings"]["io.sync_errors"] = "warn"  # a logged warning of the far side's barrier
    path.write_text(json.dumps(document), encoding="utf-8")
    plan = jobs(near, 4)
    assert run(project, "--json", *selected(plan), near_name, f"cluster:{far_name}") == 0
    lines = capsys.readouterr().err.splitlines()
    prefix = f"cluster:{far_name}: "
    synced = [line for line in lines if line.startswith((f"{prefix}made ", f"{prefix}syncing "))]
    assert len(synced) == 2 and all(" landed file(s) " in line for line in synced)
    warned = [line for line in lines if line.startswith(f"{prefix}io.sync_errors is 'warn', not fail")]
    assert len(warned) == 2  # once a segment: its barrier's one reading of the settings
    assert f"delivering 2 bundle(s) to cluster:{far_name} (segment 2 of 2)" in lines
    assert lines.count(f"releasing 2 hold(s) on {near.root}") == 2  # each segment's, right after it
    # The far side's own report of missing workflows is the client's to make, once.
    assert not [line for line in lines if line.startswith(f"{prefix}warning: these workflows are not installed")]


def test_a_whole_segment_refusal_stops_the_push_even_when_this_side_s_upload_failed(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 3)
    scripted(monkeypatch, {1: {"refuse": {"refused": "Disk quota exceeded", "workspace_id": far.workspace_id}}})
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 6)
    real = tarfile.TarFile.add
    tops: list[str] = []

    def add(archive: tarfile.TarFile, name: Any, arcname: Any = None, *rest: Any, **options: Any) -> None:
        if arcname is not None and "/" not in str(arcname):
            tops.append(str(arcname))
            if len(tops) == 2:
                shutil.rmtree(name)  # this side's upload fails too
        real(archive, name, arcname, *rest, **options)

    monkeypatch.setattr(tarfile.TarFile, "add", add)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    by_id = statuses_by_id(capsys)
    # The far side's own refusal (no stream_aborted) is of the destination: nothing more is sent, and every hold
    # this run made is taken back (but the one that vanished).
    assert len(adopts(requests)) == 1
    assert by_id.pop(tops[1])["status"] == "failed"
    assert [outcome["status"] for outcome in by_id.values()] == ["not_moved"] * 5
    assert all("Disk quota exceeded" in outcome["message"] for outcome in by_id.values())


def test_a_final_sync_failed_refusal_does_not_stop_the_push(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    failed = {"refuse": {"sync_failed": True, "workspace_id": far.workspace_id}}
    scripted(monkeypatch, {1: failed, 2: failed})
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    # The first segment's retry failed its commit too: refused, taken back; the second segment still goes.
    assert [outcome["status"] for outcome in transfers] == ["not_moved"] * 2 + ["adopted"] * 2
    assert [header.get("sync") for header in adopts(requests)] == [None, "fsync", "fsync"]


def test_a_refusal_answered_ok_is_an_unknown_outcome(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, _far, far_name = ssh
    scripted(monkeypatch, {1: {"ok_refused": True}})
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2  # never taken back
    assert all("gave no usable adoption report" in outcome["message"] for outcome in transfers)
    assert len(outgoing(near)) == 2


def test_an_installation_claimed_for_an_aborted_upload_stops_the_push_before_any_segment(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    requests = recorded_requests(monkeypatch)
    real = TransferSession.request

    def request(session: TransferSession, op: str, header: Any = None, **options: Any) -> dict[str, Any]:
        if op == "install":  # a stub answer: this side's upload aborted, the far side claiming an installation
            document: dict[str, Any] = {"workflows": [], "built": []}
            raise RequestAborted("install was aborted", response={"ok": True, "document": document, "returncode": 0})
        return real(session, op, header, **options)

    monkeypatch.setattr(TransferSession, "request", request)
    job_id = workflow_job(near, workflows(near, tmp_path), state="ready")
    assert run(project, "--json", "-q", "--job", job_id, near_name, f"cluster:{far_name}") == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "failed" and "failed before anything was adopted" in outcome["message"]
    assert f"httk job transfer --resume {near.root} cluster:{far_name}" in outcome["message"]
    assert set(shipped(outcome).values()) == {"failed"}
    assert all("aborted workflow upload as installed" in entry["reason"] for entry in outcome["workflows"])
    assert not {"adopt", "stage"} & set(ops(requests)) and find(far, job_id) is None and len(outgoing(near)) == 1


def test_the_installed_workflows_are_reported_when_every_segment_is_refused(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    job_id = workflow_job(near, workflows(near, tmp_path), state="ready")
    # Every segment refused as a whole (a staged landing is an owner scratch, which the workflows' install needs too:
    # the refusal is scripted rather than made by an unwritable landing).
    scripted(monkeypatch, {1: {"refuse": {"workspace_id": far.workspace_id}}})
    assert run(project, "--json", "--job", job_id, near_name, f"cluster:{far_name}") == 1
    captured = capsys.readouterr()
    (outcome,) = json.loads(captured.out)["transfers"]
    assert outcome["status"] == "not_moved"
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert f"installed these workflows in the destination: {GIT_ID}, local:caller" in captured.err
    assert _store.lookup(far, GIT_ID) is not None
