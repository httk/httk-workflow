"""A pull over one transfer session (plan WORKFLOW_TRANSFER_SESSION §7, T2).

The far side's ``hold``, ``holds``, ``send`` and ``release`` ops (the documents of ``job transfer --hold --json``,
``transfer status --json`` and ``job transfer --release``, and the download of held bundles and store entries, over a
real session), and the client's pull: one connection and one start, segment by segment, with per-segment outcomes.
"""

import errno
import io
import json
import os
import tarfile
import uuid
from pathlib import Path
from typing import Any, BinaryIO

import pytest
from httk.core.cli import CLIContext

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _frames, _fs, _kernel, _landing, _moving, _store
from httk.workflow._state import read_state_unowned
from httk.workflow.adapters import RequestAborted, SessionFailed, TransferSession
from httk.workflow.errors import StreamAborted
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from test_deliver import hold_jobs, outgoing_dir
from test_job_transfer import (
    adapter_calls,
    away_of,
    everyday,
    jobs,
    outcomes,
    outgoing,
    recorded_requests,
    registered,
    run,
    selected,
    single,
    starts,
    tree,
)
from test_moving import assert_in, find, io_workers  # noqa: F401 (io_workers: fixture)
from test_session_push import PATCHED_HTTK, local_project, ops
from test_sync_errors_retry import Syncfs
from test_transfer_workflows import GIT_ID, shipped, workflows
from test_transfer_workflows import job as workflow_job

#: Faults of a pull's far side, added to :data:`test_session_push.PATCHED_HTTK`: a slow hold and release; the n-th distinct held
#: bundle of the session vanishing once the download of its first member begins; the n-th send request ending the
#: process; the n-th download (the opening echo's is the first) aborted after its whole archive; a refused release;
#: and a send answering its names reversed.
PULL_PATCHES = """
if argv[:2] == ["workflow", "transfer-session"]:
    import shutil

    from httk.workflow import _moving as pulled

    if os.environ.get("HTTK_TEST_SLOW_HOLD"):
        real_hold = pulled.hold

        def slow_hold(*arguments, **options):
            time.sleep(float(os.environ["HTTK_TEST_SLOW_HOLD"]))
            return real_hold(*arguments, **options)

        pulled.hold = slow_hold
    if os.environ.get("HTTK_TEST_SLOW_RELEASE"):
        real_release = pulled.release_hold

        def slow_release(*arguments, **options):
            time.sleep(float(os.environ["HTTK_TEST_SLOW_RELEASE"]))
            return real_release(*arguments, **options)

        pulled.release_hold = slow_release
    vanish = int(os.environ.get("HTTK_TEST_VANISH_NTH", "0"))
    if vanish:
        real_archive = _transfer_session._archive
        seen = []

        def archive(tar, path, arcname, *rest):
            top = str(arcname).split("/", 1)[0]
            if "/" in str(arcname) and top not in seen:
                seen.append(top)
                if len(seen) == vanish:
                    shutil.rmtree(os.path.dirname(path))
            return real_archive(tar, path, arcname, *rest)

        _transfer_session._archive = archive
    exit_on_send = int(os.environ.get("HTTK_TEST_EXIT_ON_SEND", "0"))
    wrong_sent = os.environ.get("HTTK_TEST_WRONG_SENT")
    real_send = _transfer_session.OPS["send"]
    sends = []

    def send(request):
        sends.append(1)
        if len(sends) == exit_on_send:
            os._exit(3)
        response = real_send(request)
        if wrong_sent == "short":
            response["sent"] = response["sent"][:-1]  # a prefix, but not all that arrived
        elif wrong_sent:
            response["sent"] = list(reversed(response["sent"]))
        return response

    _transfer_session.OPS["send"] = send
    abort_at = int(os.environ.get("HTTK_TEST_ABORT_DOWNLOAD", "0"))
    if abort_at:
        real_close = _frames.StreamWriter.close
        closes = []

        def close(self, ok=True, abort=None):
            if ok and not self.closed:
                closes.append(1)
                if len(closes) == abort_at:
                    self.flush()
                    return real_close(self, False, "a scripted abort after the whole archive")
            return real_close(self, ok, abort)

        _frames.StreamWriter.close = close
    if os.environ.get("HTTK_TEST_REFUSE_RELEASE"):

        def refuse(request):
            raise _transfer_session.Refused("a scripted release refusal")

        _transfer_session.OPS["release"] = refuse
    if os.environ.get("HTTK_TEST_RELEASE_PARTIAL"):
        real_release = _transfer_session.OPS["release"]

        def partial(request):
            # The first released id answered as gone already, the last one not at all.
            response = dict(real_release(request))
            released = response["released"]
            response.update(released=released[1:-1], missing=[*response["missing"], released[0]])
            return response

        _transfer_session.OPS["release"] = partial
    if os.environ.get("HTTK_TEST_T1B_OPS"):
        for op in ("hold", "holds", "send", "release"):
            del _transfer_session.OPS[op]
    if os.environ.get("HTTK_TEST_HOLDS_MAX"):
        _transfer_session._SESSION_HOLDS_MAX = int(os.environ["HTTK_TEST_HOLDS_MAX"])
    if os.environ.get("HTTK_TEST_SLOW_MANIFEST"):
        real_read_manifest = pulled.read_manifest

        def slow_read_manifest(*arguments, **options):
            time.sleep(float(os.environ["HTTK_TEST_SLOW_MANIFEST"]))
            return real_read_manifest(*arguments, **options)

        pulled.read_manifest = slow_read_manifest
    if os.environ.get("HTTK_TEST_OWNER_FAILS"):
        from httk.workflow.workflow_cli import _transfer as transferring

        def no_owner(*arguments, **options):
            raise OSError(errno.EIO, "the owner's registration failed")

        transferring._owner = no_owner
    broken_at = int(os.environ.get("HTTK_TEST_BROKEN_DOWNLOAD", "0"))
    if broken_at:
        real_end = _frames.StreamWriter.close
        ends = []

        def end(self, ok=True, abort=None):
            if ok and not self.closed:
                ends.append(1)
                if len(ends) == broken_at:
                    raise BrokenPipeError(errno.EPIPE, "the download's output is gone")
            return real_end(self, ok, abort)

        _frames.StreamWriter.close = end
    if os.environ.get("HTTK_TEST_SLOW_RESOLVE"):
        from httk.workflow.introspection import _reading

        real_resolve_one = _reading.JobSelectorResolver.resolve_one

        def slow_resolve_one(self, selector):
            time.sleep(float(os.environ["HTTK_TEST_SLOW_RESOLVE"]))
            return real_resolve_one(self, selector)

        _reading.JobSelectorResolver.resolve_one = slow_resolve_one
    if os.environ.get("HTTK_TEST_SLOW_READ"):
        real_slow_readinto = _transfer_session._Reading.readinto

        def slow_readinto(self, buffer):
            time.sleep(float(os.environ["HTTK_TEST_SLOW_READ"]))
            return real_slow_readinto(self, buffer)

        _transfer_session._Reading.readinto = slow_readinto
    retype = os.environ.get("HTTK_TEST_RETYPE_ON_SEND")
    if retype:
        # "file" or "directory": a symlink named link becomes that between the walk's lstat and the entry's stat.
        real_lstat = os.lstat
        retyped = []

        def retyping_lstat(path, *rest, **options):
            found = real_lstat(path, *rest, **options)
            if str(path).endswith("/link") and not retyped:
                retyped.append(path)
                os.unlink(path)
                if retype == "file":
                    with open(path, "wb") as stream:
                        stream.write(b"x" * 8521)
                else:
                    os.mkdir(path)
            return found

        os.lstat = retyping_lstat
    if os.environ.get("HTTK_TEST_BROKEN_ABORT"):
        real_abort = _frames.StreamWriter.close

        def broken_abort(self, ok=True, abort=None):
            if not ok and not self.closed:
                self.closed = True
                raise BrokenPipeError(errno.EPIPE, "the download's output is gone")
            return real_abort(self, ok, abort)

        _frames.StreamWriter.close = broken_abort
    if os.environ.get("HTTK_TEST_JSON_LIMIT"):
        _frames.JSON_LIMIT = int(os.environ["HTTK_TEST_JSON_LIMIT"])
    if os.environ.get("HTTK_TEST_NOISE"):
        from httk.workflow.workflow_cli import _transfer as noisy

        real_status_document = noisy.status_document

        def noisy_status_document(*arguments, **options):
            print("\U0001f600" * int(os.environ["HTTK_TEST_NOISE"]), file=sys.stderr)
            return real_status_document(*arguments, **options)

        noisy.status_document = noisy_status_document
    if os.environ.get("HTTK_TEST_NO_MANIFEST_READS"):

        def no_read(*arguments, **options):
            raise RuntimeError("a manifest was read")

        pulled.read_manifest = no_read
    grow = os.environ.get("HTTK_TEST_GROW_ON_SEND")
    if grow:
        real_readinto = _transfer_session._Reading.readinto
        grown = []

        def readinto(self, buffer):
            path = os.readlink(f"/proc/self/fd/{self._source.fileno()}")
            if path.endswith("/" + grow) and not grown:
                grown.append(path)
                with open(path, "ab") as stream:
                    stream.write(b"LOST")  # grows while it is copied, past the size it was listed with
            return real_readinto(self, buffer)

        _transfer_session._Reading.readinto = readinto
    grow_after_check = os.environ.get("HTTK_TEST_GROW_AFTER_CHECK")
    if grow_after_check:
        real_fstat = os.fstat
        checked = []

        def growing_fstat(descriptor, *rest, **options):
            found = real_fstat(descriptor, *rest, **options)
            path = os.readlink(f"/proc/self/fd/{descriptor}")
            if path.endswith("/" + grow_after_check) and not checked:
                checked.append(path)
                with open(path, "ab") as stream:
                    stream.write(b"LOST")  # grows right after the descriptor's check
            return found

        os.fstat = growing_fstat
    replace = os.environ.get("HTTK_TEST_REPLACE_ON_SEND")
    if replace:
        real_open = os.open
        replaced = []

        def replacing_open(path, flags, *rest, **options):
            if str(path).endswith("/" + replace) and flags & os.O_NOFOLLOW and not flags & os.O_DIRECTORY:
                if not replaced:
                    replaced.append(path)
                    with open(str(path) + ".new", "wb") as stream:
                        stream.write(b"another file of the same name")
                    os.replace(str(path) + ".new", path)  # replaced between its lstat and its open
            return real_open(path, flags, *rest, **options)

        os.open = replacing_open
"""
PATCHED_PULL = PATCHED_HTTK.replace('sys.argv = ["httk", *argv]\n', PULL_PATCHES + 'sys.argv = ["httk", *argv]\n')
assert PATCHED_PULL != PATCHED_HTTK


def patched_pull(tmp_path: Path, remote: Remote) -> None:
    """Make the stand-in cluster's ``httk`` (the ``local`` adapter's too) :data:`PATCHED_PULL`."""

    wrapper = tmp_path / "patched_pull.py"
    wrapper.write_text(PATCHED_PULL, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')


def opened(bundle: Path) -> TransferSession:
    session = TransferSession(bundle, needs={"hold", "holds", "send", "release"}, timeout=60)
    session.open()
    return session


def collect(into: dict[str, Any]) -> Any:
    """A download callable that keeps the whole download (``data``) or what aborted it (``aborted``)."""

    def download(stream: BinaryIO) -> None:
        data = bytearray()
        try:
            while chunk := stream.read(1 << 16):
                data += chunk
        except StreamAborted as exc:
            into["aborted"], into["data"] = exc, bytes(data)
            raise
        into["data"] = bytes(data)

    return download


def members(data: bytes) -> list[str]:
    with tarfile.open(fileobj=io.BytesIO(data), mode="r|") as archive:
        return [member.name for member in archive]


def sized_holds(workspace: Workspace, sizes: list[int]) -> list[_moving.Hold]:
    """Hold one job per size, each with a payload file of that many bytes besides its own files."""

    holds, plan = hold_jobs(workspace, len(sizes))
    for hold, size in zip(holds, sizes, strict=True):
        (Path(hold.path) / "padding.bin").write_bytes(b"p" * size)
    del plan
    return holds


def landed_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file() and not item.is_symlink())


# -- the far side: hold, holds, send, release -----------------------------------------------------------------------


def test_the_hello_lists_the_pull_ops(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    with opened(bundle) as session:
        assert {"hold", "holds", "send", "release"} <= session.ops


def test_the_hold_op_answers_what_job_transfer_hold_json_prints(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    destination, destination_id = "near:somewhere", str(uuid.uuid4())
    # A hold already bound for the destination, and one bound elsewhere: only the first is held_before.
    with _moving_owner(far) as owner:
        earlier, elsewhere = (
            _moving.hold(far, owner, _claim(far, owner, single(far)[0]["id"]), tree=True, destination_locator=locator)
            for locator in (destination, "other:place")
        )
    family = tree(far)
    loose = [single(far) for _ in range(2)]
    header = {
        "workspace": far_name,
        "jobs": [str(mapping["id"]) for mapping, _, _ in loose],
        "state": ["succeeded"],
        "step": [],
        "placement": None,
        "tree": True,
        "destination": destination,
        "destination_id": destination_id,
    }
    with opened(bundle) as session:
        response = session.request("hold", header)
    assert response["ok"] is True, response
    holds = [_moving.Hold.from_mapping(hold) for hold in response["holds"]]
    held_ids = {member for hold in holds for member in hold.members}
    # The two named jobs, then every succeeded root: the family's succeeded child moves only with its root (paused).
    assert all(find(far, mapping["id"]) is None for mapping, _, _ in loose)
    assert find(far, family[1][0]["id"]) is not None
    assert {hold.destination_locator for hold in holds} == {destination}
    assert {hold.destination_workspace_id for hold in holds} == {destination_id}
    assert all(hold.workflows is not None for hold in holds) and len(held_ids) == len(holds)
    assert [hold["transfer_id"] for hold in response["held_before"]] == [earlier.transfer_id]
    assert elsewhere.transfer_id not in {hold["transfer_id"] for hold in response["held_before"]}
    assert response["errors"] == []
    assert "move only with their root" in response["stderr"]
    # The command line prints the same document for the same selection (nothing left to hold but held_before).
    argv = ["--hold", "--json", "--destination-id", destination_id, "--state", "succeeded", "--tree"]
    assert run(tmp_path, *argv, far_name, destination) == 0
    printed = outcomes(capsys)
    assert printed["holds"] == [] and printed["errors"] == []
    assert sorted(hold["transfer_id"] for hold in printed["held_before"]) == sorted(
        [earlier.transfer_id, *(hold.transfer_id for hold in holds)]
    )


@pytest.mark.parametrize(
    "change",
    [
        {"workspace": "no-such-workspace"},
        {"state": ["away"]},
        {"state": [], "jobs": []},
        {"step": ["a/b"]},
        {"destination_id": "not-a-uuid"},
        {"placement": "proj", "state": []},
        {"jobs": ["00000000-0000-4000-8000-000000000000"]},
    ],
    ids=["workspace", "state", "selection", "step", "destination-id", "placement", "unknown-job"],
)
def test_the_hold_op_refuses_a_bad_request_and_holds_nothing(
    tmp_path: Path, remote: Remote, change: dict[str, Any]
) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    mapping, _, _ = single(far)
    header = {"workspace": far_name, "jobs": [str(mapping["id"])], "state": ["succeeded"], "destination": "x:y"}
    with opened(bundle) as session:
        response = session.request("hold", {**header, **change})
        assert response["ok"] is False and "fatal" not in response
        assert session.is_open  # a refusal, not a failed session
    assert find(far, mapping["id"]) is not None and _moving.held(far) == []


def test_a_slow_hold_and_release_report_their_progress_and_are_no_stall(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest
) -> None:
    # Five holds, then five releases, of a second each, sequential (named jobs; the releases one by one with
    # io.workers=1), against a bound of 3 seconds: the count of the phase moves with each one, so the requests live;
    # a count standing still for the bound would fail them.
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    monkeypatch.setenv("HTTK_TEST_SLOW_HOLD", "1.0")
    monkeypatch.setenv("HTTK_TEST_SLOW_RELEASE", "1.0")
    far, far_name = registered(tmp_path, "far")
    plan = [single(far) for _ in range(5)]
    header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
    phases: list[tuple[str, Any]] = []
    session = opened(bundle)
    session._limit = 3.0
    _watch_progress(monkeypatch, phases)
    with session:
        response = session.request("hold", header)
        assert response["ok"] is True and len(response["holds"]) == 5, response
        ids = [hold["transfer_id"] for hold in response["holds"]]
        released = session.request("release", {"workspace": far_name, "transfer_ids": ids})
        assert released["ok"] is True and released["released"] == ids, released
    for name in ("hold", "release"):
        counts = [count for phase, count in phases if phase == name and count is not None]
        assert counts and counts == sorted(counts)
        # Pooled releases (io.workers=8) all end together, faster than the bound: only the sequential ones count up.
        if name == "hold" or request.getfixturevalue("io_workers") == 1:
            assert len(set(counts)) >= 3, (name, counts)


def test_the_holds_op_answers_transfer_status_json(
    tmp_path: Path, remote: Remote, capsys: pytest.CaptureFixture[str]
) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(far, 2)
    with opened(bundle) as session:
        response = session.request("holds", {"workspace": far_name})
        refused = session.request("holds", {"workspace": "no-such-workspace"})
    assert response["ok"] is True and refused["ok"] is False
    document = response["document"]
    from httk.workflow.workflow_cli import transfer_command

    assert transfer_command(["status", "--json", far_name], CLIContext("httk", tmp_path)) == 0
    printed = json.loads(capsys.readouterr().out)
    for listed in (document, printed):
        for hold in listed["holds"]:
            hold.pop("age_seconds")
    assert document == printed
    assert sorted(hold["transfer_id"] for hold in document["holds"]) == sorted(hold.transfer_id for hold in holds)


def test_send_carries_whole_bundles_in_order_up_to_the_byte_limit(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    holds = sized_holds(far, [5000, 5000, 5000, 5000])
    ids = [hold.transfer_id for hold in reversed(holds)]  # any order the client asks for
    sizes = [landed_bytes(outgoing_dir(far) / transfer_id) for transfer_id in ids]
    with opened(bundle) as session:
        # The limit is reached inside the second bundle: the bundle with which it is reached goes whole, then stop.
        got: dict[str, Any] = {}
        limit = sizes[0] + 1
        response = session.request(
            "send", {"workspace": far_name, "kind": "holds", "names": ids, "limit_bytes": limit}, download=collect(got)
        )
        assert response["ok"] is True and response["sent"] == ids[:2] and response["bytes"] == len(got["data"])
        names = members(got["data"])
        assert [name.split("/", 1)[0] for name in names if "/" not in name] == ids[:2]
        top = [name.split("/", 1)[0] for name in names]
        assert top == sorted(top, key=ids.index)  # whole bundles, in the order asked for
        for transfer_id in ids[:2]:
            assert any(name == f"{transfer_id}/padding.bin" for name in names)
        # Exactly at the limit: the first bundle alone; without a limit: everything.
        got = {}
        header = {"workspace": far_name, "kind": "holds", "names": ids, "limit_bytes": sizes[0]}
        assert session.request("send", header, download=collect(got))["sent"] == ids[:1]
        got = {}
        response = session.request(
            "send", {"workspace": far_name, "kind": "holds", "names": ids}, download=collect(got)
        )
        assert response["sent"] == ids
    # Read only: the holds are all still there.
    assert sorted(hold.transfer_id for hold in _moving.held(far)) == sorted(ids)


def test_send_counts_a_hard_link_with_its_target_s_size(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    holds = sized_holds(far, [10, 10])
    first = Path(holds[0].path)
    (first / "big.bin").write_bytes(b"b" * 100000)
    os.link(first / "big.bin", first / "linked.bin")
    ids = [hold.transfer_id for hold in holds]
    with opened(bundle) as session:
        got: dict[str, Any] = {}
        # Below the regular files alone, above them with the link counted as the copy it lands as.
        limit = landed_bytes(first) - 50000
        response = session.request(
            "send", {"workspace": far_name, "kind": "holds", "names": ids, "limit_bytes": limit}, download=collect(got)
        )
    assert response["sent"] == ids[:1]


def test_send_refuses_a_name_that_is_gone_before_any_data(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(far, 2)
    gone = "f" * 32
    called: dict[str, Any] = {}
    with opened(bundle) as session:
        header = {"workspace": far_name, "kind": "holds", "names": [holds[0].transfer_id, gone, holds[1].transfer_id]}
        response = session.request("send", header, download=collect(called))
        assert response["ok"] is False and response["missing"] == [gone] and gone in response["error"]
        assert called == {}  # no download at all
        for bad in (["../x"], ["F" * 32], [], [holds[0].transfer_id] * 2):
            assert session.request("send", {**header, "names": bad}, download=collect(called))["ok"] is False
        assert session.request("send", {**header, "kind": "other"}, download=collect(called))["ok"] is False
        assert session.request("send", {**header, "limit_bytes": 0}, download=collect(called))["ok"] is False
        assert session.is_open and called == {}


def test_a_hold_vanishing_while_it_is_sent_aborts_the_download_and_names_it(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    # Large enough for the first bundle's frames to leave before the second one vanishes.
    holds = sized_holds(far, [3 << 20, 3 << 20, 10])
    ids = [hold.transfer_id for hold in holds]
    monkeypatch.setenv("HTTK_TEST_VANISH_NTH", "2")
    got: dict[str, Any] = {}
    with opened(bundle) as session:
        with pytest.raises(RequestAborted) as aborted:
            session.request("send", {"workspace": far_name, "kind": "holds", "names": ids}, download=collect(got))
        response = aborted.value.response
        assert response["ok"] is False and response["bundle"] == ids[1] and ids[1] in response["error"]
        # The first bundle went, but the download ended in an abort, never as a complete (partial) archive.
        assert isinstance(got["aborted"], StreamAborted) and f"{ids[0]}/padding.bin".encode() in got["data"]
        assert session.is_open


def test_send_carries_store_entries(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    caller = workflows(far, tmp_path / "packages")
    entries = sorted(path.name for path in (far.root / "workflows").iterdir() if (path / "install.json").exists())
    assert caller.directory.name in entries
    got: dict[str, Any] = {}
    with opened(bundle) as session:
        response = session.request(
            "send", {"workspace": far_name, "kind": "store", "names": entries}, download=collect(got)
        )
        assert response["ok"] is True and response["sent"] == entries
        missing = session.request("send", {"workspace": far_name, "kind": "store", "names": ["absent--0"]})
        assert missing["ok"] is False and missing["missing"] == ["absent--0"]
        limited = session.request("send", {"workspace": far_name, "kind": "store", "names": entries, "limit_bytes": 1})
        assert limited["ok"] is False
    names = members(got["data"])
    for entry in entries:
        assert f"{entry}/install.json" in names and f"{entry}/package" in names
    assert not any("/builds" in name for name in names)
    # What arrives installs as the copy it is.
    target, _ = registered(tmp_path, "target")
    landing = tmp_path / "landing"
    with tarfile.open(fileobj=io.BytesIO(got["data"]), mode="r|") as archive:
        archive.extractall(landing, filter="data")
    with _moving_owner(target) as owner:
        status, installed = _store.install_copy(target, owner, landing / caller.directory.name, source_label="test")
    assert status == "installed" and installed.id == caller.id


def test_the_release_op_releases_keeps_away_copies_and_lists_what_was_gone(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(far, 3)
    ids = [hold.transfer_id for hold in holds]
    destination_id = str(uuid.uuid4())
    gone = "e" * 32
    with opened(bundle) as session:
        refused = session.request(
            "release", {"workspace": far_name, "transfer_ids": ids[:1], "destination_id": destination_id}
        )
        assert refused["ok"] is False and "keep" in refused["error"]
        assert session.request("release", {"workspace": far_name, "transfer_ids": ["x"]})["ok"] is False
        assert session.request("release", {"workspace": far_name, "transfer_ids": []})["ok"] is False
        assert _moving.held(far) != [] and len(_moving.held(far)) == 3
        response = session.request(
            "release",
            {
                "workspace": far_name,
                "transfer_ids": [ids[0], gone, ids[1]],
                "keep": True,
                "destination_id": destination_id,
            },
        )
        assert response["ok"] is True and response["released"] == [ids[0], ids[1]] and response["missing"] == [gone]
        plain = session.request("release", {"workspace": far_name, "transfer_ids": [ids[2], ids[0]]})
        assert plain["released"] == [ids[2]] and plain["missing"] == [ids[0]]
    assert _moving.held(far) == []
    for (mapping, state, _), kept in zip(plan, (True, True, False), strict=True):
        ref = find(far, mapping["id"])
        if not kept:
            assert ref is None
            continue
        assert ref is not None and ref.state == "away"
        doc = read_state_unowned(ref.path / "state.json")[0]
        assert doc is not None and doc.away is not None
        assert doc.away["workspace_id"] == destination_id and doc.away["state"] == state


# -- the client: a pull over one session ----------------------------------------------------------------------------


@pytest.fixture
def ssh(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster (its ``httk`` :data:`PATCHED_PULL`), a SRC there and a local DST."""

    project = tmp_path / "project"
    initialize_project(project, name="session-pull-ssh")
    fake_remote(project)
    patched_pull(tmp_path, remote)
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


def durable(cwd: Path, *argv: str) -> int:
    """``job transfer`` with durable publication (``run`` passes ``--no-durable``), so a landing gets its barrier."""

    return command(["job", "transfer", *argv], CLIContext("httk", cwd))


def landing_scratches(workspace: Workspace) -> list[Path]:
    return list((workspace.control / "tmp").glob("*.landing.*"))


def sends(requests: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    return [header for op, header in requests if op == "send"]


def test_a_pull_is_one_connection_and_one_start_whatever_its_segments(
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
        plan = jobs(far, count)
        assert run(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
        assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * count
        # No adapter call at all: one session holds, then carries each segment and releases it right after it.
        assert calls == []
        assert ops(requests) == ["hold", *["send", "release"] * segments]
        assert all(header["kind"] == "holds" and header["workspace"] == far_name for header in sends(requests))
        commands = remote.commands()[before:]
        measured.append((len(commands), starts(commands)))
        assert "workflow transfer-session" in commands[0]
        assert_in(near, plan)
        assert outgoing(far) == [] and landing_scratches(near) == []
    assert measured == [(1, 1), (1, 1)]


def test_workflows_arrive_before_the_first_adoption_in_the_one_start(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    job_id = workflow_job(far, workflows(far, tmp_path / "far-packages"))
    assert run(project, "--json", "--job", job_id, f"cluster:{far_name}", near_name) == 0
    (outcome,) = outcomes(capsys)["transfers"]
    assert shipped(outcome) == {GIT_ID: "installed", "local:caller": "installed"}
    assert calls == [] and ops(requests) == ["hold", "send", "send", "release"]
    store, held = sends(requests)
    assert store["kind"] == "store" and held["kind"] == "holds" and "limit_bytes" not in store
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert _store.closure(near, "local:caller", check_builds=True).ok  # built here, after the release
    assert find(near, job_id) is not None and landing_scratches(near) == []


def test_an_older_far_side_costs_one_refused_start_then_the_adapter_path(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_OLD_FAR_SIDE", "1")
    calls = adapter_calls(monkeypatch)
    plan = jobs(far, 3)
    assert run(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    assert f"cluster:{far_name} serves no transfer session" in captured.err
    assert calls == ["job transfer", "pull", "job transfer"]
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (4, 3) and "workflow transfer-session" in commands[0]
    assert_in(near, plan)
    assert outgoing(far) == []


def test_a_selection_is_one_hold_request_carrying_every_selector(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    argv = ["--json", "--tree", "--state", "succeeded", "--step", "start", "--placement", "proj"]
    assert run(project, *argv, *selected(plan[:1]), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    (hold,) = [header for op, header in requests if op == "hold"]
    assert hold == {
        "workspace": far_name,
        "jobs": [str(plan[0][0]["id"])],
        "state": ["succeeded"],
        "step": ["start"],
        "placement": "proj",
        "tree": True,
        "destination": str(near.root),
        "destination_id": near.workspace_id,
    }


def test_the_terminal_is_validated_before_the_commit_and_the_commit_before_any_adoption(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    near.configure_settings({"io.sync": "syncfs"})
    events: list[str] = []

    def logged(owner: Any, name: str, label: str) -> None:
        real = getattr(owner, name)

        def call(*arguments: Any, **options: Any) -> Any:
            result = real(*arguments, **options)
            events.append(label)
            return result

        monkeypatch.setattr(owner, name, call)

    logged(_frames.StreamReader, "finish", "terminal")
    logged(_fs.Barrier, "commit", "commit")
    logged(_moving, "adopt", "adopt")
    plan = jobs(far, 2)
    assert durable(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    commit = events.index("commit")
    # The opening echo's terminal, then the download's, validated before its landing is committed; the adoptions
    # only after that commit.
    assert events[:commit] == ["terminal", "terminal"] and "adopt" in events[commit:]
    assert_in(near, plan)


def test_a_download_aborted_after_its_whole_archive_adopts_nothing_and_leaves_no_landing(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_ABORT_DOWNLOAD", "2")  # the first send's (the echo's is the first)
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 4)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 4
    assert all("--resume" in outcome["message"] for outcome in transfers)
    assert any("aborted the download" in outcome["message"] for outcome in transfers)
    assert ops(requests) == ["hold", "send"]  # a far side out of step is asked nothing more
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert len(outgoing(far)) == 4 and landing_scratches(near) == []
    monkeypatch.delenv("HTTK_TEST_ABORT_DOWNLOAD")
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 4
    assert_in(near, plan)


def test_a_send_carries_a_prefix_and_the_rest_of_its_segment_follows(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 3)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BYTES", 1)  # SRC closes every send after its first bundle
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 5)
    assert run(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 5
    ids = sorted(outcome["transfer_id"] for outcome in transfers)
    asked = [header["names"] for header in sends(requests)]
    # Segments of 3 and 2: each send asks for what is left of its segment and gets its first bundle.
    assert [len(names) for names in asked] == [3, 2, 1, 2, 1]
    assert asked[0][1:] == asked[1] and asked[1][1:] == asked[2] and asked[3][1:] == asked[4]
    assert sorted(names[0] for names in asked) == ids
    assert {header["limit_bytes"] for header in sends(requests)} == {1}
    assert ops(requests) == ["hold", *["send", "release"] * 5]
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(near, plan)


@pytest.mark.parametrize("when", ["before", "during"])
def test_a_hold_gone_before_or_while_it_is_sent_fails_alone_and_the_rest_arrive(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    when: str,
) -> None:
    project, near, near_name, far, far_name = ssh
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 3)
    if when == "during":
        monkeypatch.setenv("HTTK_TEST_VANISH_NTH", "2")
    else:
        real = TransferSession.request

        def released_first(session: TransferSession, op: str, header: Any = None, **options: Any) -> Any:
            if op == "send" and len(sends(requests)) == 1:  # another actor releases a hold before it is sent
                with _moving_owner(far) as owner:
                    assert _moving.release_hold(far, owner, header["names"][1])
            return real(session, op, header, **options)

        monkeypatch.setattr(TransferSession, "request", released_first)
        requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = sends(requests)
    gone = first["names"][1]
    assert second["names"] == [name for name in first["names"] if name != gone]  # the rest, once more
    assert by_id[gone]["status"] == "failed" and "could not send it" in by_id[gone]["message"]
    assert [by_id[name]["status"] for name in second["names"]] == ["adopted"] * 2
    assert ops(requests) == ["hold", "send", "send", "release"]
    assert sum(find(near, mapping["id"]) is not None for mapping, _, _ in plan) == 2
    assert landing_scratches(near) == []


def test_a_failed_commit_is_fetched_once_more_with_an_fsync_of_every_file_and_later_segments_too(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    near.configure_settings({"io.sync": "syncfs"})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    Syncfs(monkeypatch, [None, errno.EIO])  # the first landing's prime, then its commit
    fsyncs = receptions(monkeypatch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 4)
    assert durable(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 4
    assert fsyncs == [False, True, True]
    assert ops(requests) == ["hold", "send", "send", "release", "send", "release"]
    assert captured.err.count("again, with an fsync of every file") == 1
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(near, plan)
    assert outgoing(far) == [] and landing_scratches(near) == []


def test_a_second_failure_after_the_fsync_refetch_stops_the_pull_and_resume_adopts(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    near.configure_settings({"io.sync": "syncfs"})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    Syncfs(monkeypatch, [None, errno.EIO])
    fsyncs = receptions(monkeypatch, failing_fsync=True)  # the fsync retry's landing fails too
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    assert durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    (asked,) = {header["names"][0] for header in sends(requests)}
    later = next(name for name in by_id if name != asked)
    assert by_id[asked]["status"] == "failed" and "an fsync failed" in by_id[asked]["message"]
    assert by_id[later]["status"] == "failed"
    assert "not delivered, since the transfer session stopped" in by_id[later]["message"]
    assert all("--resume" in outcome["message"] for outcome in by_id.values())
    # The second segment was never asked for.
    assert fsyncs == [False, True] and ops(requests) == ["hold", "send", "send"]
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert len(outgoing(far)) == 2 and landing_scratches(near) == []
    monkeypatch.setattr(_landing, "receive_tar", _REAL_RECEIVE)
    assert durable(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    assert_in(near, plan)


def test_a_far_side_dying_in_a_segment_keeps_it_and_the_later_ones_held_and_resume_lists_them(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 6)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    first = sends(requests)[0]["names"]
    by_id = {outcome["transfer_id"]: outcome for outcome in transfers}
    assert [by_id[name]["status"] for name in first] == ["adopted"] * 2
    rest = [outcome for outcome in transfers if outcome["transfer_id"] not in first]
    assert [outcome["status"] for outcome in rest] == ["failed"] * 4
    assert all(f"--resume cluster:{far_name} {near.root}" in outcome["message"] for outcome in rest)
    assert ops(requests) == ["hold", "send", "release", "send"]
    assert sum(find(near, mapping["id"]) is not None for mapping, _, _ in plan) == 2
    assert sorted(outgoing(far)) == sorted(outcome["transfer_id"] for outcome in rest)
    assert landing_scratches(near) == []
    # --resume lists the holds over a new session (one connection, one start) and delivers them.
    monkeypatch.delenv("HTTK_TEST_EXIT_ON_SEND")
    requests.clear()
    before = len(remote.commands())
    assert run(project, "--json", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 4
    assert ops(requests) == ["holds", "send", "release", "send", "release"]
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (1, 1)
    assert_in(near, plan)
    assert outgoing(far) == []


def test_the_holds_an_earlier_run_left_are_driven_with_the_new_ones_from_held_before(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "1")
    earlier = jobs(far, 2)
    assert run(project, "--json", "-q", *selected(earlier), f"cluster:{far_name}", near_name) == 1
    capsys.readouterr()
    assert len(outgoing(far)) == 2
    monkeypatch.delenv("HTTK_TEST_EXIT_ON_SEND")
    requests = recorded_requests(monkeypatch)
    later = jobs(far, 1)
    assert run(project, "--json", *selected(later), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 3
    # The hold's answer listed the earlier holds bound for this destination: no holds request.
    assert ops(requests) == ["hold", "send", "release"] and len(sends(requests)[0]["names"]) == 3
    assert_in(near, [*earlier, *later])
    assert outgoing(far) == []


def test_a_wrong_answer_to_a_send_adopts_nothing_and_stops(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_WRONG_SENT", "1")
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("not the prefix of them that arrived" in outcome["message"] for outcome in transfers)
    assert ops(requests) == ["hold", "send"]
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert len(outgoing(far)) == 2 and landing_scratches(near) == []


def test_a_refused_release_leaves_the_jobs_here_and_names_the_remedy(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_REFUSE_RELEASE", "1")
    plan = jobs(far, 2)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 2
    for outcome in transfers:
        assert f"the jobs are in {near.root}, but the hold was not released" in outcome["message"]
        assert "a scripted release refusal" in outcome["message"]
        assert f"httk job transfer --release {outcome['transfer_id']} cluster:{far_name}" in outcome["message"]
    assert_in(near, plan)
    assert len(outgoing(far)) == 2
    monkeypatch.delenv("HTTK_TEST_REFUSE_RELEASE")
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["already_adopted"] * 2
    assert outgoing(far) == []


def test_a_session_failing_in_a_release_stops_the_pull(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "release")
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 4)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first = sends(requests)[0]["names"]
    assert ops(requests) == ["hold", "send", "release"]
    for name in first:
        assert by_id[name]["status"] == "adopted" and "the hold was not released" in by_id[name]["message"]
    rest = [outcome for name, outcome in by_id.items() if name not in first]
    assert [outcome["status"] for outcome in rest] == ["failed"] * 2
    assert all("not delivered" in outcome["message"] for outcome in rest)
    assert sum(find(near, mapping["id"]) is not None for mapping, _, _ in plan) == 2
    assert len(outgoing(far)) == 4


def test_keep_records_this_workspace_s_id_in_the_away_copies(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    assert run(project, "--json", "--keep", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["kept"] for outcome in outcomes(capsys)["transfers"]] == [True, True]
    (release,) = [header for op, header in requests if op == "release"]
    assert release["keep"] is True and release["destination_id"] == near.workspace_id
    for mapping, state, _ in plan:
        ref = find(far, mapping["id"])
        assert ref is not None and ref.state == "away"
        doc = read_state_unowned(ref.path / "state.json")[0]
        assert doc is not None and doc.away is not None
        assert (doc.away["workspace_id"], doc.away["state"]) == (near.workspace_id, state)
    assert_in(near, plan)


@pytest.mark.parametrize("failure", ["open", "hold"])
def test_a_session_that_cannot_open_or_fails_in_the_hold_drives_nothing(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv(*(("HTTK_FAKE_SSH_REFUSE", "1") if failure == "open" else ("HTTK_TEST_EXIT_ON_OP", "hold")))
    calls = adapter_calls(monkeypatch)
    plan = jobs(far, 2)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    document = outcomes(capsys)
    assert document["transfers"] == []
    (error,) = document["errors"]
    expected = "did not open" if failure == "open" else f"--resume cluster:{far_name} {near.root}"
    assert expected in error
    assert calls == [] and len(remote.commands()) == 1  # never a second login
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)


# -- review corrections (round 1) -------------------------------------------------------------------------------------


@pytest.mark.parametrize("change", ["grow", "grow-after-check", "replace"])
def test_a_file_that_grows_or_is_replaced_while_it_is_sent_is_never_adopted(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    change: str,
) -> None:
    project, near, near_name, far, far_name = ssh
    variable = {"grow": "GROW_ON_SEND", "grow-after-check": "GROW_AFTER_CHECK", "replace": "REPLACE_ON_SEND"}[change]
    monkeypatch.setenv(f"HTTK_TEST_{variable}", "input.txt")
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 1)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "failed" and "could not be sent whole" in outcome["message"]
    expected = {"grow": "grew", "grow-after-check": "changed size", "replace": "replaced or changed size"}[change]
    assert expected in outcome["message"]
    assert "stays held" in outcome["message"]
    assert ops(requests) == ["hold", "send"]  # nothing adopted, so nothing released
    assert find(near, plan[0][0]["id"]) is None and landing_scratches(near) == []
    assert outgoing(far) == [outcome["transfer_id"]]


def test_send_closes_a_request_at_the_member_limit_never_splitting_a_bundle(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    far, far_name = registered(tmp_path, "far")
    holds = sized_holds(far, [10, 10, 10])
    first = Path(holds[0].path)
    for index in range(30):  # a file-heavy first bundle
        (first / f"many-{index}").write_bytes(b"m")
    ids = [hold.transfer_id for hold in holds]
    counts = [sum(1 for _ in (outgoing_dir(far) / name).rglob("*")) + 1 for name in ids]
    with opened(bundle) as session:
        for limit, expected in ((counts[0], 1), (counts[0] + 1, 2), (counts[0] + counts[1], 2), (1, 1)):
            got: dict[str, Any] = {}
            header = {"workspace": far_name, "kind": "holds", "names": ids, "limit_members": limit}
            response = session.request("send", header, download=collect(got))
            assert response["sent"] == ids[:expected], (limit, response)
            assert len(members(got["data"])) == sum(counts[:expected])
        refused = session.request("send", {"workspace": far_name, "kind": "store", "names": ["x"], "limit_members": 1})
        assert refused["ok"] is False and "limit_members" in refused["error"]


def test_file_heavy_bundles_travel_in_requests_closed_at_the_member_limit(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_MEMBERS", 5)  # every bundle has more members than that
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    assert run(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    asked = sends(requests)
    assert [len(header["names"]) for header in asked] == [2, 1] and {h["limit_members"] for h in asked} == {5}
    assert ops(requests) == ["hold", "send", "release", "send", "release"]
    assert_in(near, plan)


def test_slow_listings_of_holds_report_their_progress_and_are_no_stall(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Each hold's manifest takes 0.8 seconds to read against a bound of 4 seconds: the listing of the holds (and the
    # held_before listing of a hold) takes longer than the bound, but its count moves with every hold read.
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    monkeypatch.setenv("HTTK_TEST_SLOW_MANIFEST", "0.8")
    far, far_name = registered(tmp_path, "far")
    with _moving_owner(far) as owner:
        for _ in range(7):
            _moving.hold(far, owner, _claim(far, owner, single(far)[0]["id"]), tree=True, destination_locator="x:y")
    mapping, _, _ = single(far)
    phases: list[tuple[str, Any]] = []
    session = opened(bundle)
    session._limit = 4.0
    _watch_progress(monkeypatch, phases)
    with session:
        listed = session.request("holds", {"workspace": far_name})
        assert listed["ok"] is True and len(listed["document"]["holds"]) == 7, listed
        held = session.request("hold", {"workspace": far_name, "jobs": [str(mapping["id"])], "destination": "x:y"})
        assert held["ok"] is True and len(held["held_before"]) == 7, held
    counts = [count for phase, count in phases if phase == "list" and count is not None]
    assert len(set(counts)) >= 4


def test_a_slow_resolution_of_named_jobs_reports_its_progress_and_is_no_stall(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Five --job selectors resolved in 0.8 seconds each against a bound of 2 seconds: the count moves with each.
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    monkeypatch.setenv("HTTK_TEST_SLOW_RESOLVE", "0.8")
    far, far_name = registered(tmp_path, "far")
    plan = [single(far) for _ in range(5)]
    session = opened(bundle)
    session._limit = 2.0
    with session:
        header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
        response = session.request("hold", header)
    assert response["ok"] is True and len(response["holds"]) == 5, response


def test_a_slow_read_of_one_large_file_reports_its_progress_and_is_no_stall(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    # One file of 1 MiB read in pieces of 0.05 seconds each against a bound of 1 second: no member is written while
    # it is read, but the bytes read move the count.
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    monkeypatch.setattr(_frames, "TICK", 0.1)
    monkeypatch.setenv("HTTK_TEST_TICK", "0.1")
    monkeypatch.setenv("HTTK_TEST_SLOW_READ", "0.05")
    far, far_name = registered(tmp_path, "far")
    holds = sized_holds(far, [1 << 20])
    session = opened(bundle)
    session._limit = 1.0
    got: dict[str, Any] = {}
    with session:
        header = {"workspace": far_name, "kind": "holds", "names": [holds[0].transfer_id]}
        response = session.request("send", header, download=collect(got))
    assert response["ok"] is True and response["sent"] == [holds[0].transfer_id]


@pytest.mark.parametrize("resume", [False, True], ids=["pull", "resume"])
def test_an_answer_too_large_for_one_frame_takes_the_adapter_path_for_the_command(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    resume: bool,
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(far, 3)
    if resume:
        monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "1")  # leaves the three held
        assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
        capsys.readouterr()
        monkeypatch.delenv("HTTK_TEST_EXIT_ON_SEND")
    monkeypatch.setenv("HTTK_TEST_HOLDS_MAX", "2")
    calls = adapter_calls(monkeypatch)
    before = len(remote.commands())
    argv = ["--resume"] if resume else selected(plan)
    assert run(project, "--json", *argv, f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 3
    assert "more than one answer carries" in captured.err and "using the adapter operations" in captured.err
    # The refused session, then the adapter path: the hold (or the listing) call, the pull, the release call.
    assert calls == ["transfer status" if resume else "job transfer", "pull", "job transfer"]
    commands = remote.commands()[before:]
    assert (len(commands), starts(commands)) == (4, 3)
    assert_in(near, plan)
    assert outgoing(far) == []


def test_the_hold_op_refuses_a_selection_too_large_before_holding_anything(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 1)  # an earlier hold counts too
    plan = [single(far) for _ in range(2)]
    header = {"workspace": far_name, "jobs": [str(mapping["id"]) for mapping, _, _ in plan], "destination": "x:y"}
    monkeypatch.setenv("HTTK_TEST_HOLDS_MAX", "2")
    with opened(bundle) as session:
        response = session.request("hold", header)
        assert response["ok"] is False and response["too_large"] is True, response
        assert session.request("holds", {"workspace": far_name})["ok"] is True  # 1 hold: it fits
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan) and len(_moving.held(far)) == 1


@pytest.mark.parametrize("op", ["hold", "release"])
def test_an_error_from_the_owner_s_registration_on_is_fatal(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, op: str
) -> None:
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(far, 1)
    mapping, _, _ = single(far)
    header = (
        {"workspace": far_name, "jobs": [str(mapping["id"])], "destination": "x:y"}
        if op == "hold"
        else {"workspace": far_name, "transfer_ids": [holds[0].transfer_id]}
    )
    monkeypatch.setenv("HTTK_TEST_OWNER_FAILS", "1")
    session = opened(bundle)
    with pytest.raises(SessionFailed, match="registration failed") as failed:
        session.request(op, header)
    session.close()
    assert failed.value.response is not None and failed.value.response["fatal"] is True


def test_a_download_that_cannot_be_written_is_fatal(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    holds, _ = hold_jobs(far, 1)
    monkeypatch.setenv("HTTK_TEST_BROKEN_DOWNLOAD", "2")  # the first send's (the echo's is the first)
    session = opened(bundle)
    with pytest.raises(SessionFailed) as failed:
        session.request(
            "send", {"workspace": far_name, "kind": "holds", "names": [holds[0].transfer_id]}, download=collect({})
        )
    session.close()
    assert failed.value.response is not None and failed.value.response["fatal"] is True
    assert "_SendFailed" in failed.value.response["error"]


def test_store_names_that_are_not_one_plain_name_are_refused(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    _, far_name = registered(tmp_path, "far")
    with opened(bundle) as session:
        for name in ("..", ".", "a/b", "../workflows"):
            response = session.request("send", {"workspace": far_name, "kind": "store", "names": [name]})
            assert response["ok"] is False and "not a store name" in response["error"], name


def test_a_mixed_destination_resume_syncs_every_later_landing_file_by_file_after_one_failed_commit(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    other, other_name = registered(tmp_path, "other")
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "1")
    plans = {}
    for workspace, name in ((near, near_name), (other, other_name)):
        workspace.configure_settings({"io.sync": "syncfs"})
        plans[name] = jobs(far, 1)
        assert run(project, "--json", "-q", *selected(plans[name]), f"cluster:{far_name}", name) == 1
        capsys.readouterr()
    monkeypatch.delenv("HTTK_TEST_EXIT_ON_SEND")
    Syncfs(monkeypatch, [None, errno.EIO])  # the first destination's first landing: its prime, then its commit
    fsyncs = receptions(monkeypatch)
    requests = recorded_requests(monkeypatch)
    assert durable(project, "--json", "-q", "--resume", f"cluster:{far_name}") == 0
    assert [outcome["status"] for outcome in outcomes(capsys)["transfers"]] == ["adopted"] * 2
    # The first destination's landing again with an fsync of every file, and the other destination's directly.
    assert fsyncs == [False, True, True]
    assert ops(requests) == ["holds", "send", "send", "release", "send", "release"]
    assert_in(near, plans[near_name])
    assert_in(other, plans[other_name])


def test_a_hold_gone_from_src_says_so_and_a_second_refusal_leaves_the_rest_of_its_segment_unfetched(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    real = TransferSession.request
    asked: list[dict[str, Any]] = []

    def released_ahead(session: TransferSession, op: str, header: Any = None, **options: Any) -> Any:
        if op == "send":  # another actor releases the second hold asked for, before each of the first two sends
            asked.append(dict(header))
            if len(asked) <= 2:
                with _moving_owner(far) as owner:
                    assert _moving.release_hold(far, owner, header["names"][1])
        return real(session, op, header, **options)

    monkeypatch.setattr(TransferSession, "request", released_ahead)
    plan = jobs(far, 4)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    by_id = {outcome["transfer_id"]: outcome for outcome in outcomes(capsys)["transfers"]}
    first, second = asked
    gone = [first["names"][1], second["names"][1]]
    for name in gone:
        message = by_id[name]["message"]
        assert by_id[name]["status"] == "failed" and f"it is gone from cluster:{far_name}" in message
        assert "stays held" not in message
    rest = [name for name in second["names"] if name not in gone]
    assert len(rest) == 2 and len(asked) == 2  # not fetched a third time
    for name in rest:
        message = by_id[name]["message"]
        assert message.startswith("not fetched: another hold of its segment was refused twice")
        assert f"({gone[1]}: " in message and f"stays held on cluster:{far_name}: --resume retries it" in message
        assert message.count("stays held") == 1
    assert sorted(outgoing(far)) == sorted(rest)
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_refusal_here_naming_no_hold_stops_the_pull(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    receptions(monkeypatch, refusing="refused the stream, nothing of it was used: [Errno 122] Disk quota exceeded")
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 3)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 3
    stopped = [
        outcome for outcome in transfers if "not delivered, since the transfer session stopped" in outcome["message"]
    ]
    (refused,) = [outcome for outcome in transfers if outcome not in stopped]
    assert len(stopped) == 2 and "refused in" in refused["message"] and "Disk quota exceeded" in refused["message"]
    assert all("Disk quota exceeded" in outcome["message"] for outcome in stopped)  # the reason it stopped
    assert all("--resume" in outcome["message"] for outcome in transfers)
    assert ops(requests) == ["hold", "send"]
    assert len(outgoing(far)) == 3 and landing_scratches(near) == []


def test_sent_naming_fewer_holds_than_arrived_adopts_nothing(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_WRONG_SENT", "short")
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("not the prefix of them that arrived" in outcome["message"] for outcome in transfers)
    assert ops(requests) == ["hold", "send"]
    assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    assert len(outgoing(far)) == 2 and landing_scratches(near) == []


def test_quiet_prints_no_sync_line_on_a_durable_pull(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    near.configure_settings({"io.sync": "syncfs"})
    for quiet in (False, True):
        plan = jobs(far, 1)
        argv = ["--json", *(["-q"] if quiet else []), *selected(plan), f"cluster:{far_name}", near_name]
        assert durable(project, *argv) == 0
        captured = capsys.readouterr()
        assert ("landed file(s)" in captured.err) is not quiet, captured.err


def test_a_release_answering_gone_counts_as_confirmed_and_an_unanswered_one_is_reported(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_RELEASE_PARTIAL", "1")
    plan = jobs(far, 3)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["adopted"] * 3
    unconfirmed = [outcome for outcome in transfers if "message" in outcome]
    (outcome,) = unconfirmed  # the one answered neither released nor gone
    assert "but the hold was not released: not confirmed by the source" in outcome["message"]
    assert "--resume" in outcome["message"]
    assert_in(near, plan)


def test_a_far_side_serving_only_the_push_ops_falls_back_for_a_pull(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, _near, near_name, far, far_name = ssh
    monkeypatch.setenv("HTTK_TEST_T1B_OPS", "1")
    calls = adapter_calls(monkeypatch)
    plan = jobs(far, 2)
    assert run(project, "--json", *selected(plan), f"cluster:{far_name}", near_name) == 0
    captured = capsys.readouterr()
    assert [outcome["status"] for outcome in json.loads(captured.out)["transfers"]] == ["adopted"] * 2
    assert "lacks the ops hold, holds, release, send" in captured.err
    assert calls == ["job transfer", "pull", "job transfer"]
    commands = remote.commands()
    assert (len(commands), starts(commands)) == (4, 3)


def test_a_remote_source_keeps_its_copies_through_the_session_s_release(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    requests = recorded_requests(monkeypatch)
    ready = single(far, "ready", 12)
    assert everyday("push", project, "--json", "-q", f"cluster:{far_name}", near_name) == 0
    assert [outcome["kept"] for outcome in outcomes(capsys)["transfers"]] == [True]
    (release,) = [header for op, header in requests if op == "release"]
    assert release["keep"] is True and release["destination_id"] == near.workspace_id
    away = away_of(far, ready[0]["id"])
    assert (away["workspace_id"], away["locator"]) == (near.workspace_id, str(near.root))
    # A refused release keeps no copies and says why; the hand release relays --keep to the remote.
    monkeypatch.setenv("HTTK_TEST_REFUSE_RELEASE", "1")
    failed = single(far, "failed", 13)
    argv = ["--json", "-q", "--keep", "--job", str(failed[0]["id"]), f"cluster:{far_name}", near_name]
    assert run(project, *argv) == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["kept"] is False and "but the hold was not released: " in outcome["message"]
    assert "a scripted release refusal" in outcome["message"]
    (held,) = outgoing(far)
    assert f"--release {held} --keep --destination-id {near.workspace_id} cluster:{far_name}" in outcome["message"]
    monkeypatch.delenv("HTTK_TEST_REFUSE_RELEASE")
    assert run(project, "--release", held, "--keep", "--destination-id", near.workspace_id, f"cluster:{far_name}") == 0
    assert capsys.readouterr().out == f"released {held}\n"
    assert away_of(far, failed[0]["id"])["state"] == "failed"


def test_workflows_are_installed_before_the_first_adoption(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, _near, near_name, far, far_name = ssh
    events: list[str] = []
    for owner, name, label in ((_store, "install_copy", "install"), (_moving, "adopt", "adopt")):
        real = getattr(owner, name)

        def logged(*arguments: Any, _real: Any = real, _label: str = label, **options: Any) -> Any:
            result = _real(*arguments, **options)
            events.append(_label)
            return result

        monkeypatch.setattr(owner, name, logged)
    job_id = workflow_job(far, workflows(far, tmp_path / "far-packages"))
    assert run(project, "--json", "--job", job_id, f"cluster:{far_name}", near_name) == 0
    assert outcomes(capsys)["transfers"][0]["status"] == "adopted"
    assert events == ["install", "install", "adopt"]


@pytest.mark.parametrize("failure", ["ignore", "unsupported-fail", "unsupported-ignore"])
def test_the_sync_settings_of_a_session_pull(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str,
) -> None:
    # Under ignore a commit error is trusted (one fetch); an unsupported syncfs at the commit is fetched again with
    # an fsync of every file under fail, and under ignore (where only that is not trusted) stops the pull.
    project, near, near_name, far, far_name = ssh
    policy = "fail" if failure == "unsupported-fail" else "ignore"
    near.configure_settings({"io.sync": "syncfs", "io.sync_errors": policy})
    Syncfs(monkeypatch, [None, errno.EIO, None] if failure == "ignore" else [None, errno.ENOSYS])
    fsyncs = receptions(monkeypatch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 2)
    code = durable(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name)
    statuses = [outcome["status"] for outcome in outcomes(capsys)["transfers"]]
    if failure == "unsupported-ignore":
        assert code == 1 and statuses == ["failed"] * 2 and fsyncs == [False]
        assert ops(requests) == ["hold", "send"] and len(outgoing(far)) == 2
        assert all(find(near, mapping["id"]) is None for mapping, _, _ in plan)
    else:
        assert code == 0 and statuses == ["adopted"] * 2
        assert fsyncs == ([False] if failure == "ignore" else [False, True])
        assert_in(near, plan)


# -- review corrections (round 2) -------------------------------------------------------------------------------------


@pytest.mark.parametrize("retype", ["file", "directory"])
def test_a_symlink_that_changes_its_type_while_it_is_sent_is_never_adopted(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    retype: str,
) -> None:
    project, near, near_name, far, far_name = ssh
    plan = jobs(far, 1)
    ref = find(far, plan[0][0]["id"])
    assert ref is not None
    (ref.path / "link").symlink_to("input.txt")
    monkeypatch.setenv("HTTK_TEST_RETYPE_ON_SEND", retype)
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    (outcome,) = outcomes(capsys)["transfers"]
    assert outcome["status"] == "failed" and "changed its type while it was sent" in outcome["message"]
    assert "could not be sent whole" in outcome["message"] and "stays held" in outcome["message"]
    assert ops(requests) == ["hold", "send"]
    assert find(near, plan[0][0]["id"]) is None and landing_scratches(near) == []
    assert outgoing(far) == [outcome["transfer_id"]]


def test_a_regular_file_turned_directory_before_its_open_leaves_no_descriptor_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.workflow_cli import _transfer_session as far_side

    source = tmp_path / "bundle"
    source.mkdir()
    (source / "file").write_bytes(b"payload")
    real_open = os.open

    def turning_open(path: Any, flags: int, *rest: Any, **options: Any) -> int:
        if str(path) == str(source / "file") and not flags & os.O_DIRECTORY:
            os.unlink(path)
            os.mkdir(path)  # a directory now, opened all the same: O_RDONLY without O_DIRECTORY
        return real_open(path, flags, *rest, **options)

    monkeypatch.setattr(os, "open", turning_open)
    before = len(os.listdir("/proc/self/fd"))
    with tarfile.open(fileobj=io.BytesIO(), mode="w|") as archive, pytest.raises(OSError):
        far_side._archive(archive, str(source / "file"), "bundle/file", lambda _info: None, lambda _count: None)
    assert len(os.listdir("/proc/self/fd")) == before


def test_an_abort_that_cannot_be_written_is_fatal(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    holds = sized_holds(far, [10])
    monkeypatch.setenv("HTTK_TEST_REPLACE_ON_SEND", "padding.bin")  # a changed source: the download is aborted
    monkeypatch.setenv("HTTK_TEST_BROKEN_ABORT", "1")  # and its abort cannot be written
    session = opened(bundle)
    with pytest.raises(SessionFailed) as failed:
        session.request(
            "send", {"workspace": far_name, "kind": "holds", "names": [holds[0].transfer_id]}, download=collect({})
        )
    session.close()
    assert failed.value.response is not None and failed.value.response["fatal"] is True
    assert "_SendFailed" in failed.value.response["error"] and "abort" in failed.value.response["error"]


def test_the_holds_answer_is_measured_whole_and_counted_before_anything_is_read(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, bundle = local_project(tmp_path)
    patched_pull(tmp_path, remote)
    far, far_name = registered(tmp_path, "far")
    hold_jobs(far, 2)
    monkeypatch.setenv("HTTK_TEST_JSON_LIMIT", "200000")
    for noise, fits in ((20000, False), (2000, True)):
        # 20,000 characters of diagnostics that each take 12 bytes once escaped: 240,000 bytes, beyond the limit.
        monkeypatch.setenv("HTTK_TEST_NOISE", str(noise))
        with opened(bundle) as session:
            response = session.request("holds", {"workspace": far_name})
        assert response["ok"] is fits, response.get("error")
        assert fits or response["too_large"] is True
    # Over the count, refused before any manifest is read (reading one would be fatal here).
    monkeypatch.delenv("HTTK_TEST_NOISE")
    monkeypatch.setenv("HTTK_TEST_HOLDS_MAX", "1")
    monkeypatch.setenv("HTTK_TEST_NO_MANIFEST_READS", "1")
    with opened(bundle) as session:
        response = session.request("holds", {"workspace": far_name})
    assert response["ok"] is False and response["too_large"] is True and "fatal" not in response


def test_a_landing_that_cannot_be_made_stops_the_pull_before_its_request(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = ssh
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    real_scratch = _kernel.Owner.scratch
    made: list[str] = []

    def scratch(owner: Any, purpose: str) -> Path:
        if purpose == "landing" and not made:
            made.append(purpose)
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_scratch(owner, purpose)

    monkeypatch.setattr(_kernel.Owner, "scratch", scratch)
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 3)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 3
    assert all("--resume" in outcome["message"] for outcome in transfers)
    assert sum("no landing could be made" in outcome["message"] for outcome in transfers) == 3
    assert sum("not delivered, since the transfer session stopped" in o["message"] for o in transfers) == 2
    assert ops(requests) == ["hold"]  # no segment asked for, nothing released
    assert len(outgoing(far)) == 3 and all(find(near, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_too_large_listing_that_is_not_the_first_request_is_no_fallback(
    ssh: tuple[Path, Workspace, str, Workspace, str],
    remote: Remote,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, _near, near_name, far, far_name = ssh
    hold_jobs(far, 1)  # one hold there: more than a listing may carry with the cap at 0
    monkeypatch.setenv("HTTK_TEST_HOLDS_MAX", "0")
    calls = adapter_calls(monkeypatch)
    requests = recorded_requests(monkeypatch)
    # A job that is not there: the hold is refused (not as too large), then the listing of the holds is.
    missing = str(uuid.UUID(int=12345))
    code = run(project, "--json", "-q", "--job", missing, f"cluster:{far_name}", near_name)
    captured = capsys.readouterr()
    assert code != 0 and "transfer status failed" in captured.err and "more than one answer carries" in captured.err
    assert "using the adapter operations" not in captured.err
    assert ops(requests) == ["hold", "holds"] and calls == []
    assert len(remote.commands()) == 1  # the one session: no second login


# -- helpers ----------------------------------------------------------------------------------------------------------


def _moving_owner(workspace: Workspace) -> Any:
    from v3_helpers import cli_owner

    return cli_owner(workspace)


def _claim(workspace: Workspace, owner: Any, job_id: object) -> Any:
    from test_moving import claim_root

    return claim_root(workspace, owner, job_id)


def _watch_progress(monkeypatch: pytest.MonkeyPatch, phases: list[tuple[str, Any]]) -> None:
    """Record every progress frame this process's sessions see, as ``(phase, count)``."""

    from httk.workflow import adapters

    real = adapters._Watch.saw

    def saw(watch: Any, frame: Any, at: float) -> None:
        if frame.kind == _frames.PROGRESS:
            phases.append((frame.document.get("phase"), frame.document.get("count")))
        real(watch, frame, at)

    monkeypatch.setattr(adapters._Watch, "saw", saw)


#: The receiver as the transfer module calls it, before any test wraps it.
_REAL_RECEIVE = _landing.receive_tar


def receptions(
    monkeypatch: pytest.MonkeyPatch, *, failing_fsync: bool = False, refusing: str | None = None
) -> list[bool]:
    """Record whether each landing of a pull is received with an fsync of every file; with *failing_fsync* such a
    landing fails as an fsync error would (a stand-in: the fsync of a real file cannot be made to fail here); with
    *refusing* every landing is refused so, naming no bundle (a full quota, say)."""

    seen: list[bool] = []

    def receive(workspace: Workspace, landing: Path, stream: Any, **options: Any) -> list[str]:
        seen.append(bool(options.get("force_fsync")))
        if failing_fsync and options.get("force_fsync"):
            raise _landing.StreamRefused("refused the stream, nothing of it was used: an fsync failed")
        if refusing is not None:
            raise _landing.StreamRefused(refusing)
        return _REAL_RECEIVE(workspace, landing, stream, **options)

    monkeypatch.setattr(_landing, "receive_tar", receive)
    return seen
