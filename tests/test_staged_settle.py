"""Pipelined settle (plan WORKFLOW_TRANSFER_SESSION §7.10, items 1 and 1a).

The settle split into its first and final scans (``_kernel.scan_first``/``scan_final``, ``_moving.settle_first``/
``settle_final``), the stage state machine and its owner (``_moving.Stage``), the far side's ``stage`` and
``adopt_staged`` ops, and the staged push, pull, relay and local → local transfers.
"""

import argparse
import contextlib
import json
import os
import tarfile
import tempfile
import time
import uuid
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

import pytest

from conftest import Remote, fake_remote
from httk.workflow import Workspace, _fs, _kernel, _moving
from httk.workflow.adapters import SessionFailed, TransferSession
from httk.workflow.projects import initialize_project
from httk.workflow.seals import workspace_seal_path
from httk.workflow.workflow_cli import _transfer as transfer_cli
from test_deliver import hold_jobs, outgoing_dir, stream
from test_job_transfer import jobs, outcomes, outgoing, recorded_requests, registered, run, selected
from test_moving import find, job_ids_below, single
from test_paged_holds import family, ids, paged
from test_remote_to_remote import DST_HOST, SRC_HOST, Ends, ends, held, relays_in  # noqa: F401 (ends: the fixture)
from test_session_pull import PATCHED_PULL
from test_session_push import local_project, writer
from v3_helpers import cli_owner

# -- the settle, split ----------------------------------------------------------------------------------------------


def _workspace(tmp_path: Path, deadline: float = 0.2) -> Workspace:
    return Workspace.initialize(tmp_path / "ws", durable=False, policy={"visibility_deadline_seconds": deadline})


def test_the_first_scan_dates_its_deadline_from_its_end_and_the_final_scan_waits_it_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path, 5.0)
    clock = [100.0]
    pauses: list[float] = []

    def pause(seconds: float) -> None:
        pauses.append(seconds)
        clock[0] += seconds

    real_list = _kernel.list_names

    def list_names(directory: Path) -> list[str]:
        clock[0] += 0.5  # every listing takes half a second: the first scan ends well after it began
        return real_list(directory)

    monkeypatch.setattr(_kernel, "time", type("Clock", (), {"monotonic": staticmethod(lambda: clock[0])}))
    monkeypatch.setattr(_kernel, "_pause", pause)
    monkeypatch.setattr(_kernel, "list_names", list_names)
    missing = str(uuid.uuid4())
    first = _kernel.scan_first(ws, [missing], placements=[PurePosixPath("a")])
    ended = clock[0]
    assert ended > 100.0 and first.deadline == ended + 5.0  # from the end of the scan, not its start
    assert not first.final and first.found == {} and first.remaining() == 5.0
    clock[0] += 2.0  # the time spent elsewhere is not waited again
    assert _kernel.scan_final(ws, first) == {}
    assert pauses == [3.0]
    assert first.remaining() == 0.0


def test_a_zero_deadline_makes_the_first_scan_final(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = _workspace(tmp_path, 0.0)
    scans: list[bool] = []
    real = _kernel._scan

    def scan(*arguments: Any, final: bool = False) -> dict[str, _kernel.JobRef]:
        scans.append(final)
        return real(*arguments, final=final)

    monkeypatch.setattr(_kernel, "_scan", scan)
    first = _kernel.scan_first(ws, [str(uuid.uuid4())], placements=[PurePosixPath("a")])
    assert first.final and first.remaining() == 0.0
    assert _kernel.scan_final(ws, first) == {}
    assert scans == [True]


def test_seen_in_the_first_scan_and_hidden_in_the_final_one_is_never_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = _workspace(tmp_path, 0.2)
    mapping, _state, _ = single(ws, "succeeded")
    job_id = str(mapping["id"])
    gone = str(uuid.uuid4())
    ref = find(ws, job_id)
    assert ref is not None
    hidden = tmp_path / "hidden"
    real_pause = _kernel._pause

    def pause(seconds: float) -> None:
        # Another actor moves the job out of sight between the two scans (as a claim being renamed would).
        if ref.path.exists():
            os.rename(ref.path, hidden)
        real_pause(seconds)

    monkeypatch.setattr(_kernel, "_pause", pause)
    first = _kernel.scan_first(ws, [job_id, gone], placements=[ref.placement or PurePosixPath()])
    assert set(first.found) == {job_id}
    found = _kernel.scan_final(ws, first)
    assert hidden.exists() and set(found) == {job_id}  # the first scan's observation stays


def test_settle_final_keeps_what_the_first_scan_saw(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path, 0.05)
    holds, plan = hold_jobs(src, 1)
    bundle = Path(holds[0].path)
    with cli_owner(dst) as owner:
        assert _moving.adopt(dst, owner, _copy(bundle, tmp_path / "copy"), untrusted=False, synced=True)
    job_id = str(plan[0][0]["id"])
    ref = find(dst, job_id)
    assert ref is not None
    first = _moving.settle_first(dst, [bundle])
    assert job_id in first.scan.found
    os.rename(ref.path, tmp_path / "hidden")  # out of sight before the final scan
    assert _moving.settle_final(dst, first).absent == frozenset()


def test_a_job_published_after_the_first_scan_is_seen_by_the_final_one(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path, 0.2)
    holds, plan = hold_jobs(src, 1)
    bundle = Path(holds[0].path)
    first = _moving.settle_first(dst, [bundle])
    job_id = str(plan[0][0]["id"])
    assert job_id in first.scan.wanted and job_id not in first.scan.found
    # Another actor publishes that job in DST meanwhile: the final scan, a deadline after the first, sees it.
    with cli_owner(dst) as owner:
        report = _moving.adopt(dst, owner, _copy(bundle, tmp_path / "copy"), untrusted=False, synced=True)
    assert report is not None and not report.already_adopted
    settled = _moving.settle_final(dst, first)
    assert job_id not in settled.absent


def test_settle_absent_is_the_first_scan_then_the_final_one(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path, 0.05)
    holds, plan = hold_jobs(src, 3)
    bundles = [Path(hold.path) for hold in holds]
    whole = _moving.settle_absent(dst, bundles)
    split = _moving.settle_final(dst, _moving.settle_first(dst, bundles))
    assert whole == split
    assert whole.absent == frozenset(str(mapping["id"]) for mapping, _, _ in plan)


def _copy(bundle: Path, target: Path) -> Path:
    _fs.copy_tree(bundle, target / bundle.name, durable=False)
    return target / bundle.name


# -- the stage and its owner ----------------------------------------------------------------------------------------


class Crash(BaseException):
    """A simulated process death: no cleanup runs."""


def _stage(dst: Workspace, holds: list[_moving.Hold]) -> _moving.Stage:
    """A committed stage of copies of *holds* in *dst*, as a receiver makes one."""

    stage = _moving.Stage(dst, cli_owner(dst))
    assert stage.state == "received"
    stage.landing = stage.owner.scratch("landing")
    for hold in holds:
        _fs.copy_tree(Path(hold.path), stage.landing / "outgoing" / hold.transfer_id, durable=False)
    stage.commit([stage.landing / "outgoing" / hold.transfer_id for hold in holds])
    return stage


def _scratch(ws: Workspace) -> list[str]:
    return sorted(os.listdir(ws.control / "tmp")) if (ws.control / "tmp").exists() else []


def _owners(ws: Workspace) -> list[str]:
    return [item.owner_id for item in _kernel.list_owners(ws) if item.record is not None]


def _recover(ws: Workspace, dead: str) -> _kernel.RecoveryReport:
    _kernel.attest_dead(ws, dead, by="operator", evidence=[], reason="test kill")
    with cli_owner(ws) as owner:
        return _kernel.recover(ws, owner, dead)


def test_a_stage_goes_received_committed_adopting_adopted(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path, 0.05)
    holds, plan = hold_jobs(src, 2)
    stage = _stage(dst, holds)
    assert stage.state == "committed" and stage.remaining() <= 0.05
    assert [path.name for path in stage.bundles] == [hold.transfer_id for hold in holds]
    with pytest.raises(ValueError, match="committed stage cannot be committed"):
        stage.commit(list(stage.bundles))
    settled = stage.begin()
    assert stage.state == "adopting" and stage.remaining() == 0.0
    assert not stage.abandon()  # an adoption that began is never abandoned
    with pytest.raises(ValueError, match="adopting stage cannot be adopted"):
        stage.begin()
    for bundle in stage.bundles:
        report = _moving.adopt(dst, stage.owner, bundle, untrusted=False, absent=settled.absent, synced=True)
        assert report is not None and not report.already_adopted
    stage.close()
    assert stage.state == "adopted"
    assert all(find(dst, mapping["id"]) is not None for mapping, _, _ in plan)
    assert _scratch(dst) == [] and _owners(dst) == []
    assert all(Path(hold.path).exists() for hold in holds)  # copies: the holds are untouched


def test_an_abandoned_committed_stage_discards_its_copies_only(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path)
    holds, plan = hold_jobs(src, 2)
    stage = _stage(dst, holds)
    assert stage.abandon() and stage.state == "abandoned"
    assert not stage.abandon()
    assert _scratch(dst) == [] and _owners(dst) == []
    assert all(find(dst, mapping["id"]) is None for mapping, _, _ in plan)
    assert all(Path(hold.path).exists() for hold in holds)


def test_an_abandoned_stage_of_original_holds_never_discards_them(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    holds, _plan = hold_jobs(src, 1)
    stage = _moving.Stage(src, cli_owner(src))  # same filesystem, no --keep: the originals in place
    stage.commit([Path(hold.path) for hold in holds])
    assert stage.abandon()
    assert all(Path(hold.path).exists() for hold in holds) and _owners(src) == []


def test_a_received_stage_abandons_too(tmp_path: Path) -> None:
    dst = _workspace(tmp_path)
    stage = _moving.Stage(dst, cli_owner(dst))
    stage.landing = stage.owner.scratch("landing")
    (stage.landing / "outgoing").mkdir()
    assert stage.abandon() and _scratch(dst) == [] and _owners(dst) == []


def test_recovery_of_a_dead_owner_in_committed_discards_the_stage(tmp_path: Path) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path)
    holds, plan = hold_jobs(src, 2)
    stage = _stage(dst, holds)
    report = _recover(dst, stage.owner.owner_id)  # its process died: nothing ever adopts the stage
    assert len(report.scratch) == 1 and not report.kept_scratch and _scratch(dst) == []
    assert all(find(dst, mapping["id"]) is None for mapping, _, _ in plan)
    assert all(Path(hold.path).exists() for hold in holds)


def test_recovery_of_a_dead_owner_in_adopting_completes_the_started_adoption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = Workspace.initialize(tmp_path / "src", durable=False)
    dst = _workspace(tmp_path)
    holds, plan = hold_jobs(src, 2)
    stage = _stage(dst, holds)
    settled = stage.begin()
    real_finish = _moving._finish

    def crash(*arguments: Any, **options: Any) -> Any:
        raise Crash  # the process dies right after the first bundle was taken into its adopt scratch

    monkeypatch.setattr(_moving, "_finish", crash)
    first, second = stage.bundles
    with pytest.raises(Crash):
        _moving.adopt(dst, stage.owner, first, untrusted=False, absent=settled.absent, synced=True)
    monkeypatch.setattr(_moving, "_finish", real_finish)
    report = _recover(dst, stage.owner.owner_id)
    assert len(report.scratch) == 2 and not report.kept_scratch  # the adopt scratch (resumed), the landing
    published = [mapping["id"] for mapping, _, _ in plan if find(dst, mapping["id"]) is not None]
    assert len(published) == 1  # the started bundle was published by recovery; the unstarted copy discarded
    assert not second.exists() and _scratch(dst) == []


# -- the far side: stage and adopt_staged ---------------------------------------------------------------------------


def _staged_session(bundle: Path) -> TransferSession:
    session = TransferSession(bundle, timeout=60)
    session.open()
    return session


def _stage_request(
    session: TransferSession, far_name: str, near: Workspace, holds: list[_moving.Hold], **header: Any
) -> dict[str, Any]:
    ids = [hold.transfer_id for hold in holds]
    data = stream(outgoing_dir(near), ids)
    declared = sum(transfer_cli._measure(Path(hold.path))[0] for hold in holds)  # truthfully, as a push does
    return session.request(
        "stage", {"workspace": far_name, "transfer_ids": ids, "bytes": declared, **header}, upload=writer(data)
    )


def _adopt_request(session: TransferSession, far_name: str, staged: dict[str, Any], **header: Any) -> dict[str, Any]:
    request = {"workspace": far_name, "token": staged["token"], "transfer_ids": staged["transfer_ids"], **header}
    return session.request("adopt_staged", request)


def _far_owners_and_scratch(far: Workspace) -> tuple[list[str], list[str]]:
    return _owners(far), _scratch(far)


def test_a_stage_is_adopted_only_by_its_adopt_staged(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    with _staged_session(bundle) as session:
        staged = _stage_request(session, far_name, near, holds)
        assert staged["ok"] is True and staged["workspace_id"] == far.workspace_id
        assert staged["transfer_ids"] == [hold.transfer_id for hold in holds]
        assert staged["bytes"] == sum(transfer_cli._measure(Path(hold.path))[0] for hold in holds) > 0
        assert 0 <= staged["matures_in"] <= 0.05
        # Staged: committed in the stage's own landing scratch, held by its own owner; nothing published.
        assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
        owners, scratch = _far_owners_and_scratch(far)
        assert len(owners) == 1 and len(scratch) == 1 and ".landing." in scratch[0]
        response = _adopt_request(session, far_name, staged)
        assert response["ok"] is True and response["returncode"] == 0
        document = response["document"]
        assert document["workspace_id"] == far.workspace_id
        assert sorted(entry["bundle"] for entry in document["bundles"]) == sorted(staged["transfer_ids"])
        assert [entry["status"] for entry in document["bundles"]] == ["adopted"] * 2
        # Consumed: the same token again is refused, nothing touched.
        again = _adopt_request(session, far_name, staged)
        assert again["ok"] is False and "unknown, or adopted already" in again["error"]
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)
    assert _far_owners_and_scratch(far) == ([], [])


def test_a_misused_token_is_refused_before_anything_is_touched_and_the_stage_stays(
    tmp_path: Path, remote: Remote
) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    _other, other_name = registered(tmp_path, "other")
    holds, plan = hold_jobs(near, 2)
    with _staged_session(bundle) as session:
        first = _stage_request(session, far_name, near, holds[:1])
        second = _stage_request(session, far_name, near, holds[1:])
        refusals = [
            _adopt_request(session, far_name, {**first, "token": "a" * 16}),  # unknown
            _adopt_request(session, far_name, second),  # not the oldest
            _adopt_request(session, far_name, {**first, "transfer_ids": second["transfer_ids"]}),  # other ids
            _adopt_request(session, other_name, first),  # another workspace
            _adopt_request(session, "no-such-workspace", first),
        ]
        assert [response["ok"] for response in refusals] == [False] * 5
        assert all("nothing was adopted" in response["error"] for response in refusals)
        assert "document" not in refusals[0] and "fatal" not in refusals[0]
        assert all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
        # Both stages stay, and adopt in staging order.
        assert _adopt_request(session, far_name, first)["ok"] is True
        assert _adopt_request(session, far_name, second)["ok"] is True
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)


def test_a_stage_beyond_the_capacity_or_of_a_staged_id_is_refused_before_its_data(
    tmp_path: Path, remote: Remote
) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 10)
    with _staged_session(bundle) as session:
        staged = [_stage_request(session, far_name, near, [hold]) for hold in holds[:8]]
        assert all(response["ok"] for response in staged)
        refused = _stage_request(session, far_name, near, holds[8:9])
        assert refused["ok"] is False and refused["capacity"] is True and "document" not in refused
        duplicate = _stage_request(session, far_name, near, holds[:1])
        assert duplicate["ok"] is False and "already staged" in duplicate["error"]
        assert "capacity" not in duplicate
        assert _adopt_request(session, far_name, staged[0])["ok"] is True
        # Room for one: the bytes count too, the incoming segment included.
        over = _stage_request(session, far_name, near, holds[8:9], bytes=32 << 30)
        assert over["ok"] is False and over["capacity"] is True
        for response in staged[1:]:
            assert _adopt_request(session, far_name, response)["ok"] is True
        # Nothing staged: several bundles above the cap are refused; a single larger bundle alone is staged.
        several = _stage_request(session, far_name, near, holds[8:], bytes=40 << 30)
        assert several["ok"] is False and several["capacity"] is True
        alone = _stage_request(session, far_name, near, holds[8:9], bytes=40 << 30)
        assert alone["ok"] is True
        # It counts what actually landed from then on (a few KiB here), not its declaration.
        assert 0 < alone["bytes"] < 1 << 20
        after = _stage_request(session, far_name, near, holds[9:], bytes=(32 << 30) - alone["bytes"])
        assert after["ok"] is True
        assert _adopt_request(session, far_name, alone)["ok"] is True
        assert _adopt_request(session, far_name, after)["ok"] is True
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)
    assert _far_owners_and_scratch(far) == ([], [])


def test_an_underdeclared_stage_counts_its_landed_bytes_for_the_next_admission(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 2)
    landed = transfer_cli._measure(Path(holds[0].path))[0]
    with _staged_session(bundle) as session:
        first = _stage_request(session, far_name, near, holds[:1], bytes=0)  # declares nothing
        assert first["ok"] is True and first["bytes"] == landed > 0
        # The next one fits a declaration of 0 bytes staged, not the bytes that landed.
        second = _stage_request(session, far_name, near, holds[1:], bytes=(32 << 30) - landed + 1)
        assert second["ok"] is False and second["capacity"] is True
        assert _adopt_request(session, far_name, first)["ok"] is True
        second = _stage_request(session, far_name, near, holds[1:], bytes=(32 << 30) - landed + 1)
        assert second["ok"] is True and _adopt_request(session, far_name, second)["ok"] is True
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)


def test_a_refused_stage_disposes_of_its_owner_and_landing_and_the_session_goes_on(
    tmp_path: Path, remote: Remote
) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    fifo = tarfile.TarInfo(f"{holds[0].transfer_id}/pipe")
    fifo.type = tarfile.FIFOTYPE
    ids = [holds[0].transfer_id]
    data = stream(outgoing_dir(near), ids, [fifo])
    with _staged_session(bundle) as session:
        header = {"workspace": far_name, "transfer_ids": ids, "bytes": 0}
        refused = session.request("stage", header, upload=writer(data))
        assert refused["ok"] is False and refused["document"]["bundle"] == ids[0]
        # Its owner closed and its landing discarded while the session goes on.
        assert _far_owners_and_scratch(far) == ([], [])
        staged = _stage_request(session, far_name, near, holds)
        assert _adopt_request(session, far_name, staged)["ok"] is True
    assert find(far, plan[0][0]["id"]) is not None


def test_a_sealed_workspace_refuses_adopt_staged_and_keeps_the_stage(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 1)
    seal = workspace_seal_path(far)
    with _staged_session(bundle) as session:
        staged = _stage_request(session, far_name, near, holds)
        seal.parent.mkdir(parents=True, exist_ok=True)
        seal.write_text("{}")  # sealed since the stage (the guard looks for the seal file)
        refused = _adopt_request(session, far_name, staged)
        assert refused["ok"] is False and "nothing was adopted" in refused["error"] and "sealed" in refused["error"]
        assert find(far, plan[0][0]["id"]) is None
        seal.unlink()
        assert _adopt_request(session, far_name, staged)["ok"] is True  # the stage stayed
    assert find(far, plan[0][0]["id"]) is not None


@pytest.mark.parametrize("end", ["eof", "bye"])
def test_the_end_of_the_session_abandons_its_unadopted_stages(
    tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch, end: str
) -> None:
    if end == "eof":
        real_request = TransferSession._request

        def no_bye(session: TransferSession, op: str, *arguments: Any, **options: Any) -> dict[str, Any]:
            if op == "bye":
                raise SessionFailed("no bye: the input just ends")
            return real_request(session, op, *arguments, **options)

        monkeypatch.setattr(TransferSession, "_request", no_bye)
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, plan = hold_jobs(near, 3)
    session = _staged_session(bundle)
    try:
        assert _adopt_request(session, far_name, _stage_request(session, far_name, near, holds[:1]))["ok"] is True
        assert _stage_request(session, far_name, near, holds[1:])["ok"] is True
        if end == "bye":
            assert session.request("bye")["ok"] is True
            # Discarded before the far side answered the bye.
            assert _far_owners_and_scratch(far) == ([], [])
    finally:
        session.close()
    deadline = time.monotonic() + 30
    while _far_owners_and_scratch(far) != ([], []) and time.monotonic() < deadline:
        time.sleep(0.05)  # the far side abandons once it read the end of its input
    assert _far_owners_and_scratch(far) == ([], [])
    assert [find(far, mapping["id"]) is not None for mapping, _, _ in plan] == [True, False, False]
    assert all(Path(hold.path).exists() for hold in holds)  # copies were staged: the holds are untouched


def test_a_malformed_stage_header_is_refused(tmp_path: Path, remote: Remote) -> None:
    _, bundle = local_project(tmp_path)
    near, _ = registered(tmp_path, "near")
    far, far_name = registered(tmp_path, "far")
    holds, _plan = hold_jobs(near, 1)
    with _staged_session(bundle) as session:
        malformed: list[tuple[dict[str, Any], str]] = [
            ({"bytes": -1}, "bytes as a nonnegative integer"),
            ({"bytes": True}, "bytes as a nonnegative integer"),
            ({"sync": "syncfs"}, "sync fsync or none"),
            ({"transfer_ids": []}, "transfer_ids, each once"),
            ({"transfer_ids": ["x"]}, "each a transfer id"),
        ]
        for header, error in malformed:
            response = _stage_request(session, far_name, near, holds, **header)
            assert response["ok"] is False and error in response["error"]
        # An upload carrying other bundles than the header names is fatal after the commit: its owner is left for
        # recovery, which discards the landing.
        with pytest.raises(SessionFailed, match="not those its header named"):
            _stage_request(session, far_name, near, holds, transfer_ids=["0" * 32])
    (dead,) = _owners(far)
    assert len(_recover(far, dead).scratch) == 1 and _far_owners_and_scratch(far) == ([], [])
    assert find(far, _plan[0][0]["id"]) is None


# -- the client: push, pull, relay and local → local ----------------------------------------------------------------

#: The far side of these tests: :data:`test_session_pull.PATCHED_PULL` (with the push's scripted faults), and:
#: ``HTTK_TEST_FAR_STAGES`` (its capacity
#: in stages), ``HTTK_TEST_EXIT_AFTER_STAGE`` (the n-th ``stage`` commits, then the process ends before it answers) and
#: ``HTTK_TEST_EXIT_IN_ADOPTION`` (the n-th bundle adopted, then the process ends: a stage left adopting).
STAGED_PATCHES = """
if argv[:2] == ["workflow", "transfer-session"]:
    if os.environ.get("HTTK_TEST_FAR_STAGES"):
        _transfer_session._STAGES = int(os.environ["HTTK_TEST_FAR_STAGES"])
    if os.environ.get("HTTK_TEST_FAR_STAGED_BYTES"):
        _transfer_session._STAGED_BYTES = int(os.environ["HTTK_TEST_FAR_STAGED_BYTES"])
    exit_after_stage = int(os.environ.get("HTTK_TEST_EXIT_AFTER_STAGE", "0"))
    if exit_after_stage:
        stage_real = _transfer_session.OPS["stage"]
        staged_seen = []

        def stage_then_exit(request):
            response = stage_real(request)
            staged_seen.append(1)
            if len(staged_seen) == exit_after_stage:
                os._exit(3)  # staged and committed, the answer lost
            return response

        _transfer_session.OPS["stage"] = stage_then_exit
    exit_in_adoption = int(os.environ.get("HTTK_TEST_EXIT_IN_ADOPTION", "0"))
    if exit_in_adoption:
        real_adopt_one = _job._adopt_one
        adopted_seen = []

        def adopt_one_then_exit(*arguments, **options):
            result = real_adopt_one(*arguments, **options)
            adopted_seen.append(1)
            if len(adopted_seen) == exit_in_adoption:
                os._exit(3)  # in the middle of a staged segment's adoption
            return result

        _job._adopt_one = adopt_one_then_exit
"""
PATCHED_STAGED = PATCHED_PULL.replace('sys.argv = ["httk", *argv]\n', STAGED_PATCHES + 'sys.argv = ["httk", *argv]\n')
assert PATCHED_STAGED != PATCHED_PULL


@pytest.fixture
def cluster(tmp_path: Path, remote: Remote) -> tuple[Path, Workspace, str, Workspace, str]:
    """A project with the stand-in ssh cluster (its ``httk`` :data:`PATCHED_STAGED`), a local and a remote workspace."""

    project = tmp_path / "project"
    initialize_project(project, name="staged")
    fake_remote(project)
    wrapper = tmp_path / "patched_staged.py"
    wrapper.write_text(PATCHED_STAGED, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')
    near, near_name = registered(tmp_path, "near")
    far, far_name = registered(remote.root, "far")
    return project, near, near_name, far, far_name


def _statuses(capsys: pytest.CaptureFixture[str]) -> list[str]:
    return [outcome["status"] for outcome in outcomes(capsys)["transfers"]]


def _once(workspace: Workspace, plan: list[tuple[dict[str, Any], str, int]]) -> None:
    """Every job of *plan* is in *workspace* exactly once (no duplicate, no loss)."""

    found = job_ids_below(workspace.jobs)
    assert all(found.count(str(mapping["id"])) == 1 for mapping, _, _ in plan)


def _quiet(workspace: Workspace) -> None:
    """No owner registered and no scratch left in *workspace*, waiting for a far side to end."""

    deadline = time.monotonic() + 30
    while (_owners(workspace), _scratch(workspace)) != ([], []) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert (_owners(workspace), _scratch(workspace)) == ([], [])


def test_a_push_settles_its_segments_while_it_copies_the_next(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    deadline = 1.0
    far.set_policy({"visibility_deadline_seconds": deadline})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 4)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 4
    _once(far, plan)
    assert outgoing(near) == []
    sent = [op for op, _ in requests]
    assert [op for op in sent if op != "adopt_staged"] == ["workflows", *["stage"] * 4]
    assert sent.count("adopt_staged") == 4
    # Copying goes on while the first segment's deadline runs: at least two stages before its adoption.
    assert sent.index("adopt_staged") > sent.index("stage") + 1
    _quiet(far)


def test_a_pull_settles_its_landings_while_it_fetches_the_next(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    deadline = 1.0
    near.set_policy({"visibility_deadline_seconds": deadline})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    requests = recorded_requests(monkeypatch)
    waited: list[float] = []
    real_pause = _kernel._pause

    def pause(seconds: float) -> None:
        waited.append(seconds)
        real_pause(seconds)

    monkeypatch.setattr(_kernel, "_pause", pause)
    first_scans: list[int] = []
    real_first = _moving.settle_first

    def settle_first(*arguments: Any, **options: Any) -> Any:
        first_scans.append(sum(1 for op, _ in requests if op == "send"))
        return real_first(*arguments, **options)

    monkeypatch.setattr(_moving, "settle_first", settle_first)
    plan = jobs(far, 4)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert _statuses(capsys) == ["adopted"] * 4
    _once(near, plan)
    assert outgoing(far) == [] and _scratch(near) == [] and _owners(near) == []
    # Each landing's first scan follows its own send; the deadlines ran while the next ones were fetched, so the
    # waits add up to about one deadline, not four.
    assert first_scans == [1, 2, 3, 4]
    assert sum(waited) < 2 * deadline
    sent = [op for op, _ in requests]
    assert sent.count("send") == 4 and sent.count("release") == 4
    assert sent.index("release") > sent.index("send") + 1  # the second send went before the first release


@pytest.mark.parametrize("stages", [8, 2], ids=["cap-8", "cap-2"])
def test_local_to_local_holds_the_next_pages_while_the_staged_ones_mature(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    stages: int,
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 1.0})
    monkeypatch.setattr(transfer_cli, "_STAGES", stages)
    plan = jobs(source, 4)
    paged(monkeypatch, 1)
    events: list[str] = []
    real_hold, real_adopt = _moving.hold, _moving.adopt

    def hold(*arguments: Any, **options: Any) -> Any:
        events.append("hold")
        return real_hold(*arguments, **options)

    def adopt(*arguments: Any, **options: Any) -> Any:
        events.append("adopt")
        return real_adopt(*arguments, **options)

    monkeypatch.setattr(_moving, "hold", hold)
    monkeypatch.setattr(_moving, "adopt", adopt)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 0
    assert _statuses(capsys) == ["adopted"] * 4
    # The pages are held while the first ones mature: the first adoption comes only once the cap is reached (or
    # at the end), and never more than the cap stage ahead of the adoptions.
    assert sorted(events) == ["adopt"] * 4 + ["hold"] * 4
    assert events.index("adopt") == min(stages + 1, 4)
    assert all(events[:end].count("hold") - events[:end].count("adopt") <= stages + 1 for end in range(9))
    _once(target, plan)
    assert outgoing(source) == [] and _scratch(target) == [] and _owners(target) == []


def test_a_local_to_local_stop_abandons_the_staged_page_and_keeps_its_original_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 5.0})
    plan = jobs(source, 3)
    paged(monkeypatch, 1)
    real_next = transfer_cli._Selection.next_page
    pages: list[int] = []

    def next_page(selection: Any, **limits: Any) -> Any:
        pages.append(1)
        if len(pages) == 2:
            raise OSError(5, "the source could not be read")  # while the first page's stage matures
        return real_next(selection, **limits)

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    start = time.monotonic()
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 1
    assert time.monotonic() - start < 5.0  # abandoned, never waited out
    document = outcomes(capsys)
    (outcome,) = document["transfers"]
    assert outcome["status"] == "failed" and "not adopted, since the transfer stopped" in outcome["message"]
    assert f"stays held on {source.root}" in outcome["message"]
    # Staged in place: nothing was copied, nothing discarded anywhere.
    assert "nothing of it was copied" in outcome["message"] and "discarded there" not in outcome["message"]
    (held,) = _moving.held(source)  # the original hold, staged in place, never discarded
    assert held.transfer_id == outcome["transfer_id"] and Path(held.path).exists()
    assert job_ids_below(target.jobs) == [] and _scratch(target) == [] and _owners(target) == []
    monkeypatch.setattr(transfer_cli._Selection, "next_page", real_next)
    target.set_policy({"visibility_deadline_seconds": 0.05})
    assert run(tmp_path, "--json", "-q", "--resume", source_name, target_name) == 0
    assert _statuses(capsys) == ["adopted"]
    assert sum(find(target, mapping["id"]) is not None for mapping, _, _ in plan) == 1


def test_a_relay_stages_at_dst_like_a_push(
    ends: Ends,  # noqa: F811 (the fixture)
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ends.far.set_policy({"visibility_deadline_seconds": 1.0})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 3)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 0
    assert _statuses(capsys) == ["adopted"] * 3
    _once(ends.far, plan)
    assert outgoing(ends.near) == [] and relays_in(ends.relays) == []
    sent = [op for op, _ in requests]
    assert sent.count("stage") == 3 and sent.count("adopt_staged") == 3 and "adopt" not in sent
    # The relay goes once the segment is staged; the next segment is fetched before the first is adopted.
    assert sent.index("adopt_staged") > [index for index, op in enumerate(sent) if op == "send"][1]
    # Each segment released on SRC only after its adoption.
    assert sent.index("release") > sent.index("adopt_staged")


def test_a_lost_stage_answer_stops_with_the_holds_kept_and_resume_delivers_once(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 5.0})  # the first stage is still pending at the stop
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_STAGE", "2")
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 3
    messages = sorted(outcome["message"] for outcome in transfers)
    assert sum("not adopted, since the transfer stopped" in message for message in messages) == 1  # stage 1
    assert sum("whether the jobs arrived is unknown" in message for message in messages) == 1  # stage 2
    assert sum("not delivered, since the transfer session stopped" in message for message in messages) == 1
    assert [op for op, _ in requests] == ["workflows", "stage", "stage"]
    assert len(outgoing(near)) == 3  # all sent or not, none taken back
    # The far side died with two committed stages: never adopted; recovery discards their copies.
    assert job_ids_below(far.jobs) == []
    for dead in _owners(far):
        _recover(far, dead)
    assert _scratch(far) == []
    monkeypatch.delenv("HTTK_TEST_EXIT_AFTER_STAGE")
    far.set_policy({"visibility_deadline_seconds": 0.05})
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 3
    _once(far, plan)
    assert outgoing(near) == []


def test_a_lost_adopt_staged_answer_is_unknown_for_its_segment_and_resume_releases_it(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 0})  # each stage adopted at once
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_ADOPT", "1")  # the first adopt_staged adopts, its answer lost
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    unknown = [outcome for outcome in transfers if "whether the jobs arrived is unknown" in outcome["message"]]
    assert len(unknown) == 1 and "adopted a staged segment" in unknown[0]["message"]
    assert len(outgoing(near)) == 2  # kept held, the adopted one too
    adopted = [mapping for mapping, _, _ in plan if find(far, mapping["id"]) is not None]
    assert len(adopted) == 1
    monkeypatch.delenv("HTTK_TEST_EXIT_AFTER_ADOPT")
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert sorted(_statuses(capsys)) == ["adopted", "already_adopted"]
    _once(far, plan)
    assert outgoing(near) == []


def test_a_far_side_dying_while_it_adopts_a_stage_loses_and_duplicates_nothing(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.configure_settings({"io.workers": 1})
    far.set_policy({"visibility_deadline_seconds": 5.0})  # the second segment staged before the first adopts
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 2)
    monkeypatch.setattr(transfer_cli, "_STAGES", 2)  # the third stage waits for the first's adoption
    monkeypatch.setenv("HTTK_TEST_EXIT_IN_ADOPTION", "1")  # one bundle of the first segment adopted, then death
    plan = jobs(near, 6)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    assert _statuses(capsys) == ["failed"] * 6 and len(outgoing(near)) == 6
    assert sum(find(far, mapping["id"]) is not None for mapping, _, _ in plan) == 1
    # Recovery of the dead stages: the adopting one keeps today's recovery (its unstarted copy discarded), the
    # committed one is discarded; nothing is adopted by it.
    for dead in _owners(far):
        _recover(far, dead)
    assert _scratch(far) == [] and sum(find(far, mapping["id"]) is not None for mapping, _, _ in plan) == 1
    monkeypatch.delenv("HTTK_TEST_EXIT_IN_ADOPTION")
    far.set_policy({"visibility_deadline_seconds": 0.05})
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert sorted(_statuses(capsys)) == ["adopted"] * 5 + ["already_adopted"]
    _once(far, plan)
    assert outgoing(near) == []


def test_a_stage_refused_at_dst_s_capacity_waits_for_the_oldest_adoption(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 0.3})
    monkeypatch.setenv("HTTK_TEST_FAR_STAGES", "1")  # DST holds one stage; this side would stage eight
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 3
    _once(far, plan)
    sent = [op for op, _ in requests]
    # A refused stage (nothing of it used), the oldest adopted, the stage again.
    assert sent[:4] == ["workflows", "stage", "stage", "adopt_staged"] and sent.count("stage") == 5
    assert sent.count("adopt_staged") == 3


def test_a_stop_abandons_the_staged_segments_and_never_takes_them_back(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 30.0})  # never matured during the push
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"3": {"refuse": {"workspace_id": far.workspace_id}}}))
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 4)
    start = time.monotonic()
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    assert time.monotonic() - start < 30.0
    transfers = outcomes(capsys)["transfers"]
    by_id = {outcome["transfer_id"]: outcome for outcome in transfers}
    staged = [header["transfer_ids"][0] for op, header in requests if op == "stage"]
    assert len(staged) == 3 and "adopt_staged" not in [op for op, _ in requests]
    # The two staged segments: sent, so held for --resume, never taken back.
    for transfer_id in staged[:2]:
        assert by_id[transfer_id]["status"] == "failed"
        assert "not adopted, since the transfer stopped" in by_id[transfer_id]["message"]
    # The refused segment and the one never sent: this run's holds, taken back.
    assert [outcome["status"] for outcome in transfers if outcome["transfer_id"] not in staged[:2]] == ["not_moved"] * 2
    assert sorted(outgoing(near)) == sorted(staged[:2])
    assert job_ids_below(far.jobs) == []
    _quiet(far)  # the end of the session abandoned the stages: their copies discarded, their owners closed
    far.set_policy({"visibility_deadline_seconds": 0.05})
    monkeypatch.delenv("HTTK_TEST_SCRIPT")
    assert run(project, "--json", "-q", "--resume", near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 2


def test_a_failed_commit_stages_once_more_with_fsync_and_the_rest_go_so(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_SCRIPT", json.dumps({"1": {"refuse": {"sync_failed": True}}}))
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 3
    assert [header.get("sync") for op, header in requests if op == "stage"] == [None, "fsync", "fsync", "fsync"]
    _once(far, plan)


def test_a_pull_stages_only_after_the_send_answer_is_validated(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    monkeypatch.setenv("HTTK_TEST_WRONG_SENT", "1")
    first_scans: list[object] = []
    real_first = _moving.settle_first

    def settle_first(*arguments: Any, **options: Any) -> Any:
        first_scans.append(1)
        return real_first(*arguments, **options)

    monkeypatch.setattr(_moving, "settle_first", settle_first)
    plan = jobs(far, 2)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    assert _statuses(capsys) == ["failed"] * 2
    assert first_scans == []  # the landing was committed, but SRC's answer was wrong: never staged
    assert _scratch(near) == [] and _owners(near) == [] and job_ids_below(near.jobs) == []
    assert len(outgoing(far)) == 2


def test_a_pull_s_source_failure_abandons_its_local_stages(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    near.set_policy({"visibility_deadline_seconds": 30.0})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_SEND", "2")  # SRC dies at the second send: the first is staged here
    plan = jobs(far, 2)
    start = time.monotonic()
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 1
    assert time.monotonic() - start < 30.0
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert sum("not adopted, since the transfer stopped" in outcome["message"] for outcome in transfers) == 1
    # The stage was abandoned here (its landing discarded, its owner closed); both holds stay on SRC.
    assert job_ids_below(near.jobs) == [] and _scratch(near) == [] and _owners(near) == []
    assert len(outgoing(far)) == 2
    monkeypatch.delenv("HTTK_TEST_EXIT_ON_SEND")
    near.set_policy({"visibility_deadline_seconds": 0.05})
    assert run(project, "--json", "-q", "--resume", f"cluster:{far_name}", near_name) == 0
    assert _statuses(capsys) == ["adopted"] * 2
    _once(near, plan)


@pytest.mark.parametrize("tamper", ["duplicate", "extra"])
def test_an_adoption_answer_not_naming_exactly_the_staged_bundles_releases_nothing(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tamper: str,
) -> None:
    project, near, near_name, far, far_name = cluster
    real = TransferSession.request

    def request(session: TransferSession, op: str, header: Any = None, **options: Any) -> dict[str, Any]:
        response = real(session, op, header, **options)
        if op == "adopt_staged" and response.get("ok") is True:
            bundles = response["document"]["bundles"]
            entry = dict(bundles[0]) if tamper == "duplicate" else {**bundles[0], "bundle": "f" * 32}
            response["document"]["bundles"] = [*bundles, entry]
        return response

    monkeypatch.setattr(TransferSession, "request", request)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("no usable adoption report of a staged segment" in outcome["message"] for outcome in transfers)
    assert len(outgoing(near)) == 2  # adopted in DST, but nothing released on that answer
    assert all(find(far, mapping["id"]) is not None for mapping, _, _ in plan)


# -- correction round 1 ---------------------------------------------------------------------------------------------


def _tampered(monkeypatch: pytest.MonkeyPatch, op: str, change: Any) -> list[dict[str, Any]]:
    """Let *change* rewrite the response of each *op* request this process makes (it gets the request's number,
    from 1, and the response); return every response seen."""

    real = TransferSession.request
    seen: list[dict[str, Any]] = []

    def request(session: TransferSession, name: str, header: Any = None, **options: Any) -> dict[str, Any]:
        response = real(session, name, header, **options)
        if name == op:
            seen.append(response)
            response = change(len(seen), response)
        return response

    monkeypatch.setattr(TransferSession, "request", request)
    return seen


def test_src_failing_in_a_relayed_release_stops_before_another_adoption(
    ends: Ends,  # noqa: F811 (the fixture)
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    ends.far.set_policy({"visibility_deadline_seconds": 4.0})  # all three staged before the first matures
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "release")  # SRC dies at its first release
    requests = recorded_requests(monkeypatch)
    plan = jobs(ends.near, 3)
    assert run(ends.project, "--json", "-q", *selected(plan), ends.src, ends.dst) == 1
    document = outcomes(capsys)
    transfers = document["transfers"]
    sent = [op for op, _ in requests]
    assert sent.count("stage") == 3 and sent.count("adopt_staged") == 1 and sent.count("release") == 1
    adopted = [outcome for outcome in transfers if outcome["status"] == "adopted"]
    assert len(adopted) == 1 and "the hold was not released" in adopted[0]["message"]
    left = [outcome for outcome in transfers if outcome["status"] != "adopted"]
    assert len(left) == 2 and all("not adopted, since the transfer stopped" in o["message"] for o in left)
    assert any("stopped" in error and "--resume" in error for error in document["errors"])
    assert len(outgoing(ends.near)) == 3 and sum(find(ends.far, m["id"]) is not None for m, _, _ in plan) == 1


@pytest.mark.parametrize("where", ["copy", "first scan"])
def test_an_interrupt_while_a_local_page_is_staged_leaves_no_owner_and_no_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, where: str
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.configure_settings({"io.workers": 1})
    jobs(source, 2)

    def interrupt(*arguments: Any, **options: Any) -> Any:
        raise KeyboardInterrupt

    if where == "copy":
        real_copy = _fs.copy_tree
        copies: list[object] = []

        def copy_then_interrupt(*arguments: Any, **options: Any) -> Any:
            copies.append(1)
            if len(copies) == 2:
                raise KeyboardInterrupt  # the first hold copied, the second not
            return real_copy(*arguments, **options)

        monkeypatch.setattr(_fs, "copy_tree", copy_then_interrupt)
    else:
        monkeypatch.setattr(_moving, "settle_first", interrupt)
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, "--json", "-q", "--keep", "--state", "succeeded", source_name, target_name)
    assert _owners(target) == [] and _scratch(target) == [] and job_ids_below(target.jobs) == []
    held = _moving.held(source)
    assert len(held) == 2 and all(Path(hold.path).exists() for hold in held)  # the originals, untouched


def test_an_interrupt_abandons_the_staged_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 30.0})
    jobs(source, 3)
    paged(monkeypatch, 1)
    real_next = transfer_cli._Selection.next_page
    pages: list[int] = []

    def next_page(selection: Any, **limits: Any) -> Any:
        pages.append(1)
        if len(pages) == 3:
            raise KeyboardInterrupt  # two pages staged, waiting
        return real_next(selection, **limits)

    monkeypatch.setattr(transfer_cli._Selection, "next_page", next_page)
    start = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        run(tmp_path, "--json", "-q", "--keep", "--state", "succeeded", source_name, target_name)
    assert time.monotonic() - start < 30.0  # nothing waited out
    assert _owners(target) == [] and _scratch(target) == [] and job_ids_below(target.jobs) == []
    held = _moving.held(source)
    assert len(held) == 2 and all(Path(hold.path).exists() for hold in held)


def test_a_stage_answer_naming_other_bundles_is_unknown(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    _tampered(monkeypatch, "stage", lambda _number, response: {**response, "transfer_ids": ["f" * 32]})
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    assert all("the stage of a segment: a stage of other bundles" in outcome["message"] for outcome in transfers)
    assert len(outgoing(near)) == 2 and all(find(far, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_stage_answered_by_another_workspace_adopts_nothing_and_stops(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 0})  # the first segment is adopted before the second is staged
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    other = "00000000-0000-4000-8000-000000000000"
    _tampered(
        monkeypatch, "stage", lambda number, response: {**response, "workspace_id": other} if number == 2 else response
    )
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    statuses = sorted(outcome["status"] for outcome in transfers)
    assert statuses == ["adopted", "failed", "failed"]
    staged_elsewhere = [
        outcome
        for outcome in transfers
        if outcome.get("message", "").startswith(f"cluster:{far_name} staged the segment in workspace {other}")
    ]
    assert len(staged_elsewhere) == 1 and "nothing of it was adopted" in staged_elsewhere[0]["message"]
    assert "adopted the jobs into" not in staged_elsewhere[0]["message"]
    assert [op for op, _ in requests].count("stage") == 2  # the pair stopped: the third never sent
    assert len(outgoing(near)) == 2 and sum(find(far, m["id"]) is not None for m, _, _ in plan) == 1


def test_a_refused_adopt_staged_adopts_nothing_and_keeps_the_holds(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    real = TransferSession.request

    def request(session: TransferSession, op: str, header: Any = None, **options: Any) -> dict[str, Any]:
        if op == "adopt_staged":
            # Never sent: the stage stays unadopted. The far side's own refusals say that nothing was adopted.
            return {"ok": False, "error": "a refusal for the test; nothing was adopted"}
        return real(session, op, header, **options)

    monkeypatch.setattr(TransferSession, "request", request)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    transfers = outcomes(capsys)["transfers"]
    assert [outcome["status"] for outcome in transfers] == ["failed"] * 2
    for outcome in transfers:
        assert "refused to adopt a staged segment: a refusal for the test" in outcome["message"]
        assert "is unknown" not in outcome["message"]
        assert outcome["message"].count("nothing") == 1  # said once
    assert len(outgoing(near)) == 2 and all(find(far, mapping["id"]) is None for mapping, _, _ in plan)
    _quiet(far)  # the stage was discarded at the end of the session


def test_nothing_is_staged_after_an_adoption_stopped_the_pair(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 0.3})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setattr(transfer_cli, "_STAGES", 1)  # the second segment waits for the first's adoption
    monkeypatch.setenv("HTTK_TEST_EXIT_AFTER_ADOPT", "1")  # which adopts, and its answer is lost
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 1
    assert [op for op, _ in requests] == ["workflows", "stage", "adopt_staged"]
    messages = sorted(outcome["message"] for outcome in outcomes(capsys)["transfers"])
    assert any("not delivered, since the transfer session stopped" in message for message in messages)
    assert len(outgoing(near)) == 2


def test_a_push_makes_room_before_it_uploads_so_nothing_is_uploaded_in_vain(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 0.3})
    monkeypatch.setenv("HTTK_TEST_FAR_STAGES", "1")
    monkeypatch.setattr(transfer_cli, "_STAGES", 1)  # this side counts as DST does
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    answers = _tampered(monkeypatch, "stage", lambda _number, response: response)
    requests = recorded_requests(monkeypatch)
    plan = jobs(near, 3)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 3
    assert [op for op, _ in requests] == ["workflows", *["stage", "adopt_staged"] * 3]
    assert all(answer["ok"] is True for answer in answers)  # no stage refused for capacity
    _once(far, plan)


def test_a_local_to_local_adoption_failure_stops_the_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 0.2})
    monkeypatch.setattr(transfer_cli, "_STAGES", 1)
    jobs(source, 3)
    paged(monkeypatch, 1)

    def broken(stage: _moving.Stage) -> Any:
        raise OSError(5, "the final scan failed")

    monkeypatch.setattr(_moving.Stage, "begin", broken)
    assert run(tmp_path, "--json", "-q", "--state", "succeeded", source_name, target_name) == 1
    document = outcomes(capsys)
    assert [outcome["status"] for outcome in document["transfers"]] == ["failed"] * 2
    assert any("the final scan failed" in outcome["message"] for outcome in document["transfers"])
    (error,) = document["errors"]
    assert "stopped" in error and "1 selected job(s) were not attempted" in error  # the third page never held
    assert len(outgoing(source)) == 2 and job_ids_below(target.jobs) == []
    assert _owners(target) == [] and _scratch(target) == []


def test_a_pull_reserves_one_fetch_before_it_asks_for_it(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    near.set_policy({"visibility_deadline_seconds": 1.0})
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setattr(transfer_cli, "_STAGED_BYTES", 1 << 20)  # less than one fetch's reservation
    requests = recorded_requests(monkeypatch)
    plan = jobs(far, 3)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert _statuses(capsys) == ["adopted"] * 3
    # Each staged landing is adopted (and released) before the next fetch is asked for.
    assert [op for op, _ in requests if op in ("send", "release")] == ["send", "release"] * 3
    _once(near, plan)


def test_an_adoption_entry_without_its_bundle_is_an_unusable_report() -> None:
    document = {"bundles": [{"status": "adopted"}], "workspace_id": str(uuid.uuid4())}
    with pytest.raises(ValueError, match="without its bundle"):
        transfer_cli._checked_adoption(document, ["a" * 32])


# -- correction round 2 ---------------------------------------------------------------------------------------------


@pytest.fixture
def relay_ends(tmp_path: Path, remote: Remote, monkeypatch: pytest.MonkeyPatch) -> Ends:
    """Two remote workspaces on the stand-in cluster (``cluster`` SRC's host, ``other`` DST's) whose ``httk`` is
    :data:`PATCHED_STAGED`, and this process's temporary directory (the relays') kept apart."""

    project = tmp_path / "project"
    initialize_project(project, name="relay")
    fake_remote(project, host=SRC_HOST)
    fake_remote(project, name="other", host=DST_HOST)
    wrapper = tmp_path / "patched_staged.py"
    wrapper.write_text(PATCHED_STAGED, encoding="utf-8")
    remote.install("httk", f'#!/bin/sh\nexec {{python}} {wrapper} "$@"\n')
    near, near_name = registered(remote.root, "near")
    far, far_name = registered(remote.root, "far")
    relays = tmp_path / "relays"
    relays.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(relays))
    return Ends(project, near, f"cluster:{near_name}", far, f"other:{far_name}", relays, remote)


def _sizes(workspace: Workspace) -> dict[str, int]:
    return {hold.transfer_id: transfer_cli._measure(Path(hold.path))[0] for hold in _moving.held(workspace)}


def _caps(monkeypatch: pytest.MonkeyPatch, segment: int, staged: int) -> None:
    """Scale a push's segment bytes and the staged bytes down, on this side and the far side's alike."""

    monkeypatch.setattr(transfer_cli, "_SEGMENT_BYTES", segment)
    monkeypatch.setattr(transfer_cli, "_STAGED_BYTES", staged)
    monkeypatch.setenv("HTTK_TEST_FAR_STAGED_BYTES", str(staged))


def _relayed_fully(relay_ends: Ends, plan: list[Any], capsys: pytest.CaptureFixture[str], argv: list[str]) -> list[str]:
    """Run the transfer *argv*: every hold adopted and released, every job in DST once; the transfer ids."""

    assert run(relay_ends.project, "--json", "-q", *argv) == 0
    document = outcomes(capsys)
    assert {outcome["status"] for outcome in document["transfers"]} == {"adopted"} and document["errors"] == []
    assert outgoing(relay_ends.near) == []
    _once(relay_ends.far, plan)
    return [outcome["transfer_id"] for outcome in document["transfers"]]


def test_a_send_s_small_then_large_bundles_are_staged_in_segments(
    relay_ends: Ends, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    made = held(relay_ends.near, relay_ends.dst, 2)
    plan = [entry for _, entry in made]
    holds = sorted(_moving.held(relay_ends.near), key=lambda hold: hold.transfer_id)
    limit = 2 * max(_sizes(relay_ends.near).values())
    cap = 2 * limit  # the real ratio: a send closes at limit, the staged bytes at twice that
    (job,) = Path(holds[1].path).rglob("job.json")
    (job.parent / "big.bin").write_bytes(b"x" * (cap + 1))  # the second bundle alone is above the cap
    _caps(monkeypatch, limit, cap)
    requests = recorded_requests(monkeypatch)
    _relayed_fully(relay_ends, plan, capsys, ["--resume", relay_ends.src, relay_ends.dst])
    # One send carried both (it closes after the bundle that reaches its limit); they were staged apart.
    assert [len(header["names"]) for op, header in requests if op == "send"] == [2]
    stages = [header["transfer_ids"] for op, header in requests if op == "stage"]
    assert stages == [[holds[0].transfer_id], [holds[1].transfer_id]]


def test_src_failing_in_a_release_stops_the_other_destinations_too(
    relay_ends: Ends, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    other, other_name = registered(remote.root, "other-far")
    for workspace in (relay_ends.far, other):
        workspace.set_policy({"visibility_deadline_seconds": 4.0})  # every stage pending until the end
    left = [*held(relay_ends.near, relay_ends.dst, 2), *held(relay_ends.near, f"other:{other_name}", 2)]
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "release")  # SRC dies at the first release, of the first pair
    requests = recorded_requests(monkeypatch)
    assert run(relay_ends.project, "--json", "-q", "--resume", relay_ends.src) == 1
    transfers = outcomes(capsys)["transfers"]
    ops = [op for op, _ in requests]
    assert ops.count("adopt_staged") == 1  # nothing more, for either destination
    assert ops.count("release") == 1 and ops[ops.index("release") + 1 :] == []  # no request after SRC died
    adopted = [outcome for outcome in transfers if outcome["status"] == "adopted"]
    assert len(adopted) == 1 and "the hold was not released" in adopted[0]["message"]
    rest = [outcome for outcome in transfers if outcome["status"] != "adopted"]
    # Left staged and abandoned ("not adopted"), or never staged when the first pair matured before the other's page
    # was driven ("not delivered"): either way stopped, and held.
    stopped = ("not adopted, since the transfer stopped", "not delivered, since the transfer")
    assert len(rest) == 3 and all(any(text in o["message"] for text in stopped) for o in rest)
    assert all("stopped" in o["message"] for o in rest)
    assert len(outgoing(relay_ends.near)) == 4
    # Whichever pair went first (the holds go in transfer id order): its one job arrived, nothing else anywhere.
    (staged,) = [header for op, header in requests if op == "adopt_staged"]
    arrived = {
        name: sum(find(workspace, entry[0]["id"]) is not None for _, entry in left)
        for name, workspace in ((relay_ends.dst.split(":", 1)[1], relay_ends.far), (other_name, other))
    }
    assert arrived == {name: int(name == staged["workspace"]) for name in arrived}
    assert adopted[0]["destination"] == f"other:{staged['workspace']}"


def test_src_failing_in_a_page_s_release_adopts_no_other_pair_s_matured_stage(
    relay_ends: Ends, remote: Remote, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Pair A's page leaves a staged segment; it has matured when pair B's page is driven, whose segment is adopted at
    # once and whose release kills SRC. The sweep after B's page must not adopt A's stage: SRC is known dead.
    from httk.workflow import _bundles

    other, other_name = registered(remote.root, "other-far")
    relay_ends.far.set_policy({"visibility_deadline_seconds": 0.1})  # A: staged, matured by B's page
    other.set_policy({"visibility_deadline_seconds": 0})  # B: adopted (and released) within its page
    made = []
    for number, locator in ((1, relay_ends.dst), (2, f"other:{other_name}")):  # A's hold first, then B's
        monkeypatch.setattr(_bundles, "uuid", SimpleNamespace(uuid4=lambda number=number: uuid.UUID(int=number)))
        made += held(relay_ends.near, locator, 1)
    monkeypatch.setattr(_bundles, "uuid", uuid)
    monkeypatch.setattr(transfer_cli, "_PAGE_HOLDS", 1)
    real_page = transfer_cli._Transfer._drive_page
    pages: list[int] = []

    def drive_page(run_: Any, *arguments: Any, **options: Any) -> None:
        pages.append(1)
        if len(pages) == 2:
            time.sleep(0.5)  # A's stage has matured before B's page
        real_page(run_, *arguments, **options)

    monkeypatch.setattr(transfer_cli._Transfer, "_drive_page", drive_page)
    monkeypatch.setenv("HTTK_TEST_EXIT_ON_OP", "release")  # SRC dies at the first release: B's
    requests = recorded_requests(monkeypatch)
    assert run(relay_ends.project, "--json", "-q", "--resume", relay_ends.src) == 1
    transfers = {outcome["destination"]: outcome for outcome in outcomes(capsys)["transfers"]}
    ops = [op for op, _ in requests]
    assert ops.count("release") == 1 and ops[ops.index("release") + 1 :] == []  # nothing adopted after SRC died
    assert [header["workspace"] for op, header in requests if op == "adopt_staged"] == [other_name]
    a, b = transfers[relay_ends.dst], transfers[f"other:{other_name}"]
    assert b["status"] == "adopted" and "the hold was not released" in b["message"]
    assert a["status"] == "failed" and "not adopted, since the transfer stopped" in a["message"]
    assert len(outgoing(relay_ends.near)) == 2  # both held: B's unreleased, A's never adopted
    assert find(relay_ends.far, made[0][1][0]["id"]) is None and find(other, made[1][1][0]["id"]) is not None


@pytest.mark.parametrize("where", ["setup", "fetch"])
def test_an_interrupt_in_a_pull_s_fetch_leaves_no_owner_and_no_landing(
    cluster: tuple[Path, Workspace, str, Workspace, str], monkeypatch: pytest.MonkeyPatch, where: str
) -> None:
    project, near, near_name, far, far_name = cluster
    plan = jobs(far, 2)
    if where == "setup":
        real_scratch = _kernel.Owner.scratch

        def scratch(owner: _kernel.Owner, purpose: str) -> Path:
            if purpose == "landing":
                raise KeyboardInterrupt  # the stage's owner registered, its landing not made
            return real_scratch(owner, purpose)

        monkeypatch.setattr(_kernel.Owner, "scratch", scratch)
    else:

        def receive(*arguments: Any, **options: Any) -> Any:
            raise KeyboardInterrupt  # while the fetch lands

        monkeypatch.setattr(transfer_cli, "_receive_part", receive)
    with pytest.raises(KeyboardInterrupt):
        run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name)
    assert _owners(near) == [] and _scratch(near) == [] and job_ids_below(near.jobs) == []


def test_an_interrupt_in_an_adoption_marks_the_pair_and_releases_only_over_a_usable_session() -> None:
    src = transfer_cli._End("cluster:near", binding=cast(Any, SimpleNamespace(name="cluster:near", remote="cluster")))
    dst = transfer_cli._End("/tmp/never-used")
    arguments = argparse.Namespace(quiet=True, keep=False)
    pair = transfer_cli._Pair(dst)
    settled = transfer_cli._Deliveries(src, dst, {}, frozenset(), cast(Any, None), arguments, pair)
    settled.session = cast(Any, SimpleNamespace(is_open=False))  # the interrupt took SRC's session with it

    def adopt() -> None:
        raise KeyboardInterrupt

    transfer_cli._stage_pending(pair, transfer_cli._PendingStage([], settled, 0, 0.0, adopt, lambda _reason: None))
    with pytest.raises(KeyboardInterrupt):
        transfer_cli._flush(pair)
    assert pair.interrupted
    settled.release(["a" * 32])  # what a concluding batch would release
    assert settled.unreleased == [("a" * 32, "the transfer was interrupted")]
    # And an interrupt anywhere in the transfer marks every pair so.
    run_ = transfer_cli._Transfer(src, dst, {}, cast(Any, None), arguments, contextlib.ExitStack())
    other = run_.pair(dst)
    with pytest.raises(KeyboardInterrupt), run_.interruptible():
        raise KeyboardInterrupt
    assert other.interrupted and other.stopped == "the transfer was interrupted"


def test_an_error_in_an_earlier_page_s_adoption_still_reports_every_hold(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 0.3})
    paged(monkeypatch, 1)
    monkeypatch.setattr(transfer_cli, "_STAGES", 1)  # the first page's stage is adopted while the second is driven
    real_relay = transfer_cli._relay
    relays: list[int] = []

    def relay(*arguments: Any, **options: Any) -> None:
        relays.append(1)
        if len(relays) == 1:
            raise BrokenPipeError(32, "stderr went away")  # between the settle and the release
        real_relay(*arguments, **options)

    monkeypatch.setattr(transfer_cli, "_relay", relay)
    plan = jobs(near, 2)
    assert run(project, "--json", "-q", "--state", "succeeded", near_name, f"cluster:{far_name}") == 1
    document = outcomes(capsys)  # the report renders
    transfers = document["transfers"]
    assert len(transfers) == 2 and all(outcome.get("status") == "failed" for outcome in transfers)
    first = [outcome for outcome in transfers if "failed while its staged segment was adopted" in outcome["message"]]
    assert len(first) == 1 and "unless it was adopted" in first[0]["message"] and "--resume" in first[0]["message"]
    # Truthful: that segment was adopted there and not released; the other was never adopted.
    assert len(outgoing(near)) == 2 and sum(find(far, mapping["id"]) is not None for mapping, _, _ in plan) == 1
    assert any("stopped" in error for error in document["errors"])


@pytest.mark.parametrize("direction", ["pull", "local-to-local"])
def test_a_destination_sealed_since_the_stage_adopts_nothing(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    direction: str,
) -> None:
    project, near, near_name, far, far_name = cluster
    if direction == "pull":
        source, target, argv = far, near, [f"cluster:{far_name}", near_name]
    else:
        target, target_name = registered(tmp_path, "target")
        source, argv = near, [near_name, target_name]
    target.set_policy({"visibility_deadline_seconds": 0.2})
    plan = jobs(source, 2)
    seal = workspace_seal_path(target)
    real_first = _moving.settle_first

    def settle_first(*arguments: Any, **options: Any) -> Any:
        first = real_first(*arguments, **options)
        seal.parent.mkdir(parents=True, exist_ok=True)
        seal.write_text("{}")  # sealed once staged, before the adoption
        return first

    monkeypatch.setattr(_moving, "settle_first", settle_first)
    assert run(project, "--json", "-q", *selected(plan), *argv) == 1
    document = outcomes(capsys)
    for outcome in document["transfers"]:
        assert outcome["status"] == "failed" and "sealed" in outcome["message"]
        assert "nothing of it was adopted" in outcome["message"]
    assert job_ids_below(target.jobs) == [] and _owners(target) == [] and _scratch(target) == []
    assert len(outgoing(source)) == 2 and all(Path(hold.path).exists() for hold in _moving.held(source))
    seal.unlink()
    monkeypatch.setattr(_moving, "settle_first", real_first)
    resume = ["--resume", *argv] if direction == "pull" else ["--resume", near_name, argv[1]]
    assert run(project, "--json", "-q", *resume) == 0
    assert _statuses(capsys) == ["adopted"] * 2
    _once(target, plan)


def test_a_pull_waits_for_the_first_stage_once_two_would_exceed_the_staged_bytes(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    near.set_policy({"visibility_deadline_seconds": 2.0})
    plan = jobs(far, 3)
    one = max(_sizes_of_jobs(far, plan))
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BYTES", 1)  # a fetch of one bundle, a reservation of one byte
    monkeypatch.setattr(transfer_cli, "_STAGED_BYTES", one + one // 2)  # one stage fits, two do not
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), f"cluster:{far_name}", near_name) == 0
    assert _statuses(capsys) == ["adopted"] * 3
    sent = [op for op, _ in requests if op in ("send", "release")]
    # The second landing staged, the first adopted (and released) before the third is fetched.
    assert sent.index("release") < [index for index, op in enumerate(sent) if op == "send"][2]
    _once(near, plan)


def test_a_kept_local_copy_waits_for_the_first_stage_once_two_would_exceed_the_staged_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 2.0})
    plan = jobs(source, 3)
    paged(monkeypatch, 1)
    one = max(_sizes_of_jobs(source, plan))
    monkeypatch.setattr(transfer_cli, "_STAGED_BYTES", one + one // 2)
    events: list[str] = []
    real_hold, real_adopt = _moving.hold, _moving.adopt

    def hold(*arguments: Any, **options: Any) -> Any:
        events.append("hold")
        return real_hold(*arguments, **options)

    def adopt(*arguments: Any, **options: Any) -> Any:
        events.append("adopt")
        return real_adopt(*arguments, **options)

    monkeypatch.setattr(_moving, "hold", hold)
    monkeypatch.setattr(_moving, "adopt", adopt)
    assert run(tmp_path, "--json", "-q", "--keep", "--state", "succeeded", source_name, target_name) == 0
    assert _statuses(capsys) == ["adopted"] * 3
    # The second page's copies would exceed the staged bytes: the first page is adopted before they are made.
    assert events.index("adopt") == 2
    _once(target, plan)


def _sizes_of_jobs(workspace: Workspace, plan: list[Any]) -> list[int]:
    """The regular-file bytes of each job of *plan* in *workspace* (what a hold of it lands)."""

    sizes = []
    for mapping, _, _ in plan:
        ref = find(workspace, mapping["id"])
        assert ref is not None
        sizes.append(transfer_cli._measure(ref.path)[0])
    return sizes


@pytest.mark.parametrize("keep", [False, True], ids=["move", "keep"])
def test_a_parent_in_a_later_page_is_held_while_its_child_s_page_is_staged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], keep: bool
) -> None:
    # With a long deadline the child's page is still staged (a hold, not an away copy) when the parent's page is
    # held: the parent is held by itself, and both are adopted (the positive-deadline branch of the documented
    # --keep-between-pages difference).
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    target.set_policy({"visibility_deadline_seconds": 3.0})
    made = family(source, "camp/p", {"c1": ("succeeded", "camp/p/c1")})
    paged(monkeypatch, 1)
    argv = ["--json", "-q", *(["--keep"] if keep else []), *ids(made["c1"], made["parent"]), source_name, target_name]
    assert run(tmp_path, *argv) == 0
    document = outcomes(capsys)
    assert [outcome["status"] for outcome in document["transfers"]] == ["adopted"] * 2 and document["errors"] == []
    assert all(outcome["kept"] is keep for outcome in document["transfers"])
    for name in ("c1", "parent"):
        assert find(target, made[name]["id"]) is not None
        here = find(source, made[name]["id"])
        assert (here is not None and here.state == "away") if keep else here is None
    assert outgoing(source) == []


def test_a_push_counts_each_staged_segment_s_landed_bytes(
    cluster: tuple[Path, Workspace, str, Workspace, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    project, near, near_name, far, far_name = cluster
    far.set_policy({"visibility_deadline_seconds": 1.0})
    plan = jobs(near, 3)
    one = max(_sizes_of_jobs(near, plan))
    monkeypatch.setattr(transfer_cli, "_SEGMENT_BUNDLES", 1)
    monkeypatch.setattr(transfer_cli, "_STAGED_BYTES", one + one // 2)  # one staged segment fits, two do not
    requests = recorded_requests(monkeypatch)
    assert run(project, "--json", "-q", *selected(plan), near_name, f"cluster:{far_name}") == 0
    assert _statuses(capsys) == ["adopted"] * 3
    # Each segment's landed bytes, as DST answered them, count: the oldest is adopted before the next is staged.
    assert [op for op, _ in requests] == ["workflows", *["stage", "adopt_staged"] * 3]
    _once(far, plan)
