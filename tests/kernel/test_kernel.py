"""Unit tests of :mod:`httk.workflow._kernel` on a real filesystem, including multi-process races."""

import json
import multiprocessing
import os
import random
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pytest

from conftest import TestProfile as _TestProfile
from httk.workflow import _fs, _kernel
from httk.workflow._fs import Loc
from httk.workflow._kernel import (
    OWNED,
    JobRef,
    ListingCache,
    OwnedJob,
    Owner,
    OwnerDeclaredDead,
    OwnerLost,
    Release,
    ReleasedJobError,
    attest_dead,
    claim,
    claim_exchange_name,
    exchange_translation_applied,
    format_job_name,
    list_jobs,
    list_owners,
    locate,
    locate_many,
    parse_job_name,
    post_request,
    prune_empty_placements,
    reconcile_scratch,
    record_exchange_translation,
    recover,
    register_owner,
    register_reconciler,
    submit,
    sweep_unrecorded_shm,
    take,
)
from httk.workflow._state import StateDoc, read_state_unowned
from httk.workflow.errors import FormatError, WorkflowError

DEADLINE = 0.2


@dataclass(frozen=True)
class FakeWorkspace:
    """The kernel's view of a workspace."""

    root: Path
    control: Path
    jobs: Path
    durable: bool
    visibility_deadline: float


def make_workspace(root: Path, *, durable: bool = False) -> FakeWorkspace:
    return FakeWorkspace(root, root / ".httk-workspace", root / "jobs", durable, DEADLINE)


@pytest.fixture()
def ws(tmp_path: Path) -> FakeWorkspace:
    return make_workspace(tmp_path / "ws")


@pytest.fixture(autouse=True)
def _clean_kernel(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(_kernel, "_RECONCILERS", {})
    yield
    _fs.set_fault_injector(None)


def new_owner(ws: FakeWorkspace, kind: str = "manager") -> Owner:
    return register_owner(ws, kind=kind, label=None, allocation=None, advertised={})


def write_payload(
    directory: Path, *, job_id: str | None = None, tag: str | None = "t", placement: str = "p/q", priority: int = 500
) -> str:
    job_id = job_id or str(uuid.uuid4())
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": job_id,
        "tag": tag,
        "placement": placement,
        "priority": priority,
    }
    (directory / "job.json").write_text(json.dumps(document))
    return job_id


def new_job(ws: FakeWorkspace, owner: Owner, *, state: str = "ready", **payload: object) -> JobRef:
    staging = owner.scratch("build") / "job"
    write_payload(staging, **payload)  # type: ignore[arg-type]
    return submit(ws, owner, staging, state=state)


def job_ids_in(directory: Path) -> list[str]:
    """Every job UUID found anywhere below *directory* (names only; job directories are not entered)."""

    found = []
    for path, names, _files in os.walk(directory):
        for name in list(names):
            if "~" in name:
                found.append(parse_job_name(name).job_id)
                names.remove(name)
        del path
    return found


def everywhere(ws: FakeWorkspace) -> Counter[str]:
    return Counter(job_ids_in(ws.jobs))


def request_doc(job_id: str, request_id: str | None = None) -> dict[str, object]:
    return {"request_id": request_id or str(uuid.uuid4()), "job_id": job_id, "action": "cancel"}


def owned_name(job_id: str, from_state: str = "ready", tag: str = "t") -> str:
    return format_job_name(f"{tag}--{job_id}", 500, _fs.fresh_token(), from_state)


# -- 1. names -------------------------------------------------------------------------------------------------------


def test_name_grammars_round_trip() -> None:
    job_id = str(uuid.uuid4())
    token = _fs.fresh_token()
    for key in (job_id, f"si.relax-1--{job_id}"):
        unowned = format_job_name(key, 7, token)
        assert unowned == f"{key}~p007~{token}"
        assert parse_job_name(unowned) == (key, job_id, 7, token, None)
        owned = format_job_name(key, 999, token, "paused")
        assert owned == f"{key}~p999~{token}~paused"
        assert parse_job_name(owned) == (key, job_id, 999, token, "paused")
    ref = JobRef.from_path(Path("/w/jobs/ready/a/b") / unowned, state="ready", placement=PurePosixPath("a/b"))
    assert (ref.priority, ref.token, ref.owner_id, ref.from_state) == (7, token, None, None)
    assert ref.cursor == f"a/b/{unowned}"
    owned_ref = JobRef.from_path(Path("/w/jobs/owned") / ("b" * 32) / owned, state=OWNED, owner_id="b" * 32)
    assert (owned_ref.placement, owned_ref.from_state, owned_ref.owner_id) == (None, "paused", "b" * 32)


@pytest.mark.parametrize(
    "name",
    [
        "x",
        "{id}~p500",
        "{id}~p500~{t}~ready~x",
        "{id}~p50~{t}",
        "{id}~p1000~{t}",
        "{id}~q500~{t}",
        "{id}~p500~{T}",
        "{id}~p500~{t}x",
        "{id}~p500~{t}~owned",
        "{id}~p500~{t}~",
        "bad~tag--{id}~p500~{t}",
        "Tag--{id}~p500~{t}",
        "{ID}~p500~{t}",
    ],
)
def test_malformed_names_are_refused(name: str) -> None:
    job_id, token = str(uuid.uuid4()), _fs.fresh_token()
    with pytest.raises(FormatError):
        parse_job_name(name.format(id=job_id, ID=job_id.upper(), t=token, T=token.upper()))


def test_format_and_ref_refusals() -> None:
    job_id, token = str(uuid.uuid4()), _fs.fresh_token()
    for args in ((job_id, 1000, token), (job_id, -1, token), (job_id, 1, "short"), ("x~y", 1, token)):
        with pytest.raises(ValueError):
            format_job_name(*args)
    with pytest.raises(ValueError):
        format_job_name(job_id, 1, token, "owned")
    unowned, owned = format_job_name(job_id, 1, token), format_job_name(job_id, 1, token, "ready")
    with pytest.raises(FormatError):
        JobRef.from_path(Path("/w") / owned, state="ready", placement=PurePosixPath())
    with pytest.raises(FormatError):
        JobRef.from_path(Path("/w") / unowned, state=OWNED, owner_id="a" * 32)
    with pytest.raises(FormatError):
        JobRef.from_path(Path("/w") / owned, state=OWNED, owner_id="A" * 32)
    with pytest.raises(FormatError):
        JobRef.from_path(Path("/w") / unowned, state="ready")


# -- 2. submit, list, claim ------------------------------------------------------------------------------------------


def test_submit_list_claim(ws: FakeWorkspace) -> None:
    owner, rival = new_owner(ws), new_owner(ws)
    ref = new_job(ws, owner, priority=42)
    assert ref.path.parent == ws.jobs / "ready" / "p" / "q"
    assert ref.priority == 42 and ref.state == "ready"
    assert list(list_jobs(ws, "ready")) == [ref]
    job = claim(ws, owner, ref)
    assert isinstance(job, OwnedJob)
    assert job.path.parent == ws.jobs / OWNED / owner.owner_id
    assert (job.from_state, job.from_priority, job.job_id) == ("ready", 42, ref.job_id)
    assert job.placement() == PurePosixPath("p/q")
    header = job.header()
    assert (header.tag, header.priority, header.job_key) == ("t", 42, ref.job_key)
    job.require_quiescent()
    assert claim(ws, rival, ref) is None
    with pytest.raises(ValueError):
        claim(ws, rival, job.ref)
    assert list(list_jobs(ws, "ready")) == []
    with pytest.raises(ValueError):
        list(list_jobs(ws, OWNED))


def test_submit_refusals(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    staging = owner.scratch("build") / "job"
    with pytest.raises(OwnerLost):
        submit(ws, owner, staging)
    for placement, priority in (("a~b", 1), ("/abs", 1), ("a/", 1), ("a/./b", 1), ("a", 1000)):
        write_payload(staging, placement=placement, priority=priority)
        with pytest.raises(FormatError):
            submit(ws, owner, staging)
    write_payload(staging)
    with pytest.raises(ValueError):
        submit(ws, owner, staging, state=OWNED)


def test_list_jobs_prefixes_limit_and_cursor(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    for placement in ("a", "a/x", "b", "", "c/d"):
        new_job(ws, owner, placement=placement)
        new_job(ws, owner, placement=placement)
    every = list(list_jobs(ws, "ready"))
    assert len(every) == 10
    assert [ref.cursor for ref in every] == sorted((ref.cursor for ref in every), key=lambda c: c.split("/"))
    assert {ref.placement for ref in list_jobs(ws, "ready", prefixes=[PurePosixPath("a")])} == {
        PurePosixPath("a"),
        PurePosixPath("a/x"),
    }
    overlapping = list(list_jobs(ws, "ready", prefixes=[PurePosixPath("a/x"), PurePosixPath("a"), PurePosixPath("c")]))
    assert len(overlapping) == 6
    pages, start = [], None
    while page := list(list_jobs(ws, "ready", limit=3, start=start)):
        pages += page
        start = page[-1].cursor
    assert pages == every
    # A damaged name is skipped, not entered; a symlinked directory is not followed.
    (ws.jobs / "ready" / "a" / "junk~name").mkdir()
    os.symlink(ws.jobs / "ready" / "b", ws.jobs / "ready" / "link")
    assert list(list_jobs(ws, "ready")) == every


# -- 3. a real claim race -------------------------------------------------------------------------------------------


def _claim_worker(ws: FakeWorkspace, start: object, results: object) -> None:
    owner = new_owner(ws)
    start.wait()  # type: ignore[attr-defined]
    refs = list(list_jobs(ws, "ready"))
    random.shuffle(refs)
    won = [job.job_id for ref in refs if (job := claim(ws, owner, ref)) is not None]
    results.put((owner.owner_id, won))  # type: ignore[attr-defined]


@pytest.mark.slow
def test_concurrent_claims_are_exclusive(ws: FakeWorkspace, test_profile: _TestProfile) -> None:
    workers = test_profile.scale(normal=8, extended=32)
    jobs = test_profile.scale(normal=20, extended=200)
    creator = new_owner(ws)
    ids = {new_job(ws, creator, placement=f"p{index % 4}").job_id for index in range(jobs)}
    context = multiprocessing.get_context("spawn")
    start, results = context.Event(), context.Queue()
    processes = [context.Process(target=_claim_worker, args=(ws, start, results)) for _ in range(workers)]
    for process in processes:
        process.start()
    start.set()
    reports = [results.get(timeout=120) for _ in processes]
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    winners: dict[str, str] = {}
    for owner_id, won in reports:
        for job_id in won:
            assert job_id not in winners, f"{job_id} claimed twice"
            winners[job_id] = owner_id
    assert set(winners) == ids
    assert everywhere(ws) == Counter(ids)
    for owner_id in {owner for owner, _ in reports}:
        on_disk = job_ids_in(ws.jobs / OWNED / owner_id)
        assert sorted(on_disk) == sorted(job for job, holder in winners.items() if holder == owner_id)


# -- 4. the release rule ----------------------------------------------------------------------------------------------


def test_release_rule(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    ref = new_job(ws, owner)
    job = claim(ws, owner, ref)
    assert job is not None
    applied = post_request(ws, request_doc(job.job_id))
    pending = post_request(ws, request_doc(job.job_id))
    request_id = applied.name.split(".")[1]
    released = job.release(StateDoc.empty(job.job_id), Release("failed", 17, (request_id,)))
    assert released.state == "failed" and released.priority == 17
    assert released.path.parent == ws.jobs / "failed" / "p" / "q"
    assert not applied.exists() and pending.exists()
    doc, damaged = read_state_unowned(released.path / "state.json")
    assert not damaged and doc is not None
    assert doc.release_to == Release("failed", 17) and doc.applied_requests == (request_id,)
    for call in (job.read_state, job.request_files, job.pending_release, job.header, job.discard):
        with pytest.raises(ReleasedJobError):
            call()
    with pytest.raises(ReleasedJobError):
        job.release(doc, Release("ready", 1))
    # Claimed again from the target, nothing is pending.
    again = claim(ws, owner, released)
    assert again is not None and again.pending_release() is None
    # The applied id is skipped and its leftover file would be deleted; the pending one is listed.
    assert again.request_files() == [pending]


def test_release_to_the_claimed_state_and_priority(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner, priority=500))
    assert job is not None
    released = job.release(StateDoc.empty(job.job_id), Release("ready", 500))
    assert released.state == "ready" and released.priority == 500 and released.path != job.ref.path
    again = claim(ws, owner, released)
    assert again is not None and again.pending_release() is None


def test_release_requires_quiescence_and_valid_target(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    attempt = str(uuid.uuid4())
    job.begin_attempt(attempt)
    with pytest.raises(WorkflowError, match="live"):
        job.release(StateDoc.empty(job.job_id), Release("ready", 1))
    with pytest.raises(WorkflowError, match="live"):
        job.discard()
    job.end_attempt(attempt)
    with pytest.raises(ValueError):
        job.write_state(StateDoc.empty(str(uuid.uuid4())))
    job.release(StateDoc.empty(job.job_id), Release("ready", 1))


def _crash_on_release(op: str, phase: str, src: Loc | None, dst: Loc | None) -> None:
    del src
    if op == "rename" and phase == "before" and dst is not None and "/jobs/failed/" in str(dst.path):
        raise SystemExit("crash before the release move")


def test_pending_release_is_finished_by_the_next_claimant(ws: FakeWorkspace) -> None:
    first = new_owner(ws)
    job = claim(ws, first, new_job(ws, first))
    assert job is not None
    request = post_request(ws, request_doc(job.job_id))
    request_id = request.name.split(".")[1]
    _fs.set_fault_injector(_crash_on_release)
    with pytest.raises(SystemExit):
        job.release(StateDoc.empty(job.job_id), Release("failed", 3, (request_id,)))
    _fs.set_fault_injector(None)
    assert not request.exists()
    attest_dead(ws, first.owner_id, by="operator", evidence=[], operator="test", reason="crashed")
    second = new_owner(ws)
    report = recover(ws, second, first.owner_id)
    (returned,) = report.returned
    assert returned.state == "ready"
    again = claim(ws, second, returned)
    assert again is not None
    pending = again.pending_release()
    assert pending == Release("failed", 3)
    doc = again.read_state()
    assert doc is not None and doc.applied_requests == (request_id,)
    assert pending is not None
    final = again.release(doc, pending)
    assert final.state == "failed" and final.priority == 3
    third = claim(ws, second, final)
    assert third is not None and third.pending_release() is None


# -- 5. request files ---------------------------------------------------------------------------------------------------


def test_request_files_filter_and_delete_applied(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    other = new_job(ws, owner)
    assert job is not None
    assert job.request_files() == []
    applied = post_request(ws, request_doc(job.job_id))
    pending = post_request(ws, request_doc(job.job_id))
    foreign = post_request(ws, request_doc(other.job_id))
    (ws.control / "requests" / f".{job.job_id}.x.json.{_fs.fresh_token()}.tmp").write_text("{}")
    (ws.control / "requests" / f"{job.job_id}.not-a-uuid.json").write_text("{}")
    applied_id = applied.name.split(".")[1]
    doc = StateDoc.empty(job.job_id).with_release(Release("ready", 500, (applied_id,)))
    job.write_state(doc)
    assert job.request_files() == [pending]
    assert not applied.exists() and foreign.exists()
    # The id is dropped at the next write, since its file is gone.
    job.write_state(doc)
    stored = job.read_state()
    assert stored is not None and stored.applied_requests == ()
    with pytest.raises(ValueError):
        post_request(ws, {"job_id": job.job_id, "request_id": "x"})


def test_applied_ids_without_files_are_dropped(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    request_id = post_request(ws, request_doc(job.job_id)).name.split(".")[1]
    released = job.release(StateDoc.empty(job.job_id), Release("ready", 500, (request_id,)))
    again = claim(ws, owner, released)
    assert again is not None
    doc = again.read_state()
    assert doc is not None and doc.applied_requests == (request_id,)
    # release deleted the file itself, so the next claimant sees no leftover: the id must still shrink away.
    assert again.request_files() == []
    again.write_state(doc)
    stored = again.read_state()
    assert stored is not None and stored.applied_requests == ()


# -- 6. lost ownership --------------------------------------------------------------------------------------------------


def test_lost_ownership_raises(ws: FakeWorkspace, tmp_path: Path) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    os.rename(job.path, tmp_path / "stolen")
    with pytest.raises(OwnerLost):
        job.write_state(StateDoc.empty(job.job_id))
    with pytest.raises(OwnerLost):
        job.release(StateDoc.empty(job.job_id), Release("ready", 1))
    with pytest.raises(OwnerLost):
        job.append_log("claimed")
    with pytest.raises(OwnerLost):
        job.discard()


def test_symlinked_placement_is_refused(ws: FakeWorkspace, tmp_path: Path) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner, placement="link/sub"))
    assert job is not None
    (tmp_path / "elsewhere" / "sub").mkdir(parents=True)
    assert prune_empty_placements(ws, budget=10) == 2  # the claim emptied ready/link/sub
    os.symlink(tmp_path / "elsewhere", ws.jobs / "ready" / "link")
    with pytest.raises(_fs.UnsafePath):
        job.release(StateDoc.empty(job.job_id), Release("ready", 500))
    assert job.path.exists() and owner.owned() == [job.ref]
    assert os.listdir(tmp_path / "elsewhere" / "sub") == []
    staging = owner.scratch("build") / "job"
    write_payload(staging, placement="link/sub")
    with pytest.raises(_fs.UnsafePath):
        submit(ws, owner, staging)
    assert staging.exists()
    # A non-directory placement component is refused the same way.
    (ws.jobs / "failed").mkdir()
    (ws.jobs / "failed" / "link").write_text("x")
    with pytest.raises(_fs.UnsafePath):
        job.release(StateDoc.empty(job.job_id), Release("failed", 500))
    assert job.path.exists()


# -- 7. owners ----------------------------------------------------------------------------------------------------------


def test_register_owner_record(ws: FakeWorkspace) -> None:
    owner = register_owner(
        ws,
        kind="cli",
        label="me",
        allocation={"probe": "slurm", "kind": "slurm", "identity": {"job": "1"}, "end_time": None},
        advertised={"pools": ["default"], "prefixes": []},
    )
    record = json.loads((owner.path / "owner.json").read_text())
    assert record["format"] == "httk-workflow-owner" and record["format_version"] == 1
    assert record["owner_id"] == owner.owner_id == owner.path.name and len(owner.owner_id) == 32
    assert record["pid"] == os.getpid() and record["kind"] == "cli" and record["pools"] == ["default"]
    assert isinstance(record["process_start_ticks"], int) and isinstance(record["boot_id"], str)
    owner.check_alive()
    owner.heartbeat()
    assert json.loads((owner.path / "heartbeat.json").read_text())["owner_id"] == owner.owner_id
    listed = {entry.owner_id: entry for entry in list_owners(ws)}
    assert listed[owner.owner_id].record == record and listed[owner.owner_id].tombstone is None
    with pytest.raises(ValueError):
        register_owner(ws, kind="robot", label=None, allocation=None, advertised={})
    with pytest.raises(ValueError):
        register_owner(ws, kind="cli", label=None, allocation=None, advertised={"secret": 1})


def test_check_alive_fail_stop(ws: FakeWorkspace) -> None:
    dead = new_owner(ws)
    attest_dead(ws, dead.owner_id, by="probe", evidence=[{"subject": "s", "rule": "process_gone", "detail": "d"}])
    with pytest.raises(OwnerDeclaredDead):
        dead.check_alive()
    tombstone = json.loads((dead.path / "dead.json").read_text())
    assert tombstone["format"] == "httk-workflow-tombstone" and tombstone["by"] == "probe"
    assert "operator" not in tombstone
    assert {entry.owner_id: entry for entry in list_owners(ws)}[dead.owner_id].tombstone == tombstone
    missing = new_owner(ws)
    (missing.path / "owner.json").unlink()
    with pytest.raises(OwnerDeclaredDead):
        missing.check_alive()
    foreign = new_owner(ws)
    (foreign.path / "owner.json").write_text(json.dumps({"owner_id": "c" * 32}))
    with pytest.raises(OwnerDeclaredDead):
        foreign.check_alive()
    (foreign.path / "owner.json").write_text("{torn")
    with pytest.raises(OwnerDeclaredDead):
        foreign.check_alive()
    for bad in ({"by": "nobody", "evidence": []}, {"by": "probe", "evidence": [{"subject": "s"}]}):
        with pytest.raises(ValueError):
            attest_dead(ws, dead.owner_id, **bad)


def test_close_refuses_held_jobs_and_removes_everything(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    owner.heartbeat()
    with pytest.raises(WorkflowError, match="holds"):
        owner.close()
    job.release(StateDoc.empty(job.job_id), Release("ready", 500))
    owner.close()
    assert not owner.path.exists()
    assert not (ws.jobs / OWNED / owner.owner_id).exists()
    assert [name for name in os.listdir(ws.control / "tmp") if name.startswith(owner.owner_id)] == []
    owner.close()  # idempotent


def test_close_keeps_owner_when_scratch_or_launch_remains(ws: FakeWorkspace) -> None:
    calls: list[Path] = []

    def keep(owner: Owner, path: Path) -> bool:
        calls.append(path)
        return False

    def done(owner: Owner, path: Path) -> bool:
        calls.append(path)
        return True

    register_reconciler("eject", keep)
    register_reconciler("adopt", done)
    with pytest.raises(ValueError):
        register_reconciler("Bad.Purpose", done)
    owner = new_owner(ws)
    kept = owner.scratch("eject")
    (kept / "bundle.json").write_text("{}")
    finished = owner.scratch("adopt")
    (finished / "x").write_text("x")
    empty_adopt = owner.scratch("adopt")
    owner.close()
    assert calls == [kept, finished] or calls == [finished, kept]
    assert kept.exists() and not finished.exists() and not empty_adopt.exists()
    assert (owner.path / "owner.json").exists()
    # With the scratch gone but a launch directory present, the owner directory still stays.
    _kernel._RECONCILERS.clear()
    launch = owner.launch_dir(str(uuid.uuid4()), "0")
    owner.close()
    assert not kept.exists() and launch.exists() and (owner.path / "owner.json").exists()
    attempt = launch.name.split(".")[0]
    owner.remove_launch(attempt, "0")
    owner.close()
    assert not owner.path.exists()
    with pytest.raises(ValueError):
        owner.launch_dir("not-a-uuid", "0")
    with pytest.raises(ValueError):
        owner.launch_dir(str(uuid.uuid4()), "1")
    with pytest.raises(ValueError):
        owner.scratch("a.b")


def test_exit_on_interrupt_returns_jobs(ws: FakeWorkspace) -> None:
    creator = new_owner(ws)
    first, second = new_job(ws, creator, state="paused"), new_job(ws, creator, priority=3)
    with pytest.raises(KeyboardInterrupt), new_owner(ws, "cli") as owner:
        assert claim(ws, owner, first) is not None and claim(ws, owner, second) is not None
        raise KeyboardInterrupt
    assert {ref.job_id for ref in list_jobs(ws, "paused")} == {first.job_id}
    (back,) = list_jobs(ws, "ready")
    assert back.job_id == second.job_id and back.priority == 3
    assert not owner.path.exists()


def test_exit_on_error_touches_nothing(ws: FakeWorkspace) -> None:
    creator = new_owner(ws)
    ref = new_job(ws, creator)
    with pytest.raises(RuntimeError), new_owner(ws) as owner:
        assert claim(ws, owner, ref) is not None
        raise RuntimeError("bug")
    assert len(owner.owned()) == 1 and (owner.path / "owner.json").exists()
    with new_owner(ws) as clean:
        pass
    assert not clean.path.exists()


# -- 8. recovery ----------------------------------------------------------------------------------------------------------


def _plant_owned(ws: FakeWorkspace, owner_id: str, *, placement: str = "p/q", from_state: str = "ready") -> str:
    job_id = str(uuid.uuid4())
    write_payload(ws.jobs / OWNED / owner_id / owned_name(job_id, from_state), job_id=job_id, placement=placement)
    return job_id


def test_recover_requires_tombstone(ws: FakeWorkspace) -> None:
    dead, owner = new_owner(ws), new_owner(ws)
    with pytest.raises(WorkflowError, match="tombstone"):
        recover(ws, owner, dead.owner_id)
    with pytest.raises(ValueError):
        recover(ws, owner, owner.owner_id)
    with pytest.raises(ValueError):
        recover(ws, owner, "x")


def test_recover_returns_jobs_scratch_launches_and_keeps_tombstone(
    ws: FakeWorkspace, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead, owner = new_owner(ws), new_owner(ws)
    ready_job = claim(ws, dead, new_job(ws, dead, placement="a", priority=9))
    paused_job = claim(ws, dead, new_job(ws, dead, state="paused", placement=""))
    assert ready_job is not None and paused_job is not None
    ready_job.write_state(StateDoc.empty(ready_job.job_id))
    damaged = ws.jobs / OWNED / dead.owner_id / owned_name(str(uuid.uuid4()))
    damaged.mkdir()
    (damaged / "job.json").write_text("{torn")
    junk = ws.jobs / OWNED / dead.owner_id / "not-a-job"
    junk.mkdir()
    adopt = dead.scratch("adopt")
    (adopt / "bundle").mkdir()
    build = dead.scratch("build")
    (build / "partial").write_text("x")
    empty = dead.scratch("copy")
    dead.launch_dir(str(uuid.uuid4()), "0")
    dead.heartbeat()
    reconciled: list[Path] = []

    def reconcile_adopt(by: Owner, path: Path) -> bool:
        assert by is owner and path.name.startswith(owner.owner_id)
        reconciled.append(path)
        return True

    register_reconciler("adopt", reconcile_adopt)
    planted: list[str] = []
    real_pause = _kernel._pause

    def pause_and_plant(seconds: float) -> None:
        # A job appearing in owned/<dead>/ after step 1 must be found by the settled re-list, not deleted.
        if not planted:
            planted.append(_plant_owned(ws, dead.owner_id, placement="late", from_state="waiting"))
        real_pause(seconds)

    monkeypatch.setattr(_kernel, "_pause", pause_and_plant)
    attest_dead(ws, dead.owner_id, by="operator", evidence=[], operator="op")
    report = recover(ws, owner, dead.owner_id)
    returned = {ref.job_id: ref for ref in report.returned}
    assert set(returned) == {ready_job.job_id, paused_job.job_id, planted[0]}
    assert returned[ready_job.job_id].path.parent == ws.jobs / "ready" / "a"
    assert returned[ready_job.job_id].priority == 9
    assert returned[ready_job.job_id].token != ready_job.ref.token
    assert returned[paused_job.job_id].path.parent == ws.jobs / "paused"
    assert returned[planted[0]].path.parent == ws.jobs / "waiting" / "late"
    assert (returned[ready_job.job_id].path / "state.json").exists()  # recovery never writes job content
    assert len(report.quarantined) == 2
    assert all(path.parent.parent == ws.control / "quarantine" for path in report.quarantined)
    assert reconciled == [ws.control / "tmp" / f"{owner.owner_id}{adopt.name[32:]}"]
    assert len(report.scratch) >= 3 and report.kept_scratch == ()
    assert [name for name in os.listdir(ws.control / "tmp") if name.startswith(dead.owner_id)] == []
    assert not adopt.exists() and not build.exists() and not empty.exists()
    assert report.launches_discarded == 1
    assert not (ws.jobs / OWNED / dead.owner_id).exists()
    assert sorted(os.listdir(dead.path)) == ["dead.json"]
    with pytest.raises(OwnerDeclaredDead):
        dead.check_alive()
    # A second recovery of the same owner finds nothing and keeps the tombstone.
    monkeypatch.setattr(_kernel, "_pause", real_pause)
    again = recover(ws, owner, dead.owner_id)
    assert again.returned == () and sorted(os.listdir(dead.path)) == ["dead.json"]


def test_recover_quarantines_a_job_with_a_symlinked_placement(ws: FakeWorkspace, tmp_path: Path) -> None:
    dead, owner = new_owner(ws), new_owner(ws)
    good = claim(ws, dead, new_job(ws, dead, placement="fine"))
    bad = claim(ws, dead, new_job(ws, dead, placement="link/sub"))
    assert good is not None and bad is not None
    prune_empty_placements(ws, budget=10)
    (tmp_path / "elsewhere" / "sub").mkdir(parents=True)
    os.symlink(tmp_path / "elsewhere", ws.jobs / "ready" / "link")
    attest_dead(ws, dead.owner_id, by="probe", evidence=[])
    report = recover(ws, owner, dead.owner_id)
    assert [ref.job_id for ref in report.returned] == [good.job_id]
    (quarantined,) = report.quarantined
    assert json.loads((quarantined / "job.json").read_text())["id"] == bad.job_id
    assert (quarantined / "job.json").exists() and os.listdir(tmp_path / "elsewhere" / "sub") == []
    assert sorted(os.listdir(dead.path)) == ["dead.json"]


def test_recover_keeps_unfinished_scratch(ws: FakeWorkspace) -> None:
    dead, owner = new_owner(ws), new_owner(ws)
    (dead.scratch("eject") / "bundle.json").write_text("{}")
    register_reconciler("eject", lambda by, path: False)
    attest_dead(ws, dead.owner_id, by="probe", evidence=[])
    report = recover(ws, owner, dead.owner_id)
    (kept,) = report.kept_scratch
    assert kept.exists() and kept.name.startswith(owner.owner_id)
    with pytest.raises(ValueError):
        reconcile_scratch(owner, dead.path)


def _recover_worker(ws: FakeWorkspace, dead_owner_id: str, start: object, results: object) -> None:
    owner = new_owner(ws)
    start.wait()  # type: ignore[attr-defined]
    try:
        report = recover(ws, owner, dead_owner_id)
        owner.close()
        results.put(([ref.job_id for ref in report.returned], None))  # type: ignore[attr-defined]
    except Exception as exc:
        results.put(([], repr(exc)))  # type: ignore[attr-defined]


@pytest.mark.slow
def test_concurrent_recoverers(ws: FakeWorkspace) -> None:
    dead = new_owner(ws)
    ids = set()
    for index in range(100):
        job = claim(ws, dead, new_job(ws, dead, placement=f"p{index % 5}", state=random.choice(["ready", "paused"])))
        assert job is not None
        ids.add(job.job_id)
    dead.launch_dir(str(uuid.uuid4()), "0")
    attest_dead(ws, dead.owner_id, by="probe", evidence=[])
    context = multiprocessing.get_context("spawn")
    start, results = context.Event(), context.Queue()
    processes = [context.Process(target=_recover_worker, args=(ws, dead.owner_id, start, results)) for _ in range(4)]
    for process in processes:
        process.start()
    start.set()
    reports = [results.get(timeout=120) for _ in processes]
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    assert [error for _, error in reports if error] == []
    returned = [job_id for job_ids, _ in reports for job_id in job_ids]
    assert sorted(returned) == sorted(ids)
    assert everywhere(ws) == Counter(ids)
    assert len(job_ids_in(ws.jobs / "ready")) + len(job_ids_in(ws.jobs / "paused")) == 100
    assert not (ws.jobs / OWNED / dead.owner_id).exists()
    assert sorted(os.listdir(dead.path)) == ["dead.json"]


# -- 9. self-healing ----------------------------------------------------------------------------------------------------


def test_unheld_owned_job_is_adopted(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    ref = new_job(ws, owner)
    target = ws.jobs / OWNED / owner.owner_id / format_job_name(ref.job_key, 500, _fs.fresh_token(), "ready")
    target.parent.mkdir(parents=True)
    os.rename(ref.path, target)  # a won claim that settle=0 reported as LOST
    (listed,) = owner.owned()
    assert listed.path == target and listed.from_state == "ready"
    job = owner.adopt_owned(listed)
    assert owner.adopt_owned(listed) is job
    assert job.read_state() is None
    job.append_log("claimed", step="prepare")
    job.append_log("released")
    lines = [json.loads(line) for line in (target / "logs" / "runlog.jsonl").read_text().splitlines()]
    assert [line["event"] for line in lines] == ["claimed", "released"]
    assert lines[0]["step"] == "prepare" and lines[0]["owner_id"] == owner.owner_id
    with pytest.raises(ValueError):
        job.append_log("x", event="y")
    with pytest.raises(ValueError):
        new_owner(ws).adopt_owned(listed)
    job.release(StateDoc.empty(job.job_id), Release("waiting", 1))


def test_close_returns_unheld_jobs_unchanged(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    plain = _plant_owned(ws, owner.owner_id, from_state="paused")
    committing = _plant_owned(ws, owner.owner_id)
    (committing_path,) = [ref.path for ref in owner.owned() if ref.job_id == committing]
    mapping = StateDoc.empty(committing).as_mapping()
    mapping["commit"] = {"attempt_id": "a", "action": "advance"}
    (committing_path / "state.json").write_text(json.dumps(mapping))
    owner.close()
    assert {ref.job_id for ref in list_jobs(ws, "paused")} == {plain}
    (back,) = list_jobs(ws, "ready")
    doc, damaged = read_state_unowned(back.path / "state.json")
    # Returned without a state write: the unfinished commit intent survives for the next reconcile.
    assert not damaged and doc is not None and doc.commit is not None and doc.release_to is None
    assert not owner.path.exists()


def test_leftover_temporaries_and_discard(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    job.append_log("claimed")
    leftovers = [job.path / f".state.json.{_fs.fresh_token()}.tmp", job.path / "logs" / f".x.{_fs.fresh_token()}.tmp"]
    keep = [job.path / f".other.{_fs.fresh_token()}.tmp", job.path / "files.tmp"]
    for path in leftovers + keep:
        path.write_text("x")
    job.remove_leftover_temporaries()
    assert [path.exists() for path in leftovers + keep] == [False, False, True, True]
    request = post_request(ws, request_doc(job.job_id))
    exchange_name = str(uuid.uuid4())
    assert claim_exchange_name(ws, owner, exchange_name, job.job_id, job.placement())
    mapping = StateDoc.empty(job.job_id).as_mapping()
    mapping.update(origin="exchange", exchange_name=exchange_name)
    job.write_state(StateDoc.from_mapping(mapping))
    job.discard()
    assert not job.path.exists() and not request.exists()
    assert not (ws.control / "exchange-jobs" / exchange_name).exists()
    assert everywhere(ws) == Counter()
    with pytest.raises(ReleasedJobError):
        job.append_log("x")
    owner.close()
    assert not owner.path.exists()


def test_planted_temporary_lookalikes_do_not_wedge(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    directory = job.path / f".state.json.{_fs.fresh_token()}.tmp"
    directory.mkdir()
    fifo = job.path / f".state.json.{_fs.fresh_token()}.tmp"
    os.mkfifo(fifo)
    link = job.path / f".state.json.{_fs.fresh_token()}.tmp"
    os.symlink(job.path / "job.json", link)
    job.remove_leftover_temporaries()
    job.remove_leftover_temporaries()
    assert directory.is_dir() and fifo.exists() and not os.path.lexists(link)
    assert (job.path / "job.json").exists()


def test_discard_subtree(ws: FakeWorkspace, tmp_path: Path) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    attempt = job.path / "attempts" / "a"
    (attempt / "txn" / "1.tmp").mkdir(parents=True)
    (attempt / "out.txt").write_text("x")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("keep")
    os.symlink(outside, job.path / "run")
    assert job.discard_subtree("attempts/a/out.txt")
    assert not (attempt / "out.txt").exists()
    assert job.discard_subtree(PurePosixPath("attempts/a"))
    assert not attempt.exists() and (job.path / "attempts").is_dir()
    assert job.discard_subtree("run")
    assert not os.path.lexists(job.path / "run") and (outside / "keep").read_text() == "keep"
    assert not job.discard_subtree("attempts/a")
    assert not job.discard_subtree("absent/deeper")
    # A symlinked parent is never traversed.
    os.symlink(outside, job.path / "files")
    with pytest.raises(_fs.UnsafePath):
        job.discard_subtree("files/keep")
    assert (outside / "keep").exists()
    for refused in (
        "",
        ".",
        "/abs",
        "../x",
        "a/../b",
        "a//b",
        "./a",
        "a/",
        "job.json",
        "state.json",
        "logs",
        "logs/stdio.out",
    ):
        with pytest.raises(ValueError):
            job.discard_subtree(refused)
    assert (job.path / "job.json").exists()
    job.release(StateDoc.empty(job.job_id), Release("ready", 500))
    with pytest.raises(ReleasedJobError):
        job.discard_subtree("attempts")


def test_discard_with_unreadable_state(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    (job.path / "state.json").write_text("{torn")
    job.discard()
    assert everywhere(ws) == Counter()


def test_close_removes_trash_before_owner_record(ws: FakeWorkspace, monkeypatch: pytest.MonkeyPatch) -> None:
    owner = new_owner(ws)
    owner.remove_launch(owner.launch_dir(str(uuid.uuid4()), "0").name.split(".")[0], "0")
    assert [name for name in os.listdir(ws.control / "tmp") if ".trash." in name]
    real_remove_file = _fs.remove_file
    seen: list[list[str]] = []

    def remove_file(target: Loc, *, durable: bool) -> None:
        if target.path.name == "owner.json":
            seen.append([name for name in os.listdir(ws.control / "tmp") if name.startswith(owner.owner_id)])
        real_remove_file(target, durable=durable)

    monkeypatch.setattr(_fs, "remove_file", remove_file)
    owner.close()
    assert seen == [[]] and not owner.path.exists()


# -- 10. take -------------------------------------------------------------------------------------------------------------


def test_take(ws: FakeWorkspace, tmp_path: Path) -> None:
    owner, rival = new_owner(ws), new_owner(ws)
    held = ws.control / "transfers" / "incoming" / "T1"
    held.mkdir(parents=True)
    (held / "bundle.json").write_text("{}")
    scratch = take(ws, owner, _fs.loc(held), "adopt")
    assert scratch is not None and (scratch / "T1" / "bundle.json").exists()
    assert scratch.name.startswith(f"{owner.owner_id}.adopt.")
    assert take(ws, rival, _fs.loc(held), "adopt") is None
    assert [name for name in os.listdir(ws.control / "tmp") if name.startswith(rival.owner_id)] == []
    inbox = tmp_path / "exchange" / "inbox"
    (inbox / "entry").mkdir(parents=True)
    descriptor = _fs.open_dir(inbox)
    try:
        taken = take(ws, owner, _fs.anchored(descriptor, "entry"), "adopt")
        assert taken is not None and (taken / "entry").is_dir()
        assert take(ws, rival, _fs.anchored(descriptor, "entry"), "adopt") is None
    finally:
        os.close(descriptor)


# -- 11. exchange index ----------------------------------------------------------------------------------------------------


def test_exchange_names(ws: FakeWorkspace) -> None:
    first, second = new_owner(ws), new_owner(ws)
    name, job_id, request_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    assert claim_exchange_name(ws, first, name, job_id, PurePosixPath("x/y"))
    # Another job is refused the name; a rerun for the same job (an adopt reconciler whose adopter died) is not.
    assert not claim_exchange_name(ws, second, name, str(uuid.uuid4()), PurePosixPath())
    assert claim_exchange_name(ws, second, name, job_id, PurePosixPath("x/y"))
    entry = ws.control / "exchange-jobs" / name
    assert json.loads((entry / "index.json").read_text()) == {"job_id": job_id, "placement": "x/y"}
    assert (entry / ".nonce").read_text().startswith(f"{first.owner_id}.")
    assert [item for item in os.listdir(ws.control / "tmp") if ".claim." in item] == []
    assert not exchange_translation_applied(ws, name, request_id)
    record_exchange_translation(ws, name, request_id)
    record_exchange_translation(ws, name, request_id)
    assert exchange_translation_applied(ws, name, request_id)
    with pytest.raises(WorkflowError):
        record_exchange_translation(ws, str(uuid.uuid4()), request_id)
    with pytest.raises(FormatError):
        claim_exchange_name(ws, first, "client", job_id, PurePosixPath())


# -- 12. locate ---------------------------------------------------------------------------------------------------------------


def test_locate(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    waiting = new_job(ws, owner, state="waiting", placement="a/b", tag=None)
    held = claim(ws, owner, new_job(ws, owner, placement="a/b"))
    assert held is not None
    hint = PurePosixPath("a/b")
    assert locate(ws, waiting.job_id, placement_hint=hint) == waiting
    assert locate(ws, waiting.job_id, placement_hint=PurePosixPath("other")) is None
    assert locate(ws, waiting.job_id, placement_hint=None, exhaustive=True) == waiting
    found = locate(ws, held.job_id, placement_hint=hint)
    assert found is not None and found.path == held.path and found.owner_id == owner.owner_id
    assert locate(ws, held.job_id, placement_hint=hint, include_owned=False) is None
    cache = ListingCache()
    assert locate(ws, waiting.job_id, placement_hint=hint, cache=cache) == waiting
    moved = held.release(StateDoc.empty(held.job_id), Release("ready", 1))
    # The cached listing is a pass's snapshot: the released job is not seen until a fresh pass.
    assert locate(ws, moved.job_id, placement_hint=hint, cache=cache) is None
    assert locate(ws, moved.job_id, placement_hint=hint) == moved
    started = time.monotonic()
    assert locate(ws, str(uuid.uuid4()), placement_hint=hint, settle=True) is None
    assert DEADLINE <= time.monotonic() - started < 4 * DEADLINE


def test_locate_many_settles_once(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    present = [new_job(ws, owner, placement=f"p{index % 3}") for index in range(5)]
    missing = [str(uuid.uuid4()) for _ in range(20)]
    placements = [PurePosixPath(f"p{index}") for index in range(3)]
    started = time.monotonic()
    found = locate_many(ws, [ref.job_id for ref in present] + missing, placements=placements, settle=True)
    elapsed = time.monotonic() - started
    assert found == {ref.job_id: ref for ref in present}
    assert DEADLINE <= elapsed < 2 * DEADLINE
    started = time.monotonic()
    assert locate_many(ws, missing, placements=placements, settle=False) == {}
    assert time.monotonic() - started < DEADLINE


# -- 13. pruning ------------------------------------------------------------------------------------------------------------


def test_prune_empty_placements(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    kept = new_job(ws, owner, placement="a/b")
    (ws.jobs / "ready" / "a" / "empty" / "deeper").mkdir(parents=True)
    (ws.jobs / "failed" / "x" / "y").mkdir(parents=True)
    owned_empty = ws.jobs / OWNED / owner.owner_id
    owned_empty.mkdir(parents=True)
    # Children first, one attempt each: the occupied a/b uses the first attempt, a/empty/deeper the second.
    assert prune_empty_placements(ws, budget=2) == 1
    assert prune_empty_placements(ws, budget=100) == 3
    assert kept.path.exists() and owned_empty.exists()
    assert sorted(os.listdir(ws.jobs / "ready")) == ["a"] and os.listdir(ws.jobs / "ready" / "a") == ["b"]
    assert os.listdir(ws.jobs / "failed") == []
    assert prune_empty_placements(ws, budget=100) == 0


# -- 14. shared-memory sweep ------------------------------------------------------------------------------------------------


def test_sweep_unrecorded_shm(ws: FakeWorkspace, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    owner = new_owner(ws)
    shm = tmp_path / "shm"
    orphan, recorded, late = (uuid.uuid4().hex for _ in range(3))
    for token in (orphan, recorded, late):
        (shm / f"httk-{token}" / "segment").mkdir(parents=True)
    (shm / "unrelated").mkdir()
    runner = owner.launch_dir(str(uuid.uuid4()), "0")
    (runner / "process.json").write_text("{}")
    launch = owner.launch_dir(str(uuid.uuid4()), uuid.uuid4().hex)
    (launch / "launch.json").write_text(json.dumps({"token": recorded}))
    real_pause = _kernel._pause

    def pause_and_record(seconds: float) -> None:
        # The record of "late" becomes visible only between the candidate listing and the settled re-listing.
        late_launch = owner.launch_dir(str(uuid.uuid4()), uuid.uuid4().hex)
        (late_launch / "launch.json").write_text(json.dumps({"token": late}))
        real_pause(seconds)

    monkeypatch.setattr(_kernel, "_pause", pause_and_record)
    assert sweep_unrecorded_shm(ws, shm) == 1
    assert sorted(os.listdir(shm)) == sorted(["unrelated", f"httk-{recorded}", f"httk-{late}"])
    monkeypatch.setattr(_kernel, "_pause", real_pause)
    # An unreadable record names an unknown token: nothing is swept.
    (shm / f"httk-{orphan}").mkdir()
    (launch / "launch.json").write_text("{torn")
    assert sweep_unrecorded_shm(ws, shm) == 0
    assert (shm / f"httk-{orphan}").exists()
    assert sweep_unrecorded_shm(ws, tmp_path / "absent") == 0


# -- 15. attempts -----------------------------------------------------------------------------------------------------------


def test_attempt_bookkeeping(ws: FakeWorkspace) -> None:
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    first, second = str(uuid.uuid4()), str(uuid.uuid4())
    job.require_quiescent()
    job.begin_attempt(first)
    with pytest.raises(WorkflowError):
        job.require_quiescent()
    with pytest.raises(WorkflowError):
        job.begin_attempt(second)
    with pytest.raises(WorkflowError):
        job.end_attempt(second)
    job.end_attempt(first)
    job.require_quiescent()
    with pytest.raises(FormatError):
        job.begin_attempt("not-a-uuid")
    # A held job's handle (and its bookkeeping) is the one adoption returns.
    job.begin_attempt(second)
    (ref,) = owner.owned()
    assert owner.adopt_owned(ref) is job
    with pytest.raises(WorkflowError):
        owner.adopt_owned(ref).require_quiescent()
    job.end_attempt(second)


def test_durable_mode_round_trip(tmp_path: Path) -> None:
    ws = make_workspace(tmp_path / "durable", durable=True)
    owner = new_owner(ws)
    job = claim(ws, owner, new_job(ws, owner))
    assert job is not None
    job.write_state(StateDoc.empty(job.job_id))
    job.append_log("claimed")
    released = job.release(StateDoc.empty(job.job_id), Release("succeeded", 0))
    assert released.path.parent == ws.jobs / "succeeded" / "p" / "q"
    owner.close()
    assert not owner.path.exists()


# -- 16. probe_owner ------------------------------------------------------------------------------------------------------


def _reaped_process() -> tuple[int, int | None]:
    """The pid and start ticks of a process that has exited and been reaped."""

    import subprocess

    from httk.workflow._death import process_identity

    child = subprocess.Popen(["sleep", "30"])
    ticks = process_identity(child.pid).start_ticks
    child.kill()
    child.wait()
    return child.pid, ticks


def _rewrite_record(owner: Owner, **members: object) -> None:
    path = owner.path / "owner.json"
    record = json.loads(path.read_text())
    record.update(members)
    path.write_text(json.dumps(record))


def test_probe_owner_tombstones_a_dead_owner_for_recovery(ws: FakeWorkspace) -> None:
    from httk.workflow._death import Liveness, SchedulerQueries

    dead, owner = new_owner(ws), new_owner(ws)
    job = claim(ws, dead, new_job(ws, dead))
    assert job is not None
    pid, ticks = _reaped_process()
    _rewrite_record(dead, pid=pid, process_start_ticks=ticks)
    with pytest.raises(WorkflowError, match="tombstone"):
        recover(ws, owner, dead.owner_id)
    assert _kernel.probe_owner(ws, dead.owner_id, scheduler=SchedulerQueries()) is Liveness.DEAD
    tombstone = json.loads((dead.path / "dead.json").read_text())
    assert tombstone["by"] == "probe" and tombstone["owner_id"] == dead.owner_id
    assert [item["rule"] for item in tombstone["evidence"]] == ["process_gone"]
    assert all(set(item) == {"subject", "rule", "detail"} for item in tombstone["evidence"])
    (returned,) = recover(ws, owner, dead.owner_id).returned
    assert returned.job_id == job.job_id and returned.state == "ready"
    # Dead stays dead: a later probe answers from the tombstone and leaves it as it is.
    written = (dead.path / "dead.json").read_bytes()
    assert _kernel.probe_owner(ws, dead.owner_id, scheduler=SchedulerQueries()) is Liveness.DEAD
    assert (dead.path / "dead.json").read_bytes() == written


def test_probe_owner_keeps_an_existing_tombstone(ws: FakeWorkspace) -> None:
    from httk.workflow._death import Liveness, SchedulerQueries

    dead = new_owner(ws)
    _rewrite_record(dead, hostname="elsewhere.invalid")
    attest_dead(ws, dead.owner_id, by="operator", evidence=[], operator="op", reason="node lost")
    # Unprovable from this host, yet dead: the tombstone decides before any proof, and it is not rewritten.
    assert _kernel.probe_owner(ws, dead.owner_id, scheduler=SchedulerQueries()) is Liveness.DEAD
    assert json.loads((dead.path / "dead.json").read_text())["by"] == "operator"


def test_probe_owner_alive_unknown_and_self(ws: FakeWorkspace) -> None:
    import subprocess

    from httk.workflow._death import Liveness, SchedulerQueries, process_identity

    scheduler = SchedulerQueries()
    own = new_owner(ws)
    with pytest.raises(ValueError, match="itself"):
        _kernel.probe_owner(ws, own.owner_id, scheduler=scheduler)
    with pytest.raises(ValueError):
        _kernel.probe_owner(ws, "not-an-owner", scheduler=scheduler)
    peer = new_owner(ws)
    child = subprocess.Popen(["sleep", "30"])
    try:
        _rewrite_record(peer, pid=child.pid, process_start_ticks=process_identity(child.pid).start_ticks)
        assert _kernel.probe_owner(ws, peer.owner_id, scheduler=scheduler) is Liveness.ALIVE
    finally:
        child.kill()
        child.wait()
    _rewrite_record(peer, hostname="elsewhere.invalid")
    assert _kernel.probe_owner(ws, peer.owner_id, scheduler=scheduler) is Liveness.UNKNOWN
    never = uuid.uuid4().hex
    assert _kernel.probe_owner(ws, never, scheduler=scheduler) is Liveness.UNKNOWN
    # Only a DEAD proof writes, and nothing else is ever created.
    assert not (ws.control / "owners" / never).exists()
    assert not (peer.path / "dead.json").exists()


def test_probe_owner_needs_every_launch_dead(ws: FakeWorkspace) -> None:
    from httk.workflow._death import Liveness, SchedulerQueries

    dead = new_owner(ws)
    pid, ticks = _reaped_process()
    _rewrite_record(dead, pid=pid, process_start_ticks=ticks)
    launch = dead.launch_dir(str(uuid.uuid4()), "0")
    (launch / "process.json").write_text("{torn")
    # A launch record that cannot be read might describe running ranks: no tombstone, no recovery.
    assert _kernel.probe_owner(ws, dead.owner_id, scheduler=SchedulerQueries()) is Liveness.UNKNOWN
    assert not (dead.path / "dead.json").exists()


def test_probe_owner_does_not_tombstone_a_closed_owner(ws: FakeWorkspace, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow._death import Liveness, SchedulerQueries

    closing = new_owner(ws)
    pid, ticks = _reaped_process()
    _rewrite_record(closing, pid=pid, process_start_ticks=ticks)
    real_pause = _kernel._pause

    def close_during_the_proof(seconds: float) -> None:
        # The probe read owner.json; the owner then finished close() and exited, all before the proof ended.
        (closing.path / "owner.json").unlink()
        closing.path.rmdir()
        real_pause(seconds)

    monkeypatch.setattr(_kernel, "_pause", close_during_the_proof)
    assert _kernel.probe_owner(ws, closing.owner_id, scheduler=SchedulerQueries()) is Liveness.UNKNOWN
    # A tombstone here would recreate owners/<id>/ for an owner that left nothing behind.
    assert not closing.path.exists()
