"""Tests of :mod:`httk.workflow._moving`: eject, adopt, holds and their crash reconcilers, on a real filesystem."""

import dataclasses
import json
import os
import shutil
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from httk.workflow import TaskManager, Workspace, _bundles, _fs, _kernel, _moving, _requests, removal, scaffold
from httk.workflow._bundles import BundleError, BundleManifest
from httk.workflow._job import JobDefinition
from httk.workflow._state import Release, StateDoc, read_state_unowned
from httk.workflow.errors import FormatError, WorkflowError
from httk.workflow.models import make_job_key
from test_bundles import family, write_bundle
from v3_helpers import cli_owner, job_mapping, submit_mapping
from v3_helpers import workspace as v3_workspace

WORKFLOW = ("demo--0123456789abcdef", "demo")


class Crash(BaseException):
    """A simulated process death: no ``except Exception`` cleanup runs."""


@pytest.fixture(autouse=True)
def _no_faults() -> Iterator[None]:
    yield
    _fs.set_fault_injector(None)


def parent_link(parent: dict[str, object], workspace_id: str) -> dict[str, object]:
    return {
        "workspace_id": workspace_id,
        "job_id": parent["id"],
        "job_key": make_job_key(str(parent["id"]), parent["tag"]),  # type: ignore[arg-type]
        "placement": parent["placement"],
        "activation_id": str(uuid.uuid4()),
        "spawn_id": str(uuid.uuid4()),
    }


def place(
    ws: Workspace, mapping: dict[str, object], state: str, priority: int = 500, children: tuple[dict, ...] = ()
) -> _kernel.JobRef:
    """Submit a job with a payload file and move it to *state* and *priority*, recording *children*."""

    ref = submit_mapping(ws, mapping, members={"input.txt": f"payload of {mapping['id']}"})
    with cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, ref)
        assert owned is not None
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial")
        entries = [
            {
                "job_id": child["id"],
                "job_key": make_job_key(str(child["id"]), child["tag"]),
                "placement": child["placement"],
            }
            for child in children
        ]
        return owned.release(doc.with_children(entries), Release(state, priority))


def tree(ws: Workspace) -> list[tuple[dict[str, object], str, int]]:
    """A root (paused) with two children and one grandchild, placed in *ws*; returned top-down."""

    root = job_mapping(WORKFLOW, {"start": "succeed"}, tag="root", placement="proj/r")
    first = job_mapping(WORKFLOW, {"start": "succeed"}, tag="a", placement="proj/r/a")
    second = job_mapping(WORKFLOW, {"start": "succeed"}, tag="b", placement="proj/r/b")
    grand = job_mapping(WORKFLOW, {"start": "succeed"}, tag="g", placement="deep/g")
    for child, parent in ((first, root), (second, root), (grand, first)):
        child["parent"] = parent_link(parent, ws.workspace_id)
    plan = [(root, "paused", 410), (first, "succeeded", 420), (second, "failed", 430), (grand, "cancelled", 440)]
    kids = {root["id"]: (first, second), first["id"]: (grand,)}
    for mapping, state, priority in reversed(plan):
        place(ws, mapping, state, priority, kids.get(mapping["id"], ()))
    return plan


def single(ws: Workspace, state: str = "succeeded", priority: int = 500) -> tuple[dict[str, object], str, int]:
    mapping = job_mapping(WORKFLOW, {"start": "succeed"}, tag="solo", placement="proj/solo")
    place(ws, mapping, state, priority)
    return mapping, state, priority


def find(ws: Workspace, job_id: object) -> _kernel.JobRef | None:
    return _kernel.locate(ws, str(job_id), placement_hint=None, exhaustive=True)


def claim_root(ws: Workspace, owner: _kernel.Owner, job_id: object) -> _kernel.OwnedJob:
    ref = find(ws, job_id)
    assert ref is not None
    owned = _kernel.claim(ws, owner, ref)
    assert owned is not None
    return owned


def assert_in(ws: Workspace, plan: list[tuple[dict[str, object], str, int]], *, rekeyed: bool = False) -> None:
    for mapping, state, priority in plan:
        ref = find(ws, mapping["id"])
        assert ref is not None, mapping["tag"]
        assert (ref.state, ref.priority) == (state, priority)
        assert (ref.path / "input.txt").read_text() == f"payload of {mapping['id']}"


def run_log_events(job: Path) -> list[str]:
    path = job / "logs" / "runlog.jsonl"
    return [json.loads(line)["event"] for line in path.read_text().splitlines()] if path.exists() else []


def job_ids_below(root: Path) -> list[str]:
    found = []
    for path in root.rglob("job.json"):
        found.append(json.loads(path.read_bytes())["id"])
    return found


def test_eject_and_adopt_a_single_job_keeps_state_priority_and_payload(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    mapping, state, _ = single(source, "succeeded", 500)
    # A set_priority request changes only the directory name, never job.json.
    ref = find(source, mapping["id"])
    assert ref is not None and ref.placement is not None
    _requests.post(
        source, action="set_priority", job_id=ref.job_id, placement=ref.placement, operator="t", reason="t", priority=77
    )
    with cli_owner(source) as owner:
        assert removal.serve(source, owner, ref)
    assert find(source, mapping["id"]).priority == 77  # type: ignore[union-attr]
    with cli_owner(source) as owner:
        report = _moving.eject(
            source, owner, claim_root(source, owner, mapping["id"]), destination=tmp_path / "out", tree=False
        )
    assert report.destination == tmp_path / "out" / ref.job_key and report.members == (ref.job_key,)
    assert find(source, mapping["id"]) is None
    assert not list((source.control / "tmp").iterdir())
    with cli_owner(target) as owner:
        adopted = _moving.adopt(target, owner, report.destination, untrusted=False)
    assert adopted is not None and not adopted.already_adopted
    assert adopted.missing_workflows == (WORKFLOW[0],)
    assert_in(target, [(mapping, state, 77)])
    assert not report.destination.exists() and not list((target.control / "tmp").iterdir())


def test_eject_and_adopt_a_tree(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    with cli_owner(source) as owner:
        report = _moving.eject(
            source, owner, claim_root(source, owner, plan[0][0]["id"]), destination=tmp_path / "out", tree=True
        )
    assert len(report.members) == 4
    assert all(find(source, mapping["id"]) is None for mapping, _, _ in plan)
    manifest = BundleManifest.from_json((report.destination / "bundle.json").read_bytes())
    assert manifest.members[0].state == "paused"
    with cli_owner(target) as owner:
        adopted = _moving.adopt(target, owner, report.destination, untrusted=False)
    assert adopted is not None and len(adopted.published) == 4
    assert_in(target, plan)


def test_detached_and_foreign_children_stay(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    root = job_mapping(WORKFLOW, {"start": "succeed"}, tag="root", placement="p")
    detached = job_mapping(WORKFLOW, {"start": "succeed"}, tag="d", placement="p/d")
    foreign = job_mapping(WORKFLOW, {"start": "succeed"}, tag="f", placement="p/f")
    detached["parent"] = parent_link(root, ws.workspace_id)
    place(ws, detached, "succeeded")
    place(ws, foreign, "succeeded")  # recorded by the root, but its job.json names no parent
    with cli_owner(ws) as owner:
        owned = claim_root(ws, owner, detached["id"])
        doc = owned.read_state()
        assert doc is not None
        owned.release(doc.updated(detached={"at": "now", "operator": "t"}), Release("succeeded", 500))
    place(ws, root, "succeeded", children=(detached, foreign))
    with cli_owner(ws) as owner:
        report = _moving.eject(ws, owner, claim_root(ws, owner, root["id"]), destination=tmp_path / "out", tree=True)
    assert len(report.members) == 1
    assert find(ws, detached["id"]) is not None and find(ws, foreign["id"]) is not None


def test_occupied_destination_rolls_back(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = tree(ws)
    root_key = make_job_key(str(plan[0][0]["id"]), "root")
    (tmp_path / "out" / root_key).mkdir(parents=True)
    (tmp_path / "out" / root_key / "something").write_text("taken")
    with cli_owner(ws) as owner:
        root = claim_root(ws, owner, plan[0][0]["id"])
        with pytest.raises(WorkflowError, match="occupied"):
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=True)
        assert not owner.holds(root.ref)
    assert_in(ws, plan)
    assert not list((ws.control / "tmp").iterdir())


def test_busy_member_moves_nothing(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = tree(ws)
    other = cli_owner(ws)
    blocker = claim_root(ws, other, plan[2][0]["id"])
    with cli_owner(ws) as owner:
        root = claim_root(ws, owner, plan[0][0]["id"])
        with pytest.raises(_moving.Busy) as caught:
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=True)
        assert caught.value.job_key == blocker.job_key
        # A refusal gives every claimed job back, the root included.
        assert not owner.holds(root.ref) and owner.owned() == []
    blocker.give_back()
    other.close()
    assert_in(ws, plan)
    assert not (tmp_path / "out").exists()


def test_busy_on_a_lost_member_claim_gives_back_the_claimed_ones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = tree(ws)
    claims = {"n": 0}
    real = _kernel.claim

    def lose_the_second(*args: object) -> _kernel.OwnedJob | None:
        claims["n"] += 1
        return None if claims["n"] == 3 else real(*args)  # type: ignore[arg-type]

    with cli_owner(ws) as owner:
        root = claim_root(ws, owner, plan[0][0]["id"])
        monkeypatch.setattr(_kernel, "claim", lose_the_second)
        with pytest.raises(_moving.Busy):
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=True)
        monkeypatch.setattr(_kernel, "claim", real)
        assert owner.owned() == []
    assert_in(ws, plan)


# -- crashes --------------------------------------------------------------------------------------------------------

#: The crash points: before and after each rename (move_once, move_owned, deliver), before each write_file's replace.
POINTS = {("rename", "before"), ("rename", "after"), ("write", "before_replace")}


def count_points(run: Callable[[], object]) -> int:
    seen = {"n": 0}

    def count(op: str, phase: str, src: object, dst: object) -> None:
        if (op, phase) in POINTS:
            seen["n"] += 1

    _fs.set_fault_injector(count)
    try:
        run()
    finally:
        _fs.set_fault_injector(None)
    return seen["n"]


def crash_at(k: int) -> _fs.Fault:
    seen = {"n": 0}

    def fault(op: str, phase: str, src: object, dst: object) -> None:
        if (op, phase) in POINTS:
            seen["n"] += 1
            if seen["n"] == k:
                raise Crash(f"{op} {phase} #{k}")

    return fault


def die_and_recover(ws: Workspace, dead: _kernel.Owner) -> None:
    _fs.set_fault_injector(None)
    _kernel.attest_dead(ws, dead.owner_id, by="operator", evidence=[], reason="test")
    with cli_owner(ws) as rescuer:
        _kernel.recover(ws, rescuer, dead.owner_id)


def run_eject(ws: Workspace, owner: _kernel.Owner, root_id: object, destination: Path, tree_: bool) -> None:
    _moving.eject(ws, owner, claim_root(ws, owner, root_id), destination=destination, tree=tree_)


@pytest.mark.parametrize("tree_", [False, True], ids=["single", "tree"])
def test_eject_crash_matrix(tmp_path: Path, tree_: bool) -> None:
    probe = v3_workspace(tmp_path / "probe")
    plan = tree(probe) if tree_ else [single(probe, "paused", 321)]
    with cli_owner(probe) as owner:
        total = count_points(lambda: run_eject(probe, owner, plan[0][0]["id"], tmp_path / "probe-out", tree_))
    assert total >= 4
    outcomes = set()
    for k in range(1, total + 1):
        ws = v3_workspace(tmp_path / f"w{k}")
        plan = tree(ws) if tree_ else [single(ws, "paused", 321)]
        out = tmp_path / f"out{k}"
        owner = cli_owner(ws)
        _fs.set_fault_injector(crash_at(k))
        with pytest.raises(Crash):
            run_eject(ws, owner, plan[0][0]["id"], out, tree_)
        die_and_recover(ws, owner)
        ids = [str(mapping["id"]) for mapping, _, _ in plan]
        here = job_ids_below(ws.jobs)
        there = job_ids_below(out) if out.exists() else []
        # Exactly one place for every job, all of them together: delivered, or back where they were.
        assert sorted(here + there) == sorted(ids), k
        if here:
            assert_in(ws, plan)
            outcomes.add("back")
        else:
            assert len(list(out.iterdir())) == 1
            outcomes.add("delivered")
        assert not list((ws.control / "tmp").iterdir()), k
    assert outcomes == {"back", "delivered"}


def crash_at_extract(k: int, when: str) -> _fs.Fault:
    seen = {"n": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        # Extraction is the move of a member into the eject scratch.
        if op == "rename" and phase == when and dst is not None and ".eject." in str(dst.path):
            seen["n"] += 1
            if seen["n"] == k:
                raise Crash(f"extract #{k} {when}")

    return fault


@pytest.mark.parametrize("phase", ["before", "after"])
def test_build_bundle_crash_after_k_of_n_extracts_rolls_back(tmp_path: Path, phase: str) -> None:
    for k in range(1, 5):
        ws = v3_workspace(tmp_path / f"{phase}{k}")
        plan = tree(ws)
        owner = cli_owner(ws)
        root = claim_root(ws, owner, plan[0][0]["id"])
        fault = crash_at_extract(k, phase)
        _fs.set_fault_injector(fault)
        with pytest.raises(Crash):
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=True)
        die_and_recover(ws, owner)
        assert_in(ws, plan)
        assert not (tmp_path / "out").exists()


def test_adopt_crash_matrix(tmp_path: Path) -> None:
    def setup(name: str) -> tuple[Workspace, list[tuple[dict[str, object], str, int]], Path]:
        source, target = v3_workspace(tmp_path / f"{name}-a"), v3_workspace(tmp_path / f"{name}-b")
        plan = tree(source)
        with cli_owner(source) as owner:
            report = _moving.eject(
                source,
                owner,
                claim_root(source, owner, plan[0][0]["id"]),
                destination=tmp_path / f"{name}-out",
                tree=True,
            )
        return target, plan, report.destination

    target, plan, bundle = setup("probe")
    with cli_owner(target) as owner:
        total = count_points(lambda: _moving.adopt(target, owner, bundle, untrusted=False))
    assert total >= 5
    for k in range(1, total + 1):
        target, plan, bundle = setup(f"k{k}")
        owner = cli_owner(target)
        _fs.set_fault_injector(crash_at(k))
        with pytest.raises(Crash):
            _moving.adopt(target, owner, bundle, untrusted=False)
        die_and_recover(target, owner)
        ids = sorted(str(mapping["id"]) for mapping, _, _ in plan)
        published = job_ids_below(target.jobs)
        at_source = job_ids_below(bundle) if bundle.exists() else []
        left = job_ids_below(target.control / "tmp")
        assert sorted(published + at_source + left) == ids, k
        if published:
            assert sorted(published) == ids, k
            assert_in(target, plan)
        assert not left, k


def test_untrusted_adopt_crash_matrix(tmp_path: Path) -> None:
    def setup(name: str) -> tuple[Workspace, list[dict[str, object]], Path]:
        bundle = tmp_path / f"{name}-inbox" / "entry"
        return v3_workspace(tmp_path / f"{name}-ws"), client_bundle(bundle), bundle

    ws, _, bundle = setup("probe")
    with cli_owner(ws) as owner:
        total = count_points(lambda: _moving.adopt(ws, owner, bundle, untrusted=True))
    assert total >= 8
    for k in range(1, total + 1):
        ws, jobs, bundle = setup(f"k{k}")
        owner = cli_owner(ws)
        _fs.set_fault_injector(crash_at(k))
        with pytest.raises(Crash):
            _moving.adopt(ws, owner, bundle, untrusted=True)
        die_and_recover(ws, owner)
        published = job_ids_below(ws.jobs)
        if published:
            assert len(set(published)) == len(published) == 3 and not bundle.exists(), k
            names = {json.loads(path.read_bytes())["exchange_name"] for path in ws.jobs.rglob("state.json")}
            assert names == {str(job["id"]) for job in jobs}, k
        else:
            assert sorted(job_ids_below(bundle)) == sorted(str(job["id"]) for job in jobs), k
        assert not job_ids_below(ws.control / "tmp"), k


def test_adopt_rerun_after_a_crash_between_members_publishes_no_duplicates(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    with cli_owner(source) as owner:
        report = _moving.eject(
            source, owner, claim_root(source, owner, plan[0][0]["id"]), destination=tmp_path / "out", tree=True
        )
    owner = cli_owner(target)
    submits = {"n": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "after" and dst is not None and str(dst.path).startswith(str(target.jobs)):
            submits["n"] += 1
            if submits["n"] == 2:
                raise Crash("between members")

    _fs.set_fault_injector(fault)
    with pytest.raises(Crash):
        _moving.adopt(target, owner, report.destination, untrusted=False)
    assert len(job_ids_below(target.jobs)) == 2
    die_and_recover(target, owner)
    published = job_ids_below(target.jobs)
    assert len(published) == len(set(published)) == 4
    assert_in(target, plan)


def test_already_adopted_and_partial_presence(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    with cli_owner(source) as owner:
        report = _moving.eject(
            source, owner, claim_root(source, owner, plan[0][0]["id"]), destination=tmp_path / "out", tree=True
        )
    copy = tmp_path / "copy" / report.destination.name
    _fs.copy_tree(report.destination, copy, durable=False)
    # A duplicate delivery the transfer machinery placed (here an incoming copy) is discarded as already adopted.
    landed = target.control / "transfers" / "incoming" / f"{report.transfer_id}.x"
    _fs.copy_tree(report.destination, landed, durable=False)
    partial = tmp_path / "partial" / report.destination.name
    _fs.copy_tree(report.destination, partial, durable=False)
    with cli_owner(target) as owner:
        assert _moving.adopt(target, owner, report.destination, untrusted=False) is not None
        again = _moving.adopt(target, owner, landed, untrusted=False)
        assert again is not None and again.already_adopted and again.published == ()
        assert not landed.exists()
        # A second copy at an operator's plain path is never destroyed: refused, and kept where it was.
        with pytest.raises(BundleError, match=f"already adopted; the bundle was left at {copy}"):
            _moving.adopt(target, owner, copy, untrusted=False)
        assert (copy / "bundle.json").is_file()
        # Delete one member: the second copy is now partially present and refused, back to where it was.
        ref = find(target, plan[3][0]["id"])
        assert ref is not None
        owned = _kernel.claim(target, owner, ref)
        assert owned is not None
        owned.discard()
        with pytest.raises(BundleError, match="already in this workspace"):
            _moving.adopt(target, owner, partial, untrusted=False)
        assert (partial / "bundle.json").is_file()


def test_a_subtree_whose_parent_is_here_is_refused(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = tree(ws)
    with cli_owner(ws) as owner:
        report = _moving.eject(
            ws, owner, claim_root(ws, owner, plan[1][0]["id"]), destination=tmp_path / "out", tree=True
        )
        assert len(report.members) == 2
        with pytest.raises(BundleError, match="parent"):
            _moving.adopt(ws, owner, report.destination, untrusted=False)
    assert report.destination.is_dir()


# -- untrusted adoption ---------------------------------------------------------------------------------------------


def client_bundle(path: Path, jobs: list[dict[str, object]] | None = None) -> list[dict[str, object]]:
    return write_bundle(path, jobs)[1]


def test_untrusted_adoption_rekeys_and_records_the_exchange_name(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = client_bundle(tmp_path / "inbox" / "entry")
    with cli_owner(ws) as owner:
        report = _moving.adopt(
            ws, owner, tmp_path / "inbox" / "entry", untrusted=True, refused_to=tmp_path / "rejected"
        )
    assert report is not None and len(report.published) == 3
    client_ids = {str(job["id"]) for job in jobs}
    names = set()
    for ref in report.published:
        assert ref.job_id not in client_ids and ref.state == "ready"
        doc, damaged = read_state_unowned(ref.path / "state.json")
        assert doc is not None and not damaged and doc.origin == "exchange"
        names.add(doc.exchange_name)
    assert names == client_ids
    assert (ws.control / "exchange-jobs" / str(jobs[0]["id"]) / "index.json").is_file()
    # A second delivery of the same client bundle is already adopted.
    client_bundle(tmp_path / "inbox" / "again", jobs)
    with cli_owner(ws) as owner:
        again = _moving.adopt(ws, owner, tmp_path / "inbox" / "again", untrusted=True, refused_to=tmp_path / "rejected")
    assert again is not None and again.already_adopted


def test_untrusted_duplicate_exchange_name_is_refused_to_the_rejected_directory(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = family(str(uuid.uuid4()))
    with cli_owner(ws) as owner:
        claim = _kernel.claim_exchange_name(
            ws, owner, str(jobs[0]["id"]), str(uuid.uuid4()), PurePosixPath("x"), nonce="other"
        )
        assert claim is _kernel.ExchangeClaim.WON
    client_bundle(tmp_path / "inbox" / "entry", jobs)
    with cli_owner(ws) as owner, pytest.raises(BundleError, match="already in use"):
        _moving.adopt(ws, owner, tmp_path / "inbox" / "entry", untrusted=True, refused_to=tmp_path / "rejected")
    (rejected,) = (tmp_path / "rejected").iterdir()
    assert (rejected / "entry" / "bundle.json").is_file()
    reason = json.loads((rejected / "reason.json").read_bytes())
    assert reason["format"] == "httk-workspace-exchange-rejection" and reason["name"] == "entry"
    assert "already in use" in reason["reason"]
    assert job_ids_below(ws.jobs) == [] and not list((ws.control / "tmp").iterdir())


def test_untrusted_exchange_name_indexed_at_another_placement(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = family(str(uuid.uuid4()))
    client_bundle(tmp_path / "inbox" / "first", jobs)
    with cli_owner(ws) as owner:
        first = _moving.adopt(ws, owner, tmp_path / "inbox" / "first", untrusted=True)
    assert first is not None
    root_id = first.published[-1].job_id
    # The client resubmits its root under another placement: the indexed job exists, so it is already adopted.
    moved = [dict(jobs[0], placement="elsewhere/root")]
    client_bundle(tmp_path / "inbox" / "second", moved)
    with cli_owner(ws) as owner:
        second = _moving.adopt(ws, owner, tmp_path / "inbox" / "second", untrusted=True)
    assert second is not None and second.already_adopted
    # An index entry at another placement whose job is gone is refused.
    other = family(str(uuid.uuid4()))[:1]
    rekeyed = str(uuid.uuid5(uuid.UUID("b480bb06-dc60-4d16-a78b-cc6b3af734b4"), f"{ws.workspace_id}/{other[0]['id']}"))
    with cli_owner(ws) as owner:
        claim = _kernel.claim_exchange_name(ws, owner, str(other[0]["id"]), rekeyed, PurePosixPath("gone"), nonce="x")
        assert claim is _kernel.ExchangeClaim.WON
    client_bundle(tmp_path / "inbox" / "third", other)
    with cli_owner(ws) as owner, pytest.raises(BundleError, match="another adoption whose job is not here"):
        _moving.adopt(ws, owner, tmp_path / "inbox" / "third", untrusted=True, refused_to=tmp_path / "rejected")
    assert root_id


def test_untrusted_bundle_with_state_json_is_refused(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    bundle = tmp_path / "inbox" / "entry"
    jobs = client_bundle(bundle)
    manifest = BundleManifest.from_json((bundle / "bundle.json").read_bytes())
    member_dir = manifest.member_dir(bundle, manifest.members[0])
    (member_dir / "state.json").write_text("{}")
    with cli_owner(ws) as owner, pytest.raises(BundleError, match="fresh"):
        _moving.adopt(ws, owner, bundle, untrusted=True, refused_to=tmp_path / "rejected")
    assert len(list((tmp_path / "rejected").iterdir())) == 1 and not bundle.exists()
    assert jobs and job_ids_below(ws.jobs) == []


def test_untrusted_rekey_crash_resumes(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    client_bundle(tmp_path / "inbox" / "entry")
    owner = cli_owner(ws)
    writes = {"n": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if (
            op == "rename"
            and phase == "after"
            and dst is not None
            and ".adopt-untrusted." in str(dst.path)
            and "jobs" in str(dst.path)
        ):
            writes["n"] += 1
            if writes["n"] == 2:
                raise Crash("mid rekey")

    _fs.set_fault_injector(fault)
    with pytest.raises(Crash):
        _moving.adopt(ws, owner, tmp_path / "inbox" / "entry", untrusted=True)
    die_and_recover(ws, owner)
    published = job_ids_below(ws.jobs)
    assert len(published) == 3
    for path in ws.jobs.rglob("state.json"):
        assert json.loads(path.read_bytes())["origin"] == "exchange"


def test_two_adopters_of_copies_of_one_client_bundle_publish_one_set_of_jobs(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs = client_bundle(tmp_path / "inbox" / "first")
    client_bundle(tmp_path / "inbox" / "second", jobs)
    client_bundle(tmp_path / "inbox" / "third", jobs)
    crashed = cli_owner(ws)

    def crash_after_the_plan(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "write" and phase == "before_replace" and dst is not None and dst.path.name == "plan.json":
            _fs.set_fault_injector(None)
            raise Crash("after the plan")

    # A claims the exchange name and records its plan, then dies before publishing.
    _fs.set_fault_injector(crash_after_the_plan)
    with pytest.raises(Crash):
        _moving.adopt(ws, crashed, tmp_path / "inbox" / "first", untrusted=True, refused_to=tmp_path / "rejected")
    # B adopts a copy meanwhile: the name carries A's nonce and A's job is not here, so B's copy is refused.
    with cli_owner(ws) as owner, pytest.raises(BundleError, match="another adoption"):
        _moving.adopt(ws, owner, tmp_path / "inbox" / "second", untrusted=True, refused_to=tmp_path / "rejected")
    assert job_ids_below(ws.jobs) == []
    # A's recovery is a rerun of the same adoption (its nonce): it publishes.
    die_and_recover(ws, crashed)
    published = job_ids_below(ws.jobs)
    assert len(published) == len(set(published)) == 3
    # A later copy finds A's jobs and is already adopted; its scratch is discarded.
    with cli_owner(ws) as owner:
        late = _moving.adopt(ws, owner, tmp_path / "inbox" / "third", untrusted=True, refused_to=tmp_path / "rejected")
    assert late is not None and late.already_adopted
    assert sorted(job_ids_below(ws.jobs)) == sorted(published)
    assert not job_ids_below(ws.control / "tmp")


def test_an_untrusted_scratch_without_its_record_refuses_to_the_exchange_rejections(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    bundle = tmp_path / "inbox" / "entry"
    client_bundle(bundle)
    (bundle / "junk").write_text("not part of the bundle")
    owner = cli_owner(ws)

    def crash_after_the_take(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "after" and dst is not None and ".adopt-untrusted." in str(dst.path):
            _fs.set_fault_injector(None)
            raise Crash("after the take")

    _fs.set_fault_injector(crash_after_the_take)
    with pytest.raises(Crash):
        _moving.adopt(ws, owner, bundle, untrusted=True, refused_to=tmp_path / "elsewhere")
    die_and_recover(ws, owner)
    (rejected,) = (ws.root / "exchange" / "outbox" / "rejected").iterdir()
    assert (rejected / "entry" / "junk").is_file() and (rejected / "reason.json").is_file()
    assert not list((ws.control / "tmp").iterdir())


def test_a_symlinked_rejection_directory_is_never_written_through(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    bundle = tmp_path / "inbox" / "entry"
    client_bundle(bundle)
    (bundle / "junk").write_text("not part of the bundle")
    (ws.root / "exchange" / "outbox").mkdir(parents=True)
    (tmp_path / "steered").mkdir()
    (ws.root / "exchange" / "outbox" / "rejected").symlink_to(tmp_path / "steered")
    owner = cli_owner(ws)
    with pytest.raises(BundleError, match="stays in"):
        _moving.adopt(ws, owner, bundle, untrusted=True, refused_to=ws.root / "exchange" / "outbox" / "rejected")
    owner.close()  # the reconciler cannot place it either: the scratch and the owner record stay for recovery
    assert list((tmp_path / "steered").iterdir()) == []
    assert [name for name in os.listdir(ws.control / "tmp") if ".adopt-untrusted." in name]
    # Once the client repairs the directory, recovery refuses the bundle into it.
    (ws.root / "exchange" / "outbox" / "rejected").unlink()
    die_and_recover(ws, owner)
    (rejected,) = (ws.root / "exchange" / "outbox" / "rejected").iterdir()
    assert (rejected / "entry" / "junk").is_file()


def test_eject_without_tree_refuses_a_root_with_children(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = tree(ws)
    with cli_owner(ws) as owner:
        root = claim_root(ws, owner, plan[0][0]["id"])
        with pytest.raises(_moving.Busy, match="child of the job"):
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=False)
        assert not owner.holds(root.ref)
    assert_in(ws, plan)
    # The request path: an eject request without tree is refused and the jobs stay; with tree it ejects them all.
    ref = find(ws, plan[0][0]["id"])
    assert ref is not None and ref.placement is not None
    for tree_ in (False, True):
        _requests.post(
            ws,
            action="eject",
            job_id=ref.job_id,
            placement=ref.placement,
            operator="t",
            reason="t",
            destination=str(tmp_path / "out"),
            tree=tree_,
        )
        current = find(ws, plan[0][0]["id"])
        assert current is not None
        with cli_owner(ws) as owner:
            assert removal.serve(ws, owner, current)
        if not tree_:
            assert all(find(ws, mapping["id"]) is not None for mapping, _, _ in plan)
    assert all(find(ws, mapping["id"]) is None for mapping, _, _ in plan)
    manifest = BundleManifest.from_json((tmp_path / "out" / ref.job_key / "bundle.json").read_bytes())
    assert len(manifest.members) == 4


def test_the_eject_verb_without_tree_refuses_a_root_with_children(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.core.cli import CLIContext

    from httk.workflow.workflow_cli import command
    from test_cli_job_eject_adopt import registered

    ws, name = registered(tmp_path, "a")
    plan = tree(ws)
    root_id = str(plan[0][0]["id"])
    argv = ["job", "eject", "--workspace", name, root_id, str(tmp_path / "out")]
    assert command(argv, CLIContext("httk", tmp_path)) != 0
    assert "child of the job" in capsys.readouterr().err
    assert_in(ws, plan)
    assert command([*argv, "--tree"], CLIContext("httk", tmp_path)) == 0
    assert all(find(ws, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_symlinked_destination_never_carries_the_token(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    (real / "bundle.json").write_bytes(b"token")
    (tmp_path / "link").symlink_to(real)
    assert _fs.carries(_fs.loc(real), "bundle.json", b"token")
    assert not _fs.carries(_fs.loc(tmp_path / "link"), "bundle.json", b"token")
    assert not _fs.carries(_fs.loc(real), "bundle.json", b"other")
    assert not _fs.carries(_fs.loc(tmp_path / "absent"), "bundle.json", b"token")


# -- across filesystems ---------------------------------------------------------------------------------------------


def test_cross_filesystem_eject_and_adopt_copy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    copies: list[Path] = []
    real_copy, real_deliver, real_move_once = _fs.copy_tree, _fs.deliver, _fs.move_once

    def spy_copy(src: Path, dst: Path, *, durable: bool, limits: _fs.WalkLimits | None = None) -> None:
        copies.append(dst)
        real_copy(src, dst, durable=durable, limits=limits)

    def once(real: Callable[..., object]) -> Callable[..., object]:
        state = {"raised": False}

        def wrapper(*args: object, **kwargs: object) -> object:
            if not state["raised"]:
                state["raised"] = True
                raise _fs.CrossDevice("simulated")
            return real(*args, **kwargs)

        return wrapper

    monkeypatch.setattr(_fs, "copy_tree", spy_copy)
    monkeypatch.setattr(_fs, "deliver", once(real_deliver))
    with cli_owner(source) as owner:
        report = _moving.eject(
            source, owner, claim_root(source, owner, plan[0][0]["id"]), destination=tmp_path / "out", tree=True
        )
    assert copies == [tmp_path / "out" / f".{report.destination.name}.partial.{report.transfer_id}"]
    assert (report.destination / "bundle.json").is_file() and not copies[0].exists()
    assert not list((source.control / "tmp").iterdir())
    monkeypatch.setattr(_fs, "move_once", once(real_move_once))
    with cli_owner(target) as owner:
        adopted = _moving.adopt(target, owner, report.destination, untrusted=False)
    assert adopted is not None and len(adopted.published) == 4
    assert len(copies) == 2 and copies[1].parent.name == ".partial"
    # A copy leaves its source in place.
    assert (report.destination / "bundle.json").is_file()
    assert_in(target, plan)


def test_incomplete_cross_filesystem_copy_is_discarded_by_the_reconciler(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    owner = cli_owner(ws)
    scratch = owner.scratch("adopt")
    (scratch / ".partial" / "bundle").mkdir(parents=True)
    (scratch / "source.json").write_text('{"source": null, "refused_to": null, "copied": true}')
    assert _kernel.reconcile_scratch(owner, scratch)
    assert not scratch.exists()
    owner.close()


# -- holds and the eject request ------------------------------------------------------------------------------------


def test_hold_held_release(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    with cli_owner(source) as owner:
        made = _moving.hold(source, owner, claim_root(source, owner, plan[0][0]["id"]), tree=True)
    (hold,) = _moving.held(source)
    path = Path(hold.path)
    assert hold == made and path.parent == source.control / "transfers" / "outgoing"
    assert hold.transfer_id == path.name and len(hold.members) == 4
    assert _moving.Hold.from_mapping(json.loads(json.dumps(hold.as_mapping()))) == hold
    # The transfer: copy to the destination, adopt there, then release the hold.
    landed = tmp_path / "landing" / hold.transfer_id
    _fs.copy_tree(path, landed, durable=False)
    with cli_owner(target) as owner:
        assert _moving.adopt(target, owner, landed, untrusted=False) is not None
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, hold.transfer_id)
        assert not _moving.release_hold(source, owner, hold.transfer_id)
        with pytest.raises(ValueError):
            _moving.release_hold(source, owner, "../x")
    assert _moving.held(source) == []
    assert_in(target, plan)


def test_eject_request_applied_by_a_manager_tick(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    mapping, state, priority = single(ws, "paused", 600)
    ref = find(ws, mapping["id"])
    assert ref is not None and ref.placement is not None
    _requests.post(
        ws,
        action="eject",
        job_id=ref.job_id,
        placement=ref.placement,
        operator="t",
        reason="t",
        destination=str(tmp_path / "out"),
    )
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.tick()
    assert find(ws, mapping["id"]) is None
    bundle = tmp_path / "out" / ref.job_key
    manifest = BundleManifest.from_json((bundle / "bundle.json").read_bytes())
    assert (manifest.members[0].state, manifest.members[0].priority) == (state, priority)
    member = manifest.member_dir(bundle, manifest.members[0])
    doc, _ = read_state_unowned(member / "state.json")
    assert doc is not None and any(entry.get("action") == "eject" for entry in doc.history_tail)
    assert "ejected" in run_log_events(member)  # the line travels in the bundle
    assert not list((ws.control / "requests").glob("*.json"))
    assert JobDefinition.from_path(member / "job.json").id == mapping["id"]


def test_eject_request_to_an_occupied_destination_returns_the_job(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    mapping, state, priority = single(ws, "failed", 500)
    ref = find(ws, mapping["id"])
    assert ref is not None and ref.placement is not None
    (tmp_path / "out" / ref.job_key / "x").mkdir(parents=True)
    _requests.post(
        ws,
        action="eject",
        job_id=ref.job_id,
        placement=ref.placement,
        operator="t",
        reason="t",
        destination=str(tmp_path / "out"),
    )
    with cli_owner(ws) as owner:
        assert removal.serve(ws, owner, ref)
    assert_in(ws, [(mapping, state, priority)])
    assert not list((ws.control / "requests").glob("*.json"))


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes into any directory")
@pytest.mark.parametrize("kind", ["symlink", "unwritable parent"])
def test_a_failed_eject_request_returns_the_job_within_the_tick(tmp_path: Path, kind: str) -> None:
    # A symlinked destination is refused before anything moves; an unwritable one fails the delivery, and the
    # reconciler's roll back runs at once instead of when the manager closes.
    ws = v3_workspace(tmp_path / "a")
    mapping, state, priority = single(ws, "paused", 600)
    ref = find(ws, mapping["id"])
    assert ref is not None and ref.placement is not None
    real = tmp_path / "real"
    real.mkdir()
    locked = tmp_path / "locked"
    locked.mkdir()
    if kind == "symlink":
        destination = tmp_path / "link"
        destination.symlink_to(real)
    else:
        destination = locked / "out"
        locked.chmod(0o555)
    _requests.post(
        ws,
        action="eject",
        job_id=ref.job_id,
        placement=ref.placement,
        operator="t",
        reason="t",
        destination=str(destination),
    )
    try:
        with TaskManager(ws, heartbeat_interval=0.01) as manager:
            manager.tick()
            assert_in(ws, [(mapping, state, priority)])
            assert not list((ws.control / "tmp").glob("*.eject.*"))
    finally:
        locked.chmod(0o755)
    assert not list(real.iterdir()) and not list(locked.iterdir())
    assert not list((ws.control / "requests").glob("*.json"))
    returned = find(ws, mapping["id"])
    assert returned is not None
    if kind == "symlink":
        doc, _ = read_state_unowned(returned.path / "state.json")
        assert doc is not None
        (dropped,) = (entry for entry in doc.history_tail if entry["event"] == "request_dropped")
        assert "symlink" in str(dropped["note"])
        assert "ejected" not in run_log_events(returned.path)
    else:
        assert "ejected" in run_log_events(returned.path)  # it was in the bundle when the delivery failed


def test_a_refused_bundle_leaves_no_ejected_line(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = v3_workspace(tmp_path / "a")
    mapping, state, priority = single(ws, "failed", 500)

    def refuse(*_args: object, **_kwargs: object) -> None:
        raise BundleError("refused for the test")

    monkeypatch.setattr(_bundles, "_check_job", refuse)
    with cli_owner(ws) as owner:
        root = claim_root(ws, owner, mapping["id"])
        with pytest.raises(BundleError, match="refused for the test"):
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=False)
        assert owner.owned() == []
    assert_in(ws, [(mapping, state, priority)])
    returned = find(ws, mapping["id"])
    assert returned is not None and "ejected" not in run_log_events(returned.path)


def test_a_recorded_child_a_stale_listing_misses_is_still_a_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = tree(ws)
    root = find(ws, plan[0][0]["id"])
    assert root is not None
    doc, _ = read_state_unowned(root.path / "state.json")
    real, calls = _kernel._scan, {"n": 0}

    def stale_first(*args: object, **kwargs: object) -> dict[str, _kernel.JobRef]:
        calls["n"] += 1
        return {} if calls["n"] == 1 else real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_kernel, "_scan", stale_first)
    members = _moving.tree_members(ws, root.job_id, doc)
    assert sorted(member.job_id for member in members) == sorted(str(mapping["id"]) for mapping, _, _ in plan[1:])


# -- submit_payload (ported from the pre-redesign adoption hardening specification) ---------------------------------


@pytest.mark.parametrize("kind", ["symlink", "fifo"])
def test_a_moving_submit_refuses_an_unsafe_job_json_and_moves_nothing(tmp_path: Path, kind: str) -> None:
    ws = v3_workspace(tmp_path / "a")
    payload = tmp_path / "payload"
    payload.mkdir()
    job = JobDefinition.from_mapping(job_mapping(WORKFLOW, {"start": "succeed"}, tag="p", placement="proj/p"))
    if kind == "symlink":
        (tmp_path / "job.json").write_bytes(job.encode())
        (payload / "job.json").symlink_to(tmp_path / "job.json")
    else:
        os.mkfifo(payload / "job.json")
    with cli_owner(ws) as owner, pytest.raises(FormatError):
        scaffold.submit_payload(ws, owner, payload, move=True)
    assert (payload / "job.json").is_symlink() if kind == "symlink" else (payload / "job.json").exists()
    assert find(ws, job.id) is None


def test_a_copying_submit_refuses_a_special_file_in_the_payload(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    payload = tmp_path / "payload"
    payload.mkdir()
    job = JobDefinition.from_mapping(job_mapping(WORKFLOW, {"start": "succeed"}, tag="p", placement="proj/p"))
    (payload / "job.json").write_bytes(job.encode())
    os.mkfifo(payload / "pipe")
    with cli_owner(ws) as owner, pytest.raises(_fs.UnsafePath, match="special file"):
        scaffold.submit_payload(ws, owner, payload, move=False)
    assert find(ws, job.id) is None and (payload / "job.json").is_file()


# -- R2: isolation of untrusted bundles, in-tick settling, the exchange index of delivered jobs ---------------------


def _plant(jobs: Path) -> None:
    """Content an untrusted bundle may never carry, in every member directory below *jobs*."""

    for job_json in jobs.rglob("job.json"):
        member = job_json.parent
        os.symlink("/etc/passwd", member / "evil-link")
        (member / ".httk-job").mkdir()
        (member / "seal.json").write_text("{}")
        os.mkfifo(member / "fifo")


@pytest.mark.parametrize("moment", ["after the take", "after validation"])
def test_a_client_descriptor_never_steers_what_adoption_publishes(tmp_path: Path, moment: str) -> None:
    # The client keeps a descriptor on its inbox entry and swaps jobs/ for a planted tree (probe_swap of review D).
    ws = v3_workspace(tmp_path / "ws")
    entry = tmp_path / "inbox" / "entry"
    client_bundle(entry)
    attacker = tmp_path / "attacker"
    shutil.copytree(entry / "jobs", attacker / "jobs")
    _plant(attacker / "jobs")
    held = os.open(entry, os.O_RDONLY | os.O_DIRECTORY)
    swaps: list[str] = []

    def swap(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if swaps or dst is None:
            return
        taken = op == "rename" and phase == "after" and ".adopt-untrusted." in str(dst.path)
        validated = op == "write" and phase == "before_replace" and dst.path.name == "rekey.json"
        if (taken and moment == "after the take") or (validated and moment == "after validation"):
            try:
                os.rename("jobs", "jobs.orig", src_dir_fd=held, dst_dir_fd=held)
                os.symlink(str(attacker / "jobs"), "jobs", dir_fd=held)
                swaps.append("swapped")
            except FileNotFoundError:
                swaps.append("the original is gone")

    _fs.set_fault_injector(swap)
    try:
        with cli_owner(ws) as owner:
            if moment == "after the take":
                # The copy carries the symlink, and its validation refuses the bundle.
                with pytest.raises(BundleError, match="symlink"):
                    _moving.adopt(ws, owner, entry, untrusted=True, refused_to=tmp_path / "rejected")
                report = None
            else:
                report = _moving.adopt(ws, owner, entry, untrusted=True, refused_to=tmp_path / "rejected")
    finally:
        _fs.set_fault_injector(None)
        os.close(held)
    if moment == "after the take":
        assert swaps == ["swapped"] and job_ids_below(ws.jobs) == []
        (rejected,) = (tmp_path / "rejected").iterdir()
        assert (rejected / "entry" / "jobs").is_symlink()  # the client's own copy goes back to it
    else:
        # By validation the descriptor's directory, the taken original, was discarded already.
        assert swaps == ["the original is gone"]
        assert report is not None and len(report.published) == 3
        for ref in report.published:
            assert not set(os.listdir(ref.path)) & {"evil-link", ".httk-job", "seal.json", "fifo"}
            assert ref.path.resolve().is_relative_to(ws.jobs.resolve())
    assert not list((ws.control / "tmp").iterdir())


@pytest.mark.parametrize("state", ["original", "partial copy", "complete copy", "copy only"])
def test_isolation_resumes_from_every_intermediate_state(tmp_path: Path, state: str) -> None:
    ws = v3_workspace(tmp_path / "ws")
    entry = tmp_path / "inbox" / "entry"
    jobs = client_bundle(entry)
    owner = cli_owner(ws)
    scratch = _kernel.take(ws, owner, _fs.loc(entry), "adopt-untrusted")
    assert scratch is not None
    record = _moving._Record(None, str(tmp_path / "rejected"), False, _fs.fresh_token(), True)
    staged = scratch / ".partial" / "entry"
    if state == "partial copy":
        staged.mkdir(parents=True)
        (staged / "bundle.json").write_text("half written")
    elif state != "original":
        _fs.copy_tree(scratch / "entry", staged, durable=False)
        record = dataclasses.replace(record, isolated=True)
        if state == "copy only":
            shutil.rmtree(scratch / "entry")
    _moving._write_record(scratch, record, durable=False)
    die_and_recover(ws, owner)
    published = job_ids_below(ws.jobs)
    assert len(set(published)) == len(published) == 3
    names = {json.loads(path.read_bytes())["exchange_name"] for path in ws.jobs.rglob("state.json")}
    assert names == {str(job["id"]) for job in jobs}
    assert not job_ids_below(ws.control / "tmp") and not (tmp_path / "rejected").exists()


def fail_at_extract(k: int) -> _fs.Fault:
    seen = {"n": 0}

    def fault(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "before" and dst is not None and ".eject." in str(dst.path):
            seen["n"] += 1
            if seen["n"] == k:
                raise _fs.MoveFailed(f"extract #{k}")

    return fault


def test_an_error_after_k_of_n_extracts_is_settled_within_the_call(tmp_path: Path) -> None:
    for k in range(1, 5):
        ws = v3_workspace(tmp_path / f"w{k}")
        plan = tree(ws)
        owner = cli_owner(ws)
        root = claim_root(ws, owner, plan[0][0]["id"])
        _fs.set_fault_injector(fail_at_extract(k))
        with pytest.raises(_fs.MoveFailed):
            _moving.eject(ws, owner, root, destination=tmp_path / "out", tree=True)
        _fs.set_fault_injector(None)
        # Every job is back before the owner closes: no eject scratch waits for close or recovery.
        assert_in(ws, plan)
        assert not [name for name in os.listdir(ws.control / "tmp") if ".eject." in name], k
        owner.close()
        assert not (tmp_path / "out").exists()


def _exchange_tree(ws: Workspace, tmp_path: Path) -> tuple[list[dict[str, object]], _kernel.JobRef]:
    """Adopt a client bundle and finish its jobs; return the client jobs and the indexed root."""

    jobs = client_bundle(tmp_path / "inbox" / "entry")
    with cli_owner(ws) as owner:
        report = _moving.adopt(ws, owner, tmp_path / "inbox" / "entry", untrusted=True)
        assert report is not None
        for ref in report.published:
            owned = _kernel.claim(ws, owner, ref)
            assert owned is not None
            doc = owned.read_state()
            assert doc is not None
            owned.release(doc.next_activation("start", "initial"), Release("succeeded", ref.priority))
    indexed = _kernel.exchange_index(ws, str(jobs[0]["id"]))
    assert indexed is not None
    root = find(ws, indexed[0])
    assert root is not None
    return jobs, root


def test_a_held_exchange_job_leaves_the_exchange_index(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    with cli_owner(ws) as owner:
        hold = _moving.hold(ws, owner, claim_root(ws, owner, root.job_id), tree=True)
    assert len(hold.members) == 3 and _kernel.exchange_index(ws, str(jobs[0]["id"])) is None


def _outbox(ws: Workspace, client_id: object) -> Path:
    return ws.root / "exchange" / "outbox" / str(client_id)


@pytest.mark.parametrize("where", ["outbox", "plain"])
def test_a_crash_right_after_the_delivery_rolls_forward_and_leaves_the_index(tmp_path: Path, where: str) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    destination = _outbox(ws, jobs[0]["id"]) if where == "outbox" else tmp_path / "out"
    owner = cli_owner(ws)

    def crash_after_delivery(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "after" and src is not None and src.name() == "bundle":
            raise Crash("delivered, not yet out of the index")

    _fs.set_fault_injector(crash_after_delivery)
    with pytest.raises(Crash):
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=destination, tree=True)
    # The bundle left the scratch; its eject.json keeps the scratch for the reconciler.
    (scratch,) = [name for name in os.listdir(ws.control / "tmp") if ".eject." in name]
    assert os.listdir(ws.control / "tmp" / scratch) == ["eject.json"]
    die_and_recover(ws, owner)
    assert job_ids_below(ws.jobs) == [] and len(job_ids_below(destination)) == 3
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None
    # The client fetches the return and sends the same jobs again: they are adopted.
    shutil.rmtree(destination)
    client_bundle(tmp_path / "inbox" / "again", jobs)
    with cli_owner(ws) as again:
        resent = _moving.adopt(ws, again, tmp_path / "inbox" / "again", untrusted=True)
    assert resent is not None and len(resent.published) == 3


def test_a_return_fetched_before_recovery_still_leaves_the_index(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    destination = _outbox(ws, jobs[0]["id"])
    owner = cli_owner(ws)

    def crash_after_delivery(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "after" and src is not None and src.name() == "bundle":
            raise Crash("delivered, not yet out of the index")

    _fs.set_fault_injector(crash_after_delivery)
    with pytest.raises(Crash):
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=destination, tree=True)
    shutil.rmtree(destination)  # fetched by the client before the owner is recovered
    die_and_recover(ws, owner)
    # The bundle left the scratch, which only its owner moves: the delivery happened.
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None


def test_an_index_entry_whose_removal_never_ran_is_found_and_repaired_by_fsck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.fsck import check_workspace

    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    monkeypatch.setattr(_kernel, "drop_exchange_index", lambda *_args: None)
    with cli_owner(ws) as owner:
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=tmp_path / "out", tree=True)
    monkeypatch.undo()
    assert job_ids_below(ws.jobs) == [] and _kernel.exchange_index(ws, str(jobs[0]["id"])) is not None
    report = check_workspace(ws)
    (finding,) = [finding for finding in report.findings if finding.problem == "stale_exchange_index"]
    assert finding.action == "reported" and finding.job_id == root.job_id
    repaired = check_workspace(ws, repair=True)
    assert [finding.action for finding in repaired.findings] == ["removed"]
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None and check_workspace(ws).ok
    # The client can send the same jobs again.
    client_bundle(tmp_path / "inbox" / "again", jobs)
    with cli_owner(ws) as again:
        resent = _moving.adopt(ws, again, tmp_path / "inbox" / "again", untrusted=True)
    assert resent is not None and len(resent.published) == 3


def test_fsck_leaves_index_entries_of_held_and_in_flight_jobs_alone(tmp_path: Path) -> None:
    from httk.workflow.fsck import check_workspace

    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    assert check_workspace(ws).ok
    # An index entry whose job sits in an eject scratch (the scratch of an owner that died mid-eject).
    owner = cli_owner(ws)
    _fs.set_fault_injector(crash_at_extract(3, "after"))
    with pytest.raises(Crash):
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=tmp_path / "out", tree=True)
    _fs.set_fault_injector(None)
    assert check_workspace(ws).ok
    die_and_recover(ws, owner)
    assert check_workspace(ws).ok and _kernel.exchange_index(ws, str(jobs[0]["id"])) is not None


def test_fsck_repair_keeps_the_index_of_a_job_an_eject_extracts_meanwhile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The in-flight check runs after the settled locate: an eject that extracts the job while fsck looks is seen.
    from httk.workflow import fsck

    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    owner = cli_owner(ws)
    real_locate = _kernel.locate
    ejected: list[bool] = []

    def locate_then_eject(*args: Any, **kwargs: Any) -> _kernel.JobRef | None:
        found = real_locate(*args, **kwargs)
        if not ejected:
            ejected.append(True)
            _fs.set_fault_injector(crash_at_extract(3, "after"))
            with pytest.raises(Crash):
                _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=tmp_path / "out", tree=True)
            _fs.set_fault_injector(None)
            return None  # the job was not found: it is in the eject scratch now
        return found

    monkeypatch.setattr(fsck._kernel, "locate", locate_then_eject)
    report = fsck.check_workspace(ws, repair=True)
    monkeypatch.undo()
    assert ejected and not [finding for finding in report.findings if finding.problem == "stale_exchange_index"]
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is not None
    die_and_recover(ws, owner)  # the eject rolls back: the jobs come home, still indexed
    assert len(job_ids_below(ws.jobs)) == 3 and _kernel.exchange_index(ws, str(jobs[0]["id"])) is not None


@pytest.mark.parametrize("planted", ["outbox", "outbox/name"])
def test_a_symlink_in_the_outbox_never_redirects_an_eject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, planted: str
) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    exchange = ws.root / "exchange"
    exchange.mkdir()
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / str(jobs[0]["id"])).mkdir(parents=True)
    if planted == "outbox":
        os.symlink(elsewhere, exchange / "outbox")  # the client's doing
    else:
        (exchange / "outbox").mkdir()
        os.symlink(elsewhere / str(jobs[0]["id"]), exchange / "outbox" / str(jobs[0]["id"]))
    destination = exchange / "outbox" / str(jobs[0]["id"])
    assert _moving.destination_problem(destination, ws) is not None
    with cli_owner(ws) as owner, pytest.raises(ValueError, match="unusable"):
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=destination, tree=True)
    # Planted after the check: the delivery itself opens every directory without following a link.
    monkeypatch.setattr(_moving, "destination_problem", lambda *_args: None)
    with cli_owner(ws) as owner, pytest.raises(WorkflowError, match="returned"):
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=destination, tree=True)
    assert not list(elsewhere.rglob("bundle.json")) and len(job_ids_below(ws.jobs)) == 3
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is not None


def test_an_outbox_target_is_anchored_and_a_plain_one_is_not(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "ws")
    descriptor, target = _moving._anchored_target(ws, ws.root / "exchange" / "outbox" / "name" / "key", create=True)
    try:
        assert descriptor is not None and target == _fs.anchored(descriptor, "key")
    finally:
        assert descriptor is not None
        os.close(descriptor)
    assert (ws.root / "exchange" / "outbox" / "name").is_dir()
    assert _moving._anchored_target(ws, tmp_path / "out" / "key", create=True) == (
        None,
        _fs.loc(tmp_path / "out" / "key"),
    )
    assert _moving.destination_problem(ws.root / "exchange" / ".." / "jobs", ws) is not None


def test_a_delivered_cross_filesystem_copy_rolls_forward_and_leaves_the_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = v3_workspace(tmp_path / "ws")
    jobs, root = _exchange_tree(ws, tmp_path)
    real_deliver = _fs.deliver
    calls = {"n": 0}

    def deliver(src: _fs.Loc, dst: _fs.Loc, **kwargs: Any) -> _fs.Delivered:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _fs.CrossDevice("simulated")
        real_deliver(src, dst, **kwargs)
        raise Crash("the copy is delivered; the scratch is not yet discarded")

    monkeypatch.setattr(_fs, "deliver", deliver)
    owner = cli_owner(ws)
    with pytest.raises(Crash):
        _moving.eject(ws, owner, claim_root(ws, owner, root.job_id), destination=tmp_path / "out", tree=True)
    monkeypatch.undo()
    die_and_recover(ws, owner)
    assert job_ids_below(ws.jobs) == [] and len(job_ids_below(tmp_path / "out")) == 3
    assert _kernel.exchange_index(ws, str(jobs[0]["id"])) is None


def test_a_move_failure_of_a_refusal_keeps_the_bundle_in_the_scratch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = v3_workspace(tmp_path / "ws")
    bundle = tmp_path / "inbox" / "entry"
    client_bundle(bundle)
    (bundle / "junk").write_text("not part of the bundle")

    def fail(*_args: object) -> Path:
        raise _fs.MoveFailed("simulated")

    monkeypatch.setattr(_moving, "_reject", fail)
    owner = cli_owner(ws)
    with pytest.raises(BundleError, match="stays in"):
        _moving.adopt(ws, owner, bundle, untrusted=True, refused_to=tmp_path / "rejected")
    monkeypatch.undo()
    assert [name for name in os.listdir(ws.control / "tmp") if ".adopt-untrusted." in name]
    die_and_recover(ws, owner)
    (rejected,) = (tmp_path / "rejected").iterdir()
    assert (rejected / "entry" / "junk").is_file()


def test_a_refused_copy_of_a_hold_in_transfers_incoming_is_discarded(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    place(target, plan[3][0], "failed", 100)  # one member is in the destination already
    incoming = target.control / "transfers" / "incoming"
    with cli_owner(source) as owner:
        report = _moving.eject(
            source, owner, claim_root(source, owner, plan[0][0]["id"]), destination=tmp_path / "out", tree=True
        )
    landed = incoming / f"{report.transfer_id}.{_fs.fresh_token()}"
    _fs.copy_tree(report.destination, landed, durable=False)
    with cli_owner(target) as owner, pytest.raises(BundleError, match="discarded"):
        _moving.adopt(target, owner, landed, untrusted=False)
    assert list(incoming.iterdir()) == [] and len(job_ids_below(target.jobs)) == 1
    assert (report.destination / "bundle.json").is_file()  # the original, like a hold, is untouched
