"""The ``away`` state: kept transfer copies, their replacement on adoption, and ``recover``."""

import json
import shutil
import uuid
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import Workspace, _fs, _kernel, _moving, _requests, removal
from httk.workflow._bundles import BundleError
from httk.workflow._state import (
    TERMINAL_STATES,
    UNOWNED_STATES,
    Release,
    StateDoc,
    decode_state,
    encode_state,
    read_state_unowned,
)
from httk.workflow.errors import FormatError
from test_moving import (
    WORKFLOW,
    Crash,
    assert_in,
    claim_root,
    count_points,
    crash_at,
    die_and_recover,
    find,
    job_ids_below,
    parent_link,
    place,
    tree,
)
from v3_helpers import cli_owner, job_mapping
from v3_helpers import run as run_manager
from v3_helpers import workspace as v3_workspace

type Plan = list[tuple[dict[str, object], str, int]]

LOCATOR = "cluster:far"


def away_record(**members: object) -> dict[str, object]:
    record: dict[str, object] = {
        "workspace_id": str(uuid.uuid4()),
        "locator": LOCATOR,
        "transfer_id": uuid.uuid4().hex,
        "state": "paused",
        "priority": 7,
        "at": "2026-10-25T00:00:00+00:00",
    }
    return record | members


def state_of(ref: _kernel.JobRef) -> StateDoc:
    doc, damaged = read_state_unowned(ref.path / "state.json")
    assert doc is not None and not damaged
    return doc


def held_to(source: Workspace, destination: Workspace, root_id: object) -> _moving.Hold:
    with cli_owner(source) as owner:
        return _moving.hold(
            source,
            owner,
            claim_root(source, owner, root_id),
            tree=True,
            destination_locator=LOCATOR,
            destination_workspace_id=destination.workspace_id,
        )


def deliver(hold: _moving.Hold, destination: Workspace, landing: Path) -> _moving.AdoptReport | None:
    """Copy *hold* to *landing* and adopt it, trusted, into *destination*."""

    _fs.copy_tree(Path(hold.path), landing, durable=False)
    with cli_owner(destination) as owner:
        return _moving.adopt(destination, owner, landing, untrusted=False)


def pushed(tmp_path: Path) -> tuple[Workspace, Workspace, Plan, _moving.Hold]:
    """A tree held in A for B and adopted there; the hold in A is not yet released."""

    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    hold = held_to(source, target, plan[0][0]["id"])
    assert deliver(hold, target, tmp_path / "landing" / hold.transfer_id) is not None
    return source, target, plan, hold


def kept(tmp_path: Path) -> tuple[Workspace, Workspace, Plan, _moving.Hold]:
    """:func:`pushed`, with the hold released keeping away copies in A."""

    source, target, plan, hold = pushed(tmp_path)
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, hold.transfer_id, keep=True)
    return source, target, plan, hold


def assert_away(ws: Workspace, plan: Plan, hold: _moving.Hold, destination: Workspace) -> None:
    for mapping, state, priority in plan:
        ref = find(ws, mapping["id"])
        assert ref is not None and (ref.state, ref.priority) == ("away", priority), mapping["tag"]
        assert ref.path.parent == ws.jobs / "away" / str(mapping["placement"])
        assert (ref.path / "input.txt").read_text() == f"payload of {mapping['id']}"
        doc = state_of(ref)
        assert doc.away is not None
        assert {name: doc.away[name] for name in doc.away if name != "at"} == {
            "workspace_id": destination.workspace_id,
            "locator": LOCATOR,
            "transfer_id": hold.transfer_id,
            "state": state,
            "priority": priority,
        }
        assert doc.history_tail[-1]["event"] == "away"
        assert doc.release_to == Release("away", priority)


# -- state.json -----------------------------------------------------------------------------------------------------


def test_away_is_the_last_unowned_state_and_never_terminal() -> None:
    assert UNOWNED_STATES[-1] == "away" and "away" not in TERMINAL_STATES


def test_the_away_record_round_trips_and_old_documents_read_as_none() -> None:
    job_id = str(uuid.uuid4())
    empty = StateDoc.empty(job_id)
    assert empty.away is None and empty.as_mapping()["away"] is None
    doc = empty.updated(away=away_record())
    assert decode_state(encode_state(doc)) == doc and doc.away is not None and doc.away["locator"] == LOCATOR
    old = empty.as_mapping()
    del old["away"]
    assert StateDoc.from_mapping(old).away is None


@pytest.mark.parametrize(
    "members",
    [
        {"workspace_id": "not-a-uuid"},
        {"workspace_id": None},
        {"locator": ""},
        {"transfer_id": "A" * 32},
        {"state": "away"},
        {"state": "owned"},
        {"priority": 1000},
        {"priority": True},
        {"at": 3},
        {"extra": 1},
    ],
)
def test_a_malformed_away_record_is_refused(members: dict[str, object]) -> None:
    with pytest.raises(FormatError):
        StateDoc.empty(str(uuid.uuid4())).updated(away=away_record(**members))
    missing = away_record()
    del missing["at"]
    with pytest.raises(FormatError):
        StateDoc.empty(str(uuid.uuid4())).updated(away=missing)


# -- keeping copies -------------------------------------------------------------------------------------------------


def test_a_kept_release_publishes_every_member_into_away(tmp_path: Path) -> None:
    source, target, plan, hold = kept(tmp_path)
    assert_away(source, plan, hold, target)
    assert _moving.held(source) == []
    assert_in(target, plan)
    assert not list((source.control / "tmp").iterdir())


def test_a_plain_release_still_discards(tmp_path: Path) -> None:
    source, _target, plan, hold = pushed(tmp_path)
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, hold.transfer_id)
    assert all(find(source, mapping["id"]) is None for mapping, _, _ in plan)


def test_a_workspace_without_an_away_directory_takes_a_kept_release(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    shutil.rmtree(source.jobs / "away")  # initialized before the state existed
    assert list(_kernel.list_jobs(source, "away")) == []
    plan = tree(source)
    hold = held_to(source, target, plan[0][0]["id"])
    deliver(hold, target, tmp_path / "landing" / hold.transfer_id)
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, hold.transfer_id, keep=True)
    assert len(list(_kernel.list_jobs(source, "away"))) == 4
    assert_away(source, plan, hold, target)


def test_a_crash_after_the_first_kept_member_is_finished_once_by_recovery(tmp_path: Path) -> None:
    source, target, plan, hold = pushed(tmp_path)
    away = source.jobs / "away"

    def crash_after_first_submit(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "after" and dst is not None and dst.path.is_relative_to(away):
            raise Crash("after the first submit")

    owner = cli_owner(source)
    _fs.set_fault_injector(crash_after_first_submit)
    with pytest.raises(Crash):
        _moving.release_hold(source, owner, hold.transfer_id, keep=True)
    assert len(job_ids_below(away)) == 1
    die_and_recover(source, owner)
    published = job_ids_below(source.jobs)
    assert len(published) == len(set(published)) == 4
    assert_away(source, plan, hold, target)
    assert not job_ids_below(source.control / "tmp")


# -- inert away jobs ------------------------------------------------------------------------------------------------


def serve(ws: Workspace, ref: _kernel.JobRef, action: str) -> None:
    assert ref.placement is not None
    _requests.post(ws, action=action, job_id=ref.job_id, placement=ref.placement, operator="t", reason="t")
    with cli_owner(ws) as owner:
        assert removal.serve(ws, owner, ref)


@pytest.mark.parametrize("action", ["pause", "cancel", "continue", "set_priority", "unlock"])
def test_requests_but_delete_are_refused_on_an_away_job(tmp_path: Path, action: str) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    ref = find(source, plan[0][0]["id"])
    assert ref is not None
    if action == "set_priority":
        assert ref.placement is not None
        _requests.post(
            source, action=action, job_id=ref.job_id, placement=ref.placement, operator="t", reason="t", priority=1
        )
        with cli_owner(source) as owner:
            assert removal.serve(source, owner, ref)
    else:
        serve(source, ref, action)
    after = find(source, ref.job_id)
    assert after is not None and (after.state, after.priority) == ("away", ref.priority)
    (dropped,) = [entry for entry in state_of(after).history_tail if entry.get("event") == "request_dropped"]
    assert dropped["note"] == f"the job is away at {LOCATOR}; recover it first"


def test_delete_gives_up_an_away_job(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    leaf = find(source, plan[3][0]["id"])
    assert leaf is not None
    serve(source, leaf, "delete")
    assert find(source, leaf.job_id) is None
    assert "away" in removal.REMOVABLE_KINDS


def test_an_away_root_is_never_ejected_or_held(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    with cli_owner(source) as owner:
        with pytest.raises(ValueError, match="is away; recover it first"):
            _moving.hold(source, owner, claim_root(source, owner, plan[0][0]["id"]), tree=True)
        with pytest.raises(ValueError, match="is away; recover it first"):
            _moving.eject(
                source, owner, claim_root(source, owner, plan[3][0]["id"]), destination=tmp_path / "out", tree=False
            )
    assert [find(source, mapping["id"]).state for mapping, _, _ in plan] == ["away"] * 4  # type: ignore[union-attr]


def test_a_tree_with_an_away_child_is_busy(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    with cli_owner(source) as owner:
        root = claim_root(source, owner, plan[0][0]["id"])
        doc = root.read_state()
        assert doc is not None
        root.release(doc.updated(away=None), Release("paused", 410))
        with pytest.raises(_moving.Busy, match="it is away; tree members must be terminal or paused"):
            _moving.hold(source, owner, claim_root(source, owner, plan[0][0]["id"]), tree=True)
    assert find(source, plan[0][0]["id"]).state == "paused"  # type: ignore[union-attr]


def test_a_manager_never_claims_an_away_job(tmp_path: Path) -> None:
    source, target, plan, hold = kept(tmp_path)
    run_manager(source)
    assert_away(source, plan, hold, target)


# -- recover --------------------------------------------------------------------------------------------------------


def recover(ws: Workspace, job_id: object) -> list[_kernel.JobRef]:
    with cli_owner(ws) as owner:
        return _moving.recover(ws, owner, claim_root(ws, owner, job_id))


def test_recover_returns_the_tree_to_the_states_it_left(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    released = recover(source, plan[0][0]["id"])
    assert [ref.job_id for ref in released][-1] == plan[0][0]["id"] and len(released) == 4
    assert_in(source, plan)
    for mapping, _state, _priority in plan:
        doc = state_of(find(source, mapping["id"]))  # type: ignore[arg-type]
        assert doc.away is None and doc.history_tail[-1]["event"] == "recovered"


def test_recover_finishes_a_half_recovered_tree(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    child, state, priority = plan[1]
    with cli_owner(source) as owner:
        job = claim_root(source, owner, child["id"])
        doc = job.read_state()
        assert doc is not None
        job.release(doc.updated(away=None), Release(state, priority))
    recover(source, plan[0][0]["id"])
    assert_in(source, plan)


def test_recover_refuses_a_job_not_away_and_a_member_of_an_away_tree(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    root_key = find(source, plan[0][0]["id"]).job_key  # type: ignore[union-attr]
    with pytest.raises(ValueError, match=f"is a member of the away tree of {root_key}; recover {root_key}"):
        recover(source, plan[3][0]["id"])
    assert find(source, plan[3][0]["id"]).state == "away"  # type: ignore[union-attr]
    recover(source, plan[0][0]["id"])
    with pytest.raises(ValueError, match="is paused, not away"):
        recover(source, plan[0][0]["id"])
    assert_in(source, plan)


def test_recover_refuses_an_away_job_without_its_record(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    with cli_owner(source) as owner:
        root = claim_root(source, owner, plan[0][0]["id"])
        doc = root.read_state()
        assert doc is not None
        root.write_state(doc.updated(away=None))
        root.give_back()
    with pytest.raises(ValueError, match="has no away record"):
        recover(source, plan[0][0]["id"])
    assert find(source, plan[0][0]["id"]).state == "away"  # type: ignore[union-attr]


def test_away_root_names_the_top_of_the_transfer(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    grand = find(source, plan[3][0]["id"])
    assert grand is not None
    assert _moving.away_root(source, grand).job_id == plan[0][0]["id"]
    root = find(source, plan[0][0]["id"])
    assert root is not None and _moving.away_root(source, root) == root


# -- replacing copies on adoption -----------------------------------------------------------------------------------


def returned(tmp_path: Path, source: Workspace, target: Workspace, plan: Plan) -> tuple[_moving.Hold, dict]:
    """In B, finish the root, spawn a fourth child under it and hold the tree back for A."""

    extra = job_mapping(WORKFLOW, {"start": "succeed"}, tag="late", placement="proj/r/late")
    extra["parent"] = parent_link(plan[0][0], target.workspace_id)
    place(target, extra, "succeeded", 450)
    with cli_owner(target) as owner:
        root = claim_root(target, owner, plan[0][0]["id"])
        doc = root.read_state()
        assert doc is not None
        child = {"job_id": extra["id"], "job_key": f"{extra['tag']}--{extra['id']}", "placement": "proj/r/late"}
        root.release(doc.with_children([*doc.children, child]), Release("succeeded", 411))
    return held_to(target, source, plan[0][0]["id"]), extra


def test_a_trusted_return_replaces_the_away_copies(tmp_path: Path) -> None:
    source, target, plan, _hold = kept(tmp_path)
    back, extra = returned(tmp_path, source, target, plan)
    report = deliver(back, source, tmp_path / "back" / back.transfer_id)
    assert report is not None and len(report.published) == 5
    published = job_ids_below(source.jobs)
    assert len(published) == len(set(published)) == 5
    assert list(_kernel.list_jobs(source, "away")) == []
    assert_in(source, [(plan[0][0], "succeeded", 411), *plan[1:], (extra, "succeeded", 450)])
    assert state_of(find(source, plan[0][0]["id"])).away is None  # type: ignore[arg-type]
    assert not list((source.control / "tmp").iterdir())


def rewrite_away(ws: Workspace, job_id: object, away: object) -> None:
    with cli_owner(ws) as owner:
        job = claim_root(ws, owner, job_id)
        doc = job.read_state()
        assert doc is not None
        job.write_state(doc.updated(away=away))
        job.give_back()


@pytest.mark.parametrize("record", ["mismatch", "none"])
def test_a_copy_kept_for_another_workspace_or_without_a_record_refuses(tmp_path: Path, record: str) -> None:
    source, target, plan, _hold = kept(tmp_path)
    back, _extra = returned(tmp_path, source, target, plan)
    doc = state_of(find(source, plan[2][0]["id"]))  # type: ignore[arg-type]
    assert doc.away is not None
    rewrite_away(
        source, plan[2][0]["id"], None if record == "none" else {**doc.away, "workspace_id": str(uuid.uuid4())}
    )
    landing = tmp_path / "back" / back.transfer_id
    message = (
        "is away without a record of where it went; delete the copy first"
        if record == "none"
        else f"is away at {LOCATOR}, but this bundle comes from workspace {target.workspace_id}; recover or delete"
    )
    with pytest.raises(BundleError, match=message):
        deliver(back, source, landing)
    assert (landing / "bundle.json").is_file()
    assert [find(source, mapping["id"]).state for mapping, _, _ in plan] == ["away"] * 4  # type: ignore[union-attr]


def test_a_recovered_copy_is_present_and_refuses_the_return(tmp_path: Path) -> None:
    source, target, plan, _hold = kept(tmp_path)
    back, _extra = returned(tmp_path, source, target, plan)
    recover(source, plan[0][0]["id"])
    # Partly present, but a recovered member makes it the late return, refused as such.
    with pytest.raises(BundleError, match="was recovered here after it was pushed to workspace"):
        deliver(back, source, tmp_path / "back" / back.transfer_id)
    assert_in(source, plan)


def test_an_untrusted_bundle_never_replaces_a_copy(tmp_path: Path) -> None:
    source, target, plan, _hold = kept(tmp_path)
    back, _extra = returned(tmp_path, source, target, plan)
    landing = tmp_path / "back" / back.transfer_id
    _fs.copy_tree(Path(back.path), landing, durable=False)
    with cli_owner(source) as owner, pytest.raises(BundleError):
        _moving.adopt(source, owner, landing, untrusted=True)
    assert [find(source, mapping["id"]).state for mapping, _, _ in plan] == ["away"] * 4  # type: ignore[union-attr]


def test_a_return_crash_matrix_publishes_each_job_once(tmp_path: Path) -> None:
    def setup(name: str) -> tuple[Workspace, Path, list[str]]:
        source, target, plan, _hold = kept(tmp_path / name)
        back, extra = returned(tmp_path / name, source, target, plan)
        landing = tmp_path / name / "back" / back.transfer_id
        _fs.copy_tree(Path(back.path), landing, durable=False)
        return source, landing, sorted([str(mapping["id"]) for mapping, _, _ in plan] + [str(extra["id"])])

    source, landing, _ids = setup("probe")
    with cli_owner(source) as owner:
        total = count_points(lambda: _moving.adopt(source, owner, landing, untrusted=False))
    assert total >= 10
    outcomes = set()
    for k in range(1, total + 1):
        source, landing, ids = setup(f"k{k}")
        owner = cli_owner(source)
        _fs.set_fault_injector(crash_at(k))
        with pytest.raises(Crash):
            _moving.adopt(source, owner, landing, untrusted=False)
        die_and_recover(source, owner)
        here = job_ids_below(source.jobs)
        assert len(here) == len(set(here)), k
        assert not job_ids_below(source.control / "tmp"), k
        away = list(_kernel.list_jobs(source, "away"))
        if sorted(here) == ids:
            assert away == [], k  # adopted: every copy replaced
            outcomes.add("adopted")
        else:
            # Not adopted: the copies stay and the bundle is back where it was.
            assert len(away) == len(here) == 4 and sorted(job_ids_below(landing)) == ids, k
            outcomes.add("kept")
    assert outcomes == {"adopted", "kept"}


def test_a_replay_after_the_plan_reads_the_replacements(tmp_path: Path) -> None:
    source, target, plan, _hold = kept(tmp_path)
    back, _extra = returned(tmp_path, source, target, plan)
    landing = tmp_path / "back" / back.transfer_id
    _fs.copy_tree(Path(back.path), landing, durable=False)

    away = source.jobs / "away"

    def crash_at_the_first_claim(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "before" and src is not None and src.path.is_relative_to(away):
            raise Crash("at the first claim of a copy, after the plan")

    owner = cli_owner(source)
    _fs.set_fault_injector(crash_at_the_first_claim)
    with pytest.raises(Crash):
        _moving.adopt(source, owner, landing, untrusted=False)
    (scratch,) = (source.control / "tmp").glob("*.adopt.*")
    replaces = json.loads((scratch / "replaces.json").read_bytes())
    assert sorted(replaces) == sorted(str(mapping["id"]) for mapping, _, _ in plan)
    assert set(replaces.values()) == {str(PurePosixPath(str(mapping["placement"]))) for mapping, _, _ in plan}
    die_and_recover(source, owner)
    assert list(_kernel.list_jobs(source, "away")) == []
    assert len(job_ids_below(source.jobs)) == 5


# -- late returns of recovered jobs ---------------------------------------------------------------------------------


def test_the_recovered_record_round_trips_and_is_validated() -> None:
    doc = StateDoc.empty(str(uuid.uuid4()))
    assert doc.recovered is None and doc.as_mapping()["recovered"] is None
    record = {"workspace_id": str(uuid.uuid4()), "transfer_id": uuid.uuid4().hex, "at": "2026-10-25T00:00:00+00:00"}
    recovered = doc.updated(recovered=record)
    assert decode_state(encode_state(recovered)) == recovered
    bad_records = (
        {**record, "workspace_id": None},
        {**record, "transfer_id": "x"},
        {**record, "locator": LOCATOR},
        {**record, "at": ""},
    )
    for bad in bad_records:
        with pytest.raises(FormatError):
            doc.updated(recovered=bad)


def landing_of(ws: Workspace, hold: _moving.Hold) -> Path:
    # Where a transfer lands a copy of a remote hold: a refused copy there is discarded, the hold stays.
    return ws.control / "transfers" / "incoming" / f"{hold.transfer_id}.x"


def test_the_late_return_of_a_recovered_tree_is_refused_and_stays_held(tmp_path: Path) -> None:
    source, target, plan, hold = kept(tmp_path)
    recover(source, plan[0][0]["id"])
    doc = state_of(find(source, plan[0][0]["id"]))  # type: ignore[arg-type]
    assert doc.recovered is not None and doc.recovered["workspace_id"] == target.workspace_id
    assert doc.recovered["transfer_id"] == hold.transfer_id
    before = {path: path.read_bytes() for path in source.jobs.rglob("*") if path.is_file()}
    back = held_to(target, source, plan[0][0]["id"])
    landing = landing_of(source, back)
    message = f"was recovered here after it was pushed to workspace {target.workspace_id}; this bundle is its late"
    with pytest.raises(BundleError, match=message):
        deliver(back, source, landing)
    assert {path: path.read_bytes() for path in source.jobs.rglob("*") if path.is_file()} == before
    assert not landing.exists() and [item.transfer_id for item in _moving.held(target)] == [back.transfer_id]


def test_all_present_from_another_workspace_is_still_already_adopted(tmp_path: Path) -> None:
    source, target, plan, _hold = kept(tmp_path)
    recover(source, plan[0][0]["id"])
    elsewhere = str(uuid.uuid4())
    with cli_owner(source) as owner:
        for mapping, _state, _priority in plan:
            job = claim_root(source, owner, mapping["id"])
            doc = job.read_state()
            assert doc is not None and doc.recovered is not None
            job.write_state(doc.updated(recovered={**doc.recovered, "workspace_id": elsewhere}))
            job.give_back()
    back = held_to(target, source, plan[0][0]["id"])
    report = deliver(back, source, landing_of(source, back))
    assert report is not None and report.already_adopted
    assert_in(source, plan)


def test_a_recovered_tree_pushed_again_is_replaced_by_its_return(tmp_path: Path) -> None:
    source, target, plan, _hold = kept(tmp_path)
    recover(source, plan[0][0]["id"])
    with cli_owner(target) as owner:  # B gives the first copies up
        for mapping, _state, _priority in plan:
            claim_root(target, owner, mapping["id"]).discard()
    again = held_to(source, target, plan[0][0]["id"])
    deliver(again, target, tmp_path / "again" / again.transfer_id)
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, again.transfer_id, keep=True)
    assert_away(source, plan, again, target)
    assert state_of(find(source, plan[0][0]["id"])).recovered is not None  # type: ignore[arg-type]
    back = held_to(target, source, plan[0][0]["id"])
    report = deliver(back, source, landing_of(source, back))
    assert report is not None and len(report.published) == 4
    assert list(_kernel.list_jobs(source, "away")) == []
    assert_in(source, plan)


# -- review fixes: holds without a destination id, superseded recoveries, interrupted recoveries --------------------


def test_a_hold_without_a_destination_workspace_id_keeps_no_copies(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = tree(source)
    with cli_owner(source) as owner:
        hold = _moving.hold(
            source, owner, claim_root(source, owner, plan[0][0]["id"]), tree=True, destination_locator=LOCATOR
        )
    assert hold.destination_workspace_id is None
    deliver(hold, target, tmp_path / "landing" / hold.transfer_id)
    with cli_owner(source) as owner, caplog.at_level("WARNING", logger="httk.workflow._moving"):
        assert _moving.release_hold(source, owner, hold.transfer_id, keep=True)
    assert f"kept no away copies of transfer {hold.transfer_id}" in caplog.text
    assert job_ids_below(source.jobs) == [] and _moving.held(source) == []
    assert not list((source.control / "tmp").iterdir())


def push_again(tmp_path: Path, source: Workspace, target: Workspace, plan: Plan, *, keep: bool) -> None:
    """After a recover in A: B gives up its first copies and A pushes the tree to B again."""

    with cli_owner(target) as owner:
        for mapping, _state, _priority in plan:
            claim_root(target, owner, mapping["id"]).discard()
    again = held_to(source, target, plan[0][0]["id"])
    deliver(again, target, tmp_path / "again" / again.transfer_id)
    with cli_owner(source) as owner:
        assert _moving.release_hold(source, owner, again.transfer_id, keep=keep)


@pytest.mark.parametrize("keep", [True, False], ids=["keep", "no-keep"])
def test_a_return_after_a_re_push_supersedes_the_recovery(tmp_path: Path, keep: bool) -> None:
    source, target, plan, _hold = kept(tmp_path)
    recover(source, plan[0][0]["id"])
    push_again(tmp_path, source, target, plan, keep=keep)
    back = held_to(target, source, plan[0][0]["id"])
    report = deliver(back, source, landing_of(source, back))
    assert report is not None and len(report.published) == 4
    assert all(state_of(find(source, m["id"])).recovered is None for m, _, _ in plan)  # type: ignore[arg-type]
    # A duplicate delivery of the same hold (an at-least-once re-drive) is already adopted, not a late return.
    again = deliver(back, source, landing_of(source, back))
    assert again is not None and again.already_adopted
    assert_in(source, plan)


def test_an_interrupted_recover_is_finished_by_a_second_one(tmp_path: Path) -> None:
    source, _target, plan, _hold = kept(tmp_path)
    root_id = str(plan[0][0]["id"])
    owned = source.jobs / "owned"

    def crash_at_the_root_s_move(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        # The root is released last: its state.json is written, then it moves out of owned/.
        if src is None or dst is None or (op, phase) != ("rename", "before"):
            return
        if src.path.is_relative_to(owned) and root_id in src.path.name and not dst.path.is_relative_to(owned):
            raise Crash("between the root's write_state and its move")

    owner = cli_owner(source)
    root = claim_root(source, owner, root_id)
    _fs.set_fault_injector(crash_at_the_root_s_move)
    with pytest.raises(Crash):
        _moving.recover(source, owner, root)
    die_and_recover(source, owner)
    ref = find(source, root_id)
    assert ref is not None and ref.state == "away" and state_of(ref).away is None
    assert_in(source, plan[1:])
    released = recover(source, root_id)
    assert [item.job_id for item in released][-1] == root_id
    assert_in(source, plan)
