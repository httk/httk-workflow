"""The lock-free sealing transaction (plan 4.4, 4.6): ejection, export, addressed transfer, abort and recovery."""

import json
import os
import shutil
import time
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import Workspace, _sealing, _txn, transfers
from httk.workflow._bundle import TRANSFER_DIRECTORY, VerifiedBundle, verify_bundle
from httk.workflow._job_tree import record_spawns
from httk.workflow._receipts import CLOCK_SKEW_NS, FRESHNESS_WINDOW_NS, receipt_path
from httk.workflow.errors import FormatError
from httk.workflow.models import Marker, make_job_key
from httk.workflow.workflow_cli import command
from test_eject_adopt import _payload

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


class Crash(Exception):
    """A simulated process death at one protocol step."""


@pytest.fixture
def workspaces(tmp_path: Path) -> tuple[Workspace, Workspace]:
    configure_identity()
    source = Workspace.initialize(tmp_path / "source")
    source.set_policy({"visibility_deadline_seconds": 0.5})
    return Workspace(source.root), Workspace.initialize(tmp_path / "destination")


@pytest.fixture(autouse=True)
def _no_hook() -> Iterator[None]:
    yield
    _txn._HOOK = None


def _hook_at(step: str, action: Callable[[], object], occurrence: int = 1) -> None:
    """Run *action* once, at the *occurrence*-th time *step* is reached."""

    seen = 0

    def hook(name: str) -> None:
        nonlocal seen
        if name != step:
            return
        seen += 1
        if seen == occurrence:
            _txn._HOOK = None
            action()

    _txn._HOOK = hook


def _crash() -> None:
    raise Crash()


def _owner_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_sealing, "_owner_gone", lambda *args, **kwargs: True)


def _child(workspace: Workspace, root: Path, tag: str, parent: Marker, placement: str) -> Marker:
    payload = _payload(root, tag)
    document = json.loads((payload / "job.json").read_text())
    document["parent"] = {
        "job_id": parent.job_id,
        "job_key": parent.job_key,
        "placement": parent.placement.as_posix() if parent.placement.parts else "",
    }
    (payload / "job.json").write_text(json.dumps(document))
    marker = workspace.submit(payload, placement)
    return _finish(workspace, marker)


def _finish(workspace: Workspace, marker: Marker, kind: str = "succeeded") -> Marker:
    with workspace.open_journal_writer() as writer:
        return workspace.transition(writer, marker, kind, {"reason": "test"})


def _tree(workspace: Workspace, root: Path) -> tuple[Marker, list[Marker]]:
    """Root (with a shared workspace runner) at ``jobs/tree``; two children and a grandchild at the empty placement."""

    (root / "store").mkdir(parents=True)
    (root / "store" / "run.py").write_text("#!/bin/sh\nexit 0\n")
    reference = workspace.publish_runner(root / "store" / "run.py", name="tree/run.py")
    parent = _finish(workspace, workspace.submit(_payload(root / "p", "root", runner=dict(reference)), "tree"))
    first = _child(workspace, root / "c", "first", parent, "kids")
    second = _child(workspace, root / "c", "second", parent, "kids")
    grand = _child(workspace, root / "g", "grand", first, "")
    parent_payload = workspace.payload_path(parent.placement, parent.job_key)
    record_spawns(
        parent_payload,
        "a1",
        [{"job_key": child.job_key, "label": child.job_key[:5], "placement": "kids"} for child in (first, second)],
        durable=False,
    )
    record_spawns(
        workspace.payload_path(first.placement, first.job_key),
        "a2",
        [{"job_key": grand.job_key, "label": "grand", "placement": ""}],
        durable=False,
    )
    return parent, [first, second, grand]


def _tmp_entries(workspace: Workspace) -> list[str]:
    return sorted(name for name in os.listdir(workspace.control / "tmp") if not name.startswith("trash."))


def _transferring(workspace: Workspace) -> list[Marker]:
    return list(workspace.scan_markers(("transferring",)))


def _home(workspace: Workspace, marker: Marker) -> Path:
    return workspace.payload_path(marker.placement, marker.job_key)


def _assert_home(workspace: Workspace, markers: list[Marker]) -> None:
    """Every job is back at its placement, unfenced, in its original kind, and carries no envelope."""

    for marker in markers:
        current = workspace.find_marker_by_id(marker.job_id, kinds=("transferring", *_KINDS))
        assert current is not None and current.kind == marker.kind, (marker.job_key, current)
        assert _home(workspace, marker).is_dir()
        assert not (_home(workspace, marker) / TRANSFER_DIRECTORY).exists()
    assert not _transferring(workspace)
    assert _tmp_entries(workspace) == []


def _assert_ejected(
    workspace: Workspace, destination: Workspace, markers: list[Marker], bundle: Path
) -> VerifiedBundle:
    verified = verify_bundle(bundle, workspace=destination, source="adopt")
    assert sorted(job.job_id for job in verified.jobs) == sorted(marker.job_id for marker in markers)
    for marker in markers:
        assert workspace.find_marker_by_id(marker.job_id, kinds=("transferring", *_KINDS)) is None
        assert not _home(workspace, marker).exists()
    assert _tmp_entries(workspace) == []
    return verified


_KINDS = ("submitted", "ready", "waiting", "paused", "failed", "succeeded", "cancelled")


def _age_and_sweep(workspace: Workspace) -> None:
    """Let the orphan sweep act on transaction directories no marker names (as a day later)."""

    old = time.time() - _sealing.ORPHAN_SECONDS - 60
    for name in _tmp_entries(workspace):
        os.utime(workspace.control / "tmp" / name, (old, old))
    _sealing.recover(workspace)
    _sealing.recover(workspace)


# ---------------------------------------------------------------------------
# Ejection
# ---------------------------------------------------------------------------


def test_an_ejected_job_is_a_verified_bundle_and_leaves_nothing_behind(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs/a")
    out = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    assert out == tmp_path / "loose"
    verified = _assert_ejected(source, destination, [marker], out)
    assert verified.manifest.prior_kind == "submitted"
    assert verified.manifest.destination_workspace_id is None
    assert verified.manifest.sealed_at <= time.time_ns()
    assert sorted(os.listdir(out / TRANSFER_DIRECTORY)) == ["manifest.json", "markers", "runners", "tree"]


def test_a_tree_ejects_as_one_directory_with_its_runner(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    out = transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    verified = _assert_ejected(source, destination, [parent, *members], out)
    assert [job.job_key for job in verified.top_down][:2] == [parent.job_key, members[0].job_key]
    grand = verified.job(members[2].job_id)
    assert grand.envelope_relative is not None and grand.envelope_relative.parts == ("tree", members[2].job_key)
    assert [runner.path.as_posix() for runner in verified.manifest.runners] == ["tree/run.py"]


def test_a_tree_with_an_active_member_or_a_bound_child_is_refused_untouched(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, _destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    with pytest.raises(ValueError, match="travels with its parent"):
        transfers.eject_job(source, members[0].job_id, tmp_path / "child")
    # A member that is runnable again blocks the whole tree.
    with source.open_journal_writer() as writer:
        ready = source.transition(writer, members[1], "ready", {"reason": "test"})
    with pytest.raises(ValueError, match="its tree cannot leave yet"):
        transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    _assert_home(source, [parent, members[0], members[2]])
    assert source.find_marker_by_id(ready.job_id) == ready


def test_a_member_transferring_elsewhere_blocks_the_tree(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, _destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    with source.open_journal_writer() as writer:
        source.transition(writer, members[2], "transferring", {"prior_kind": "succeeded"})
    with pytest.raises(ValueError, match="is transferring"):
        transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    assert not (tmp_path / "loose").exists()


def test_an_adoption_claim_blocks_the_ejection(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    claims = source.control / "transfers" / "adopting"
    claims.mkdir(parents=True)
    (claims / marker.job_id).touch()
    with pytest.raises(ValueError, match="still being adopted"):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    _assert_home(source, [marker])


def test_a_planted_envelope_is_refused_before_fencing(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (_home(source, marker) / TRANSFER_DIRECTORY).mkdir()
    (_home(source, marker) / TRANSFER_DIRECTORY / "manifest.json").write_text("{}")
    with pytest.raises(ValueError, match="of its own"):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    assert source.find_marker_by_id(marker.job_id) == marker


def test_retire_refuses_an_ejection_in_progress(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S3.fenced", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    with pytest.raises(ValueError, match="ejection in progress"):
        transfers.retire_transfers(source, [marker.job_id])
    with pytest.raises(ValueError, match="no addressed transfer"):
        transfers.reclaim_transfer(source, marker.job_id)


def test_a_job_under_a_symlinked_placement_ejects_and_aborts_through_the_link(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (source.jobs / "project").symlink_to(scratch, target_is_directory=True)
    first = source.submit(_payload(tmp_path / "payloads", "first"), "project/runs")
    second = source.submit(_payload(tmp_path / "payloads", "second"), "project/runs")
    _hook_at("S6.commit", lambda: _sealing._decide_abort(source, _tmp_entries(source)[0].removeprefix("eject.")))
    with pytest.raises(ValueError):
        transfers.eject_job(source, first.job_id, tmp_path / "loose")
    _assert_home(source, [first])
    assert (scratch / "runs" / first.job_key).is_dir()
    _assert_ejected(source, destination, [second], transfers.eject_job(source, second.job_id, tmp_path / "second"))


@pytest.mark.parametrize("planted", ["symlink", "file"])
def test_a_planted_envelope_that_is_not_a_directory_is_refused(
    planted: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    outside = tmp_path / "outside"
    outside.mkdir()
    envelope = _home(source, marker) / TRANSFER_DIRECTORY
    if planted == "symlink":
        envelope.symlink_to(outside, target_is_directory=True)
    else:
        envelope.write_text("not a directory")
    with pytest.raises(FormatError, match="is a symlink or not a directory"):
        transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    assert os.listdir(outside) == [] and source.find_marker_by_id(marker.job_id) == marker


def test_ejection_into_the_exchange_outbox_commits_by_descriptor(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    outbox = _sealing.exchange_outbox(source)
    outbox.mkdir(parents=True)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    out = transfers.eject_job(source, marker.job_id, outbox)
    assert out == outbox / marker.job_key
    _assert_ejected(source, destination, [marker], out)


def test_a_symlinked_exchange_outbox_is_refused(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = workspaces
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    outbox = _sealing.exchange_outbox(source)
    outbox.parent.mkdir(parents=True)
    outbox.symlink_to(elsewhere)
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    with pytest.raises(ValueError, match="not a real directory"):
        transfers.eject_job(source, marker.job_id, outbox)
    with pytest.raises(ValueError, match="not a real directory"):
        transfers.eject_job(source, marker.job_id, elsewhere / "named")
    assert os.listdir(elsewhere) == []
    _assert_home(source, [marker])


def test_a_digest_drift_aborts_and_restores_the_job(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S4.runners", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    # A lingering attempt changes the payload after the manifest was linked in.
    (_home(source, marker) / "files" / "runner").write_text("#!/bin/sh\nexit 3\n")
    _owner_gone(monkeypatch)
    [record] = transfers.recover_transfers(source)
    assert record["status"] == "aborted"
    _assert_home(source, [marker])
    assert not (tmp_path / "loose").exists()


def test_a_commit_onto_a_taken_target_aborts(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    target = tmp_path / "loose"

    def take() -> None:
        target.mkdir()
        (target / "someone").write_text("else")

    _hook_at("S6.commit", take)
    with pytest.raises(ValueError, match="was aborted"):
        transfers.eject_job(source, marker.job_id, target)
    _assert_home(source, [marker])
    assert os.listdir(target) == ["someone"]


# ---------------------------------------------------------------------------
# Crashes and aborts at every step (plan 4.10)
# ---------------------------------------------------------------------------

_EJECT_STEPS = (
    "S0.checked",
    "S1.born",
    "birth.built",
    "S2.member",
    "S3.root",
    "S3.fenced",
    "link_new.written",
    "S4.manifest",
    "S4.runner",
    "S4.runners",
    "S4.member",
    "S4.envelope",
    "S5.detach",
    "phase.read",
    "S5.envelope",
    "S6.commit",
    "S7.cleanup",
    "S7.marker",
    "S7.trash",
    "trash.renamed",
)
#: Before S3 the root is not fenced: recovery aborts. From S3 on it continues forward.
_ABORTED_BEFORE = {"S0.checked", "S1.born", "birth.built", "S2.member", "S3.root"}
_NORMAL_STEPS = {"S1.born", "S2.member", "S3.fenced", "S4.member", "S5.envelope", "S6.commit", "S7.marker"}


def _marked(steps: tuple[str, ...], normal: set[str]) -> list[Any]:
    """One parameter per step; only the *normal* ones run outside the extended profile."""

    return [pytest.param(step, marks=() if step in normal else pytest.mark.extended) for step in steps]


def _profiled(steps: tuple[str, ...]) -> list[Any]:
    return _marked(steps, _NORMAL_STEPS)


@pytest.mark.parametrize("step", _profiled(_EJECT_STEPS))
def test_a_crash_at_any_step_recovers_to_exactly_one_copy(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    target = tmp_path / "loose"
    _hook_at(step, _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, parent.job_id, target)
    _owner_gone(monkeypatch)
    transfers.recover_transfers(source)
    _age_and_sweep(source)
    if step in _ABORTED_BEFORE:
        _assert_home(source, [parent, *members])
        assert not target.exists()
        # Nothing was fenced or committed, so a fresh ejection simply works.
        transfers.eject_job(source, parent.job_id, target)
    _assert_ejected(source, destination, [parent, *members], target)


@pytest.mark.parametrize("step", _profiled(_EJECT_STEPS[3:]))
def test_an_aborter_at_any_step_wins_or_finds_the_commit(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    target = tmp_path / "loose"

    def abort() -> None:
        # Another actor decides the abort while the ejector is at this step.
        for name in os.listdir(source.control / "tmp"):
            if name.startswith("eject."):
                _sealing._decide_abort(source, name.removeprefix("eject."))

    _hook_at(step, abort)
    try:
        out: Path | None = transfers.eject_job(source, parent.job_id, target)
    except ValueError:
        out = None
    _sealing.recover(source)  # anything an interrupted abort left, as the owner's own recovery
    if out is None or not target.exists():
        _assert_home(source, [parent, *members])
        assert not target.exists()
    else:
        _assert_ejected(source, destination, [parent, *members], target)


def test_an_abort_decided_after_the_commit_only_cleans_up(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    _hook_at("S7.cleanup", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    [name] = _tmp_entries(source)
    # A spurious abort (say, the orphan sweep missing a marker) after the commit.
    assert _sealing._decide_abort(source, name.removeprefix("eject."))
    _owner_gone(monkeypatch)
    [record] = transfers.recover_transfers(source)
    assert record["status"] == "ejected"
    _assert_ejected(source, destination, [parent, *members], tmp_path / "loose")


def test_two_ejectors_of_one_tree_meet_at_the_first_fence(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")
    other = Workspace(source.root)
    won: list[Path] = []
    _hook_at("S1.born", lambda: won.append(transfers.eject_job(other, parent.job_id, tmp_path / "second")))
    with pytest.raises(ValueError):
        transfers.eject_job(source, parent.job_id, tmp_path / "first")
    assert won == [tmp_path / "second"]
    assert not (tmp_path / "first").exists()
    _assert_ejected(source, destination, [parent, *members], tmp_path / "second")


def test_a_committed_reader_racing_the_abort_rename_never_mistakes_it_for_a_commit(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S5.envelope", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    [transfer_id] = [name.removeprefix("eject.") for name in _tmp_entries(source)]
    # The recoverer reads the phase; between its look at E/payload and the rest, E becomes A.
    _hook_at("phase.read", lambda: _sealing._decide_abort(source, transfer_id))
    _owner_gone(monkeypatch)
    [record] = transfers.recover_transfers(source)
    assert record["status"] == "aborted"
    _assert_home(source, [marker])


def test_a_stale_fence_after_a_completed_abort_is_unfenced(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    parent, members = _tree(source, tmp_path / "tree")

    def recover_everything() -> None:
        # The slow owner is taken for dead: its members are unfenced and E is gone.
        with monkeypatch.context() as patch:
            patch.setattr(_sealing, "_owner_gone", lambda *args, **kwargs: True)
            _sealing.recover(source)

    _hook_at("S3.root", recover_everything)
    with pytest.raises(ValueError):
        transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    _assert_home(source, [parent, *members])


def test_a_cross_host_recoverer_finishes_the_same_way(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S5.envelope", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    real_lstat = os.lstat

    def other_host(path: Any, *args: Any, **kwargs: Any) -> os.stat_result:
        result = real_lstat(path, *args, **kwargs)
        fields = list(result[:10])
        fields[2] += 7  # st_dev differs on another NFS client
        return os.stat_result(fields)

    monkeypatch.setattr(os, "lstat", other_host)
    _owner_gone(monkeypatch)
    [record] = transfers.recover_transfers(source)
    monkeypatch.undo()
    assert record["status"] == "ejected"
    _assert_ejected(source, destination, [marker], tmp_path / "loose")


def test_an_inode_mismatch_stops_and_moves_nothing(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S5.envelope", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    [name] = _tmp_entries(source)
    payload = source.control / "tmp" / name / "payload"
    shutil.copytree(payload, payload.with_name("copy"))
    shutil.rmtree(payload)
    payload.with_name("copy").rename(payload)
    _owner_gone(monkeypatch)
    [record] = transfers.recover_transfers(source)
    assert record["status"] == "failed"
    assert payload.is_dir() and not (tmp_path / "loose").exists()
    assert [item.job_id for item in _transferring(source)] == [marker.job_id]


def test_recovery_leaves_a_live_owner_alone(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S3.fenced", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    # This very process is the recorded owner, alive on this host and boot.
    assert [record["status"] for record in transfers.recover_transfers(source)] == ["pending"]


def _crashed_ejections(source: Workspace, tmp_path: Path, count: int) -> list[Marker]:
    """Eject *count* jobs, each crashing after its fences (``E`` present, this live process its owner)."""

    markers = []
    for index in range(count):
        marker = source.submit(_payload(tmp_path / "payloads", f"j{index}"), "jobs")
        _hook_at("S3.fenced", _crash)
        with pytest.raises(Crash):
            transfers.eject_job(source, marker.job_id, tmp_path / f"loose-{index}")
        markers.append(marker)
    return markers


def test_one_recovery_pass_lists_the_transferring_markers_once(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    _crashed_ejections(source, tmp_path, 8)
    scans: list[tuple[str, ...]] = []
    reads: list[str] = []
    scan_markers, read_state = Workspace.scan_markers, Workspace.read_state

    def counted_scan(self: Workspace, kinds: Any = None) -> Any:
        scans.append(tuple(kinds or ()))
        return scan_markers(self, kinds)

    def counted_read(self: Workspace, marker: Marker) -> dict[str, Any]:
        reads.append(marker.job_id)
        return read_state(self, marker)

    monkeypatch.setattr(Workspace, "scan_markers", counted_scan)
    monkeypatch.setattr(Workspace, "read_state", counted_read)
    assert [record["status"] for record in _sealing.recover(source)] == ["pending"] * 8
    assert scans == [("transferring",)] and len(reads) == 8


def test_stale_fences_are_undone_in_the_pass_that_leaves_a_live_owner_alone(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, _destination = workspaces
    live, stale = _crashed_ejections(source, tmp_path, 2)
    transfer_of = {
        marker.job_id: transfer_id
        for transfer_id, fenced in _sealing.fenced_transactions(source).items()
        for marker, _frame in fenced.all()
    }
    # The crashed CLI's transaction directory is gone: neither E nor A names its fences.
    shutil.rmtree(_sealing.eject_directory(source, transfer_of[stale.job_id]))
    statuses = {str(record["transfer_id"]): record["status"] for record in _sealing.recover(source)}
    assert statuses == {transfer_of[live.job_id]: "pending", transfer_of[stale.job_id]: "unfenced"}
    home = source.find_marker_by_id(stale.job_id)
    assert home is not None and home.kind == stale.kind and _home(source, stale).is_dir()
    assert [marker.job_id for marker in _transferring(source)] == [live.job_id]


def test_the_orphan_sweep_aborts_old_unfenced_transaction_directories(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    _hook_at("S1.born", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    [name] = _tmp_entries(source)
    (source.control / "tmp" / "birth.leftover").mkdir()
    _sealing.recover(source)
    assert sorted(_tmp_entries(source)) == ["birth.leftover", name]  # too young
    old = time.time() - _sealing.ORPHAN_SECONDS - 60
    for entry in ("birth.leftover", name):
        os.utime(source.control / "tmp" / entry, (old, old))
    _sealing.recover(source)
    assert _tmp_entries(source) == [name.replace("eject.", "abort.")]
    _sealing.recover(source)
    _assert_home(source, [marker])


def _runner_job(workspace: Workspace, root: Path, shape: str) -> Marker:
    """One job pinning a workspace runner below a store directory: a file, or a directory with ``run``."""

    if shape == "file":
        (root / "store").mkdir(parents=True)
        (root / "store" / "run.py").write_text("#!/bin/sh\nexit 0\n")
        reference = workspace.publish_runner(root / "store" / "run.py", name="tools/nested/run.py")
    else:
        (root / "store" / "runner" / "lib").mkdir(parents=True)
        (root / "store" / "runner" / "run").write_text("#!/bin/sh\nexit 0\n")
        (root / "store" / "runner" / "run").chmod(0o755)
        (root / "store" / "runner" / "lib" / "data.txt").write_text("data\n")
        reference = workspace.publish_runner(root / "store" / "runner", name="tools/runner")
    return _finish(workspace, workspace.submit(_payload(root / "p", "job", runner=dict(reference)), "jobs"))


@pytest.mark.parametrize("shape", ["file", "directory", "tree"])
def test_an_aborter_between_the_manifest_and_the_runner_embed_never_resurrects_e(
    shape: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    """Review blocker: no step after the abort rename may re-create ``tmp/eject.<T>`` (I2)."""

    source, _destination = workspaces
    if shape == "tree":
        parent, members = _tree(source, tmp_path / "tree")
        jobs = [parent, *members]
    else:
        parent = _runner_job(source, tmp_path / "job", shape)
        jobs = [parent]
    reappeared: list[str] = []
    seen: list[str] = []

    def watch(name: str) -> None:
        if any(entry.startswith("eject.") for entry in os.listdir(source.control / "tmp")):
            reappeared.append(name)

    def abort() -> None:
        # The owner stalls after its manifest link; another actor aborts the
        # transaction completely: E -> A, every job home and unfenced, A trashed.
        [name] = [entry for entry in _tmp_entries(source) if entry.startswith("eject.")]
        transfer_id = name.removeprefix("eject.")
        assert _sealing._decide_abort(source, transfer_id)
        assert _sealing.settle(Workspace(source.root), transfer_id, force=True) == "aborted"
        seen.append(transfer_id)
        _txn._HOOK = watch

    _hook_at("S4.runner", abort)
    with pytest.raises(ValueError):
        transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    assert seen and reappeared == []
    assert not (tmp_path / "loose").exists()
    _assert_home(source, jobs)
    # Recovery has nothing left to do, and E never comes back.
    assert [record["status"] for record in _sealing.recover(source)] == []
    assert _tmp_entries(source) == []


@pytest.mark.parametrize("shape", ["file", "directory"])
def test_a_workspace_runner_travels_inside_the_envelope(
    shape: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = _runner_job(source, tmp_path / "job", shape)
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    verified = _assert_ejected(source, destination, [marker], loose)
    [runner] = verified.manifest.runners
    assert runner.path.parts[0] == "tools"
    assert transfers.adopt_job(destination, loose).job_id == marker.job_id
    assert destination.runner_store_path(runner.path).exists()


# ---------------------------------------------------------------------------
# The abort steps: crashes and a concurrent second aborter
# ---------------------------------------------------------------------------

_ABORT_STEPS = ("abort.decide", "abort.envelope", "abort.member", "abort.root", "abort.unfence", "abort.trash")
_NORMAL_ABORT_STEPS = {"abort.member", "abort.unfence"}


def _abort_profiled(steps: tuple[str, ...]) -> list[Any]:
    return _marked(steps, _NORMAL_ABORT_STEPS)


def _committed_privately(source: Workspace, tmp_path: Path) -> tuple[str, list[Marker]]:
    """A tree whose ejector died with everything inside ``E/payload`` (just before S6), then decided aborted."""

    parent, members = _tree(source, tmp_path / "tree")
    _hook_at("S6.commit", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(source, parent.job_id, tmp_path / "loose")
    [name] = _tmp_entries(source)
    return name.removeprefix("eject."), [parent, *members]


@pytest.mark.parametrize("step", _abort_profiled(_ABORT_STEPS))
def test_a_crash_at_any_abort_step_is_finished_by_recovery(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    transfer_id, jobs = _committed_privately(source, tmp_path)
    if step != "abort.decide":
        assert _sealing._decide_abort(source, transfer_id)
    _owner_gone(monkeypatch)
    occurrence = 2 if step in {"abort.member", "abort.unfence"} else 1
    _hook_at(step, _crash, occurrence=occurrence)
    with pytest.raises(Crash):
        if step == "abort.decide":
            _sealing._decide_abort(source, transfer_id)
        else:
            _sealing.recover(source)
    if step == "abort.decide":
        # Crashed before deciding: the transaction is still forward and finishes there.
        transfers.recover_transfers(source)
        _assert_ejected(source, _destination, jobs, tmp_path / "loose")
        return
    # After the unfence (abort.trash) nothing is fenced any more: the sweep discards A.
    records = transfers.recover_transfers(source)
    assert [record["status"] for record in records] == ([] if step == "abort.trash" else ["aborted"]), records
    _assert_home(source, jobs)
    assert not (tmp_path / "loose").exists()


@pytest.mark.parametrize("step", _abort_profiled(_ABORT_STEPS[1:]))
def test_two_aborters_running_the_abort_steps_at_once_agree(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, _destination = workspaces
    transfer_id, jobs = _committed_privately(source, tmp_path)
    assert _sealing._decide_abort(source, transfer_id)
    _owner_gone(monkeypatch)
    inner: list[str] = []
    # A second actor runs the whole abort while the first is at this step.
    _hook_at(step, lambda: inner.append(_sealing.settle(Workspace(source.root), transfer_id, force=True)))
    outer = _sealing.settle(source, transfer_id, force=True)
    # At abort.trash the first aborter has unfenced everything: the second only discards A.
    assert inner == (["none"] if step == "abort.trash" else ["aborted"]) and outer in {"aborted", "none"}
    _sealing.recover(source)
    _assert_home(source, jobs)


# ---------------------------------------------------------------------------
# The orphan sweep versus committed transfers
# ---------------------------------------------------------------------------


def _missed_listing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every marker listing misses every transferring marker (a slow NFS listing)."""

    monkeypatch.setattr(_sealing, "fenced_transactions", lambda _workspace: {})


@pytest.mark.parametrize("commit", ["outgoing", "export"])
def test_the_orphan_sweep_never_aborts_a_committed_transfer_whose_marker_it_missed(
    commit: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    if commit == "outgoing":
        transfer_id = transfers.detach_job(
            source, marker.job_id, destination_workspace_id=destination.workspace_id
        ).name
    else:
        _hook_at("S7.cleanup", _crash)
        with pytest.raises(Crash):
            _export(source, marker, tmp_path / "far" / "job")
        [name] = _tmp_entries(source)
        transfer_id = name.removeprefix("eject.")
    eject = _sealing.eject_directory(source, transfer_id)
    old = time.time() - _sealing.ORPHAN_SECONDS - 60
    os.utime(eject, (old, old))
    with monkeypatch.context() as patch:
        _missed_listing(patch)
        _sealing.recover(source)
    assert eject.is_dir() and not _sealing.abort_directory(source, transfer_id).exists()
    if commit == "outgoing":
        # Still offered, still retirable.
        assert _transferring(source)[0].job_id == marker.job_id
        bundle = _sealing.outgoing_path(source, transfer_id)
        retired = transfers.acknowledge_transfer(source, _ack(source, destination, bundle))
        assert retired.is_dir() and not _transferring(source) and _tmp_entries(source) == []
    else:
        _owner_gone(monkeypatch)
        _sealing.recover(source)
        assert source.find_marker_by_id(marker.job_id, kinds=("transferring", *_KINDS)) is None
        assert _tmp_entries(source) == []
        assert (_sealing.exports_directory(source, transfer_id) / marker.job_key).is_dir()


def test_a_spurious_abort_of_a_committed_addressed_transfer_is_never_reclaimed_automatically(
    workspaces: tuple[Workspace, Workspace],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An abort decided after the commit leaves the bundle in doubt: offered, retirable, reclaimed only by an operator.

    The end of the freshness window forbids new acceptance; it does not prove that
    the destination never accepted it (design 13.1).
    """

    source, destination = workspaces
    marker = _finish(source, source.submit(_payload(tmp_path / "payloads"), "jobs"))
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    transfer_id = bundle.name
    assert _sealing._decide_abort(source, transfer_id)
    _owner_gone(monkeypatch)
    with caplog.at_level("DEBUG", logger="httk.workflow._sealing"):
        for _ in range(3):
            assert [record["status"] for record in _sealing.recover(source)] == ["committed"]
    deferred = [record for record in caplog.records if getattr(record, "event", None) == "transfer_abort_in_doubt"]
    # Reported once at warning level, then only at debug level.
    assert [record.levelname for record in deferred] == ["WARNING", "DEBUG", "DEBUG"]
    assert bundle.is_dir() and _transferring(source)[0].job_id == marker.job_id
    # Still deliverable: asking for the transfer again hands out the same bundle.
    assert transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id) == bundle
    [offer] = transfers.offer_transfers(source, destination_workspace_id=destination.workspace_id)
    assert offer["transfer_id"] == transfer_id
    later = time.time_ns() + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    # Past the window recovery still leaves it alone: only the operator decides.
    assert [record["status"] for record in _sealing.recover(source, now=later)] == ["committed"]
    assert bundle.is_dir() and _transferring(source)[0].job_id == marker.job_id
    monkeypatch.setattr(_sealing.time, "time_ns", lambda: later)
    assert transfers.reclaim_transfer(source, marker.job_id)["status"] == "reclaimed"
    _assert_home(source, [marker])
    assert not bundle.exists()


@pytest.mark.parametrize("ending", ["reclaim", "acknowledgement"])
def test_a_deferred_abort_whose_directory_a_missed_listing_discarded_stays_committed(
    ending: str,
    workspaces: tuple[Workspace, Workspace],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    transfer_id = bundle.name
    assert _sealing._decide_abort(source, transfer_id)
    abort = _sealing.abort_directory(source, transfer_id)
    with monkeypatch.context() as patch:
        _missed_listing(patch)
        _sealing.recover(source)
    assert abort.is_dir()  # the fixed-name guard keeps A while outgoing/<T> holds the bundle
    # Neither E nor A (as an older sweep, or an operator, could leave it): committed, quietly.
    _txn.remove_tree(abort)
    _owner_gone(monkeypatch)
    with caplog.at_level("WARNING"):
        assert [record["status"] for record in _sealing.recover(source)] == ["committed"]
    assert not [record for record in caplog.records if getattr(record, "event", None) == "transfer_orphan_marker"]
    assert transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id) == bundle
    if ending == "acknowledgement":
        retired = transfers.acknowledge_transfer(source, _ack(source, destination, bundle))
        assert retired.is_dir() and not _transferring(source) and _tmp_entries(source) == []
        return
    with pytest.raises(ValueError, match="may still be delivered"):
        transfers.reclaim_transfer(source, marker.job_id)
    later = time.time_ns() + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    monkeypatch.setattr(_sealing.time, "time_ns", lambda: later)
    assert transfers.reclaim_transfer(source, marker.job_id)["status"] == "reclaimed"
    _assert_home(source, [marker])
    assert not bundle.exists()


def test_an_acknowledgement_after_a_spurious_abort_still_retires(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    assert _sealing._decide_abort(source, bundle.name)
    _owner_gone(monkeypatch)
    _sealing.recover(source)
    retired = transfers.acknowledge_transfer(source, _ack(source, destination, bundle))
    assert retired.is_dir() and not _transferring(source)
    _sealing.recover(source)
    assert source.find_marker_by_id(marker.job_id, kinds=("transferring", *_KINDS)) is None
    assert _tmp_entries(source) == []


# ---------------------------------------------------------------------------
# Addressed transfers
# ---------------------------------------------------------------------------


def _ack(source: Workspace, destination: Workspace, bundle: Path) -> dict[str, object]:
    manifest = json.loads((bundle / TRANSFER_DIRECTORY / "manifest.json").read_text())
    return {
        "format": "httk-workflow-transfer-acknowledgement",
        "format_version": 3,
        "transfer_id": manifest["transfer_id"],
        "source_workspace_id": source.workspace_id,
        "destination_workspace_id": destination.workspace_id,
        "payload_sha256": manifest["payload_sha256"],
        "job_id": manifest["job_id"],
        "job_key": manifest["job_key"],
    }


def test_an_addressed_transfer_waits_for_its_acknowledgement(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    transfer_id = bundle.name
    assert bundle == _sealing.outgoing_path(source, transfer_id)
    verify_bundle(bundle, workspace=destination, source="incoming")
    [fenced] = _transferring(source)
    assert fenced.job_id == marker.job_id
    # Asking again resumes the same transfer rather than sealing another.
    assert transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id) == bundle
    with pytest.raises(ValueError, match="different transfer"):
        transfers.detach_job(source, marker.job_id, destination_workspace_id=str(uuid.uuid4()))
    acknowledgement = _ack(source, destination, bundle)
    with pytest.raises(FormatError, match="disagrees on payload_sha256"):
        transfers.acknowledge_transfer(source, {**acknowledgement, "payload_sha256": "0" * 64})
    retired = transfers.acknowledge_transfer(source, acknowledgement)
    assert retired == _sealing.retired_path(source, transfer_id) and retired.is_dir()
    assert not bundle.exists() and not _transferring(source) and _tmp_entries(source) == []
    # A repeated acknowledgement is harmless.
    assert transfers.acknowledge_transfer(source, acknowledgement) == retired


def test_offers_are_built_from_transferring_markers(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, destination = workspaces
    marker = _finish(source, source.submit(_payload(tmp_path / "payloads"), "jobs"))
    [offer] = transfers.offer_transfers(source, destination_workspace_id=destination.workspace_id)
    assert offer["job_id"] == marker.job_id and offer["state"] == "succeeded"
    assert Path(str(offer["bundle_path"])) == _sealing.outgoing_path(source, str(offer["transfer_id"]))
    # Offering again reports the same sealed bundle.
    assert transfers.offer_transfers(source, destination_workspace_id=destination.workspace_id) == [offer]
    [retired] = transfers.retire_transfers(source, [marker.job_id], destination_workspace_id=destination.workspace_id)
    assert retired["status"] == "retired" and not _transferring(source)
    assert transfers.retire_transfers(source, [marker.job_id]) == []


def test_a_receipt_is_kept_for_a_job_that_arrived_by_an_addressed_transfer(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    arrived = str(uuid.uuid4())
    provenance = {
        "transfer_id": arrived,
        "source_workspace_id": str(uuid.uuid4()),
        "destination_workspace_id": source.workspace_id,
        "payload_sha256": "1" * 64,
        "sealed_at": time.time_ns(),
    }
    with source.open_journal_writer() as writer:
        marker = source.transition(writer, marker, "submitted", {"transfer": provenance})
    transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    receipt = json.loads(receipt_path(source, arrived).read_text())
    assert receipt == {
        "sealed_at": provenance["sealed_at"],
        "source_workspace_id": provenance["source_workspace_id"],
        "job_id": marker.job_id,
        "payload_sha256": "1" * 64,
    }


def test_reclaim_takes_an_undelivered_bundle_back_only_after_the_window(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    context = CLIContext("httk", source.root)
    assert command(["transfer", "reclaim", marker.job_id], context) == 2
    later = time.time_ns() + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    monkeypatch.setattr(_sealing.time, "time_ns", lambda: later)
    assert command(["transfer", "reclaim", marker.job_id], context) == 0
    _assert_home(source, [marker])
    assert os.listdir(source.control / "transfers" / "outgoing") == []


def test_the_operator_retires_a_delivered_transfer_by_job_id(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    assert command(["transfer", "retire", marker.job_id], CLIContext("httk", source.root)) == 0
    assert _sealing.retired_path(source, bundle.name).is_dir() and not _transferring(source)


def test_the_protocol_retire_spelling_still_names_the_workspace_first(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    argv = ["transfer", "retire", str(source.root), marker.job_id, "--json"]
    argv += ["--acknowledgements-json", json.dumps([_ack(source, destination, bundle)])]
    argv += ["--destination-workspace-id", destination.workspace_id]
    assert command(argv, CLIContext("httk", tmp_path)) == 0
    assert _sealing.retired_path(source, bundle.name).is_dir()


@pytest.mark.parametrize("winner", ["reclaim", "acknowledgement"])
def test_an_acknowledgement_racing_a_reclaim_never_loses_the_job(
    winner: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    acknowledgement = _ack(source, destination, bundle)
    later = time.time_ns() + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    monkeypatch.setattr(_sealing.time, "time_ns", lambda: later)
    # The reclaim decides its abort first; the acknowledgement arrives before (it
    # wins: the bundle is delivered) or after (it is ignored) the bundle moved back.
    step = "abort.reclaim" if winner == "acknowledgement" else "abort.envelope"
    in_doubt: list[ValueError] = []

    def acknowledge() -> None:
        try:
            transfers.acknowledge_transfer(source, acknowledgement)
        except ValueError as exc:
            in_doubt.append(exc)

    _hook_at(step, acknowledge)
    if winner == "reclaim":
        assert transfers.reclaim_transfer(source, marker.job_id)["status"] == "reclaimed"
    else:
        with pytest.raises(ValueError, match="stays retired"):
            transfers.reclaim_transfer(source, marker.job_id)
    if winner == "reclaim":
        # Too late: reported as in doubt, never as a retirement.
        [error] = in_doubt
        assert "reclaimed" in str(error)
        _assert_home(source, [marker])
        assert not _sealing.retired_path(source, bundle.name).exists()
    else:
        assert in_doubt == []
        assert _sealing.retired_path(source, bundle.name).is_dir()
        assert source.find_marker_by_id(marker.job_id, kinds=("transferring", *_KINDS)) is None
        assert not _transferring(source) and _tmp_entries(source) == []


@pytest.mark.parametrize(
    "step",
    _marked(
        ("abort.decide", "reclaim.authorize", "abort.reclaim", "abort.root", "abort.unfence", "abort.trash"),
        {"reclaim.authorize", "abort.reclaim"},
    ),
)
def test_a_crash_while_reclaiming_is_finished_by_recovery(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    later = time.time_ns() + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    monkeypatch.setattr(_sealing.time, "time_ns", lambda: later)
    _hook_at(step, _crash)
    with pytest.raises(Crash):
        transfers.reclaim_transfer(source, marker.job_id)
    _owner_gone(monkeypatch)
    transfers.recover_transfers(source)
    if step in {"abort.decide", "reclaim.authorize"}:
        # No authorization was recorded: recovery never takes the bundle back by itself.
        assert bundle.is_dir() and _transferring(source)
        assert transfers.reclaim_transfer(source, marker.job_id)["status"] == "reclaimed"
    _assert_home(source, [marker])
    assert not bundle.exists() and not _sealing.retired_path(source, bundle.name).exists()


@pytest.mark.parametrize("probe", [1, 2, 3])
def test_an_abort_observer_never_misses_a_payload_the_reclaim_is_moving(
    probe: int, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The phase reader probes ``outgoing/<T>``, ``A/payload``, home: the order a reclaimed payload moves in."""

    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    transfer_id = bundle.name
    abort = _sealing.abort_directory(source, transfer_id)
    # An operator's reclaim decided and authorized the abort; its first move is still to come.
    assert _sealing._decide_abort(source, transfer_id)
    assert _txn.link_new(abort, _sealing.RECLAIM, b"{}\n")
    _owner_gone(monkeypatch)
    # The reclaimer moves outgoing/<T> into A/payload just before the observer's probe number *probe*.
    _hook_at("phase.probe", lambda: os.rename(bundle, abort / "payload"), occurrence=probe)
    status = _sealing.settle(source, transfer_id, force=True)
    _txn._HOOK = None
    assert status == "aborted", status
    _assert_home(source, [marker])
    quarantine = source.control / "quarantine"
    assert not quarantine.is_dir() or not list(quarantine.iterdir())


@pytest.mark.parametrize("step", ["X4.verified", "X5.witnessed"])
def test_a_stale_copy_out_owner_after_a_takeover_publishes_at_most_once(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    transfer_id = held.parent.name
    taken: list[list[Path]] = []

    def take_over() -> None:
        # The owner is taken for dead while it is between these steps.
        with monkeypatch.context() as patch:
            patch.setattr(_txn, "owner_gone", lambda *args, **kwargs: True)
            other = _txn.owner_token(str(uuid.uuid4()))
            taken.append(_sealing.resume_copy_outs(Workspace(source.root), owner=other))

    _hook_at(step, take_over)
    with pytest.raises(ValueError, match="taken over"):
        _sealing.copy_out(source, transfer_id)
    assert taken
    if step == "X4.verified":
        # No witness yet: the new owner copied it out, and the stale owner stopped at its witness.
        assert taken == [[tmp_path / "far" / "job"]]
        _assert_ejected(source, destination, [marker], tmp_path / "far" / "job")
        assert _sealing.exports_in_doubt(source) == []
    else:
        # The stale owner had witnessed: the new owner never publishes, and the stale
        # owner's copy was removed before it could be published: in doubt, held.
        assert taken == [[]]
        assert not (tmp_path / "far" / "job").exists()
        [entry] = _sealing.exports_in_doubt(source)
        doubtful = _sealing.in_doubt_directory(source, transfer_id) / marker.job_key
        assert entry["held"] == str(doubtful) and doubtful.is_dir() and not held.parent.exists()
    assert len(os.listdir(tmp_path / "far")) <= 1


def test_a_pending_copy_out_claimant_never_republishes_an_export_held_in_doubt(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-review B1: recovery holds the ambiguous bundle where no copy-out claims, while a claimant waits at X1."""

    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    transfer_id = held.parent.name
    first = _txn.owner_token(str(uuid.uuid4()))
    _hook_at("X5.published", _crash)
    with pytest.raises(Crash):
        _sealing.copy_out(source, transfer_id, owner=first)
    os.rename(tmp_path / "far" / "job", tmp_path / "consumed")
    # Only the crashed owner is gone; the claimant below is alive.
    monkeypatch.setattr(_txn, "owner_gone", lambda _control, token, **_kwargs: token == first)
    recovered: list[list[Path]] = []
    _hook_at(
        "X1.staged",
        lambda: recovered.append(
            _sealing.resume_copy_outs(Workspace(source.root), owner=_txn.owner_token(str(uuid.uuid4())))
        ),
    )
    with pytest.raises(ValueError, match="taken by another actor"):
        _sealing.copy_out(source, transfer_id)
    assert recovered == [[]]
    assert os.listdir(tmp_path / "far") == []
    [entry] = _sealing.exports_in_doubt(source)
    assert Path(entry["held"]).is_dir() and Path(entry["held"]).parent == _sealing.in_doubt_directory(
        source, transfer_id
    )
    assert _tmp_entries(source) == []


def test_a_second_takeover_still_discards_the_first_owners_witnessed_temporary(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-review B2: the witness carries the original temporary across every takeover generation."""

    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    transfer_id = held.parent.name
    monkeypatch.setattr(_txn, "owner_gone", lambda *args, **kwargs: True)
    seen: list[list[str]] = []

    def takeovers() -> None:
        # The owner O has witnessed and is paused before publishing. R1 takes its
        # staging over and dies before discarding O's temporary; R2 takes R1's over.
        seen.append(sorted(os.listdir(tmp_path / "far")))
        _hook_at("copyout.cleanup", _crash)
        with pytest.raises(Crash):
            _sealing.resume_copy_outs(Workspace(source.root), owner=_txn.owner_token(str(uuid.uuid4())))
        _txn._HOOK = None
        seen.append(sorted(os.listdir(tmp_path / "far")))
        assert _sealing.resume_copy_outs(Workspace(source.root), owner=_txn.owner_token(str(uuid.uuid4()))) == []

    _hook_at("X5.witnessed", takeovers)
    with pytest.raises(ValueError, match="taken over"):
        _sealing.copy_out(source, transfer_id)
    # O's temporary existed until R2 discarded it; O could not publish it afterwards.
    assert len(seen[0]) == 1 and seen[0][0].startswith(".httk-export.") and seen[1] == seen[0]
    assert os.listdir(tmp_path / "far") == []
    [entry] = _sealing.exports_in_doubt(source)
    assert Path(entry["held"]).is_dir()


def test_a_copy_out_published_before_a_crash_and_then_consumed_is_never_republished(
    workspaces: tuple[Workspace, Workspace],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    transfer_id = held.parent.name
    _hook_at("X5.published", _crash)
    with pytest.raises(Crash):
        _sealing.copy_out(source, transfer_id)
    # The client takes the published job away before anything resumes.
    os.rename(tmp_path / "far" / "job", tmp_path / "consumed")
    monkeypatch.setattr(_txn, "owner_gone", lambda *args, **kwargs: True)
    for _ in range(2):
        assert _sealing.resume_copy_outs(source) == []
        assert os.listdir(tmp_path / "far") == []
    [entry] = _sealing.exports_in_doubt(source)
    doubtful = _sealing.in_doubt_directory(source, transfer_id) / marker.job_key
    assert entry["held"] == str(doubtful) and doubtful.is_dir() and entry["transfer_id"] == transfer_id
    # Held apart from ordinary exports: no copy-out can claim it again.
    assert not held.parent.exists()
    with pytest.raises(FileNotFoundError):
        _sealing.copy_out(source, transfer_id)
    context = CLIContext("httk", source.root)
    capsys.readouterr()
    assert command(["job", "eject", "--resume"], context) == 1
    assert f"in doubt\t{doubtful}" in capsys.readouterr().out
    assert command(["transfer", "status", "--json"], context) == 1
    assert json.loads(capsys.readouterr().out)["details"]["exports_in_doubt"] == [entry]
    assert os.listdir(tmp_path / "far") == []
    # The operator takes it back explicitly; the record then goes.
    adopted = transfers.adopt_job(source, doubtful)
    assert adopted.job_id == marker.job_id
    _sealing.resume_copy_outs(source)
    assert _sealing.exports_in_doubt(source) == [] and not doubtful.parent.exists()
    assert command(["transfer", "status"], context) == 0


def test_an_acknowledgement_of_a_reclaimed_transfer_is_reported_in_doubt(
    workspaces: tuple[Workspace, Workspace],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    delivered = transfers.detach_job(source, other.job_id, destination_workspace_id=destination.workspace_id)
    acknowledgement = _ack(source, destination, bundle)
    later = time.time_ns() + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    with monkeypatch.context() as patch:
        patch.setattr(_sealing.time, "time_ns", lambda: later)
        transfers.reclaim_transfer(source, marker.job_id)
    back = source.find_marker_by_id(marker.job_id)
    assert back is not None and back.kind == "submitted" and _home(source, back).is_dir()
    with caplog.at_level("WARNING"), pytest.raises(ValueError, match="reclaimed"):
        transfers.acknowledge_transfer(source, acknowledgement)
    assert any(getattr(record, "event", None) == "transfer_ack_in_doubt" for record in caplog.records)
    assert not _sealing.retired_path(source, bundle.name).exists()
    # In a sweep, every other acknowledgement is still retired before the doubt is raised.
    with pytest.raises(ValueError, match="reclaimed"):
        transfers.acknowledge_transfers(source, [acknowledgement, _ack(source, destination, delivered)])
    assert _sealing.retired_path(source, delivered.name).is_dir()
    assert source.find_marker_by_id(other.job_id, kinds=("transferring", *_KINDS)) is None
    # A repeat for a job long gone from here is quietly accepted.
    assert transfers.acknowledge_transfer(
        source, _ack(source, destination, _sealing.retired_path(source, delivered.name))
    )


@pytest.mark.parametrize("step", _marked(("ack.retire", "ack.trash", "trash.renamed"), {"ack.trash"}))
@pytest.mark.parametrize("interruption", ["crash", "second"])
def test_an_interrupted_retirement_is_finished_by_a_repeat(
    step: str,
    interruption: str,
    workspaces: tuple[Workspace, Workspace],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    acknowledgement = _ack(source, destination, bundle)
    if interruption == "crash":
        _hook_at(step, _crash)
        with pytest.raises(Crash):
            transfers.acknowledge_transfer(source, acknowledgement)
        _owner_gone(monkeypatch)
        transfers.recover_transfers(source)
    else:
        _hook_at(step, lambda: transfers.acknowledge_transfer(Workspace(source.root), acknowledgement))
    retired = transfers.acknowledge_transfer(source, acknowledgement)
    assert retired == _sealing.retired_path(source, bundle.name) and retired.is_dir()
    assert source.find_marker_by_id(marker.job_id, kinds=("transferring", *_KINDS)) is None
    assert not bundle.exists() and _tmp_entries(source) == []


# ---------------------------------------------------------------------------
# Export and copy-out (4.6)
# ---------------------------------------------------------------------------


def _export(source: Workspace, marker: Marker, destination: Path) -> Path:
    """Seal an export as `job eject` does for a target on another filesystem."""

    return _sealing.seal(source, marker, _sealing.Commit("export", path=destination), reason="ejected")


def test_an_export_is_held_then_copied_out(workspaces: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    held = _export(source, marker, tmp_path / "far" / "job")
    assert held.parent.parent == source.control / "transfers" / "exports"
    assert not _transferring(source)
    (tmp_path / "far").mkdir()
    out = _sealing.copy_out(source, held.parent.name)
    _assert_ejected(source, destination, [marker], out)
    assert not held.parent.exists()


def test_a_crashed_copy_out_is_resumed(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    _hook_at("X3.copied", _crash)
    with pytest.raises(Crash):
        _sealing.copy_out(source, held.parent.name)
    monkeypatch.setattr(_txn, "owner_gone", lambda *args, **kwargs: True)
    assert command(["job", "eject", "--resume"], CLIContext("httk", source.root)) == 0
    _assert_ejected(source, destination, [marker], tmp_path / "far" / "job")
    assert [name for name in os.listdir(tmp_path / "far")] == ["job"]
    assert not (source.control / "transfers" / "exports" / held.parent.name).exists()


def test_a_copy_out_racing_an_adoption_of_the_held_export_loses_cleanly(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    adopted = tmp_path / "adopted"
    # The adoption chain's claim is one rename of the held bundle (stubbed until it exists).
    _hook_at("X1.staged", lambda: os.rename(held, adopted))
    with pytest.raises(ValueError, match="taken by another actor"):
        _sealing.copy_out(source, held.parent.name)
    assert adopted.is_dir() and os.listdir(tmp_path / "far") == []
    assert _tmp_entries(source) == []


@pytest.mark.parametrize(
    "step",
    _marked(
        ("X1.staged", "X2.claimed", "X3.copied", "X4.verified", "X5.published", "trash.renamed"),
        {"X2.claimed", "X5.published"},
    ),
)
def test_a_crash_at_any_copy_out_step_is_resumed_exactly_once(
    step: str, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    held = _export(source, marker, tmp_path / "far" / "job")
    _hook_at(step, _crash)
    with pytest.raises(Crash):
        _sealing.copy_out(source, held.parent.name)
    monkeypatch.setattr(_txn, "owner_gone", lambda *args, **kwargs: True)
    _sealing.resume_copy_outs(source)
    _assert_ejected(source, destination, [marker], tmp_path / "far" / "job")
    assert os.listdir(tmp_path / "far") == ["job"]
    assert not held.parent.exists()
    assert _sealing.resume_copy_outs(source) == []


@pytest.mark.parametrize("interrupted", [False, True])
def test_a_long_destination_name_never_overflows_a_generated_name(
    interrupted: bool, workspaces: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Temporaries and cleanup names are siblings named independently of the (client-chosen) destination name."""

    source, destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    (tmp_path / "far").mkdir()
    name = "j" * 240  # a name-derived temporary or cleanup name would exceed NAME_MAX (255)
    held = _export(source, marker, tmp_path / "far" / name)
    if not interrupted:
        _assert_ejected(source, destination, [marker], _sealing.copy_out(source, held.parent.name))
        return
    _hook_at("X5.witnessed", _crash)
    with pytest.raises(Crash):
        _sealing.copy_out(source, held.parent.name)
    [temporary] = os.listdir(tmp_path / "far")
    assert len(temporary) < 100
    monkeypatch.setattr(_txn, "owner_gone", lambda *args, **kwargs: True)
    assert _sealing.resume_copy_outs(source) == []
    # The witnessed temporary was discarded and the bundle is held in doubt.
    assert os.listdir(tmp_path / "far") == []
    [entry] = _sealing.exports_in_doubt(source)
    assert Path(entry["held"]).name == marker.job_key and Path(entry["held"]).is_dir()


def test_a_job_key_named_like_its_tree_is_never_confused(
    workspaces: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    """A placement component is never a job key, so the abort walk tells payloads from placements."""

    source, _destination = workspaces
    marker = source.submit(_payload(tmp_path / "payloads", "tree"), "tree/tree")
    _hook_at("S6.commit", lambda: _sealing._decide_abort(source, _tmp_entries(source)[0].removeprefix("eject.")))
    with pytest.raises(ValueError):
        transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    _assert_home(source, [marker])
    assert make_job_key(marker.job_id, "tree") == marker.job_key
