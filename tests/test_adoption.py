"""The lock-free adoption chain (plan 4.5, 4.8): crashes, takeovers, copies, presence, freshness, refusals."""

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
from httk.workflow import Workspace, _adoption, _bundle, _sealing, _txn, transfers
from httk.workflow._bundle import TRANSFER_DIRECTORY
from httk.workflow._receipts import CLOCK_SKEW_NS, FRESHNESS_WINDOW_NS, receipt_path
from httk.workflow.errors import FormatError
from httk.workflow.models import STATE_KINDS, Marker
from httk.workflow.workflow_cli import command
from test_eject_adopt import _payload
from test_sealing import _tree

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class Crash(Exception):
    """A simulated process death at one protocol step."""


@pytest.fixture
def pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    configure_identity()
    source = Workspace.initialize(tmp_path / "source")
    destination = Workspace.initialize(tmp_path / "destination")
    destination.set_policy({"visibility_deadline_seconds": 0.5})
    return source, Workspace(destination.root)


@pytest.fixture(autouse=True)
def _no_hook() -> Iterator[None]:
    yield
    _txn._HOOK = None


def _hook_at(step: str, action: Callable[[], object], occurrence: int = 1) -> None:
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
    monkeypatch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: True)


def _lineages(workspace: Workspace) -> list[Path]:
    return sorted((workspace.control / "tmp").glob("import.*"))


def _claims(workspace: Workspace) -> list[str]:
    directory = workspace.control / "transfers" / "adopting"
    return sorted(os.listdir(directory)) if directory.is_dir() else []


def _ejected_tree(source: Workspace, tmp_path: Path) -> tuple[Path, list[Marker]]:
    parent, members = _tree(source, tmp_path / "tree")
    return transfers.eject_job(source, parent.job_id, tmp_path / "loose"), [parent, *members]


def _assert_once(workspace: Workspace, jobs: list[Marker], transfer_id: str) -> None:
    """Every job is here exactly once, at its placement, by this transfer, and nothing of the chain is left."""

    for job in jobs:
        found = [marker for marker in workspace.scan_markers(STATE_KINDS) if marker.job_id == job.job_id]
        assert len(found) == 1, (job.job_key, found)
        assert (found[0].placement, found[0].kind) == (job.placement, job.kind)
        assert workspace.read_state(found[0])["transfer"]["transfer_id"] == transfer_id
        payload = workspace.payload_path(job.placement, job.job_key)
        assert payload.is_dir() and not (payload / TRANSFER_DIRECTORY).exists()
    assert _lineages(workspace) == [] and _claims(workspace) == []
    assert (workspace.control / "transfers" / "acks" / f"{transfer_id}.json").is_file()


# ---------------------------------------------------------------------------
# Crash after every step, then takeover
# ---------------------------------------------------------------------------

_STEPS = (
    "link_new.written",
    "V1.staged",
    "V2.claimed",
    "V3.writable",
    "V4.verified",
    "V5.checked",
    "V6.claim",
    "V6.claimed",
    "V7.rechecked",
    "V8.envelope",
    "V9.payload",
    "V9.marker",
    "V9.published",
    "V11.source",
    "V11.release",
    "V11.trash",
    "trash.renamed",
)
#: Before the claim nothing of the source moved: recovery drops the lineage and the source is adopted again.
_BEFORE_CLAIM = {"link_new.written", "V1.staged"}
_NORMAL = {"V1.staged", "V2.claimed", "V6.claim", "V8.envelope", "V9.marker", "V11.release"}


def _profiled(steps: tuple[str, ...]) -> list[Any]:
    return [pytest.param(step, marks=() if step in _NORMAL else pytest.mark.extended) for step in steps]


@pytest.mark.parametrize("step", _profiled(_STEPS))
def test_a_crash_at_any_step_is_finished_by_a_takeover_exactly_once(
    step: str, pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    loose, jobs = _ejected_tree(source, tmp_path)
    transfer_id = str(transfers.validate_bundle(loose)["transfer_id"])
    _hook_at(step, _crash, occurrence=2 if step == "V9.marker" else 1)
    with pytest.raises(Crash):
        transfers.adopt_job(destination, loose)
    _owner_gone(monkeypatch)
    transfers.recover_transfers(destination)
    if step in _BEFORE_CLAIM:
        assert loose.is_dir() and _lineages(destination) == [] and _claims(destination) == []
        monkeypatch.undo()
        transfers.adopt_job(destination, loose)
    assert not loose.exists()
    _assert_once(destination, jobs, transfer_id)


_TAKEOVER_STEPS = (
    "V2.claimed",
    "V3.writable",
    "V4.verified",
    "V5.checked",
    "V6.claimed",
    "V7.rechecked",
    "V8.envelope",
    "V9.payload",
    "V9.marker",
    "V9.published",
    "V11.source",
    "V11.release",
)


@pytest.mark.parametrize("step", _profiled(_TAKEOVER_STEPS))
def test_a_takeover_in_any_state_fences_the_stale_owner(
    step: str, pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    loose, jobs = _ejected_tree(source, tmp_path)
    transfer_id = str(transfers.validate_bundle(loose)["transfer_id"])
    other = _txn.owner_token(str(uuid.uuid4()))
    taken: list[list[dict[str, object]]] = []

    def take_over() -> None:
        # Another actor finds the owner gone, renames the lineage and finishes it.
        with monkeypatch.context() as patch:
            patch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: True)
            taken.append(_adoption.recover_lineages(destination, owner=other))

    _hook_at(step, take_over)
    # The stale owner then resumes: every claim, release and rename it makes names
    # paths below its old lineage name and fails, or finds the work already done.
    try:
        result = _adoption.adopt(destination, _adoption.Source("adopt", loose))
        assert result.status in {"lost", "imported", "replay"}
    except OSError:
        pass
    assert taken and taken[0], step
    assert not loose.exists()
    _assert_once(destination, jobs, transfer_id)


@pytest.mark.parametrize("step", _profiled(_TAKEOVER_STEPS))
def test_a_stale_owner_fenced_by_the_takeover_rename_makes_no_progress(
    step: str, pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    loose, jobs = _ejected_tree(source, tmp_path)
    transfer_id = str(transfers.validate_bundle(loose)["transfer_id"])
    other = _txn.owner_token(str(uuid.uuid4()))

    def fence() -> None:
        # Only the takeover rename: the new holder has not continued yet.
        [lineage] = _lineages(destination)
        _owner, _claimed, lineage_id = _adoption.parse_lineage_name(lineage.name) or ("", 0, "")
        os.rename(lineage, lineage.with_name(_adoption.lineage_name(other, time.time_ns(), lineage_id)))

    _hook_at(step, fence)
    try:
        result = _adoption.adopt(destination, _adoption.Source("adopt", loose))
        # Once every marker was published, only idempotent finishing steps remain.
        published = {"V9.published", "V11.source", "V11.release"}
        assert result.status in ({"imported", "lost"} if step in published else {"lost"})
    except OSError:
        pass
    # The stale owner neither published a marker nor holds a claim the new holder lost.
    [lineage] = _lineages(destination)
    assert lineage.name.split(".")[1] == other
    seen = [marker.job_id for marker in destination.scan_markers(STATE_KINDS)]
    assert len(set(seen)) == len(seen)
    # The new holder (its owner gone too, later) finishes exactly once.
    _owner_gone(monkeypatch)
    _adoption.recover_lineages(destination)
    _assert_once(destination, jobs, transfer_id)


# ---------------------------------------------------------------------------
# Copies, local duplicates, and jobs on their way out
# ---------------------------------------------------------------------------


def test_two_adopters_of_copies_of_one_bundle_import_it_once(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    loose, jobs = _ejected_tree(source, tmp_path)
    transfer_id = str(transfers.validate_bundle(loose)["transfer_id"])
    copy = tmp_path / "copy"
    shutil.copytree(loose, copy, symlinks=True)
    second: list[_adoption.AdoptionResult] = []
    other = _txn.owner_token(str(uuid.uuid4()))

    def adopt_the_copy() -> None:
        second.append(_adoption.adopt(destination, _adoption.Source("adopt", copy), owner=other))
        # Waiting, the second lineage released only its own claims: the first still holds every one.
        assert len(_claims(destination)) == len(jobs)

    # While the first holds every claim and publishes, the second copy is adopted.
    _hook_at("V8.envelope", adopt_the_copy)
    assert transfers.adopt_job(destination, loose).job_id == jobs[0].job_id
    [waiting] = second
    assert waiting.status == "waiting" and len(_lineages(destination)) == 1
    # Its recovery then finds the transfer arrived: a replay, and the redundant copy goes.
    _owner_gone(monkeypatch)
    [record] = _adoption.recover_lineages(destination)
    assert record["status"] == "replay"
    assert not copy.exists() and not list((destination.control / "quarantine").iterdir())
    _assert_once(destination, jobs, transfer_id)


def test_a_lineage_releases_only_the_claims_it_holds(pair: tuple[Workspace, Workspace]) -> None:
    _source, destination = pair
    job_id = str(uuid.uuid4())
    claims = _adoption.claims_directory(destination)
    claims.mkdir(parents=True, exist_ok=True)
    holder, other = (destination.control / "tmp" / name for name in ("import.a", "import.b"))
    for lineage in (holder, other):
        (lineage / "claims").mkdir(parents=True)
        (lineage / "claims" / job_id).write_text(lineage.name)
    os.link(holder / "claims" / job_id, claims / job_id)
    _adoption._release(destination, other)
    assert os.lstat(claims / job_id).st_ino == os.lstat(holder / "claims" / job_id).st_ino
    _adoption._release(destination, holder)
    assert not (claims / job_id).exists() and (holder / "released" / job_id).is_file()


def test_a_duplicate_uuid_of_a_local_job_is_refused_and_left(pair: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, destination = pair
    payload = _payload(tmp_path / "payloads")
    duplicate = tmp_path / "duplicate"
    shutil.copytree(payload, duplicate)
    marker = source.submit(payload, "jobs")
    destination.submit(duplicate, "local")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    with pytest.raises(FileExistsError, match="already holds job"):
        transfers.adopt_job(destination, loose)
    assert loose.is_dir() and _lineages(destination) == [] and _claims(destination) == []
    [local] = [item for item in destination.scan_markers() if item.job_id == marker.job_id]
    assert local.placement.as_posix() == "local"


def test_adopting_a_job_that_is_still_transferring_away_waits(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    first = transfers.eject_job(source, marker.job_id, tmp_path / "first")
    transfers.adopt_job(destination, first)
    # The job leaves the destination again; the ejection commits, then dies before its clean-up.
    _hook_at("S7.cleanup", _crash)
    with pytest.raises(Crash):
        transfers.eject_job(destination, marker.job_id, tmp_path / "second")
    with pytest.raises(ValueError, match="waits: job .* is still transferring away"):
        transfers.adopt_job(destination, tmp_path / "second")
    assert len(_lineages(destination)) == 1
    _owner_gone(monkeypatch)
    transfers.recover_transfers(destination)  # the ejection's clean-up
    transfers.recover_transfers(destination)  # then the adoption
    arrived = destination.find_marker_by_id(marker.job_id)
    assert arrived is not None and arrived.kind == marker.kind
    assert _lineages(destination) == [] and _claims(destination) == []


# ---------------------------------------------------------------------------
# Addressed bundles: replay, expiry, freshness (13.1)
# ---------------------------------------------------------------------------


def _addressed(source: Workspace, destination: Workspace, tmp_path: Path, tag: str = "job") -> tuple[Path, str]:
    marker = source.submit(_payload(tmp_path / "payloads", tag), "jobs")
    bundle = transfers.detach_job(source, marker.job_id, destination_workspace_id=destination.workspace_id)
    return bundle, marker.job_id


def _incoming(destination: Workspace, bundle: Path) -> Path:
    staged = destination.control / "transfers" / "incoming" / bundle.name
    shutil.copytree(bundle, staged, symlinks=True)
    return staged


def _sealed_at(bundle: Path) -> int:
    return int(json.loads((bundle / TRANSFER_DIRECTORY / "manifest.json").read_text())["sealed_at"])


def test_a_replay_of_a_received_transfer_is_acknowledged_at_any_age(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    bundle, job_id = _addressed(source, destination, tmp_path)
    acknowledgement = transfers.import_bundle(destination, _incoming(destination, bundle))
    assert receipt_path(destination, bundle.name).is_file()
    monkeypatch.setattr(_adoption, "_now", lambda: _sealed_at(bundle) + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS)
    [result] = transfers.import_bundles(destination, [_incoming(destination, bundle)])
    assert result["status"] == "replay" and result["acknowledgement"] == acknowledgement
    assert not list((destination.control / "transfers" / "incoming").iterdir())
    assert [marker.job_id for marker in destination.scan_markers()] == [job_id]


@pytest.mark.parametrize("offset", ["late", "early"])
def test_a_bundle_outside_its_window_expires_and_is_discarded(
    offset: str, pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    bundle, _job_id = _addressed(source, destination, tmp_path)
    sealed_at = _sealed_at(bundle)
    now = sealed_at + FRESHNESS_WINDOW_NS + 1 if offset == "late" else sealed_at - CLOCK_SKEW_NS - 1
    monkeypatch.setattr(_adoption, "_now", lambda: now)
    [result] = transfers.import_bundles(destination, [_incoming(destination, bundle)])
    assert result["status"] == "expired" and result["acknowledgement"] is None
    assert not list(destination.scan_markers()) and not list((destination.control / "transfers" / "incoming").iterdir())
    assert _lineages(destination) == [] and _claims(destination) == []


def test_freshness_is_rechecked_with_the_claims_held_but_never_after_v8(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    late = _addressed(source, destination, tmp_path, "late")[0]
    after = _addressed(source, destination, tmp_path, "after")[0]
    clock = {"now": time.time_ns()}
    monkeypatch.setattr(_adoption, "_now", lambda: clock["now"])

    def expire() -> None:
        clock["now"] += FRESHNESS_WINDOW_NS * 2

    _hook_at("V6.claimed", expire)
    [first] = transfers.import_bundles(destination, [_incoming(destination, late)])
    assert first["status"] == "expired" and _claims(destination) == []
    clock["now"] = time.time_ns()
    _hook_at("V8.envelope", expire)
    [second] = transfers.import_bundles(destination, [_incoming(destination, after)])
    assert second["status"] == "imported"


def test_a_transfer_is_never_imported_twice_across_the_receipts_expiry(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    sink = Workspace.initialize(tmp_path / "sink")
    bundle, job_id = _addressed(source, destination, tmp_path)
    copy = tmp_path / "copy"
    shutil.copytree(bundle, copy, symlinks=True)
    transfers.import_bundle(destination, _incoming(destination, bundle))
    # The job leaves again; the receipt still names the transfer.
    moved = transfers.detach_job(destination, job_id, destination_workspace_id=sink.workspace_id)
    transfers.acknowledge_transfer(destination, transfers.import_bundle(sink, moved))
    assert transfers.import_bundle(destination, copy)["transfer_id"] == bundle.name  # a replay
    assert destination.find_marker_by_id(job_id) is None
    # Once the receipt is collected (W + S after sealing), a late copy is too late to be accepted.
    later = _sealed_at(bundle) + FRESHNESS_WINDOW_NS + CLOCK_SKEW_NS + 10**9
    destination.collect_garbage(now=later / 1e9, categories=("transfer_records", "transfer_receipts"))
    assert not receipt_path(destination, bundle.name).exists()
    monkeypatch.setattr(_adoption, "_now", lambda: later)
    [result] = transfers.import_bundles(destination, [copy])
    assert result["status"] == "expired" and destination.find_marker_by_id(job_id) is None


# ---------------------------------------------------------------------------
# Cross-filesystem copies, refusals, exports, exchange
# ---------------------------------------------------------------------------


def test_a_cross_filesystem_adoption_killed_in_its_partial_copy_is_dropped(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    monkeypatch.setattr(transfers, "_same_filesystem", lambda _workspace, path: False)
    _hook_at("V2.copied", _crash)
    with pytest.raises(Crash):
        transfers.adopt_job(destination, loose)
    [lineage] = _lineages(destination)
    assert (lineage / "bundle.partial").is_dir() and loose.is_dir()
    with monkeypatch.context() as patch:
        patch.setattr(_txn, "owner_gone", lambda *_args, **_kwargs: True)
        transfers.recover_transfers(destination)
    assert _lineages(destination) == [] and loose.is_dir() and not list(destination.scan_markers())
    # Adopted again, the copy is verified, published, and the source removed.
    assert transfers.adopt_job(destination, loose).job_id == marker.job_id
    assert not loose.exists()


def test_a_takeover_during_the_cross_filesystem_copy_never_resurrects_the_old_lineage(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The copy is built under ``tmp/birth.*`` and moved into ``S`` by one rename that fails once ``S`` is renamed."""

    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    other = _txn.owner_token(str(uuid.uuid4()))
    renamed: list[Path] = []

    def fence() -> None:
        # The copy is complete in tmp/birth.*; a takeover renames S before it moves in.
        [lineage] = _lineages(destination)
        _owner, _claimed, lineage_id = _adoption.parse_lineage_name(lineage.name) or ("", 0, "")
        renamed.append(lineage.with_name(_adoption.lineage_name(other, time.time_ns(), lineage_id)))
        os.rename(lineage, renamed[0])

    _hook_at("birth.built", fence)
    result = _adoption.adopt(destination, _adoption.Source("adopt", loose, copy=True, remove_source=True))
    assert result.status == "lost"
    assert _lineages(destination) == renamed and not (renamed[0] / "bundle.partial").exists()
    assert not [name for name in os.listdir(destination.control / "tmp") if name.startswith("birth.")]
    assert loose.is_dir() and not list(destination.scan_markers())
    _owner_gone(monkeypatch)
    transfers.recover_transfers(destination)
    assert _lineages(destination) == []
    monkeypatch.undo()
    assert transfers.adopt_job(destination, loose).job_id == marker.job_id


def _exchange_dirs(tmp_path: Path) -> Path:
    exchange = tmp_path / "exchange"
    (exchange / "inbox").mkdir(parents=True)
    (exchange / "outbox" / "rejected").mkdir(parents=True)
    return exchange


def _adopt_from_inbox(destination: Workspace, exchange: Path, name: str) -> _adoption.AdoptionResult:
    inbox = os.open(exchange / "inbox", os.O_RDONLY | os.O_DIRECTORY)
    rejected = os.open(exchange / "outbox" / "rejected", os.O_RDONLY | os.O_DIRECTORY)
    try:
        return _adoption.adopt(
            destination,
            _adoption.Source("exchange_inbox", exchange / "inbox" / name, directory_fd=inbox, rejected_fd=rejected),
        )
    finally:
        os.close(inbox)
        os.close(rejected)


@pytest.mark.parametrize("via", ["path", "inbox", "inbox-refused"])
def test_a_read_only_bundle_root_is_still_claimed(via: str, pair: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    """Moving a directory to another parent needs write permission on it: V2 adds ``u+w`` by descriptor first."""

    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    if via == "path":
        loose.chmod(0o555)
        assert transfers.adopt_job(destination, loose).job_id == marker.job_id
        assert not loose.exists()
        return
    exchange = _exchange_dirs(tmp_path)
    entry = exchange / "inbox" / "bundle"
    os.rename(loose, entry)
    if via == "inbox-refused":
        (entry / "files" / "runner").write_text("tampered")
    entry.chmod(0o555)
    result = _adopt_from_inbox(destination, exchange, "bundle")
    assert not entry.exists() and _lineages(destination) == []
    if via == "inbox":
        assert result.status == "imported" and result.job_id == marker.job_id
    else:
        assert result.status == "refused"
        [unique] = list((exchange / "outbox" / "rejected").iterdir())
        assert (unique / "bundle" / "job.json").is_file()


def _descriptors() -> int:
    return len(os.listdir("/proc/self/fd"))


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="counts descriptors through /proc")
def test_a_wide_bundle_is_adopted_without_holding_a_descriptor_per_directory(
    pair: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    """Exchange review blocker: V3 must not keep one descriptor per pending directory."""

    resource = pytest.importorskip("resource")
    source, destination = pair
    payload = _payload(tmp_path / "payloads")
    for index in range(2000):
        (payload / "files" / f"d{index:04d}").mkdir(mode=0o555)
    marker = source.submit(payload, "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    before = _descriptors()
    if before > 200 or hard < 256:
        pytest.skip(f"{before} descriptors already open")
    resource.setrlimit(resource.RLIMIT_NOFILE, (256, hard))
    try:
        adopted = transfers.adopt_job(destination, loose)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
    assert _descriptors() == before
    assert adopted.job_id == marker.job_id
    home = destination.payload_path(adopted.placement, adopted.job_key)
    assert os.stat(home / "files" / "d1999").st_mode & 0o700 == 0o700


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="counts descriptors through /proc")
@pytest.mark.parametrize("bound", ["entries", "depth"])
def test_the_writable_walk_is_bounded_and_closes_everything_when_it_refuses(
    bound: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "bundle"
    if bound == "entries":
        for index in range(20):
            (root / f"d{index:02d}").mkdir(parents=True)
        monkeypatch.setattr(_bundle, "_ADOPT_MAX_ENTRIES", 10)
        match = "more than 10 entries"
    else:
        root.joinpath("a", "b", "c", "d", "e").mkdir(parents=True)
        monkeypatch.setattr(_bundle, "_ADOPT_MAX_DEPTH", 3)
        match = "nests deeper than 3"
    before = _descriptors()
    with pytest.raises(FormatError, match=match):
        _adoption._make_writable(root)
    assert _descriptors() == before


def test_nothing_is_made_writable_before_the_bundle_verifies(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """V3 runs after V4: a bundle refused by the verification walk keeps its modes."""

    source, destination = pair
    payload = _payload(tmp_path / "payloads")
    (payload / "files" / "locked").mkdir(mode=0o555)
    marker = source.submit(payload, "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    monkeypatch.setattr(_bundle, "_ADOPT_MAX_ENTRIES", 3)
    result = _adoption.adopt(destination, _adoption.Source("adopt", loose))
    assert result.status == "refused" and "more than 3 entries" in str(result.error)
    assert loose.is_dir() and _lineages(destination) == []
    assert os.stat(loose / "files" / "locked").st_mode & 0o777 == 0o555


def test_a_refusal_after_the_claims_releases_them_and_puts_the_bundle_back(
    pair: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = pair
    payload = _payload(tmp_path / "payloads")
    duplicate = tmp_path / "duplicate"
    shutil.copytree(payload, duplicate)
    marker = source.submit(payload, "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    # A local job with the same id appears between the first presence check and the recheck.
    _hook_at("V6.claimed", lambda: destination.submit(duplicate, "local"))
    with pytest.raises(FileExistsError, match="already holds job"):
        transfers.adopt_job(destination, loose)
    assert loose.is_dir() and _lineages(destination) == [] and _claims(destination) == []
    assert [item.placement.as_posix() for item in destination.scan_markers()] == ["local"]


def test_a_held_export_is_taken_back_with_job_adopt(pair: tuple[Workspace, Workspace], tmp_path: Path) -> None:
    source, _destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs/a")
    held = _sealing.seal(source, marker, _sealing.Commit("export", path=tmp_path / "far" / "job"), reason="ejected")
    assert source.find_marker_by_id(marker.job_id) is None
    assert command(["job", "adopt", str(held)], CLIContext("httk", source.root)) == 0
    back = source.find_marker_by_id(marker.job_id)
    assert back is not None and back.placement.as_posix() == "jobs/a" and back.kind == "submitted"
    assert not held.parent.exists()
    assert _sealing.resume_copy_outs(source) == []


def test_a_job_adopted_below_a_symlinked_placement_lands_through_the_link(
    pair: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = pair
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (destination.jobs / "project").symlink_to(scratch, target_is_directory=True)
    marker = source.submit(_payload(tmp_path / "payloads"), "project/runs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    adopted = transfers.adopt_job(destination, loose)
    assert adopted.placement.as_posix() == "project/runs"
    assert (scratch / "runs" / marker.job_key / "job.json").is_file()


def test_an_exchange_inbox_adoption_filters_prior_state_and_records_its_origin(
    pair: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    with source.open_journal_writer() as writer:
        marker = source.transition(writer, marker, "paused", {"reason": "operator_pause", "invented": "x"})
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    result = _adoption.adopt(destination, _adoption.Source("exchange_inbox", loose))
    assert result.status == "imported" and result.job_id == marker.job_id
    adopted = destination.find_marker_by_id(marker.job_id)
    assert adopted is not None
    state = destination.read_state(adopted)
    assert adopted.kind == "paused" and state["origin"] == "exchange" and state["reason"] == "operator_pause"
    assert "invented" not in state
    # Another adoption source keeps the prior members as they were (and records no origin).
    other = source.submit(_payload(tmp_path / "payloads", "other"), "jobs")
    with source.open_journal_writer() as writer:
        source.transition(writer, other, "paused", {"invented": "y"})
    adopted = transfers.adopt_job(destination, transfers.eject_job(source, other.job_id, tmp_path / "other"))
    state = destination.read_state(adopted)
    assert state["invented"] == "y" and "origin" not in state


def test_an_exchange_refusal_goes_to_a_fresh_rejected_directory_by_descriptor(
    pair: tuple[Workspace, Workspace], tmp_path: Path
) -> None:
    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    (loose / "files" / "runner").write_text("tampered")
    exchange = tmp_path / "exchange"
    (exchange / "inbox").mkdir(parents=True)
    (exchange / "outbox" / "rejected").mkdir(parents=True)
    os.rename(loose, exchange / "inbox" / "bundle")
    inbox = os.open(exchange / "inbox", os.O_RDONLY | os.O_DIRECTORY)
    rejected = os.open(exchange / "outbox" / "rejected", os.O_RDONLY | os.O_DIRECTORY)
    try:
        result = _adoption.adopt(
            destination,
            _adoption.Source("exchange_inbox", exchange / "inbox" / "bundle", directory_fd=inbox, rejected_fd=rejected),
        )
    finally:
        os.close(inbox)
        os.close(rejected)
    assert result.status == "refused" and isinstance(result.error, FormatError)
    [unique] = list((exchange / "outbox" / "rejected").iterdir())
    assert (unique / "bundle" / "job.json").is_file()
    reason = json.loads((unique / "reason.json").read_text())
    assert reason["format"] == "httk-workspace-exchange-rejection" and "digest" in reason["reason"]
    assert not list((exchange / "inbox").iterdir()) and _lineages(destination) == []


# ---------------------------------------------------------------------------
# Addressed round trip through the real CLI paths
# ---------------------------------------------------------------------------


def test_an_addressed_transfer_round_trips_through_receive_and_retire(
    pair: tuple[Workspace, Workspace], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, destination = pair
    bundle, job_id = _addressed(source, destination, tmp_path)
    staged = _incoming(destination, bundle)
    context = CLIContext("httk", tmp_path)
    receive = ["transfer", "receive", "--workspace", str(destination.root), "--bundle", str(staged)]
    assert command(receive, context) == 0
    [result] = json.loads(capsys.readouterr().out)["results"]
    assert result["status"] == "imported"
    retire = ["transfer", "retire", str(source.root), job_id, "--json"]
    retire += ["--destination-workspace-id", destination.workspace_id]
    retire += ["--acknowledgements-json", json.dumps([result["acknowledgement"]])]
    assert command(retire, context) == 0
    capsys.readouterr()
    assert source.find_marker_by_id(job_id, kinds=STATE_KINDS) is None
    assert (source.control / "transfers" / "retired" / bundle.name).is_dir()
    arrived = destination.find_marker_by_id(job_id)
    assert arrived is not None and destination.read_state(arrived)["transfer"]["transfer_id"] == bundle.name


# ---------------------------------------------------------------------------
# Recovery at manager attach, and the operator report
# ---------------------------------------------------------------------------


def test_a_manager_attaching_finishes_an_adoption_whose_owner_is_gone(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow import TaskManager

    source, destination = pair
    marker = source.submit(_payload(tmp_path / "payloads"), "jobs")
    loose = transfers.eject_job(source, marker.job_id, tmp_path / "loose")
    _hook_at("V9.payload", _crash)
    with pytest.raises(Crash):
        transfers.adopt_job(destination, loose)
    _owner_gone(monkeypatch)
    with TaskManager(destination, heartbeat_interval=0.01):
        pass
    arrived = destination.find_marker_by_id(marker.job_id)
    assert arrived is not None and _lineages(destination) == [] and _claims(destination) == []


def test_hygiene_reports_held_exports_outgoing_in_doubt_and_orphaned_claims(
    pair: tuple[Workspace, Workspace], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from httk.workflow.hygiene import _check_transfers

    source, destination = pair
    assert _check_transfers(source.root).status == "ok"
    exported = source.submit(_payload(tmp_path / "payloads", "exported"), "jobs")
    _sealing.seal(source, exported, _sealing.Commit("export", path=tmp_path / "far" / "job"), reason="ejected")
    sent = source.submit(_payload(tmp_path / "payloads", "sent"), "jobs")
    transfers.detach_job(source, sent.job_id, destination_workspace_id=destination.workspace_id)
    claims = _adoption.claims_directory(source)
    claims.mkdir(parents=True, exist_ok=True)
    (claims / str(uuid.uuid4())).write_text("")
    monkeypatch.setattr(time, "time_ns", lambda: _real_time_ns() + FRESHNESS_WINDOW_NS * 2)
    finding = _check_transfers(source.root)
    assert finding.status == "warning"
    details: dict[str, Any] = finding.details
    assert len(details["held_exports"]) == 1
    assert [item["job_key"] for item in details["outgoing_in_doubt"]] == [sent.job_key]
    assert len(details["stale_claims"]) == 1


_real_time_ns = time.time_ns
