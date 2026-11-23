"""Batch transfer loops on the bounded pool (``io.workers``): hold, adopt and release with 8 workers give what 1 gives.

Every equivalence test runs one scenario twice, in fresh workspaces holding the same jobs, with ``io.workers`` 1
and 8, and compares exit codes, standard output and error, and the final workspaces (only tokens, transfer and
owner ids, workspace ids and the scenario root normalized). With 8 workers, two pooled items are forced into the
filesystem at once by a barrier in the ``_fs`` fault hook (armed only in pool threads), and the test asserts that
at least two items were in flight together.
"""

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace, _fs, _kernel, _moving, _pool
from httk.workflow._bundles import read_manifest
from httk.workflow._setting_catalog import classify_setting
from httk.workflow._state import Release
from httk.workflow.errors import WorkflowError
from httk.workflow.models import make_job_key
from httk.workflow.workflow_cli import _transfer as transfer_cli
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._transfer import run_batch
from test_moving import WORKFLOW, claim_root, find, parent_link, place
from v3_helpers import cli_owner, job_mapping
from v3_helpers import workspace as v3_workspace

#: How long anything that should happen promptly may take before the test fails.
PATIENCE = 30.0
#: The workspace id recorded in the parent links of the planned jobs (never checked by a transfer).
PARENT_WORKSPACE = "11111111-1111-4111-8111-111111111111"
SOURCE = Path(__file__).resolve().parents[1] / "src"


class Boom(Exception):
    pass


@dataclass
class Planned:
    """One job of a scenario, placed identically into every run's source workspace."""

    mapping: dict[str, Any]
    state: str = "succeeded"
    priority: int = 500
    children: tuple[dict[str, Any], ...] = ()
    detached: bool = False


@dataclass
class Plan:
    jobs: list[Planned] = field(default_factory=list)

    def add(
        self,
        tag: str,
        placement: str,
        *,
        state: str = "succeeded",
        parent: "Planned | None" = None,
        detached: bool = False,
    ) -> Planned:
        mapping = job_mapping(WORKFLOW, {"start": "succeed"}, tag=tag, placement=placement)
        if parent is not None:
            mapping["parent"] = parent_link(parent.mapping, PARENT_WORKSPACE)
            parent.children = (*parent.children, mapping)
        planned = Planned(mapping, state, detached=detached)
        self.jobs.append(planned)
        return planned

    def singles(self, count: int, prefix: str = "s") -> list[Planned]:
        return [self.add(f"{prefix}{index}", f"loose/{prefix}{index}") for index in range(count)]

    def populate(self, ws: Workspace) -> None:
        # Children first: a parent records them by reference.
        for planned in reversed(self.jobs):
            place(ws, planned.mapping, planned.state, planned.priority, planned.children)
            if planned.detached:
                with cli_owner(ws) as owner:
                    owned = claim_root(ws, owner, planned.mapping["id"])
                    doc = owned.read_state()
                    assert doc is not None
                    detached = doc.updated(detached={"at": "now", "operator": "t"})
                    owned.release(detached, Release(planned.state, planned.priority))

    def ids(self) -> list[str]:
        return [str(planned.mapping["id"]) for planned in self.jobs]


class Overlap:
    """Wraps :func:`httk.workflow._pool.run_ordered`: counts the items in flight at once and, through the ``_fs``
    fault hook, makes the first two pool threads of each pooled call meet inside a filesystem call."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.lock = threading.Lock()
        self.flying = self.peak = self.pooled = 0
        self.barrier: threading.Barrier | None = None
        self.arrived: set[int] = set()
        real = _pool.run_ordered

        def run_ordered(
            function: Callable[[Any], Any], items: Any, workers: int, emit: Any = None, **options: Any
        ) -> list[Any]:
            pooled = workers > 1 and len(items) > 1
            self.pooled += pooled
            self.barrier, self.arrived = (threading.Barrier(2, timeout=PATIENCE) if pooled else None), set()

            def counted(item: Any) -> Any:
                with self.lock:
                    self.flying += 1
                    self.peak = max(self.peak, self.flying)
                try:
                    return function(item)
                finally:
                    with self.lock:
                        self.flying -= 1

            return real(counted, items, workers, emit, **options)

        monkeypatch.setattr(_pool, "run_ordered", run_ordered)
        _fs.set_fault_injector(self.hook)

    def reset(self) -> None:
        self.flying = self.peak = self.pooled = 0

    def hook(self, op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        barrier = self.barrier
        if barrier is None or threading.current_thread() is threading.main_thread():
            return
        with self.lock:
            if len(self.arrived) >= 2 or threading.get_ident() in self.arrived:
                return
            self.arrived.add(threading.get_ident())
        barrier.wait()


@pytest.fixture
def overlap(monkeypatch: pytest.MonkeyPatch) -> Iterator[Overlap]:
    yield Overlap(monkeypatch)
    _fs.set_fault_injector(None)


#: One ``_moving.adopt`` call: the bundle's member keys, its path, and whether it ran on the main thread.
type Adoption = tuple[frozenset[str], Path, bool]


@pytest.fixture
def adoptions(monkeypatch: pytest.MonkeyPatch) -> list[Adoption]:
    """Record every adoption the commands make, and the thread it ran on."""

    calls: list[Adoption] = []
    real = _moving.adopt

    def adopt(workspace: Workspace, owner: _kernel.Owner, source: Path | _fs.Loc, **options: Any) -> object:
        path = Path(source.path if isinstance(source, _fs.Loc) else source)
        try:
            read = read_manifest(path / "bundle.json")
        except Exception:
            read = None
        members = frozenset() if read is None else frozenset(member.job_key for member in read[1].members)
        calls.append((members, path, threading.current_thread() is threading.main_thread()))
        return real(workspace, owner, source, **options)

    monkeypatch.setattr(_moving, "adopt", adopt)
    return calls


def key(planned: "Planned") -> str:
    return make_job_key(str(planned.mapping["id"]), planned.mapping["tag"])


@pytest.fixture(autouse=True)
def _no_faults() -> Iterator[None]:
    yield
    _fs.set_fault_injector(None)


_TOKEN = re.compile(r"(?<![0-9a-z])[a-z2-7]{16}(?![0-9a-z])")
_HEX32 = re.compile(r"(?<![0-9a-f])[0-9a-f]{32}(?![0-9a-f])")
_CLOCK = re.compile(r'"(created_at|age_seconds)": [^,\n]*')


def normalizer(root: Path, *workspaces: Workspace) -> Callable[[str], str]:
    def normalize(text: str) -> str:
        text = text.replace(str(root), "ROOT")
        for index, ws in enumerate(workspaces):
            text = text.replace(ws.workspace_id, f"WORKSPACE{index}")
        return _CLOCK.sub(r'"\1": "CLOCK"', _HEX32.sub("HEX", _TOKEN.sub("TOKEN", text)))

    return normalize


def snapshot(ws: Workspace, normalize: Callable[[str], str]) -> list[str]:
    """Every path in *ws* (normalized), with the payload of each job, and nothing below ``owners/``."""

    entries = []
    for path in ws.root.rglob("*"):
        relative = path.relative_to(ws.root)
        if relative.parts[:2] == (".httk-workspace", "owners"):
            continue
        entry = normalize(str(relative))
        if path.name == "input.txt":
            entry += "=" + path.read_text(encoding="utf-8")
        entries.append(entry)
    return sorted(entries)


@dataclass
class Run:
    code: int
    out: str
    err: str
    workspaces: list[list[str]]


def compare(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    overlap: Overlap,
    plan: Plan,
    act: Callable[[Path, Workspace, Workspace], int],
    *,
    pooled: bool = True,
    canonical: Callable[[str], str] = lambda out: out,
) -> Run:
    """Run *act* with io.workers 1 and then 8 on fresh workspaces ``a`` and ``b`` (``a`` holding *plan*), assert
    identical results, and that the 8-worker run overlapped items when *pooled* (else that it pooled nothing)."""

    runs: dict[int, Run] = {}
    for workers in (1, 8):
        root = tmp_path / f"workers-{workers}"
        source, target = v3_workspace(root / "a"), v3_workspace(root / "b")
        for ws in (source, target):
            ws.configure_settings({"io.workers": workers})
        plan.populate(source)
        overlap.reset()
        code = act(root, source, target)
        out, err = capsys.readouterr()
        normalize = normalizer(root, source, target)
        out, err = canonical(normalize(out)), normalize(err)
        runs[workers] = Run(code, out, err, [snapshot(ws, normalize) for ws in (source, target)])
        if workers == 1 or not pooled:
            assert overlap.pooled == 0
        else:
            assert overlap.pooled >= 1 and overlap.peak >= 2
    assert runs[8] == runs[1]
    return runs[1]


def by_members(out: str) -> str:
    """A ``job move --json`` document with its transfers in member order (they come in transfer id order)."""

    document = json.loads(out)
    document["transfers"].sort(key=lambda outcome: outcome["members"])
    return json.dumps(document, indent=2, sort_keys=True)


def transfer(root: Path, *argv: str) -> int:
    return command(["job", "move", "--no-durable", *argv], CLIContext("httk", root))


def adopt(root: Path, workspace: str, *bundles: str) -> int:
    return command(
        ["job", "adopt", "--no-durable", "--json", "--workspace", workspace, *bundles], CLIContext("httk", root)
    )


def two_trees(plan: Plan) -> None:
    for name in ("x", "y"):
        root = plan.add(f"{name}root", f"trees/{name}", state="paused")
        first = plan.add(f"{name}a", f"trees/{name}/a", parent=root)
        plan.add(f"{name}b", f"trees/{name}/b", state="failed", parent=root)
        plan.add(f"{name}g", f"deep/{name}g", state="cancelled", parent=first)


# -- the setting --------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value", [0, 33, "x", 1.5, True, "8"])
def test_io_workers_is_validated_before_anything_is_written(tmp_path: Path, value: object) -> None:
    ws = v3_workspace(tmp_path / "a")
    before = (ws.control / "format.json").read_bytes()
    with pytest.raises(ValueError, match="io.workers"):
        ws.configure_settings({"io.workers": value})
    with pytest.raises(ValueError, match="io.workers"):
        ws.seed_settings({"io.workers": value})
    assert (ws.control / "format.json").read_bytes() == before
    assert ws.io_workers == 8


def test_io_workers_takes_1_to_32_and_is_a_recognized_engine_setting(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    assert ws.configure_settings({"io.workers": 1})["io.workers"] == 1
    assert Workspace(ws.root).io_workers == 1
    ws.configure_settings({"io.workers": 32})
    assert Workspace(ws.root).io_workers == 32
    assert classify_setting("io.workers", 8).state == "recognized"
    assert classify_setting("io.other", 8).state == "unrecognized"


@pytest.mark.parametrize("stored", ["x", 0, 99, None])
def test_an_invalid_stored_io_workers_falls_back_to_8_with_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, stored: object
) -> None:
    ws = v3_workspace(tmp_path / "a")
    document = json.loads((ws.control / "format.json").read_bytes())
    document["settings"]["io.workers"] = stored
    (ws.control / "format.json").write_text(json.dumps(document), encoding="utf-8")
    with caplog.at_level("WARNING", logger="httk.workflow.workspace"):
        assert Workspace(ws.root).io_workers == 8
    assert [record.getMessage().startswith("using io.workers = 8") for record in caplog.records] == [True]


def test_io_workers_is_read_once_per_workspace_and_again_after_its_settings_change(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    ws = v3_workspace(tmp_path / "a")
    assert ws.io_workers == 8
    ws.configure_settings({"io.workers": 3})
    assert ws.io_workers == 3  # the same object sees what it wrote
    document = json.loads((ws.control / "format.json").read_bytes())
    document["settings"]["io.workers"] = "x"
    (ws.control / "format.json").write_text(json.dumps(document), encoding="utf-8")
    assert ws.io_workers == 3  # cached until the settings are re-read
    with caplog.at_level("WARNING", logger="httk.workflow.workspace"):
        ws.refresh_format()
        assert [ws.io_workers for _ in range(5)] == [8] * 5
    assert len(caplog.records) == 1


# -- the owner ----------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("ending", [None, KeyboardInterrupt])
def test_an_owner_left_with_operations_in_flight_fail_stops(tmp_path: Path, ending: type[BaseException] | None) -> None:
    # Without a flight, a normal exit closes (returning the job) and an interrupt gives the job back: both touch it.
    ws = v3_workspace(tmp_path / "a")
    plan = Plan()
    (only,) = plan.singles(1)
    plan.populate(ws)
    owner = cli_owner(ws)
    try:
        with owner:
            owner.enter_flight()
            with pytest.raises(WorkflowError, match="0 jobs, 1 operations in flight"):
                owner.close()  # holding nothing, but an operation may still claim
            claim_root(ws, owner, only.mapping["id"])
            if ending is not None:
                raise ending
    except KeyboardInterrupt:
        pass
    # No give-back and no close: the job stays owned and the owner registered, for recovery.
    ref = find(ws, only.mapping["id"])
    assert ref is not None and ref.state == _kernel.OWNED
    assert (owner.path / "owner.json").exists()
    owner.leave_flight()
    with owner:  # once nothing is in flight, the owner returns the job and closes as usual
        assert ref.path.parent.name == owner.owner_id
        owner.adopt_owned(ref).give_back()
    assert find(ws, only.mapping["id"]).state == "succeeded"  # type: ignore[union-attr]
    assert not owner.path.exists()


def test_a_handle_built_while_another_thread_retired_the_job_is_never_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = Plan()
    (only,) = plan.singles(1)
    plan.populate(ws)
    built, finish = threading.Event(), threading.Event()
    original = _kernel.OwnedJob.__init__

    def init(job: _kernel.OwnedJob, owner: _kernel.Owner, ref: _kernel.JobRef) -> None:
        original(job, owner, ref)
        if threading.current_thread() is not threading.main_thread():
            built.set()
            assert finish.wait(PATIENCE)

    monkeypatch.setattr(_kernel.OwnedJob, "__init__", init)
    with cli_owner(ws) as owner:
        ref = claim_root(ws, owner, only.mapping["id"]).ref
        owner._held.clear()  # as before the first handle
        outcome: list[object] = []

        def late() -> None:
            try:
                outcome.append(owner.adopt_owned(ref))
            except _kernel.OwnerLost as exc:
                outcome.append(exc)

        thread = threading.Thread(target=late)
        thread.start()
        assert built.wait(PATIENCE)  # A has built its handle and not inserted it yet
        owner.adopt_owned(ref).give_back()  # B inserts, then retires: the job left owned/
        finish.set()
        thread.join(PATIENCE)
        assert isinstance(outcome[0], _kernel.OwnerLost)
        assert not owner.holds(ref) and ref.path not in owner._held
    assert find(ws, only.mapping["id"]).state == "succeeded"  # type: ignore[union-attr]


def test_concurrent_handles_of_one_job_are_one_handle(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = Plan()
    (only,) = plan.singles(1)
    plan.populate(ws)
    with cli_owner(ws) as owner:
        ref = claim_root(ws, owner, only.mapping["id"]).ref
        owner._held.clear()  # as before the first handle: every thread races to create it
        handles = _pool.run_ordered(lambda _index: owner.adopt_owned(ref), list(range(16)), 8)
        assert all(handle is handles[0] for handle in handles) and owner.holds(ref)
        handles[0].give_back()


def test_many_claims_and_give_backs_leave_no_per_owner_state(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    plan = Plan()
    (only,) = plan.singles(1)
    plan.populate(ws)
    with cli_owner(ws) as owner:
        ref = find(ws, only.mapping["id"])
        for _ in range(3000):
            assert ref is not None
            job = _kernel.claim(ws, owner, ref)
            assert job is not None
            ref = job.give_back()
        grown = {
            name: len(value) for name, value in vars(owner).items() if isinstance(value, (dict, set, list)) and value
        }
        assert grown == {}


# -- transfers: io.workers 8 against 1 ----------------------------------------------------------------------------


def test_a_transfer_of_many_jobs_and_trees_holds_adopts_and_releases_alike(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap
) -> None:
    plan = Plan()
    plan.singles(10)
    two_trees(plan)
    run = compare(
        tmp_path,
        capsys,
        overlap,
        plan,
        lambda root, _a, _b: transfer(root, "--json", "--state", "succeeded", "--state", "paused", "--tree", "a", "b"),
        canonical=by_members,
    )
    assert run.code == 0
    outcomes = json.loads(run.out)["transfers"]
    assert len(outcomes) == 12 and {outcome["status"] for outcome in outcomes} == {"adopted"}
    # The 8-worker run moved every job exactly once, payloads intact.
    root = tmp_path / "workers-8"
    for job_id in plan.ids():
        assert find(Workspace(root / "a"), job_id) is None
        found = find(Workspace(root / "b"), job_id)
        assert found is not None and (found.path / "input.txt").read_text() == f"payload of {job_id}"


def test_a_kept_transfer_and_its_return_replace_away_copies_alike(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap
) -> None:
    plan = Plan()
    plan.singles(9)

    def act(root: Path, _a: Workspace, _b: Workspace) -> int:
        assert transfer(root, "--keep", "--state", "succeeded", "a", "b") == 0
        # Back again: the adoption claims and discards the away copies it replaces.
        return transfer(root, "--state", "succeeded", "b", "a")

    assert compare(tmp_path, capsys, overlap, plan, act).code == 0


def test_a_release_of_many_holds_alike(tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap) -> None:
    plan = Plan()
    plan.singles(9)

    def act(root: Path, a: Workspace, _b: Workspace) -> int:
        assert transfer(root, "--hold", "--json", "--state", "succeeded", "a", "b") == 0
        ids = [hold.transfer_id for hold in _moving.held(a)]
        assert len(ids) == 9
        # A repeated id is released once; the repeat reports no such hold, as in the plain loop.
        releases = [item for transfer_id in [*ids, ids[3]] for item in ("--release", transfer_id)]
        return transfer(root, *releases, "a")

    run = compare(tmp_path, capsys, overlap, plan, act)
    assert run.code == 1 and run.out.count("released") == 9 and run.err.count("no such hold") == 1


# -- holds -------------------------------------------------------------------------------------------------------


def family(plan: Plan, *, detached: bool = False) -> tuple[Planned, Planned]:
    parent = plan.add("parent", "fam/p")
    child = plan.add("child", "fam/p/c", parent=parent, detached=detached)
    return parent, child


def hold(*argv: str) -> Callable[[Path, Workspace, Workspace], int]:
    return lambda root, _a, _b: transfer(root, "--hold", "--json", *argv, "a", "b")


@pytest.mark.parametrize("order", ["child-first", "parent-first"])
def test_explicit_parent_and_child_hold_alike_in_either_order(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap, order: str
) -> None:
    plan = Plan()
    parent, child = family(plan)
    plan.singles(8)
    first, second = (child, parent) if order == "child-first" else (parent, child)
    jobs = ["--job", str(first.mapping["id"]), "--job", str(second.mapping["id"])]
    run = compare(tmp_path, capsys, overlap, plan, hold(*jobs, "--state", "succeeded"))
    document = json.loads(run.out)
    if order == "parent-first":
        # The parent is refused while its child is here; the child moves alone.
        assert any("eject the whole tree" in error for error in document["errors"])


@pytest.mark.parametrize("tree", [False, True])
def test_a_detached_child_listed_with_its_parent_holds_alike(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap, tree: bool
) -> None:
    plan = Plan()
    family(plan, detached=True)
    plan.singles(8)
    argv = ["--state", "succeeded", *(["--tree"] if tree else [])]
    # A listed root with its parent here makes the whole hold sequential.
    run = compare(tmp_path, capsys, overlap, plan, hold(*argv), pooled=False)
    assert len(json.loads(run.out)["holds"]) == 10


def test_a_three_level_tree_with_a_detached_grandchild_holds_alike(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap
) -> None:
    plan = Plan()
    root = plan.add("r", "lvl/r")
    middle = plan.add("c", "lvl/r/c", parent=root)
    plan.add("d", "lvl/r/c/d", parent=middle, detached=True)
    plan.singles(8)
    run = compare(tmp_path, capsys, overlap, plan, hold("--state", "succeeded", "--tree"), pooled=False)
    holds = json.loads(run.out)["holds"]
    assert sorted(len(item["members"]) for item in holds) == [1] * 9 + [2]


def test_overlapping_tree_selectors_and_listed_roots_hold_alike(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap
) -> None:
    plan = Plan()
    two_trees(plan)
    plan.singles(8)
    root, child = plan.jobs[0], plan.jobs[1]
    jobs = ["--job", str(child.mapping["id"]), "--job", str(root.mapping["id"])]
    run = compare(tmp_path, capsys, overlap, plan, hold("--tree", *jobs, "--state", "succeeded", "--state", "paused"))
    # x's child with its own subtree, x's root with the rest of its tree, y's tree, and the singles.
    assert sorted(len(item["members"]) for item in json.loads(run.out)["holds"]) == [1] * 8 + [2, 2, 4]


def test_refusals_are_reported_alike(tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap) -> None:
    plan = Plan()
    family(plan)  # moves as one tree
    busy_tree = plan.add("busyroot", "busy/r")
    busy = plan.add("busychild", "busy/r/c", state="failed", parent=busy_tree)
    plan.singles(8)
    blockers: list[_kernel.Owner] = []

    def act(root: Path, a: Workspace, _b: Workspace) -> int:
        blocker = cli_owner(a)
        blockers.append(blocker)
        claim_root(a, blocker, busy.mapping["id"])
        return transfer(root, "--hold", "--json", "--state", "succeeded", "--tree", "a", "b")

    try:
        run = compare(tmp_path, capsys, overlap, plan, act)
    finally:
        for blocker in blockers:
            for ref in blocker.owned():
                blocker.adopt_owned(ref).give_back()
            blocker.close()
    errors = json.loads(run.out)["errors"]
    assert len(errors) == 1 and "busychild" in errors[0]


# -- job adopt ---------------------------------------------------------------------------------------------------


def eject_all(ws: Workspace, plan: Plan, out: Path) -> dict[str, Path]:
    """Eject every planned job alone (children first), each into its own bundle below *out*."""

    bundles = {}
    for planned in reversed(plan.jobs):
        with cli_owner(ws) as owner:
            root = claim_root(ws, owner, planned.mapping["id"])
            report = _moving.eject(ws, owner, root, destination=out, tree=False)
        bundles[str(planned.mapping["id"])] = report.destination
    return bundles


@pytest.mark.parametrize("order", ["child-first", "parent-first"])
def test_adopting_a_batch_with_a_parent_child_pair_and_duplicates_goes_alike(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap, adoptions: list[Adoption], order: str
) -> None:
    plan = Plan()
    parent, child = family(plan)
    singles = plan.singles(8)
    serial: set[Path] = set()

    def act(root: Path, a: Workspace, b: Workspace) -> int:
        bundles = eject_all(a, plan, root / "out")
        copy = root / "out" / "copy"
        shutil.copytree(bundles[str(singles[0].mapping["id"])], copy)
        pair = [bundles[str(child.mapping["id"])], bundles[str(parent.mapping["id"])]]
        pair = pair if order == "child-first" else pair[::-1]
        loose = [bundles[str(planned.mapping["id"])] for planned in singles]
        # The same path twice, and a copy of a bundle at another path.
        batch = [loose[0], *pair[:1], *loose[1:5], loose[2], *pair[1:], *loose[5:], copy]
        adoptions.clear()
        serial.clear()
        serial.update({loose[0], copy, loose[2], *pair})
        return adopt(root, str(b.root), *map(str, batch))

    run = compare(tmp_path, capsys, overlap, plan, act)
    statuses = Counter(entry["status"] for entry in json.loads(run.out)["bundles"])
    # The repeated path is taken by its first adoption; the copy finds its job here; a child after its parent is
    # refused (adopt the parent's whole tree instead).
    parent_first = order == "parent-first"
    assert statuses == Counter({"adopted": 10 - parent_first, "taken": 1, "refused": 1 + parent_first})
    # With 8 workers, the related bundles were adopted in the calling thread, the others in pool threads.
    assert len(adoptions) == 12 and all(on_main == (path in serial) for _, path, on_main in adoptions)


def related_holds(plan: Plan) -> tuple[set[str], list[str]]:
    """A parent and its child (held alone, child first) and 8 singles: their keys, and the selection."""

    parent, child = family(plan)
    plan.singles(8)
    return {key(parent), key(child)}, ["--job", str(child.mapping["id"]), "--job", str(parent.mapping["id"])]


def test_a_local_delivery_adopts_related_holds_in_the_calling_thread(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], overlap: Overlap, adoptions: list[Adoption]
) -> None:
    # One run only: the holds are adopted in transfer id order, so whether the child comes before its parent (and
    # is adopted) or after it (and is refused) differs between runs whatever the pool does.
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    for ws in (source, target):
        ws.configure_settings({"io.workers": 8})
    plan = Plan()
    keys, jobs = related_holds(plan)
    plan.populate(source)
    transfer(tmp_path, "--json", *jobs, "--state", "succeeded", "a", "b")
    statuses = Counter(outcome["status"] for outcome in json.loads(capsys.readouterr().out)["transfers"])
    assert statuses["adopted"] >= 9 and overlap.peak >= 2
    assert len(adoptions) == 10 and all(on_main == bool(members & keys) for members, _, on_main in adoptions)


def test_taking_holds_back_adopts_related_holds_in_the_calling_thread(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], adoptions: list[Adoption]
) -> None:
    source = v3_workspace(tmp_path / "a")
    source.configure_settings({"io.workers": 8})
    plan = Plan()
    keys, jobs = related_holds(plan)
    plan.populate(source)
    assert transfer(tmp_path, "--hold", "--json", *jobs, "--state", "succeeded", "a", "b") == 0
    capsys.readouterr()
    end = transfer_cli._End("a", workspace=Workspace(source.root))
    # The child's hold first: taken back after its parent, the child would be refused (as in the plain loop).
    holds = sorted(_moving.held(source), key=lambda item: not item.members[0].startswith("child--"))
    updates = transfer_cli._take_back(end, holds, "refused for the test")
    assert len(updates) == 10 and {update["status"] for update in updates.values()} == {"not_moved"}
    assert len(adoptions) == 10 and all(on_main == bool(members & keys) for members, _, on_main in adoptions)
    assert not _moving.held(source) and all(find(source, job_id).state == "succeeded" for job_id in plan.ids())  # type: ignore[union-attr]


def test_a_repeated_release_id_is_released_once_and_reported_in_order(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    source = v3_workspace(tmp_path / "a")
    source.configure_settings({"io.workers": 8})
    plan = Plan()
    plan.singles(9)
    plan.populate(source)
    assert transfer(tmp_path, "--hold", "--json", "--state", "succeeded", "a", "b") == 0
    capsys.readouterr()
    ids = [hold.transfer_id for hold in _moving.held(source)]
    order = [ids[3], *ids, ids[3], ids[0]]
    calls: list[tuple[str, bool]] = []
    real = _moving.release_hold

    def release_hold(workspace: Workspace, owner: _kernel.Owner, transfer_id: str, **options: Any) -> bool:
        calls.append((transfer_id, threading.current_thread() is threading.main_thread()))
        return real(workspace, owner, transfer_id, **options)

    monkeypatch.setattr(_moving, "release_hold", release_hold)
    assert transfer(tmp_path, *[part for transfer_id in order for part in ("--release", transfer_id)], "a") == 1
    # Deterministically: the repeats ran after the pool, in order, in the calling thread; the others in the pool.
    assert [transfer_id for transfer_id, on_main in calls if on_main] == [ids[3], ids[3], ids[0]]
    assert sum(not on_main for _, on_main in calls) == 9
    out, err = capsys.readouterr()
    # Not normalized: the first occurrence of each id is the one released, every repeat says no such hold.
    assert out.splitlines() == [f"released {transfer_id}" for transfer_id in [ids[3], *ids[:3], *ids[4:]]]
    assert [line.partition(":")[0] for line in err.splitlines()] == [ids[3], ids[3], ids[0]]
    assert all("no such hold" in line for line in err.splitlines())


def test_settle_absent_makes_related_bundles_serial(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    plan = Plan()
    parent, child = family(plan)
    grand_parent = plan.add("gp", "chain/gp")
    middle = plan.add("mid", "chain/gp/m", parent=grand_parent)
    leaf = plan.add("leaf", "chain/gp/m/l", parent=middle)
    singles = plan.singles(3)
    plan.populate(source)
    bundles = eject_all(source, plan, tmp_path / "out")
    copy = tmp_path / "out" / "copy"
    shutil.copytree(bundles[str(singles[0].mapping["id"])], copy)
    (tmp_path / "out" / "broken").mkdir()

    def path(planned: Planned) -> Path:
        return bundles[str(planned.mapping["id"])]

    batch = [*map(path, (singles[0], child, singles[1], parent, singles[1], leaf, grand_parent, singles[2]))]
    batch += [path(middle), copy, tmp_path / "out" / "broken"]
    settled = _moving.settle_absent(target, batch)
    # A copy shares UUIDs; a repeated path too; a parent and its child, transitively along a chain.
    serial = {path(singles[0]), copy, path(singles[1]), path(parent), path(child)}
    assert settled.serial == serial | {path(grand_parent), path(middle), path(leaf)}
    assert set(plan.ids()) <= settled.absent


def test_run_batch_holds_one_flight_while_its_pool_can_run_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ws = v3_workspace(tmp_path / "a")
    seen: list[tuple[int, int]] = []

    def work(item: int) -> int:
        seen.append((item, owner._flights))
        return item

    with cli_owner(ws) as owner:
        run_batch(owner, work, list(range(6)), 4, serial={5})
        # One flight for the pooled items; none for the serial one, which runs after the pool is quiet.
        assert sorted(seen) == [(item, 1) for item in range(5)] + [(5, 0)] and owner._flights == 0
        seen.clear()
        run_batch(owner, work, list(range(6)), 1)
        assert seen == [(item, 0) for item in range(6)]

        def escaped(*_arguments: object, **_options: object) -> list[int]:
            raise KeyboardInterrupt  # as if raised before the pool was quiet: on_quiet never called

        def install(_pool: object) -> object:
            raise KeyboardInterrupt  # a SIGINT handled just before the pool's handler took over: nothing ran

        with monkeypatch.context() as patch:
            patch.setattr(_pool._Pool, "install", install)
            with pytest.raises(KeyboardInterrupt):
                run_batch(owner, work, list(range(6)), 4)
        assert owner._flights == 0
        monkeypatch.setattr(_pool, "run_ordered", escaped)
        with pytest.raises(KeyboardInterrupt):
            run_batch(owner, work, list(range(6)), 4)
        assert owner._flights == 1  # the owner now fail-stops instead of closing
        owner.leave_flight()


@pytest.mark.parametrize(("failing", "expected"), [(4, [0, 1, 3]), (6, [0, 1, 2, 3, 4, 5, 7])])
def test_run_batch_reports_results_held_back_behind_a_serial_item_when_one_fails(
    tmp_path: Path, failing: int, expected: list[int]
) -> None:
    ws = v3_workspace(tmp_path / "a")
    emitted: list[int] = []
    serial = {2, 6}
    earlier = {item for item in range(failing) if item not in serial}
    finished: set[int] = set()
    done = threading.Condition()

    def work(item: int) -> int:
        if item == failing:
            # Fail only once the earlier pooled items finished: the pool starts no item after a failure, so
            # without this wait a worker could take one of them only to find the pool already stopped.
            with done:
                assert done.wait_for(lambda: earlier <= finished, timeout=30)
            raise Boom
        with done:
            finished.add(item)
            done.notify_all()
        return item

    with cli_owner(ws) as owner, pytest.raises(Boom):
        run_batch(owner, work, list(range(8)), 4, serial=serial, emit=lambda index, _result: emitted.append(index))
    # A pooled failure (4): 3 finished but waited behind the serial 2, which never ran. A serial failure (6): the
    # pooled 7 finished long before.
    assert emitted == expected


def test_run_batch_runs_serial_items_last_in_the_calling_thread_and_emits_in_order(tmp_path: Path) -> None:
    ws = v3_workspace(tmp_path / "a")
    caller = threading.get_ident()
    ran: list[tuple[int, bool]] = []
    emitted: list[tuple[int, int]] = []

    def work(item: int) -> int:
        ran.append((item, threading.get_ident() == caller))
        return item * 10

    with cli_owner(ws) as owner:
        results = run_batch(owner, work, list(range(8)), 4, serial={1, 5}, emit=lambda i, r: emitted.append((i, r)))
        assert results == [item * 10 for item in range(8)]
        assert emitted == [(item, item * 10) for item in range(8)]
        # The pooled items ran in pool threads, before the serial ones, which ran in order in this thread.
        assert ran[-2:] == [(1, True), (5, True)] and not any(here for item, here in ran[:-2])
        ran.clear()
        run_batch(owner, work, list(range(8)), 1, serial={1, 5})
        assert ran == [(item, True) for item in range(8)]


# -- interrupts ----------------------------------------------------------------------------------------------------


class Parked:
    """A fault hook that parks pool threads at the renames *matches* selects until released."""

    def __init__(self, matches: Callable[[_fs.Loc, _fs.Loc], bool], expected: int) -> None:
        self.matches = matches
        self.expected = expected
        self.count = 0
        self.lock = threading.Lock()
        self.release = threading.Event()

    def __call__(self, op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if threading.current_thread() is threading.main_thread() or (op, phase) != ("rename", "before"):
            return
        assert src is not None and dst is not None
        if not self.matches(src, dst):
            return
        with self.lock:
            self.count += 1
        self.release.wait(PATIENCE)


def interrupt_twice(parked: Parked, pools: list[_pool._Pool[Any, Any]]) -> threading.Thread:
    """Send two SIGINTs to the main thread once the expected pool threads are parked, the second once a pool of
    *pools* (those created meanwhile) is draining, then let them go."""

    main = threading.main_thread().ident
    assert main is not None

    def run() -> None:
        deadline = time.monotonic() + PATIENCE
        while parked.count < parked.expected and time.monotonic() < deadline:
            time.sleep(0.01)
        signal.pthread_kill(main, signal.SIGINT)
        # Until the parked pool is draining and said what it waits for: released earlier, it would start more.
        while not any(pool.noted for pool in pools) and time.monotonic() < deadline:
            time.sleep(0.01)
        signal.pthread_kill(main, signal.SIGINT)
        time.sleep(0.2)  # the second interrupt is absorbed by the drain, before or after the release
        parked.release.set()

    helper = threading.Thread(target=run)
    helper.start()
    return helper


@pytest.fixture
def default_sigint() -> Iterator[None]:
    if threading.current_thread() is not threading.main_thread():
        pytest.skip("signals reach only the main thread")
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    yield
    signal.signal(signal.SIGINT, previous)


def interrupted(root: Path, parked: Parked, *argv: str) -> None:
    pools: list[_pool._Pool[Any, Any]] = []
    install = _pool._Pool.install

    def recording(pool: _pool._Pool[Any, Any]) -> object:
        pools.append(pool)
        return install(pool)

    _fs.set_fault_injector(parked)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(_pool._Pool, "install", recording)
        helper = interrupt_twice(parked, pools)
        try:
            with pytest.raises(KeyboardInterrupt):
                transfer(root, *argv)
        finally:
            parked.release.set()
            helper.join()
            _fs.set_fault_injector(None)
    assert parked.count == parked.expected


def job_counts(*directories: Path) -> Counter[str]:
    return Counter(
        json.loads(path.read_bytes())["id"] for directory in directories for path in directory.rglob("job.json")
    )


def assert_quiet(ws: Workspace) -> None:
    """Nothing owned, no scratch, no owner left registered."""

    assert not [path for path in (ws.jobs / "owned").rglob("*")]
    assert not list((ws.control / "tmp").iterdir())
    assert not [item for item in _kernel.list_owners(ws) if item.record is not None and item.tombstone is None]


def test_two_interrupts_while_holds_extract_lose_strand_and_duplicate_nothing(
    tmp_path: Path, default_sigint: None, capsys: pytest.CaptureFixture[str]
) -> None:
    source = v3_workspace(tmp_path / "a")
    source.configure_settings({"io.workers": 4})
    plan = Plan()
    plan.singles(10)
    plan.populate(source)
    parked = Parked(lambda src, dst: "/jobs/owned/" in str(src.path) and ".hold." in str(dst.path), 4)
    interrupted(tmp_path, parked, "--hold", "--json", "--state", "succeeded", "a", "b")
    assert capsys.readouterr().err.count("running operations to finish") == 1
    assert job_counts(source.root) == Counter(dict.fromkeys(plan.ids(), 1))
    held = [member for item in _moving.held(source) for member in item.members]
    assert len(held) == 4  # exactly the four parked holds finished; nothing after the interrupt started
    assert_quiet(source)


def test_two_interrupts_while_adoptions_discard_away_copies_lose_and_duplicate_nothing(
    tmp_path: Path, default_sigint: None, capsys: pytest.CaptureFixture[str]
) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    source.configure_settings({"io.workers": 4})
    plan = Plan()
    plan.singles(10)
    plan.populate(source)
    assert transfer(tmp_path, "--keep", "--state", "succeeded", "a", "b") == 0
    capsys.readouterr()
    # Back from b: each adoption claims the away copy in a and discards it through the trash.
    parked = Parked(lambda src, dst: "/jobs/owned/" in str(src.path) and dst.path.name.startswith("trash."), 4)
    interrupted(tmp_path, parked, "--state", "succeeded", "b", "a")
    assert job_counts(source.jobs) == Counter(dict.fromkeys(plan.ids(), 1)) and not job_counts(target.jobs)
    states = {job_id: str(getattr(find(source, job_id), "state", None)) for job_id in plan.ids()}
    # The four parked adoptions finished: those jobs are back, their holds gone; the others stay away, still held.
    assert Counter(states.values()) == Counter({"succeeded": 4, "away": 6})
    held = job_counts(target.control / "transfers")
    assert held == Counter({job_id: 1 for job_id, state in states.items() if state == "away"})
    assert_quiet(source)
    assert_quiet(target)


# -- SIGKILL --------------------------------------------------------------------------------------------------------

_CHILD = """
import pathlib, sys, threading, time
from httk.core.cli import CLIContext
from httk.workflow import _fs
from httk.workflow.workflow_cli import command

markers, cwd, mode = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3]
lock, parked = threading.Lock(), [0]


def wanted(src, dst):
    if mode == "hold":
        return "/jobs/owned/" in str(src.path) and ".hold." in str(dst.path)
    if mode == "release-keep":
        return ".release-keep." in str(src.path) and "/jobs/away/" in str(dst.path)
    return ".adopt." in str(src.path) and "/jobs/" in str(dst.path)


def hook(op, at, src, dst):
    if threading.current_thread() is threading.main_thread() or (op, at) != ("rename", "before"):
        return
    if wanted(src, dst):
        with lock:
            parked[0] += 1
            (markers / f"parked-{parked[0]}").touch()
        time.sleep(3600)


_fs.set_fault_injector(hook)
sys.exit(command(sys.argv[4:], CLIContext("httk", cwd)))
"""


def killed_while_parked(tmp_path: Path, ws: Workspace, mode: str, argv: list[str], purpose: str) -> str:
    """Run *argv* in a child process, SIGKILL it once two pool threads are parked, and return its owner id."""

    markers = tmp_path / "markers"
    markers.mkdir()
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(filter(None, [str(SOURCE), os.environ.get("PYTHONPATH")])),
    }
    process = subprocess.Popen(
        [sys.executable, "-c", _CHILD, str(markers), str(tmp_path), mode, *argv], env=environment
    )
    try:
        deadline = time.monotonic() + PATIENCE * 2
        while len(list(markers.iterdir())) < 2 and time.monotonic() < deadline and process.poll() is None:
            time.sleep(0.02)
        assert len(list(markers.iterdir())) >= 2
    finally:
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=PATIENCE)
    assert process.returncode == -signal.SIGKILL
    (dead,) = [item.owner_id for item in _kernel.list_owners(ws) if item.record is not None]
    in_flight = [path for path in (ws.control / "tmp").iterdir() if path.name.startswith(f"{dead}.{purpose}.")]
    assert len(in_flight) >= 2
    _kernel.attest_dead(ws, dead, by="operator", evidence=[], reason="test kill")
    with cli_owner(ws) as owner:
        _kernel.recover(ws, owner, dead)
    return dead


def assert_once_and_intact(plan: Plan, *directories: Path) -> None:
    assert job_counts(*directories) == Counter(dict.fromkeys(plan.ids(), 1))
    for directory in directories:
        for path in directory.rglob("input.txt"):
            job_id = json.loads((path.parent / "job.json").read_bytes())["id"]
            assert path.read_text(encoding="utf-8") == f"payload of {job_id}"


@pytest.mark.timing
def test_a_sigkilled_pooled_adoption_is_recovered_exactly_once(tmp_path: Path) -> None:
    source, target = v3_workspace(tmp_path / "a"), v3_workspace(tmp_path / "b")
    target.configure_settings({"io.workers": 4})
    plan = Plan()
    plan.singles(8)
    plan.populate(source)
    bundles = eject_all(source, plan, tmp_path / "out")
    argv = ["job", "adopt", "--no-durable", "--workspace", str(target.root), *map(str, bundles.values())]
    killed_while_parked(tmp_path, target, "adopt", argv, "adopt")
    assert_once_and_intact(plan, target.jobs, tmp_path / "out")
    assert_quiet(target)


@pytest.mark.timing
def test_a_sigkilled_pooled_hold_is_recovered_exactly_once(tmp_path: Path) -> None:
    source = v3_workspace(tmp_path / "a")
    source.configure_settings({"io.workers": 4})
    plan = Plan()
    plan.singles(8)
    plan.populate(source)
    argv = ["job", "move", "--no-durable", "--hold", "--state", "succeeded", "a", "b"]
    killed_while_parked(tmp_path, source, "hold", argv, "hold")
    assert_once_and_intact(plan, source.jobs, source.control / "transfers")
    assert_quiet(source)


@pytest.mark.timing
def test_a_sigkilled_pooled_kept_release_is_recovered_exactly_once(tmp_path: Path) -> None:
    source = v3_workspace(tmp_path / "a")
    source.configure_settings({"io.workers": 4})
    plan = Plan()
    plan.singles(8)
    plan.populate(source)
    assert transfer(tmp_path, "--hold", "--state", "succeeded", "a", "b") == 0
    releases = [part for item in _moving.held(source) for part in ("--release", item.transfer_id)]
    destination = "22222222-2222-4222-8222-222222222222"
    argv = ["job", "move", "--no-durable", *releases, "--keep", "--destination-id", destination, "a"]
    killed_while_parked(tmp_path, source, "release-keep", argv, "release-keep")
    assert_once_and_intact(plan, source.jobs, source.control / "transfers")
    assert_quiet(source)
    # The recovered releases finished keeping their jobs as away copies; the others are still held.
    states = Counter(getattr(find(source, job_id), "state", "held") for job_id in plan.ids())
    held = {member for item in _moving.held(source) for member in item.members}
    assert states["away"] >= 2 and states["away"] + len(held) == 8
