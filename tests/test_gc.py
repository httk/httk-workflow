"""Garbage collection of a workspace on the filesystem kernel.

Jobs are submitted and moved between states by a CLI owner (no runner is
needed); the leftovers each category collects are crafted in place and aged
with ``os.utime``.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from httk.workflow import TaskManager, Workspace, _fs, _kernel, _requests, _store
from httk.workflow._state import Release, StateDoc
from httk.workflow.gc import GC_CATEGORIES, collect_garbage, iter_report_rows
from v3_helpers import cli_owner, find, install, state_of, submit

_DAY = 86400.0
_WORKFLOW = ("demo--0123456789abcdef", "demo")
_TOKEN = "abcdefghijklmnop"


def _workspace(root: Path, **retention: object) -> Workspace:
    policy = {"visibility_deadline_seconds": 0.05, "retention": {"attempt_control_days": 1.0, **retention}}
    return Workspace.initialize(root, durable=False, policy=policy)


def _age(path: Path, days: float = 3.0) -> Path:
    moment = time.time() - days * _DAY
    os.utime(path, (moment, moment), follow_symlinks=False)
    return path


def _job(
    ws: Workspace, state: str, attempts: dict[str, float], placement: str = "p/0", current: str | None = None
) -> _kernel.JobRef:
    """A job in *state* holding ``attempts/<name>/`` aged by the given days; *current* is the one state.json names."""

    ref = submit(ws, _WORKFLOW, {"start": "succeed"}, placement=placement)
    with cli_owner(ws) as owner:
        owned = _kernel.claim(ws, owner, ref)
        assert owned is not None
        doc = StateDoc.empty(owned.job_id).next_activation("start", "initial")
        if current is not None:
            doc = doc.next_attempt("launch", unclean=False)
        for name, days in attempts.items():
            directory = owned.path / "attempts" / (str(doc.attempt["id"]) if doc.attempt and name == current else name)
            directory.mkdir(parents=True)
            (directory / "outcome.json").write_text("{}", encoding="utf-8")
            _age(directory, days)
        return owned.release(doc, Release(state, 500))


def _attempts(ws: Workspace, ref: _kernel.JobRef) -> list[str]:
    return sorted(os.listdir(find(ws, ref.job_id).path / "attempts"))


def _leftovers(ws: Workspace) -> dict[str, Path]:
    """Craft one aged entry of every category but ``attempt_control`` and ``dead_owners``."""

    requests = ws.control / "requests"
    requests.mkdir(exist_ok=True)
    malformed = requests / f"{uuid.uuid4()}.{uuid.uuid4()}.json"
    malformed.write_text("{not json", encoding="utf-8")
    orphan = _requests.post(
        ws, action="cancel", job_id=str(uuid.uuid4()), placement="p/9", operator="test", reason="gone"
    )
    temporary = requests / f".x.json.{_TOKEN}.tmp"
    temporary.write_text("", encoding="utf-8")
    trash = ws.control / "tmp" / f"trash.{_TOKEN}"
    (trash / "nested").mkdir(parents=True)
    (trash / "nested" / "file").write_text("x", encoding="utf-8")
    owner_id = uuid.uuid4().hex
    tombstone = ws.control / "owners" / owner_id / "dead.json"
    tombstone.parent.mkdir(parents=True)
    tombstone.write_text(json.dumps({"by": "probe"}), encoding="utf-8")
    log = ws.root / "logs" / "managers" / f"{owner_id}.log"
    log.parent.mkdir(parents=True)
    log.write_text("old manager\n", encoding="utf-8")
    placement = ws.jobs / "ready" / "empty" / "mirror"
    placement.mkdir(parents=True)
    entries = {
        "malformed": malformed,
        "orphan": orphan,
        "temporary": temporary,
        "trash": trash,
        "tombstone": tombstone,
        "log": log,
        "placement": placement,
    }
    for path in entries.values():
        _age(path)
    _age(tombstone, 40.0)  # owner_tombstone_days defaults to 30
    return entries


def test_a_dry_run_reports_everything_and_touches_nothing(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    entries = _leftovers(ws)
    ref = _job(ws, "succeeded", {"old": 3.0})
    report = collect_garbage(ws, dry_run=True)
    assert report.dry_run and report.removed == 0
    counts = {category.name: category.candidates for category in report.categories}
    assert counts == {
        "dead_owners": 0,
        "attempt_control": 1,
        "placement_directories": 0,  # pruning is a single rmdir pass, so a dry run reports none
        "requests": 2,
        "manager_logs": 1,
        "owner_tombstones": 1,
        "tmp_entries": 2,
    }
    assert all(path.exists() for path in entries.values())
    assert _attempts(ws, ref) == ["old"]
    assert report.bytes_reclaimed > 0


def test_each_category_collects_only_the_aged_entries(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    entries = _leftovers(ws)
    young = _requests.post(
        ws, action="cancel", job_id=str(uuid.uuid4()), placement="p/8", operator="test", reason="young"
    )
    succeeded = _job(ws, "succeeded", {"old": 3.0, "young": 0.0}, placement="p/1")
    report = collect_garbage(ws)
    assert {name: report.category(name).removed for name in GC_CATEGORIES} == {
        "dead_owners": 0,
        "attempt_control": 1,
        "placement_directories": 4,  # ready/empty/mirror and ready/empty, and ready/p/1 and ready/p the job left
        "requests": 2,
        "manager_logs": 1,
        "owner_tombstones": 1,
        "tmp_entries": 2,
    }
    assert not any(path.exists() for path in entries.values())
    assert not entries["tombstone"].parent.exists()
    assert young.exists()
    assert _attempts(ws, succeeded) == ["young"]
    # The job went back where it was, with the collection in its run log.
    doc = state_of(find(ws, succeeded.job_id))
    assert find(ws, succeeded.job_id).state == "succeeded" and doc.history_tail[-1]["event"] == "released"
    quarantined = sorted((ws.control / "quarantine").iterdir())
    assert len(quarantined) == 2
    reasons = sorted(json.loads((entry / "reason.json").read_text())["reason"] for entry in quarantined)
    assert reasons[0].startswith("malformed request") and reasons[1].startswith("no job ")
    assert all((entry / "entry").is_file() for entry in quarantined)
    # A second collection finds nothing more.
    assert collect_garbage(ws).removed == 0


def test_failed_and_cancelled_jobs_keep_the_attempt_their_state_names(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    # The attempt state.json names is kept even when another attempt directory is newer.
    failed = _job(ws, "failed", {"current": 5.0, "newer": 4.0}, placement="p/f", current="current")
    cancelled = _job(ws, "cancelled", {"current": 5.0}, placement="p/c", current="current")
    ready = _job(ws, "ready", {"current": 5.0, "newer": 4.0}, placement="p/r", current="current")
    unnamed = _job(ws, "failed", {"first": 5.0}, placement="p/u")
    assert collect_garbage(ws, dry_run=True, categories=("attempt_control",)).candidates == 4
    collect_garbage(ws, categories=("attempt_control",))
    for ref in (failed, cancelled):
        attempt = state_of(find(ws, ref.job_id)).attempt
        assert attempt is not None and _attempts(ws, ref) == [attempt["id"]]
    assert _attempts(ws, ready) == []
    assert _attempts(ws, unnamed) == []


def test_keep_disables_a_retention_category(tmp_path: Path) -> None:
    ws = Workspace.initialize(
        tmp_path / "ws",
        durable=False,
        policy={"visibility_deadline_seconds": 0.05, "retention": {"trash_days": "keep", "owner_tombstone_days": None}},
    )
    entries = _leftovers(ws)
    ref = _job(ws, "succeeded", {"old": 30.0})
    report = collect_garbage(ws)
    skipped = {item.name: item.skip_reason for item in report.categories if item.skipped}
    assert skipped == {
        "attempt_control": "retention.attempt_control_days keeps everything",
        "manager_logs": "retention.trash_days keeps everything",
        "owner_tombstones": "retention.owner_tombstone_days keeps everything",
    }
    assert entries["log"].exists() and entries["tombstone"].exists() and _attempts(ws, ref) == ["old"]
    assert report.skipped == tuple(f"{name}: {reason}" for name, reason in skipped.items())


def test_a_collection_can_select_only_named_categories(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    entries = _leftovers(ws)
    report = collect_garbage(ws, categories=("tmp_entries",))
    assert report.removed == 2 and not entries["temporary"].exists() and entries["log"].exists()
    assert [item.name for item in report.categories if not item.skipped] == ["tmp_entries"]
    with pytest.raises(ValueError, match="unknown gc categories: journal_segments"):
        collect_garbage(ws, categories=("journal_segments",))


def test_a_report_round_trips_through_json_and_rows(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    _leftovers(ws)
    report = collect_garbage(ws, dry_run=True)
    mapping = json.loads(json.dumps(report.as_mapping()))
    assert mapping["format"] == "httk-workflow-gc" and mapping["candidates"] == report.candidates
    assert [row["name"] for row in mapping["categories"]] == list(GC_CATEGORIES)
    rows = list(iter_report_rows(report))
    assert rows[-1] == ("total", report.candidates, report.removed, report.bytes_reclaimed)


_DIE = """
import os, sys
from pathlib import Path
from httk.workflow import Workspace, _kernel
ws = Workspace(Path(sys.argv[1]))
owner = _kernel.register_owner(ws, kind="cli", label="doomed", allocation=None, advertised={})
ref = next(iter(_kernel.list_jobs(ws, "ready")))
assert _kernel.claim(ws, owner, ref) is not None
owner.scratch("copy")
print(owner.owner_id, flush=True)
os._exit(0)
"""


def test_a_dead_same_host_owner_is_recovered(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    source = Path(__file__).resolve().parents[1] / "src"
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join([str(source), os.environ.get("PYTHONPATH", "")])}
    result = subprocess.run(
        [sys.executable, "-c", _DIE, str(ws.root)], capture_output=True, text=True, check=True, env=environment
    )
    dead = result.stdout.strip()
    assert find(ws, ref.job_id).state == _kernel.OWNED
    assert collect_garbage(ws, dry_run=True, categories=("dead_owners",)).category("dead_owners").candidates == 1
    report = collect_garbage(ws, categories=("dead_owners",))
    assert report.category("dead_owners").removed == 1
    assert find(ws, ref.job_id).state == "ready"
    assert sorted(os.listdir(ws.control / "owners" / dead)) == ["dead.json"]
    assert not any(name.startswith(dead) for name in os.listdir(ws.control / "tmp"))


def test_a_job_claimed_into_a_recovered_owner_is_returned(tmp_path: Path) -> None:
    # A falsely attested owner wakes after its recovery and claims: only dead.json is left, yet gc finds the job.
    ws = _workspace(tmp_path / "ws")
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    with cli_owner(ws) as recoverer:
        sleeper = cli_owner(ws)
        _kernel.attest_dead(ws, sleeper.owner_id, by="operator", evidence=[], operator="op")
        _kernel.recover(ws, recoverer, sleeper.owner_id)
    assert sorted(os.listdir(sleeper.path)) == ["dead.json"]
    assert _kernel.claim(ws, sleeper, ref) is not None
    assert collect_garbage(ws, dry_run=True, categories=("dead_owners",)).category("dead_owners").candidates == 1
    assert collect_garbage(ws, categories=("dead_owners",)).category("dead_owners").removed == 1
    assert find(ws, ref.job_id).state == "ready"
    assert sorted(os.listdir(sleeper.path)) == ["dead.json"]
    assert collect_garbage(ws, dry_run=True, categories=("dead_owners",)).category("dead_owners").candidates == 0


def test_an_interrupted_quarantine_is_finished_by_its_owner(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    with cli_owner(ws) as owner:
        scratch = owner.scratch("quarantine")
        (scratch / "taken.json").write_text("{}", encoding="utf-8")
    (entry,) = (ws.control / "quarantine").iterdir()
    assert (entry / "entry").read_text() == "{}"
    assert json.loads((entry / "reason.json").read_text())["reason"] == "an interrupted quarantine"


def test_a_manager_collects_in_the_background_every_gc_interval(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    entries = _leftovers(ws)
    with TaskManager(ws, heartbeat_interval=0.01, gc_interval=3600.0) as manager:
        manager.tick()
        assert not entries["temporary"].exists() and not entries["log"].exists()
        entries["temporary"].write_text("", encoding="utf-8")
        _age(entries["temporary"])
        manager.tick()
        assert entries["temporary"].exists()  # not again before gc_interval
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.tick()
    assert entries["temporary"].exists()  # no gc_interval, no background collection


def test_a_reinstall_interrupted_between_its_renames_keeps_the_old_tree_installed(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    installed = install(ws, tmp_path / "package")
    (tmp_path / "package" / "notes.txt").write_text("v2", encoding="utf-8")

    def interrupt(op: str, phase: str, src: _fs.Loc | None, dst: _fs.Loc | None) -> None:
        if op == "rename" and phase == "before" and dst is not None and dst.path == installed.directory:
            raise KeyboardInterrupt  # the new tree never lands

    _fs.set_fault_injector(interrupt)
    try:
        with pytest.raises(KeyboardInterrupt), cli_owner(ws) as owner:
            _store.install(ws, owner, tmp_path / "package")
    finally:
        _fs.set_fault_injector(None)
    (old,) = (path for path in (ws.root / "workflows").iterdir() if ".old." in path.name)
    assert (old / "install.json").is_file() and not installed.directory.exists()
    # The moved-aside tree is the only copy: lookups use it, and gc moves it back into place, never removes it.
    assert _store.lookup(ws, installed.id).directory == old  # type: ignore[union-attr]
    assert [found.directory for found in _store.list_installed(ws)] == [old]
    later = time.time() + 2 * _DAY
    assert collect_garbage(ws, now=later, dry_run=True, categories=("tmp_entries",)).candidates == 0
    assert old.exists()
    assert collect_garbage(ws, now=later, categories=("tmp_entries",)).removed == 0
    assert not old.exists() and (installed.directory / "install.json").is_file()
    assert _store.lookup(ws, installed.id).directory == installed.directory  # type: ignore[union-attr]


def test_gc_removes_a_moved_aside_tree_once_its_installation_exists(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    installed = install(ws, tmp_path / "package")
    aside = installed.directory.with_name(f"{installed.directory.name}.old.{'a' * 16}")
    _fs.copy_tree(installed.directory, aside, durable=False)
    assert [found.directory for found in _store.list_installed(ws)] == [installed.directory]
    assert collect_garbage(ws, dry_run=True, categories=("tmp_entries",)).candidates == 0  # moved aside just now
    assert collect_garbage(ws, now=time.time() + 2 * _DAY, categories=("tmp_entries",)).removed == 1
    assert not aside.exists() and installed.directory.is_dir()


def test_a_reinstall_replaces_the_tree_and_leaves_nothing_aside(tmp_path: Path) -> None:
    ws = _workspace(tmp_path / "ws")
    installed = install(ws, tmp_path / "package")
    (tmp_path / "package" / "notes.txt").write_text("v2", encoding="utf-8")
    with cli_owner(ws) as owner:
        _store.install(ws, owner, tmp_path / "package")
    assert (installed.package / "notes.txt").read_text(encoding="utf-8") == "v2"
    assert [path.name for path in (ws.root / "workflows").iterdir()] == [installed.directory.name]
    with cli_owner(ws) as owner:
        _store.uninstall(ws, owner, installed.id)
    assert list((ws.root / "workflows").iterdir()) == []


def test_hygiene_counts_an_old_push_copy_as_orphaned_scratch_but_not_a_fresh_one(tmp_path: Path) -> None:
    from httk.workflow.hygiene import _check_owners

    ws = _workspace(tmp_path / "ws")
    push = ws.control / "tmp" / f"push.{uuid.uuid4().hex}"
    (push / "package").mkdir(parents=True)
    assert _check_owners(ws.root, repair=False).details["orphaned_scratch"] == []
    _age(push)
    assert _check_owners(ws.root, repair=False).details["orphaned_scratch"] == [push.name]
