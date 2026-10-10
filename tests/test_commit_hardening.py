"""A job cannot redirect, stall or crash the manager through the outcome it publishes.

The commit validates the published children and the committed transactions of
a quiescent job with a bounded walk that never follows a link, then renames
them into place. Special files, hard links and trusted entries planted in a
child fail that parent alone; a symlink is an opaque leaf that is moved, never
followed; and a transaction can never reach through a link the job planted in
its own payload.
"""

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

import v3_helpers as h
from httk.workflow import TaskManager, Workspace, _children, _kernel, _store
from test_commit_ownership import _gone
from test_crash_injection import _all_jobs, _crash_and_recover, _crash_at_rename, _log

pytestmark = pytest.mark.slow

#: Spawns ``parameters.labels`` children (each succeeds) and waits for them all to end, planting
#: ``parameters.plant`` first; a job with ``parameters.child`` (every child) simply succeeds. ``parameters.txn``
#: stages one committed transaction instead, planting a link to ``parameters.outside`` in the payload first.
_RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys, time, uuid

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job_dir = pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
job = json.loads((job_dir / "job.json").read_text())
parameters = job["parameters"]
outside = pathlib.Path(parameters.get("outside", "/nonexistent"))
plant = parameters.get("plant", "")
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2, action="succeed")
draft = control / "outcome.tmp.x"
draft.mkdir()
txn = parameters.get("txn")
if txn:
    (job_dir / "data").mkdir(exist_ok=True)
    staging = control / "txn" / "000001.tmp"
    (staging / "data").mkdir(parents=True)
    if txn == "staged-symlink":
        (staging / "data" / "installed").symlink_to(outside / "secret")
    else:
        (job_dir / "data" / "link").symlink_to(outside, target_is_directory=True)
        if txn == "file-below":
            (staging / "data" / "link").mkdir()
            (staging / "data" / "link" / "sentinel").write_text("new")
        elif txn == "tree-below":
            (staging / "data" / "link" / "tree").mkdir(parents=True)
            (staging / "data" / "link" / "tree" / "file").write_text("new")
        else:
            (staging / "data" / "link").write_text("a file replacing the link")
    staging.rename(control / "txn" / "000001")
elif not parameters.get("child") and context["step"] == "start":
    entries, references = [], []
    for label in parameters.get("labels", ["only"]):
        child_id, spawn_id = str(uuid.uuid4()), str(uuid.uuid4())
        key = label + "--" + child_id
        placement = parameters.get("placement", job["placement"] + "/" + label)
        child = dict(job, id=child_id, tag=label, name=label, placement=placement, parameters={"child": True})
        child["parent"] = {
            "workspace_id": context["workspace_id"], "job_id": job["id"], "job_key": context["job_key"],
            "placement": job["placement"], "activation_id": context["activation_id"], "spawn_id": spawn_id,
        }
        bundle = draft / "children" / "jobs" / key
        (bundle / "files").mkdir(parents=True)
        (bundle / "files" / "input").write_text("input of " + label)
        (bundle / "job.json").write_text(json.dumps(child))
        if plant == "fifo":
            os.mkfifo(bundle / "files" / "pipe")
        elif plant == "hard-link":
            os.link(bundle / "files" / "input", job_dir / "alias")
        elif plant in ("state.json", "seal.json"):
            (bundle / plant).write_text("{}")
        elif plant in ("logs", "attempts"):
            (bundle / plant).mkdir()
            (bundle / plant / "planted").write_text("planted")
        elif plant == "symlink":
            (bundle / "files" / "escape").symlink_to(outside, target_is_directory=True)
        elif plant == "linger":
            # A process left in the attempt's group, holding the staged child's job.json open to rewrite it later.
            script = (
                "import os, sys, time\\n"
                "descriptor = os.open(sys.argv[1], os.O_RDWR)\\n"
                "open(sys.argv[2], 'w').write(str(os.getpid()))\\n"
                "time.sleep(2.0)\\n"
                "os.pwrite(descriptor, b'{\\"tampered\\": true}' + b' ' * 4096, 0)\\n"
            )
            subprocess.Popen([sys.executable, "-c", script, str(bundle / "job.json"), str(outside / "pid")])
            while not (outside / "pid").exists():
                time.sleep(0.01)
        entries.append({"job_key": key, "label": label, "placement": placement, "spawn_id": spawn_id})
        references.append({"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": key,
                           "placement_hint": placement})
    (draft / "children" / "spawn.json").write_text(
        json.dumps({"format": "httk-workflow-spawn", "format_version": 2, "children": entries})
    )
    outcome.update(action="wait", next_step="gather", join={"children": references, "condition": "all_terminal"})
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""


@pytest.fixture()
def ws(tmp_path: Path) -> Workspace:
    return h.workspace(tmp_path / "ws")


@pytest.fixture()
def installed(ws: Workspace, tmp_path: Path) -> _store.Installed:
    return h.install(ws, tmp_path / "hardening", executables={"run": _RUNNER})


@pytest.fixture()
def outside(tmp_path: Path) -> Path:
    outside = tmp_path / "outside"
    (outside / "tree").mkdir(parents=True)
    (outside / "tree" / "kept").write_text("outside tree\n", encoding="utf-8")
    (outside / "sentinel").write_text("outside\n", encoding="utf-8")
    (outside / "secret").write_text("secret\n", encoding="utf-8")
    return outside


def _snapshot(directory: Path) -> dict[str, Any]:
    state: dict[str, Any] = {}
    for current, directories, files in os.walk(directory):
        for name in (*directories, *files):
            path = Path(current, name)
            content = path.read_bytes() if path.is_file() and not path.is_symlink() else None
            state[str(path.relative_to(directory))] = (path.lstat().st_mode, content)
    return state


def _spawner(ws: Workspace, installed: _store.Installed, **parameters: object) -> _kernel.JobRef:
    return h.submit(ws, installed, {"start": "spawn", "gather": "succeed"}, parameters=parameters)


def _healthy(ws: Workspace, installed: _store.Installed) -> _kernel.JobRef:
    return h.submit(
        ws, installed, {"start": "succeed"}, tag="healthy", placement="project/healthy", parameters={"child": True}
    )


def _outcome(ws: Workspace, job_id: str) -> tuple[str, str | None, str]:
    ref = h.find(ws, job_id)
    doc = h.state_of(ref)
    if ref.state != "failed":
        return ref.state, None, ""
    assert doc.failure is not None
    return ref.state, str(doc.failure["code"]), str(doc.failure["message"])


def _children_of(ws: Workspace, parent_id: str) -> list[_kernel.JobRef]:
    return [ref for ref in _all_jobs(ws) if ref.job_id != parent_id and ref.job_key.split("--")[0] != "healthy"]


@pytest.mark.parametrize(
    ("plant", "message"),
    [
        ("fifo", "a special file"),
        ("hard-link", "a hard-linked file"),
        ("state.json", "carries trusted entries"),
        ("seal.json", "carries trusted entries"),
        ("logs", "carries trusted entries"),
        ("attempts", "carries trusted entries"),
    ],
)
def test_hostile_child_content_fails_the_parent_and_the_manager_keeps_running(
    ws: Workspace, installed: _store.Installed, outside: Path, plant: str, message: str
) -> None:
    before = _snapshot(outside)
    parent = _spawner(ws, installed, plant=plant, outside=str(outside))
    healthy = _healthy(ws, installed)
    h.run(ws)

    state, code, text = _outcome(ws, parent.job_id)
    assert (state, code) == ("failed", "protocol_error")
    assert message in text, text
    assert _outcome(ws, healthy.job_id)[:2] == ("succeeded", None)
    # No child was published, and the refused one stays in the failed parent's attempt as evidence.
    assert not _children_of(ws, parent.job_id)
    failed = h.find(ws, parent.job_id)
    assert len(list(failed.path.glob("attempts/*/outcome.ready/children/jobs/*"))) == 1
    assert h.state_of(failed).children == ()
    assert _snapshot(outside) == before


def test_a_symlink_in_a_child_bundle_is_published_as_an_opaque_link_never_followed(
    ws: Workspace, installed: _store.Installed, outside: Path
) -> None:
    before = _snapshot(outside)
    parent = _spawner(ws, installed, plant="symlink", outside=str(outside))
    h.run(ws)

    assert _outcome(ws, parent.job_id)[:2] == ("succeeded", None)
    (child,) = _children_of(ws, parent.job_id)
    assert child.state == "succeeded"
    escape = child.path / "files" / "escape"
    # The link was moved as an entry: it is still a link, naming the same target, and nothing behind it changed.
    assert escape.is_symlink() and os.readlink(escape) == str(outside)
    assert _snapshot(outside) == before


def test_a_refused_spawn_placement_leaves_no_child(ws: Workspace, installed: _store.Installed) -> None:
    # A placement component that parses as a job key would nest the child inside another job.
    nested = f"project/other--{'0' * 8}-0000-4000-8000-{'0' * 12}"
    parent = _spawner(ws, installed, placement=nested)
    healthy = _healthy(ws, installed)
    h.run(ws)

    state, code, text = _outcome(ws, parent.job_id)
    assert (state, code) == ("failed", "protocol_error")
    assert "job key" in text, text
    failed = h.find(ws, parent.job_id)
    assert not (failed.path / ".httk-job" / "tree" / "spawns").exists()
    assert not _children_of(ws, parent.job_id)
    assert not list((ws.jobs / "ready").glob("project/other--*"))
    assert _outcome(ws, healthy.job_id)[:2] == ("succeeded", None)


def test_a_process_the_runner_left_behind_cannot_change_a_published_child(
    ws: Workspace, installed: _store.Installed, outside: Path
) -> None:
    """The commit runs only once the attempt's whole process group is gone (the quiescence rule).

    A process the runner left in its group still holds the staged child's
    ``job.json`` open and would rewrite it after the commit renamed the child
    into place; the manager kills the group before it commits, so the
    published child is exactly what the runner proposed.
    """

    parent = _spawner(ws, installed, plant="linger", outside=str(outside))
    started = time.monotonic()
    h.run(ws)

    assert _outcome(ws, parent.job_id)[:2] == ("succeeded", None)
    assert _gone(int((outside / "pid").read_text(encoding="utf-8")))
    (child,) = _children_of(ws, parent.job_id)
    document = json.loads((child.path / "job.json").read_text(encoding="utf-8"))
    assert document["id"] == child.job_id and "tampered" not in document
    # Give a survivor the time it would have needed to write.
    time.sleep(max(0.0, 2.5 - (time.monotonic() - started)))
    assert json.loads((child.path / "job.json").read_text(encoding="utf-8")) == document


@pytest.mark.parametrize(
    ("txn", "code"),
    [("file-below", "data_conflict"), ("tree-below", "data_conflict"), ("file-over", None)],
)
def test_a_symlink_in_the_payload_never_redirects_a_transaction(
    ws: Workspace, installed: _store.Installed, outside: Path, txn: str, code: str | None
) -> None:
    before = _snapshot(outside)
    job = h.submit(ws, installed, {"start": "succeed"}, parameters={"txn": txn, "outside": str(outside)})
    healthy = _healthy(ws, installed)
    h.run(ws)

    if code is None:
        # A file staged over the link replaces the link itself; nothing behind it is touched.
        assert _outcome(ws, job.job_id)[:2] == ("succeeded", None)
        link = h.find(ws, job.job_id).path / "data" / "link"
        assert not link.is_symlink() and link.read_text(encoding="utf-8") == "a file replacing the link"
    else:
        assert _outcome(ws, job.job_id)[:2] == ("failed", code)
        assert h.state_of(h.find(ws, job.job_id)).seal is None
    assert _snapshot(outside) == before
    assert _outcome(ws, healthy.job_id)[:2] == ("succeeded", None)


def test_a_staged_symlink_is_moved_as_an_opaque_leaf(ws: Workspace, installed: _store.Installed, outside: Path) -> None:
    before = _snapshot(outside)
    job = h.submit(ws, installed, {"start": "succeed"}, parameters={"txn": "staged-symlink", "outside": str(outside)})
    h.run(ws)

    assert _outcome(ws, job.job_id)[:2] == ("succeeded", None)
    installed_link = h.find(ws, job.job_id).path / "data" / "installed"
    # The link itself was moved into the payload; its target was never read or copied.
    assert installed_link.is_symlink() and os.readlink(installed_link) == str(outside / "secret")
    assert _snapshot(outside) == before


@pytest.mark.parametrize("interruption", ["before-the-first", "between-the-two"])
def test_a_commit_interrupted_while_publishing_children_publishes_each_exactly_once(
    ws: Workspace, installed: _store.Installed, interruption: str
) -> None:
    parent = _spawner(ws, installed, labels=["first", "second"])
    phase = "before" if interruption == "before-the-first" else "after"
    _crash_and_recover(ws, _crash_at_rename(phase, lambda src, dst: "/children/jobs/" in str(src.path)))

    interrupted = h.find(ws, parent.job_id)
    assert interrupted.state == "ready" and h.state_of(interrupted).commit is not None
    published = _children_of(ws, parent.job_id)
    assert len(published) == (0 if interruption == "before-the-first" else 1)
    # The child not yet published is still where the runner staged it, complete.
    staged = list(interrupted.path.glob("attempts/*/outcome.ready/children/jobs/*"))
    assert len(staged) == 2 - len(published)
    assert all((path / "job.json").is_file() and (path / "files" / "input").is_file() for path in staged)

    h.run(ws)

    assert _outcome(ws, parent.job_id)[:2] == ("succeeded", None)
    children = _children_of(ws, parent.job_id)
    assert sorted(ref.job_key.split("--")[0] for ref in children) == ["first", "second"]
    assert all(ref.state == "succeeded" for ref in children)
    for ref in children:
        label = ref.job_key.split("--")[0]
        assert (ref.path / "files" / "input").read_text(encoding="utf-8") == f"input of {label}"
        assert [line["event"] for line in _log(ref)].count("launched") == 1
    doc = h.state_of(h.find(ws, parent.job_id))
    assert sorted(str(entry["label"]) for entry in doc.children) == ["first", "second"]


def test_a_well_formed_spawn_publishes_its_child_by_rename_and_leaves_no_staging(
    ws: Workspace, installed: _store.Installed, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A child payload is renamed into place, not copied: the published inode is the staged one."""

    parent = _spawner(ws, installed)
    staged_inodes: dict[str, int] = {}
    real = _children.publish_children

    def record(job: _kernel.OwnedJob, plans: Any) -> Any:
        for plan in plans:
            staged = job.path / plan.staged / "job.json"
            if staged.exists():
                staged_inodes[plan.job_id] = staged.stat().st_ino
        return real(job, plans)

    monkeypatch.setattr(_children, "publish_children", record)
    with TaskManager(ws, heartbeat_interval=0.01) as manager:
        manager.run_until_idle(timeout=120)
        scratch_prefix = f"{manager.manager_id}."

    assert _outcome(ws, parent.job_id)[:2] == ("succeeded", None)
    (child,) = _children_of(ws, parent.job_id)
    assert child.state == "succeeded"
    assert staged_inodes == {child.job_id: (child.path / "job.json").stat().st_ino}
    assert hashlib.sha256((child.path / "job.json").read_bytes()).hexdigest()
    # The child inherited the parent's origin in the state.json written at publication.
    assert h.state_of(child).origin == "local"
    # The parent's attempt (and the staged child in it) is gone, and no scratch of the manager is left.
    assert not (h.find(ws, parent.job_id).path / "attempts").exists()
    tmp = ws.control / "tmp"
    leftovers = [name for name in os.listdir(tmp) if name.startswith(scratch_prefix)] if tmp.is_dir() else []
    assert not leftovers


def _batch(root: Path, operations: list[dict[str, object]]) -> Path:
    transaction = root / "batch"
    (transaction / "payload").mkdir(parents=True)
    (transaction / "manifest.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-transaction",
                "format_version": 2,
                "expected_data_generation": 0,
                "operations": operations,
            }
        ),
        encoding="utf-8",
    )
    return transaction


def test_a_runner_side_replay_with_paths_still_follows_a_symlinked_workdir_directory(tmp_path: Path) -> None:
    """The runner's own replayable workdir batch follows a symlinked directory of its workdir.

    That is the runner's business (a workdir may link to scratch); the
    manager's transactions never follow a link (see above).
    """

    from httk.core.digests import tree_digest

    from httk.workflow.runtime_builders import replay_transaction

    scratch = tmp_path / "scratch"
    scratch.mkdir()
    (scratch / "old").write_text("old\n", encoding="utf-8")
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    (workdir / "scratch").symlink_to(scratch, target_is_directory=True)
    operations: list[dict[str, object]] = [
        {
            "id": "file",
            "op": "put-file",
            "path": "scratch/out.txt",
            "source": "payload/out.txt",
            "sha256": hashlib.sha256(b"out\n").hexdigest(),
        },
        {"id": "tree", "op": "put-tree", "path": "scratch/tree", "source": "payload/tree"},
        {"id": "gone", "op": "remove", "path": "scratch/old"},
    ]
    transaction = _batch(tmp_path, operations)
    (transaction / "payload" / "out.txt").write_text("out\n", encoding="utf-8")
    (transaction / "payload" / "tree").mkdir()
    (transaction / "payload" / "tree" / "inner").write_text("inner\n", encoding="utf-8")
    operations[1]["sha256"] = tree_digest(transaction / "payload" / "tree")
    (transaction / "manifest.json").write_text(
        json.dumps(
            {
                "format": "httk-workflow-transaction",
                "format_version": 2,
                "expected_data_generation": 0,
                "operations": operations,
            }
        ),
        encoding="utf-8",
    )

    assert replay_transaction(transaction, workdir, expected_generation=0, durable=True)
    assert (scratch / "out.txt").read_text(encoding="utf-8") == "out\n"
    assert (scratch / "tree" / "inner").read_text(encoding="utf-8") == "inner\n"
    assert not (scratch / "old").exists()
    assert (transaction / "trash" / "gone" / "removed").read_text(encoding="utf-8") == "old\n"
    # Idempotent on replay, as before.
    assert replay_transaction(transaction, workdir, expected_generation=0)
