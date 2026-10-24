"""Self-carried workflows: the framed digest, the ``job.json`` member and the per-job view."""

import json
import os
from pathlib import Path
from typing import cast

import pytest
from httk.core.cli import CLIContext
from httk.core.digests import tree_digest

from conftest import register_ws
from httk.workflow import _kernel, _store
from httk.workflow._carried import carried_digest
from httk.workflow._job import JobDefinition
from httk.workflow.errors import FormatError
from httk.workflow.models import is_workflow_id
from httk.workflow.runtime_builders import JobSpec
from httk.workflow.scaffold import ScaffoldedJob, new_job, new_jobs
from httk.workflow.workflow_cli import command
from test_call_slot_bindings import _callee, _caller
from test_job_creation import assert_nothing_left, install, raw_package, workspace_at
from v3_helpers import cli_owner, job_mapping, workspace


def _tree(root: Path, files: dict[str, bytes]) -> Path:
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return root


def test_framing_separates_trees_the_core_digest_confuses(tmp_path: Path) -> None:
    one = _tree(tmp_path / "one", {"a": b"x" + b"F\0b\0" + b"y"})
    two = _tree(tmp_path / "two", {"a": b"x", "b": b"y"})
    assert tree_digest(one) == tree_digest(two)  # the core digest does not length-frame contents
    assert carried_digest(one) != carried_digest(two)


def test_digest_is_stable_and_covers_exec_bit_and_directories(tmp_path: Path) -> None:
    tree = _tree(tmp_path / "t", {"run": b"#!/bin/sh\n", "sub/data": b"1"})
    first = carried_digest(tree)
    assert first == carried_digest(tree) and len(first) == 64
    os.chmod(tree / "run", 0o755)
    assert carried_digest(tree) != first
    os.chmod(tree / "run", 0o644)
    (tree / "empty").mkdir()
    assert carried_digest(tree) != first


@pytest.mark.parametrize("kind", ["symlink", "fifo", "hardlink", "pycache", "pycache_file"])
def test_refusals_name_the_path(tmp_path: Path, kind: str) -> None:
    tree = _tree(tmp_path / "t", {"run": b"x"})
    (tree / "sub").mkdir()
    if kind == "symlink":
        (tree / "sub" / "bad").symlink_to("../run")
    elif kind == "fifo":
        os.mkfifo(tree / "sub" / "bad")
    elif kind == "hardlink":
        (tmp_path / "outside").write_bytes(b"x")
        os.link(tmp_path / "outside", tree / "sub" / "bad")
    elif kind == "pycache":
        (tree / "sub" / "__pycache__").mkdir()
    else:
        (tree / "sub" / "__pycache__").write_bytes(b"")
    with pytest.raises(FormatError, match="sub/"):
        carried_digest(tree)


def test_root_symlink_is_refused_and_missing_root_raises(tmp_path: Path) -> None:
    tree = _tree(tmp_path / "t", {"run": b"x"})
    (tmp_path / "link").symlink_to(tree)
    with pytest.raises(FormatError):
        carried_digest(tmp_path / "link")
    with pytest.raises(FileNotFoundError):
        carried_digest(tmp_path / "absent")


SHA = "ab" * 32
CARRIED_ID = f"carried:demo@{SHA[:12]}"
PINNED = "git+https://example.org/w.git@" + "c" * 40


def _carried(**changes: object) -> dict[str, object]:
    return {
        "tree_sha256": SHA,
        "runner": {"command": ["{package}/run.sh", "--fast"]},
        "steps": ["start", "finish"],
        "initial_step": "start",
        "calls": {"relax": "local:relax"},
        "slots": {"band": None, "dos": "local:dos"},
        "requires": ["httk-workflow>=2.1"],
        "source": None,
        **changes,
    }


def _job(carried: object = None, *, workflow_id: str = CARRIED_ID, **changes: object) -> dict[str, object]:
    mapping = job_mapping((workflow_id, "demo"), {})
    if carried is not None:
        mapping["carried"] = carried
    return {**mapping, **changes}


def test_carried_member_round_trips() -> None:
    job = JobDefinition.from_mapping(_job(_carried(source=PINNED)))
    assert job.carried is not None and job.carried["tree_sha256"] == SHA
    assert job.as_mapping()["carried"] == _carried(source=PINNED)
    assert JobDefinition.from_bytes(job.encode()) == job
    entry = JobDefinition.from_mapping(_job(_carried(runner={"entry": "run"})))
    assert JobDefinition.from_mapping(entry.as_mapping()) == entry
    spec = JobSpec(name="demo", workflow_id=CARRIED_ID, workflow_name="demo", carried=_carried())
    assert spec.as_mapping()["carried"] == _carried()
    assert "carried" not in JobSpec(name="demo", workflow_id="local:demo", workflow_name="demo").as_mapping()


@pytest.mark.parametrize(
    ("mapping", "match"),
    [
        (_job(), "exactly when"),  # a carried: id without the member
        (_job(_carried(), workflow_id="local:demo"), "exactly when"),
        (_job(_carried(), workflow_id=f"carried:demo@{'0' * 12}"), "workflow.id must be"),
        (_job(_carried(), workflow={"id": CARRIED_ID, "name": "other"}), "workflow.id must be"),
        (_job({**_carried(), "extra": 1}), "members differ"),
        (_job({key: value for key, value in _carried().items() if key != "source"}), "members differ"),
        (_job(_carried(tree_sha256="AB" * 32)), "tree_sha256"),
        (_job(_carried(runner={"entry": "run", "command": ["x"]})), "exactly"),
        (_job(_carried(runner={})), "exactly"),
        (_job(_carried(runner={"entry": "sub/run"})), "one package member"),
        (_job(_carried(runner={"entry": ".."})), "one package member"),
        (_job(_carried(runner={"entry": ""})), "nonempty"),
        (_job(_carried(runner={"command": []})), "nonempty array"),
        (_job(_carried(runner={"command": "{package}/run"})), "nonempty array"),
        (_job(_carried(runner={"command": ["{artifacts}/run"]})), "artifacts"),
        (_job(_carried(runner={"command": ["/bin/run"]})), "program"),
        (_job(_carried(steps=[])), "nonempty array"),
        (_job(_carried(steps="start")), "nonempty array"),
        (_job(_carried(steps=["finish"], initial_step="finish")), "initial_step 'start'"),
        (_job(_carried(initial_step="other")), "carried.initial_step"),
        (_job(_carried(steps=["start", "a/b"])), "step name"),
        (_job(_carried(calls={"relax": CARRIED_ID})), "installed workflow id"),
        (_job(_carried(calls={"relax": "relax"})), "installed workflow id"),
        (_job(_carried(calls={"relax": "./path"})), "not a path"),
        (_job(_carried(calls=[])), "object"),
        (_job(_carried(slots={"band": 3})), "carried.slots.band"),
        (_job(_carried(slots={"band": "a b"})), "carried.slots.band"),
        (_job(_carried(slots={"no/label": None})), "alias"),
        (_job(_carried(requires="x>=1")), "requires"),
        (_job(_carried(requires=["x"])), "requires"),
        (_job(_carried(source="/some/dir")), "source"),
        (_job(_carried(source="git+https://example.org/w.git@main")), "source"),
        (_job(_carried(source=3)), "source"),
    ],
)
def test_carried_member_refusals(mapping: dict[str, object], match: str) -> None:
    with pytest.raises(FormatError, match=match):
        JobDefinition.from_mapping(mapping)


def test_carried_ids_are_never_installed(tmp_path: Path) -> None:
    ws = workspace(tmp_path / "ws")
    assert is_workflow_id(CARRIED_ID)
    assert _store.lookup(ws, CARRIED_ID) is None
    with cli_owner(ws) as owner, pytest.raises(ValueError, match="never installed"):
        _store.install(ws, owner, CARRIED_ID)
    entry = tmp_path / "entry"
    (entry / "package").mkdir(parents=True)
    (entry / "install.json").write_text(
        json.dumps(
            {"format": "httk-workflow-install", "format_version": 1, "id": CARRIED_ID, "reference": None}
            | {"calls": {}, "slots": {}, "tree_sha256": SHA}
        )
    )
    with cli_owner(ws) as owner, pytest.raises(ValueError, match="never installed"):
        _store.install_copy(ws, owner, entry, source_label="test")


def test_carried_view_is_thawed_and_has_no_store_tree(tmp_path: Path) -> None:
    payload = tmp_path / "job"
    payload.mkdir()
    job = JobDefinition.from_mapping(_job(_carried()))
    assert _store.carried(job, payload) is None  # no workflow/
    (payload / "workflow").symlink_to(tmp_path)
    assert _store.carried(job, payload) is None  # never followed
    (payload / "workflow").unlink()
    (payload / "workflow").mkdir()
    view = _store.carried(job, payload)
    assert view is not None and view.carried
    assert (view.id, view.name, view.directory, view.package) == (
        CARRIED_ID,
        "demo",
        payload / "workflow",
        payload / "workflow",
    )
    assert view.record == {**_carried(), "build": False}
    assert (
        type(view.record["runner"]) is dict and type(cast(dict[str, object], view.record["runner"])["command"]) is list
    )
    assert type(view.record["requires"]) is list and type(view.record["slots"]) is dict
    with pytest.raises(ValueError, match="no store installation"):
        _ = view.installed_package
    with pytest.raises(ValueError, match="never built"):
        view.build_dir("any")
    installed = JobDefinition.from_mapping(_job(workflow_id="local:demo"))
    assert _store.carried(installed, payload) is None


# Creation: new_job(carry=True) and httk job new --carry.

_HOOK = """from pathlib import Path


def instantiate(ctx):
    (ctx.payload / "made.txt").write_text("hooked", encoding="utf-8")
    ctx.parameters["hook_dir"] = str(Path(__file__).parent)
    if ctx.tag == "plant":
        (ctx.payload / "workflow").mkdir()
"""
_EXEC_HOOK = """#!/usr/bin/env python3
import json, sys
from pathlib import Path
json.load(sys.stdin)
Path("exec.txt").write_text(str(Path(__file__).parent), encoding="utf-8")
print(json.dumps({"parameters": {"exec": True}}))
"""


def _payload_job(job: ScaffoldedJob) -> JobDefinition:
    return JobDefinition.from_path(job.payload / "job.json")


def test_new_job_carries_a_package_directory_and_installs_nothing(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    package = raw_package(tmp_path / "package", "tests.carried")
    (package / "data").mkdir()
    (package / "data" / "table.txt").write_text("1 2\n", encoding="utf-8")
    job = new_job(ws, package, carry=True, tag="one")
    tree = job.payload / "workflow"
    assert (tree / "run").read_bytes() == (package / "run").read_bytes() and os.access(tree / "run", os.X_OK)
    assert (tree / "data" / "table.txt").read_text(encoding="utf-8") == "1 2\n"
    assert not os.access(tree / "data" / "table.txt", os.X_OK)
    definition = _payload_job(job)
    digest = carried_digest(tree)
    assert digest == carried_digest(package)
    assert definition.workflow_id == job.workflow == f"carried:tests.carried@{digest[:12]}"
    assert definition.workflow_name == "tests.carried"
    assert definition.as_mapping()["carried"] == {
        "tree_sha256": digest,
        "runner": {"entry": "run"},
        "steps": ["start"],
        "initial_step": "start",
        "calls": {},
        "slots": {},
        "requires": [],
        "source": None,
    }
    assert _store.list_installed(ws) == []
    assert_nothing_left(ws)
    # Installed workflows are carried from their store tree, with nothing new installed.
    install(ws, package)
    again = _payload_job(new_job(ws, "tests.carried", carry=True))
    assert again.workflow_id == definition.workflow_id and again.carried == definition.carried
    assert [found.id for found in _store.list_installed(ws)] == ["local:tests.carried"]


def test_a_campaign_gives_every_job_its_own_copy(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    package = raw_package(tmp_path / "package", "tests.carried", extra='entry = "run"\n')
    jobs = list(new_jobs(ws, package, [{"tag": f"j{index}"} for index in range(3)], carry=True))
    trees = [job.payload / "workflow" for job in jobs]
    assert len({os.stat(tree / "run").st_ino for tree in trees}) == 3
    assert len({carried_digest(tree) for tree in trees}) == 1
    assert len({_payload_job(job).workflow_id for job in jobs}) == 1
    assert_nothing_left(ws)


def test_instantiate_hooks_run_from_the_pinned_snapshot(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    package = raw_package(tmp_path / "package", "tests.carried", extra='\n[workflow.instantiate]\nfile = "hook.py"\n')
    (package / "hook.py").write_text(_HOOK, encoding="utf-8")
    job = new_job(ws, package, carry=True)
    definition = _payload_job(job)
    assert (job.payload / "made.txt").read_text(encoding="utf-8") == "hooked"
    hook_dir = Path(cast(str, definition.parameters["hook_dir"]))
    assert hook_dir != package and not hook_dir.is_relative_to(job.payload)  # the owner's snapshot
    assert definition.carried is not None
    assert carried_digest(job.payload / "workflow") == definition.carried["tree_sha256"]
    with pytest.raises(ValueError, match="instantiate hook created workflow/"):
        new_job(ws, package, carry=True, tag="plant")
    executable = raw_package(tmp_path / "exec", "tests.carried.exec", extra='\n[workflow.instantiate]\nfile = "hook"\n')
    (executable / "hook").write_text(_EXEC_HOOK, encoding="utf-8")
    (executable / "hook").chmod(0o755)
    job = new_job(ws, executable, carry=True)
    assert _payload_job(job).parameters["exec"] is True
    assert Path((job.payload / "exec.txt").read_text(encoding="utf-8")) != executable
    assert carried_digest(job.payload / "workflow") == carried_digest(executable)
    assert _store.list_installed(ws) == []
    assert_nothing_left(ws)


def test_carry_refusals(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    with pytest.raises(ValueError, match="installs nothing"):
        new_job(ws, raw_package(tmp_path / "plain", "tests.carried"), carry=True, install=True)
    legacy = raw_package(tmp_path / "legacy", "tests.carried.v1")
    (legacy / "httk_workflow.toml").write_text(
        '[workflow]\nname = "tests.carried.v1"\n[workflow.runner]\nformat = "httk-v1"\n', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="workflow realization, which a job cannot carry"):
        new_job(ws, legacy, carry=True)
    built = raw_package(
        tmp_path / "built", "tests.carried.built", extra='\n[workflow.build]\ncommand = "true"\nartifacts = ["x"]\n'
    )
    with pytest.raises(ValueError, match=r"\[workflow.build\]"):
        new_job(ws, built, carry=True)
    caller = raw_package(
        tmp_path / "caller", "tests.carried.caller", extra='\n[workflow.calls]\nhelper = "tests.helper"\n'
    )
    with pytest.raises(ValueError, match="tests.helper, which is not installed"):
        new_job(ws, caller, carry=True)
    helper = install(ws, raw_package(tmp_path / "helper", "tests.helper"))
    called = _payload_job(new_job(ws, caller, carry=True))
    assert called.carried is not None and called.carried["calls"] == {"helper": helper.id}
    cached = raw_package(tmp_path / "cached", "tests.carried.cached")
    (cached / "__pycache__").mkdir()
    with pytest.raises(FormatError, match="__pycache__"):
        new_job(ws, cached, carry=True)
    linked = raw_package(tmp_path / "linked", "tests.carried.linked")
    (linked / "alias").symlink_to("run")
    with pytest.raises(ValueError, match="symlink"):
        new_job(ws, linked, carry=True)
    assert [found.id for found in _store.list_installed(ws)] == [helper.id]
    assert_nothing_left(ws)


def test_carried_call_slots_bind_installed_workflows_only(tmp_path: Path) -> None:
    ws = workspace_at(tmp_path / "ws")
    caller = _caller(tmp_path / "caller", default="tests.binding.pair")
    with pytest.raises(ValueError, match="not installed"):
        new_job(ws, caller, carry=True)
    pair = install(ws, _callee(tmp_path / "pair"))
    definition = _payload_job(new_job(ws, caller, carry=True))
    assert definition.carried is not None and definition.carried["slots"] == {"forces.dft": pair.id}
    assert definition.as_mapping()["calls"] == {"forces.dft": {"workflow": pair.id, "parameters": {}}}


def test_cli_from_command_carry(tmp_path: Path) -> None:
    context = CLIContext("httk", tmp_path)
    ws = workspace_at(tmp_path / "ws")
    name = register_ws(context, ws.root, "w")
    argv = ["job", "new", "--workspace", name, "--from-command", "true", "--carry", "--tag", "cmd"]
    assert command(argv, context) == 0
    (ref,) = list(_kernel.list_jobs(ws, "ready"))
    definition = JobDefinition.from_path(ref.path / "job.json")
    assert definition.workflow_id.startswith("carried:command@") and definition.carried is not None
    assert definition.carried["runner"] == {"entry": "run"}
    assert os.access(ref.path / "workflow" / "run", os.X_OK)
    assert carried_digest(ref.path / "workflow") == definition.carried["tree_sha256"]
    assert _store.list_installed(ws) == []
    assert command(["job", "new", "--workspace", name, "--from-command", "true", "--carry", "--install"], context) != 0
