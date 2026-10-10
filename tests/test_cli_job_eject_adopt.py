"""The ``job eject`` and ``job adopt`` verbs."""

import json
import shutil
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow import Workspace, _kernel
from httk.workflow.registry import create_workspace
from httk.workflow.seals import seal_workspace
from httk.workflow.workflow_cli import command
from test_moving import assert_in, claim_root, find, single, tree
from v3_helpers import cli_owner


def registered(tmp_path: Path, label: str) -> tuple[Workspace, str]:
    name = f"{label}-{uuid.uuid4()}"
    create_workspace(name, tmp_path / label)
    workspace = Workspace(tmp_path / label, durable=False)
    workspace.set_policy({"visibility_deadline_seconds": 0.05})
    return workspace, name


def context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)


def test_eject_and_adopt_round_trip(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    target, target_name = registered(tmp_path, "b")
    plan = tree(source)
    root_id = str(plan[0][0]["id"])
    # A relative destination is taken from the working directory.
    argv = ["job", "eject", "--workspace", source_name, root_id, "out", "--tree", "--json"]
    assert command(argv, context(tmp_path)) == 0
    report = json.loads(capsys.readouterr().out)
    bundle = Path(report["destination"])
    assert bundle.parent == tmp_path / "out" and len(report["members"]) == 4
    assert find(source, root_id) is None
    assert command(["job", "adopt", "--workspace", target_name, str(bundle)], context(tmp_path)) == 0
    captured = capsys.readouterr()
    assert len(captured.out.splitlines()) == 4
    assert "not installed" in captured.err
    assert_in(target, plan)
    # A second adoption finds no bundle there any more.
    assert command(["job", "adopt", "--workspace", target_name, str(bundle)], context(tmp_path)) == 1


def test_adopt_json_reports_already_adopted(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    _, target_name = registered(tmp_path, "b")
    mapping, _, _ = single(source)
    assert (
        command(
            ["job", "eject", "--workspace", source_name, str(mapping["id"]), str(tmp_path / "out")], context(tmp_path)
        )
        == 0
    )
    assert "ejected 1 job(s)" in capsys.readouterr().out
    (bundle,) = (tmp_path / "out").iterdir()
    copy = tmp_path / "copy"
    copy.mkdir()
    shutil.copytree(bundle, copy / bundle.name, symlinks=True)
    assert command(["job", "adopt", "--workspace", target_name, str(bundle)], context(tmp_path)) == 0
    capsys.readouterr()
    assert (
        command(["job", "adopt", "--workspace", target_name, str(copy / bundle.name), "--json"], context(tmp_path)) == 0
    )
    document = json.loads(capsys.readouterr().out)
    assert document["already_adopted"] is True and document["published"] == []


def test_busy_member_exits_one(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    plan = tree(source)
    other = cli_owner(source)
    blocker = claim_root(source, other, plan[3][0]["id"])
    argv = ["job", "eject", "--workspace", source_name, str(plan[0][0]["id"]), str(tmp_path / "out"), "--tree"]
    assert command(argv, context(tmp_path)) == 1
    assert f"{blocker.job_key} blocks the tree" in capsys.readouterr().err
    blocker.give_back()
    other.close()
    assert_in(source, plan)
    assert not (tmp_path / "out").exists()


def test_wait_ejects_a_paused_job_and_pauses_a_running_one(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    paused, _, _ = single(source, "paused")
    argv = ["job", "eject", "--workspace", source_name, str(paused["id"]), str(tmp_path / "out"), "--wait"]
    assert command(argv, context(tmp_path)) == 0
    assert find(source, paused["id"]) is None
    # A job an owner holds gets a pause request; without anyone serving it the wait times out.
    running = single(source, "ready")[0]
    holder = cli_owner(source)
    held = claim_root(source, holder, running["id"])
    argv = [
        "job",
        "eject",
        "--workspace",
        source_name,
        str(running["id"]),
        str(tmp_path / "out2"),
        "--wait",
        "--timeout",
        "0.3",
    ]
    assert command(argv, context(tmp_path)) == 1
    assert "timed out" in capsys.readouterr().err
    (request,) = (source.control / "requests").glob(f"{running['id']}.*.json")
    assert json.loads(request.read_bytes())["action"] == "pause"
    held.give_back()
    holder.close()
    # Without --wait a held job is refused at once.
    holder = cli_owner(source)
    held = claim_root(source, holder, running["id"])
    assert command(argv[:-3], context(tmp_path)) == 1
    held.give_back()
    holder.close()


def test_occupied_destination_exits_one(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    mapping, state, priority = single(source, "failed")
    ref = find(source, mapping["id"])
    assert ref is not None
    (tmp_path / "out" / ref.job_key / "x").mkdir(parents=True)
    assert (
        command(["job", "eject", "--workspace", source_name, ref.job_id, str(tmp_path / "out")], context(tmp_path)) == 1
    )
    assert "occupied" in capsys.readouterr().err
    assert_in(source, [(mapping, state, priority)])
    assert _kernel.list_owners(source) == []


def test_sealed_workspace_refuses(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source, source_name = registered(tmp_path, "a")
    mapping, state, priority = single(source)
    seal_workspace(source)
    argv = ["job", "eject", "--workspace", source_name, str(mapping["id"]), str(tmp_path / "out")]
    assert command(argv, context(tmp_path)) != 0
    assert "sealed" in capsys.readouterr().err
    assert command(["job", "adopt", "--workspace", source_name, str(tmp_path / "anything")], context(tmp_path)) != 0
    assert "sealed" in capsys.readouterr().err
    assert_in(source, [(mapping, state, priority)])
    assert not (tmp_path / "out").exists()
