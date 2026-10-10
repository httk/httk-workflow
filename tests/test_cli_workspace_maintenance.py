"""The workspace maintenance verbs on the filesystem kernel: status, owners, attest-dead, gc and fsck."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace
from httk.workflow.workflow_cli import command
from v3_helpers import cli_owner, install, only, submit, workspace

_WORKFLOW = ("demo--0123456789abcdef", "demo")


def _context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)


def _named(tmp_path: Path) -> tuple[Workspace, CLIContext, str]:
    ws = workspace(tmp_path / "ws")
    context = _context(tmp_path)
    return ws, context, register_ws(context, ws.root)


# -- status and owners ----------------------------------------------------------------------------------------


def test_status_counts_jobs_by_state_and_lists_owners_with_their_liveness(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    submit(ws, _WORKFLOW, {"start": "succeed"})
    with cli_owner(ws) as owner:
        assert command(["workspace", "status", name], context) == 0
        out = capsys.readouterr().out
        assert "ready        1" in out and "sealed: no" in out and "owners: 1" in out
        assert f"{owner.owner_id}\tcli\talive\t" in out and "label=test" in out
        assert command(["workspace", "status", "--json", name], context) == 0
        (document,) = json.loads(capsys.readouterr().out)
    assert document["format"] == "httk-workflow-status" and document["format_version"] == 3
    assert document["counts"] == {"ready": 1} and "jobs" not in document
    (row,) = document["owners"]
    assert row["owner_id"] == owner.owner_id and row["kind"] == "cli" and row["liveness"] == "alive"
    assert row["tombstone_by"] is None and {"hostname", "pid", "started_at", "end_time", "drain_start"} <= set(row)


def test_owners_lists_a_serving_manager_and_managers_is_an_alias(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    with (
        TaskManager(ws, capabilities=["docker"], heartbeat_interval=0.01, end_time=4_000_000_000.0) as manager,
        cli_owner(ws) as other,
    ):
        assert command(["workspace", "managers", name], context) == 0
        human = capsys.readouterr().out
        assert f"{manager.manager_id}\tmanager\talive\t" in human and other.owner_id in human
        assert "end_time=2096-10-02T07:06:40+00:00" in human and "drain_start=" in human
        assert command(["workspace", "owners", "--kind", "manager", "--json", name], context) == 0
        (rows,) = json.loads(capsys.readouterr().out)
    assert [(row["owner_id"], row["kind"], row["liveness"]) for row in rows] == [
        (manager.manager_id, "manager", "alive")
    ]
    assert command(["workspace", "owners", name], context) == 0
    assert "no owner is registered" in capsys.readouterr().out


def test_workflows_lists_the_installed_workflows(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    assert command(["workspace", "workflows", name], context) == 0
    assert "no workflows are installed" in capsys.readouterr().out
    installed = install(ws, tmp_path / "package")
    assert command(["workspace", "workflows", name], context) == 0
    assert capsys.readouterr().out.startswith(f"{installed.id}\t{installed.name}\t")
    assert command(["workspace", "workflows", "--json", name], context) == 0
    ((row,),) = json.loads(capsys.readouterr().out)
    assert row["workflow"] == installed.id and row["error"] is None
    assert row["tree_sha256"] == installed.record["tree_sha256"]


# -- attest-dead and recovery ---------------------------------------------------------------------------------


_OWNER = """
import os, sys, time
from pathlib import Path
from httk.workflow import Workspace, _kernel
ws = Workspace(Path(sys.argv[1]))
owner = _kernel.register_owner(ws, kind="cli", label="other", allocation=None, advertised={})
for ref in _kernel.list_jobs(ws, "ready"):
    assert _kernel.claim(ws, owner, ref) is not None
print(owner.owner_id, flush=True)
if sys.argv[2] == "live":
    time.sleep(120)
os._exit(0)
"""


def _foreign_owner(ws: Workspace, mode: str) -> tuple[str, subprocess.Popen[str]]:
    """Register an owner in another process that claims every ready job, then exits (or, ``live``, sleeps)."""

    source = Path(__file__).resolve().parents[1] / "src"
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join([str(source), os.environ.get("PYTHONPATH", "")])}
    process = subprocess.Popen(
        [sys.executable, "-c", _OWNER, str(ws.root), mode], stdout=subprocess.PIPE, text=True, env=environment
    )
    assert process.stdout is not None
    owner_id = process.stdout.readline().strip()
    if mode != "live":
        process.wait(timeout=60)
    return owner_id, process


def test_attest_dead_needs_force_without_proof_and_gc_then_recovers_the_owned_job(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    owner_id, _process = _foreign_owner(ws, "exited")
    # The owner ran on another host: its death cannot be proven from here.
    record_path = ws.control / "owners" / owner_id / "owner.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record_path.write_text(json.dumps({**record, "hostname": "elsewhere.invalid"}), encoding="utf-8")

    argv = ["workspace", "attest-dead", owner_id, name, "--reason", "node rebooted", "--operator", "alice"]
    assert command(argv, context) == 2
    assert "cannot be proven" in capsys.readouterr().err
    assert not (ws.control / "owners" / owner_id / "dead.json").exists()
    assert command([*argv, "--force"], context) == 0
    captured = capsys.readouterr()
    assert f"{owner_id}\tattested dead" in captured.out and "workspace gc" in captured.err
    assert "without proof" in captured.err and "apply a request twice" in captured.err
    tombstone = json.loads((ws.control / "owners" / owner_id / "dead.json").read_text(encoding="utf-8"))
    assert tombstone["by"] == "operator" and tombstone["operator"] == "alice"
    assert tombstone["reason"] == "node rebooted"
    assert tombstone["evidence"] == [{"subject": f"owner {owner_id}", "rule": "operator", "detail": "node rebooted"}]
    assert command(["workspace", "owners", name], context) == 0
    assert "dead (tombstone by operator)" in capsys.readouterr().out

    assert command(["workspace", "gc", "--category", "dead_owners", name], context) == 0
    assert "dead_owners" in capsys.readouterr().out
    assert only(ws, "ready").job_id == ref.job_id
    assert command(["workspace", "status", name], context) == 0
    assert "dead (tombstone by operator)" in capsys.readouterr().out


def test_attest_dead_refuses_a_live_owner_and_an_unknown_one(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    owner_id, process = _foreign_owner(ws, "live")
    try:
        for argv in (["--reason", "x"], ["--reason", "x", "--force"]):
            assert command(["workspace", "attest-dead", owner_id, name, *argv], context) == 2
            assert f"owner {owner_id} is alive" in capsys.readouterr().err
            assert not (ws.control / "owners" / owner_id / "dead.json").exists()
    finally:
        process.kill()
        process.wait()
    with cli_owner(ws) as owner:
        assert command(["workspace", "attest-dead", owner.owner_id, name, "--reason", "x"], context) == 2
        assert "is alive" in capsys.readouterr().err
    assert command(["workspace", "attest-dead", "0" * 32, name, "--reason", "x"], context) == 2
    assert "no owner" in capsys.readouterr().err
    assert command(["workspace", "attest-dead", "0" * 32, name], context) == 2
    assert "--reason" in capsys.readouterr().err


# -- gc and fsck ----------------------------------------------------------------------------------------------


def test_gc_reports_the_selected_categories_and_rejects_an_unknown_one(tmp_path: Path, capsys) -> None:
    _ws, context, name = _named(tmp_path)
    assert (
        command(["workspace", "gc", "--json", "--category", "tmp_entries", "--category", "requests", name], context)
        == 0
    )
    (report,) = json.loads(capsys.readouterr().out)
    ran = [category["name"] for category in report["categories"] if not category["skipped"]]
    assert ran == ["requests", "tmp_entries"]
    assert command(["workspace", "gc", "--dry-run", name], context) == 0
    out = capsys.readouterr().out
    assert "owner_tombstones" in out and "dry run: nothing was removed" in out
    assert command(["workspace", "gc", "--category", "journal", name], context) == 2
    assert "invalid choice: 'journal'" in capsys.readouterr().err


def test_fsck_exits_one_while_findings_remain_and_repair_quarantines_unparsable_names(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    junk = ws.jobs / "ready" / "not-a-job"
    junk.write_text("junk", encoding="utf-8")
    assert command(["workspace", "fsck", name], context) == 1
    out = capsys.readouterr().out
    assert f"reported\tunparsable_name\t-\t{junk}\t" in out and "checked 0 jobs, 1 findings" in out
    assert command(["workspace", "fsck", "--repair", "--yes", name], context) == 0
    assert "quarantined\tunparsable_name" in capsys.readouterr().out
    assert not junk.exists()
    assert command(["workspace", "fsck", name], context) == 0
    capsys.readouterr()


def test_fsck_repair_asks_first_and_refuses_without_a_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    ws, context, name = _named(tmp_path)
    junk = ws.jobs / "ready" / "not-a-job"
    junk.write_text("junk", encoding="utf-8")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    assert command(["workspace", "fsck", "--repair", name], context) == 1
    assert "without a terminal requires --yes" in capsys.readouterr().err and junk.exists()
    prompts: list[str] = []

    def decline(prompt: str) -> str:
        prompts.append(prompt)
        return "n"

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", decline)
    assert command(["workspace", "fsck", "--repair", name], context) == 1
    assert "not repaired" in capsys.readouterr().out and junk.exists()
    assert prompts == ["Make sure no other operations are ongoing in this workspace. Continue? [y/N] "]
    monkeypatch.setattr("builtins.input", lambda _prompt: "y")
    assert command(["workspace", "fsck", "--repair", name], context) == 0
    assert "quarantined\tunparsable_name" in capsys.readouterr().out and not junk.exists()


def test_fsck_repair_is_refused_until_gc_recovers_the_owners(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    repair = ["workspace", "fsck", "--repair", "--yes", name]
    owner_id, process = _foreign_owner(ws, "live")
    try:
        assert command(repair, context) == 1
        assert f"not proven dead: {owner_id}; stop its managers, run `httk workspace gc`" in capsys.readouterr().err
    finally:
        process.kill()
        process.wait()
    # Dead now, but its claimed job is still in owned/ until a recovery returns it.
    assert command(repair, context) == 1
    assert f"dead owners not recovered: {owner_id}" in capsys.readouterr().err
    assert command(["workspace", "gc", "--category", "dead_owners", name], context) == 0
    assert only(ws, "ready").job_id == ref.job_id
    assert command(repair, context) == 0
    assert "checked 1 jobs, 0 findings" in capsys.readouterr().out


def test_unlock_is_gone(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "unlock"], _context(tmp_path)) == 2
    assert "invalid choice: 'unlock'" in capsys.readouterr().err
