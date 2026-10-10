"""The workspace maintenance verbs on the filesystem kernel: status, owners, attest-dead, gc and fsck."""

import dataclasses
import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import TaskManager, Workspace, _death, _kernel
from httk.workflow.workflow_cli import command
from v3_helpers import cli_owner, install, only, submit, workspace

_WORKFLOW = ("demo--0123456789abcdef", "demo")


def _context(cwd: Path) -> CLIContext:
    return CLIContext("httk", cwd)


def _named(tmp_path: Path) -> tuple[Workspace, CLIContext, str]:
    ws = workspace(tmp_path / "ws")
    context = _context(tmp_path)
    return ws, context, register_ws(context, ws.root)


def _not_this_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every later identity check see another process, as if the owners registered so far had crashed."""

    real = _death.process_identity()
    monkeypatch.setattr(_death, "process_identity", lambda pid=None: dataclasses.replace(real, pid=real.pid + 1))


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


def test_attest_dead_writes_an_operator_tombstone_and_gc_recovers_the_owned_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    ws, context, name = _named(tmp_path)
    ref = submit(ws, _WORKFLOW, {"start": "succeed"})
    owner = cli_owner(ws)
    assert _kernel.claim(ws, owner, ref) is not None
    _not_this_process(monkeypatch)

    argv = ["workspace", "attest-dead", owner.owner_id, name, "--reason", "node rebooted", "--operator", "alice"]
    assert command(argv, context) == 0
    captured = capsys.readouterr()
    assert f"{owner.owner_id}\tattested dead" in captured.out and "workspace gc" in captured.err
    tombstone = json.loads((owner.path / "dead.json").read_text(encoding="utf-8"))
    assert tombstone["by"] == "operator" and tombstone["operator"] == "alice"
    assert tombstone["reason"] == "node rebooted"
    assert tombstone["evidence"] == [
        {"subject": f"owner {owner.owner_id}", "rule": "operator", "detail": "node rebooted"}
    ]
    assert command(["workspace", "owners", name], context) == 0
    assert "dead (tombstone by operator)" in capsys.readouterr().out

    assert command(["workspace", "gc", "--category", "dead_owners", name], context) == 0
    assert "dead_owners" in capsys.readouterr().out
    assert only(ws, "ready").job_id == ref.job_id
    assert command(["workspace", "status", name], context) == 0
    assert "dead (tombstone by operator)" in capsys.readouterr().out


def test_attest_dead_refuses_this_process_and_an_unknown_owner(tmp_path: Path, capsys) -> None:
    ws, context, name = _named(tmp_path)
    with cli_owner(ws) as owner:
        assert command(["workspace", "attest-dead", owner.owner_id, name, "--reason", "x"], context) == 2
        assert "this very process" in capsys.readouterr().err
        assert not (owner.path / "dead.json").exists()
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
    assert command(["workspace", "fsck", "--repair", name], context) == 0
    assert "quarantined\tunparsable_name" in capsys.readouterr().out
    assert not junk.exists()
    assert command(["workspace", "fsck", name], context) == 0
    capsys.readouterr()


def test_unlock_is_gone(tmp_path: Path, capsys) -> None:
    assert command(["workspace", "unlock"], _context(tmp_path)) == 2
    assert "invalid choice: 'unlock'" in capsys.readouterr().err
