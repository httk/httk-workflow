"""`httk workflow transfer status`: the read-only operator report of transfer work (plan 4.7)."""

import json
import time
import uuid
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import configure_identity
from httk.workflow import Workspace, _adoption, _sealing, transfers
from httk.workflow._receipts import FRESHNESS_WINDOW_NS
from httk.workflow.workflow_cli import command
from test_eject_adopt import _payload

_real_time_ns = time.time_ns


@pytest.fixture
def pair(tmp_path: Path) -> tuple[Workspace, Workspace]:
    configure_identity()
    return Workspace.initialize(tmp_path / "source"), Workspace.initialize(tmp_path / "destination")


def _status(workspace: Workspace, tmp_path: Path, *extra: str) -> int:
    """Run the verb from outside the workspace, naming it by directory."""

    return command(["transfer", "status", "--workspace", str(workspace.root), *extra], CLIContext("httk", tmp_path))


def test_a_quiet_workspace_reports_nothing_and_exits_zero(
    pair: tuple[Workspace, Workspace], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, _destination = pair
    assert _status(source, tmp_path, "--json") == 0
    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "ok" and document["check"] == "transfers"
    assert document["workspace"] == str(source.root)
    assert document["details"] == {"held_exports": [], "outgoing_in_doubt": [], "stale_claims": []}
    # The enclosing workspace is the default, with the same text report.
    assert command(["transfer", "status"], CLIContext("httk", source.root)) == 0
    assert capsys.readouterr().out.startswith("ok: ")


def test_work_waiting_for_an_operator_is_listed_and_exits_one(
    pair: tuple[Workspace, Workspace],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source, destination = pair
    exported = source.submit(_payload(tmp_path / "payloads", "exported"), "jobs")
    held = _sealing.seal(source, exported, _sealing.Commit("export", path=tmp_path / "far" / "job"), reason="ejected")
    sent = source.submit(_payload(tmp_path / "payloads", "sent"), "jobs")
    bundle = transfers.detach_job(source, sent.job_id, destination_workspace_id=destination.workspace_id)
    claims = _adoption.claims_directory(source)
    claims.mkdir(parents=True, exist_ok=True)
    orphan = str(uuid.uuid4())
    (claims / orphan).write_text("")
    # Within its window an outgoing transfer is not in doubt yet.
    assert _status(source, tmp_path, "--json") == 1
    document = json.loads(capsys.readouterr().out)
    assert document["details"]["outgoing_in_doubt"] == []
    monkeypatch.setattr(time, "time_ns", lambda: _real_time_ns() + FRESHNESS_WINDOW_NS * 2)
    assert _status(source, tmp_path, "--json") == 1
    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "warning"
    assert document["details"]["held_exports"] == [held.parent.name]
    assert [item["transfer_id"] for item in document["details"]["outgoing_in_doubt"]] == [bundle.name]
    assert document["details"]["stale_claims"] == [orphan]
    assert _status(source, tmp_path) == 1
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("warning: ")
    assert f"held export\t{held.parent.name}\t(`httk job eject --resume`)" in lines
    assert any(line.startswith(f"in doubt\t{sent.job_key}\t{bundle.name}") for line in lines)
    assert f"stale claim\t{orphan}" in lines
    # Read only: nothing was retired, reclaimed, copied out or removed.
    assert bundle.is_dir() and held.is_dir() and (claims / orphan).is_file()
