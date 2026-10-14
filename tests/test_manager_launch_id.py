"""A manager records the launch id its launcher exported (``HTTK_LAUNCH_ID``) in its ``owner.json``."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _kernel, workflow_cli

_LAUNCH_ID = "0123456789abcdef0123456789abcdef"


@pytest.mark.parametrize(
    ("exported", "recorded"),
    [(_LAUNCH_ID, _LAUNCH_ID), (None, None), ("../not-a-launch-id", None), (_LAUNCH_ID.upper(), None)],
)
def test_a_cli_manager_records_its_launch_id_in_owner_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exported: str | None, recorded: str | None
) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    ws = register_ws(None, workspace.root)
    if exported is None:
        monkeypatch.delenv("HTTK_LAUNCH_ID", raising=False)
    else:
        monkeypatch.setenv("HTTK_LAUNCH_ID", exported)
    records: list[dict[str, object]] = []
    register = _kernel.register_owner

    def spy(*arguments: object, **keywords: object) -> _kernel.Owner:
        owner = register(*arguments, **keywords)  # type: ignore[arg-type]
        records.append(json.loads((workspace.control / "owners" / owner.owner_id / "owner.json").read_bytes()))
        return owner

    monkeypatch.setattr(_kernel, "register_owner", spy)
    assert workflow_cli.command(["manager", "run", "--workspace", ws], CLIContext("httk", tmp_path)) == 0
    [record] = [record for record in records if record["kind"] == "manager"]
    assert "launch_id" in record and record["launch_id"] == recorded
