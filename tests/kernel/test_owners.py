"""Owner registration records the process or explicitly supplied execution scope."""

import json
from dataclasses import dataclass
from pathlib import Path

from httk.workflow._death import ProcessIdentity
from httk.workflow._kernel import register_owner


@dataclass(frozen=True)
class Workspace:
    root: Path
    control: Path
    jobs: Path
    durable: bool = False
    visibility_deadline: float = 0.1


def _workspace(path: Path) -> Workspace:
    root = path / "workspace"
    (root / ".httk-workspace").mkdir(parents=True)
    (root / "jobs").mkdir()
    return Workspace(root, root / ".httk-workspace", root / "jobs")


def test_register_owner_records_supplied_execution_scope_verbatim(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    identity = ProcessIdentity("node-4", "11111111-2222-3333-4444-555555555555", 734, 987654)

    owner = register_owner(
        workspace,
        kind="daemon",
        label=None,
        allocation=None,
        advertised={},
        execution_scope=identity,
    )

    record = json.loads((workspace.control / "owners" / owner.owner_id / "owner.json").read_text())
    assert (record["hostname"], record["boot_id"], record["pid"], record["process_start_ticks"]) == (
        identity.hostname,
        identity.boot_id,
        identity.pid,
        identity.start_ticks,
    )


def test_register_owner_defaults_to_current_process_identity(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    owner = register_owner(workspace, kind="cli", label=None, allocation=None, advertised={})
    record = json.loads((workspace.control / "owners" / owner.owner_id / "owner.json").read_text())
    assert isinstance(record["hostname"], str)
    assert isinstance(record["pid"], int)
    assert record["process_start_ticks"] is None or isinstance(record["process_start_ticks"], int)
