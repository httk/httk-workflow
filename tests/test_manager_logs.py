"""``--log-file`` names one fixed file and is refused wherever several managers would share it."""

from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, workflow_cli
from httk.workflow.workflow_cli import _manager


def _arguments(tmp_path: Path, *extra: str):
    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    return parser.parse_args(["manager", "run", "--workspace", str(tmp_path / "ws"), "--log-file", "x.log", *extra])


@pytest.mark.parametrize("extra", [("--count", "2"), ("--detach",)])
def test_log_file_is_refused_for_shared_launches(tmp_path: Path, extra: tuple[str, ...]) -> None:
    Workspace.initialize(tmp_path / "ws")
    with pytest.raises(ValueError, match="logs/managers/<manager-id>.log"):
        _manager.launch_workspace_managers(tmp_path / "ws", _arguments(tmp_path, *extra), CLIContext("httk", tmp_path))


def test_log_file_is_refused_with_a_launcher_and_remotely(tmp_path: Path) -> None:
    Workspace.initialize(tmp_path / "ws")
    with pytest.raises(ValueError, match="always log"):
        _manager.launch_workspace_managers(
            tmp_path / "ws", _arguments(tmp_path, "--launcher", "cpu"), CLIContext("httk", tmp_path)
        )
    with pytest.raises(ValueError, match="always log"):
        _manager._remote_manager_argv(_arguments(tmp_path), "name")


def test_single_in_process_manager_honours_log_file(tmp_path: Path) -> None:
    workspace = Workspace.initialize(tmp_path / "ws")
    ws = register_ws(None, workspace.root)
    log = tmp_path / "fixed.log"
    code = workflow_cli.command(
        ["manager", "run", "--workspace", ws, "--log-file", str(log)], CLIContext("httk", tmp_path)
    )
    assert code == 0 and log.is_file()
    assert not (workspace.root / "logs" / "managers").exists()
