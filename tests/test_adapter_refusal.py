import json
from pathlib import Path

import pytest  # pyright: ignore[reportMissingImports]
from httk.core import CLIContext

from httk.workflow.adapters import ADAPTER_OPERATIONS, add_computer, run_adapter
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command


def _request(operation: str, root: Path) -> dict[str, object]:
    if operation in {"push", "pull"}:
        return {"source": str(root / "source"), "destination": str(root / "destination")}
    if operation in {"invoke", "start-manager", "status"}:
        return {"argv": ["true"]}
    return {}


def test_every_remote_adapter_operation_refuses_to_run_locally(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-refusal")
    bundle = add_computer("remote", template="ssh-slurm", project=project)
    (tmp_path / "source").mkdir()
    for operation in ADAPTER_OPERATIONS:
        with pytest.raises(RuntimeError, match="is not implemented yet"):
            run_adapter(bundle, operation, _request(operation, tmp_path))
    assert not (tmp_path / "destination").exists()


def test_local_slurm_adapter_also_refuses(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-slurm-refusal")
    bundle = add_computer("batch", template="local-slurm", project=project)
    with pytest.raises(RuntimeError, match="'local-slurm' is not implemented yet"):
        run_adapter(bundle, "status", {"argv": ["true"]})


def test_remote_refusal_reaches_the_cli_without_a_traceback(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="remote-cli")
    bundle = add_computer("remote", template="ssh-slurm", project=project)
    metadata = json.loads((bundle / "computer.json").read_text(encoding="utf-8"))
    metadata["queues"]["default"]["workspace"] = "/remote/runs"
    (bundle / "computer.json").write_text(json.dumps(metadata), encoding="utf-8")
    assert command(["tasks", "status", "remote"], CLIContext("httk", project)) == 2
    captured = capsys.readouterr()
    assert "is not implemented yet" in captured.err
    assert "Traceback" not in captured.err
    assert captured.out == ""


def test_local_adapter_keeps_working(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-still-works")
    bundle = add_computer("here", template="local", project=project)
    source = tmp_path / "source"
    (source / "files").mkdir(parents=True)
    (source / "files" / "content").write_text("payload", encoding="utf-8")
    destination = tmp_path / "destination"
    pushed = run_adapter(bundle, "push", {"source": str(source), "destination": str(destination)})
    assert pushed["ok"] is True and pushed["path"] == str(destination)
    assert (destination / "files" / "content").read_text(encoding="utf-8") == "payload"
    invoked = run_adapter(bundle, "invoke", {"argv": ["true"]})
    assert invoked["returncode"] == 0
