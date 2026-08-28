import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow.adapters import ADAPTER_OPERATIONS, add_remote, run_adapter
from httk.workflow.projects import initialize_project
from httk.workflow.workflow_cli import command


def _request(operation: str, root: Path) -> dict[str, object]:
    if operation in {"push", "pull"}:
        return {"source": str(root / "source"), "destination": str(root / "destination")}
    if operation in {"invoke", "status"}:
        return {"argv": ["true"]}
    return {}


def _unrecognized(project: Path, name: str = "elsewhere") -> Path:
    """Return a bundle whose kind no maintained implementation claims."""

    bundle = add_remote(name, template="local", project=project)
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["kind"] = "torque"
    (bundle / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    return bundle


def test_every_operation_of_an_unrecognized_kind_refuses_to_run_locally(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="unrecognized-refusal")
    bundle = _unrecognized(project)
    (tmp_path / "source").mkdir()
    for operation in ADAPTER_OPERATIONS:
        with pytest.raises(RuntimeError, match="'torque' is not implemented"):
            run_adapter(bundle, operation, _request(operation, tmp_path))
    assert not (tmp_path / "destination").exists()


def test_the_refusal_names_the_kinds_that_are_implemented(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="named-kinds")
    bundle = _unrecognized(project, "batch")
    with pytest.raises(RuntimeError, match="local, ssh"):
        run_adapter(bundle, "status", {"argv": ["true"]})


def test_refusal_reaches_the_cli_without_a_traceback(tmp_path: Path, capsys) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="refusal-cli")
    bundle = _unrecognized(project, "cluster")
    metadata = json.loads((bundle / "remote.json").read_text(encoding="utf-8"))
    metadata["settings"]["workspace_root"] = "/remote/runs"
    (bundle / "remote.json").write_text(json.dumps(metadata), encoding="utf-8")
    # `workspace status` on a remote-bound workspace reaches the far side over the
    # adapter; an unrecognized kind refuses there, and that refusal must arrive at
    # the CLI as a clean error rather than a traceback.
    context = CLIContext("httk", project)
    register_ws(context, "/remote/runs", "station", remote="cluster")
    assert command(["workspace", "status", "cluster:station"], context) == 1
    captured = capsys.readouterr()
    assert "is not implemented" in captured.err
    assert "Traceback" not in captured.err
    assert captured.out == ""


def test_local_adapter_keeps_working(tmp_path: Path) -> None:
    project = tmp_path / "project"
    initialize_project(project, name="local-still-works")
    bundle = add_remote("here", template="local", project=project)
    source = tmp_path / "source"
    (source / "files").mkdir(parents=True)
    (source / "files" / "content").write_text("payload", encoding="utf-8")
    destination = tmp_path / "destination"
    pushed = run_adapter(bundle, "push", {"source": str(source), "destination": str(destination)})
    assert pushed["ok"] is True and pushed["path"] == str(destination)
    assert (destination / "files" / "content").read_text(encoding="utf-8") == "payload"
    invoked = run_adapter(bundle, "invoke", {"argv": ["true"]})
    assert invoked["returncode"] == 0
