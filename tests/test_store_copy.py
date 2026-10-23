"""CLI coverage of ``httk workflow install --from-store``, the receiving side of shipped workflows."""

import json
import shutil
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _kernel, _store
from httk.workflow.precheck import _workflow_problems
from httk.workflow.workflow_cli import command
from test_workflow_packages import _package


def test_install_from_store_installs_copies_and_reports_each_entry(tmp_path: Path, capsys) -> None:
    context = CLIContext("httk", tmp_path)
    source = Workspace.initialize(tmp_path / "source")
    target = Workspace.initialize(tmp_path / "target")
    name = register_ws(context, target.root, "target")
    with _kernel.register_owner(source, kind="cli", label=None, allocation=None, advertised={}) as owner:
        installed = _store.install(source, owner, _package(tmp_path / "package"))
    landing = tmp_path / "landing"
    good = landing / installed.directory.name
    shutil.copytree(installed.directory, good)
    bad = landing / "bad"
    shutil.copytree(installed.directory, bad)
    (bad / "package" / "run").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")

    argv = ["install", "--workspace", name, "--from-store", str(bad), str(good), "--source-label", "transfer from s"]
    assert command(argv, context) == 1
    rows = json.loads(capsys.readouterr().out)["workflows"]
    # One row per DIR in argument order; a refusal does not stop the others.
    assert rows[0]["id"] is None and rows[0]["status"] == "refused" and "tree digest mismatch" in rows[0]["reason"]
    assert rows[1] == {"directory": str(good), "id": installed.id, "status": "installed", "build": False}
    copy = _store.lookup(target, "tests.package")
    assert copy is not None and copy.record["source"] == "transfer from s" and good.is_dir() and bad.is_dir()
    assert command(["install", "--workspace", name, "--from-store", str(good)], context) == 0
    assert json.loads(capsys.readouterr().out)["workflows"][0]["status"] == "present"
    # The precheck of a job of the shipped workflow finds it installed and needing no build.
    assert _workflow_problems(target, installed.id) == (None, [], copy)


@pytest.mark.parametrize(
    "extra",
    [["SOURCE"], ["--no-build"], ["--no-calls"], ["--format", "cwl"]],
)
def test_install_from_store_excludes_the_other_sources(tmp_path: Path, capsys, extra: list[str]) -> None:
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, Workspace.initialize(tmp_path / "workspace").root, "w")
    assert command(["install", "--workspace", name, *extra, "--from-store", str(tmp_path)], context) == 2
    assert "--from-store cannot be combined" in capsys.readouterr().err
