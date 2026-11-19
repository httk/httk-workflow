"""Coverage of the receiving side of shipped workflows: the transfer session's ``install`` op
(:func:`~httk.workflow.workflow_cli._describe.install_copies`)."""

import io
import shutil
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any, cast

import pytest
from httk.core.cli import CLIContext

from conftest import register_ws
from httk.workflow import Workspace, _kernel, _store
from httk.workflow.precheck import _workflow_problems
from httk.workflow.workflow_cli import command
from httk.workflow.workflow_cli._describe import install_copies
from test_workflow_packages import _package


def _entries(*directories: Path) -> io.BytesIO:
    """The store entries ``<dir>/{install.json,package}`` as the tar stream a push uploads."""

    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for directory in directories:
            for name in ("install.json", "package"):
                archive.add(directory / name, arcname=f"{directory.name}/{name}")
    return io.BytesIO(data.getvalue())


def test_the_install_op_installs_copies_and_reports_each_entry(tmp_path: Path) -> None:
    source = Workspace.initialize(tmp_path / "source")
    target = Workspace.initialize(tmp_path / "target")
    with _kernel.register_owner(source, kind="cli", label=None, allocation=None, advertised={}) as owner:
        installed = _store.install(source, owner, _package(tmp_path / "package"))
    landing = tmp_path / "landing"
    good = landing / installed.directory.name
    shutil.copytree(installed.directory, good)
    bad = landing / "bad"
    shutil.copytree(installed.directory, bad)
    (bad / "package" / "run").write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")

    document, returncode = install_copies(target, stream=_entries(bad, good), source_label="transfer from s")
    assert returncode == 1
    rows = cast(list[dict[str, Any]], document["workflows"])
    # One row per entry in the stream's order; a refusal does not stop the others.
    assert [PurePosixPath(str(row["directory"])).name for row in rows] == ["bad", good.name]
    assert rows[0]["id"] is None and rows[0]["status"] == "refused" and "tree digest mismatch" in rows[0]["reason"]
    assert {key: rows[1][key] for key in ("id", "status", "build")} == {
        "id": installed.id,
        "status": "installed",
        "build": False,
    }
    copy = _store.lookup(target, "tests.package")
    assert copy is not None and copy.record["source"] == "transfer from s"
    document, returncode = install_copies(target, stream=_entries(good))
    assert returncode == 0 and document["workflows"][0]["status"] == "present"  # type: ignore[index]
    # The precheck of a job of the shipped workflow finds it installed and needing no build.
    assert _workflow_problems(target, installed.id) == (None, [], copy)
    assert not [path for path in (target.control / "tmp").iterdir() if path.name.startswith("push.")]


@pytest.mark.parametrize("removed", [["--from-store", "x"], ["--stdin-tar"]])
def test_the_command_line_receiver_is_gone(tmp_path: Path, capsys, removed: list[str]) -> None:
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, Workspace.initialize(tmp_path / "workspace").root, "w")
    assert command(["install", "--workspace", name, *removed, "SOURCE"], context) == 2
    assert "unrecognized arguments" in capsys.readouterr().err
