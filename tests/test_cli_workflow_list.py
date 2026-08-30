"""The ``httk workflow list`` command: the workflows a name can select."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow.scaffold import workflow_provider
from httk.workflow.workflow_cli import _describe, command


def _context(tmp_path: Path) -> CLIContext:
    return CLIContext("httk", tmp_path)


def test_list_reports_registered_workflows(tmp_path: Path, capsys) -> None:
    assert command(["list"], _context(tmp_path)) == 0
    lines = capsys.readouterr().out.splitlines()
    # The packaged VASP domain registers these as an import side effect.
    relax = next(line for line in lines if line.startswith("httk.vasp.relax\t"))
    assert relax == "httk.vasp.relax\tvasp-relax\tregistered\trelax one structure with the reviewed remedy ladder"


def test_list_json_carries_the_source_and_summary(tmp_path: Path, capsys) -> None:
    assert command(["list", "--json"], _context(tmp_path)) == 0
    rows = json.loads(capsys.readouterr().out)
    by_id = {row["workflow"]: row for row in rows}
    entry = by_id["httk.vasp.relax"]
    assert entry["alias"] == "vasp-relax"
    assert entry["source"] == {"kind": "registered", "plugin": None}
    assert entry["summary"] == "relax one structure with the reviewed remedy ladder"


def test_list_marks_a_plugin_workflow_with_its_owner(tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = workflow_provider("httk.vasp.relax")
    assert provider is not None
    monkeypatch.setattr(_describe, "registered_workflows", lambda: ("httk.vasp.relax",))
    monkeypatch.setattr(_describe, "workflow_provider", lambda _id: provider)
    monkeypatch.setattr(_describe, "installed_plugin_workflows", lambda: {"httk.vasp.relax": provider})
    monkeypatch.setattr(_describe, "installed_plugin_workflow_owners", lambda: {"httk.vasp.relax": "demo-plugin"})

    assert command(["list"], _context(tmp_path)) == 0
    assert capsys.readouterr().out.splitlines()[0].split("\t")[2] == "plugin demo-plugin"

    assert command(["list", "--json"], _context(tmp_path)) == 0
    assert json.loads(capsys.readouterr().out)[0]["source"] == {"kind": "plugin", "plugin": "demo-plugin"}


def test_list_reports_nothing_registered(tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_describe, "registered_workflows", lambda: ())
    assert command(["list"], _context(tmp_path)) == 0
    assert capsys.readouterr().out == "no workflows registered\n"
    assert command(["list", "--json"], _context(tmp_path)) == 0
    assert capsys.readouterr().out.strip() == "[]"
