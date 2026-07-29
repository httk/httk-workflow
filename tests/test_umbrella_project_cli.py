"""The umbrella ``httk project`` command, extended by *httk-workflow*.

The project anchor lives in *httk-core*, and *httk-workflow* registers its
manifest and doctor subcommands, and a ``show`` section, into the umbrella
``httk project`` command. These tests drive that command through the *core*
entry point — ``httk.core.cli.main`` — to prove the extensions are discovered
and that ``httk project ...`` and ``httk workflow project ...`` reach the same
implementations.
"""

import json
from pathlib import Path

from httk.core.cli import main
from httk.core.project.cli import known_project_show_sections, known_project_subcommands
from httk.core.register import known_cli_commands

from httk.workflow.projects import initialize_project


def _isolate(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)


def _project(tmp_path: Path, monkeypatch, name: str = "campaign") -> Path:
    _isolate(tmp_path, monkeypatch)
    project = tmp_path / name
    initialize_project(project, name=name)
    (project / "content.txt").write_text("original\n", encoding="utf-8")
    return project


def test_workflow_registers_its_project_extensions() -> None:
    # Importing httk.core discovered the workflow handlers, which registered
    # these into the umbrella command's registry.
    assert {"doctor", "manifest"} <= set(known_project_subcommands())
    assert "workflow" in known_project_show_sections()


def test_workflow_cli_registration_is_discovered() -> None:
    assert "workflow" in known_cli_commands()


def test_umbrella_manifest_and_doctor_work_end_to_end(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch)

    assert main(["project", "manifest", "create", str(project)]) == 0
    capsys.readouterr()

    # The umbrella spelling verifies the manifest it just wrote.
    assert main(["project", "manifest", "verify", str(project)]) == 0
    printed = capsys.readouterr().out
    assert printed.splitlines()[0] == "valid"
    assert "valid_trusted" in printed

    # Doctor runs, reports, and repairs through the umbrella spelling. A freshly
    # created and signed project is healthy, so the run exits zero.
    assert main(["project", "doctor", str(project)]) == 0
    assert "problem(s)" in capsys.readouterr().out
    assert main(["project", "doctor", str(project), "--repair", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["format"] == "httk-project-doctor" and report["repair"] is True

    # A tampered tree is invalid, exit 1, through the umbrella spelling.
    (project / "content.txt").write_text("tampered\n", encoding="utf-8")
    assert main(["project", "manifest", "verify", str(project)]) == 1
    assert capsys.readouterr().out.splitlines()[0] == "invalid"


def test_umbrella_show_includes_the_workflow_section(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="described")
    assert main(["project", "manifest", "create", str(project)]) == 0
    capsys.readouterr()

    assert main(["project", "show", str(project), "--json"]) == 0
    description = json.loads(capsys.readouterr().out)
    # Anchor half, from httk-core.
    assert description["format"] == "httk-project-description"
    assert description["project"]["name"] == "described"
    assert description["keys"]["pinned"] is True
    # Workflow section, contributed through the registry.
    assert "detached-transfer-v1" in description["workspace"]["extensions"]
    assert description["workspace"]["present"] is True
    assert description["manifest"]["verdict"] == "valid_trusted"
    assert description["remotes"] == []

    assert main(["project", "show", str(project)]) == 0
    rendered = capsys.readouterr().out
    # A workflow row and an anchor row in the same rendered listing.
    assert "workspace" in rendered and "key_pinned" in rendered

    # --no-verify keeps the section from walking the tree to classify the manifest.
    assert main(["project", "show", str(project), "--no-verify", "--json"]) == 0
    cheap = json.loads(capsys.readouterr().out)
    assert cheap["manifest"]["present"] is True and cheap["manifest"]["verdict"] is None


def test_both_spellings_reach_the_same_implementation(tmp_path: Path, monkeypatch, capsys) -> None:
    project = _project(tmp_path, monkeypatch, name="both")
    assert main(["project", "manifest", "create", str(project)]) == 0
    capsys.readouterr()

    assert main(["project", "manifest", "verify", str(project)]) == 0
    umbrella = capsys.readouterr().out
    assert main(["workflow", "project", "manifest", "verify", str(project)]) == 0
    workflow = capsys.readouterr().out
    assert umbrella == workflow
